import sys
import re
import csv
import cStringIO
import pandas
import collections
import os
import yaml

# ------------------------------------------------------------------------------

EOL_RE = re.compile(r'\r?\n')
BLANKLINE_RE = re.compile(r'^((?:"")?,)*(?:"")?%s$' % EOL_RE.pattern)
RESULTSLINE_RE = re.compile(r'^(?:"Results"|Results),*%s$' % EOL_RE.pattern)
CRCLINE_RE = re.compile(r'^-- CRC --,*%s$' % EOL_RE.pattern)
SEPARATOR_RE = re.compile(r'%s(?:%s)+' % (EOL_RE.pattern, EOL_RE.pattern))
EMPTY_RE = re.compile(r'""')
NONWORDCHARS_RE = re.compile(r'\W+')
TRAILINGCOMMA_RE = re.compile(r',+((?:%s)?)$' % EOL_RE.pattern)

# ------------------------------------------------------------------------------

class NoClobberDict(dict):
    def __init__(self, *args, **kwargs):
        nargs = len(args)
        if nargs > 1:
            raise TypeError('%s expected at most 1 argument, got %d' %
                            (type(self).__name__, nargs))
        elif nargs == 1:
            items = list(args[0]) + kwargs.items()
        else:
            items = kwargs.items()

        for key, value in items:
            self.__setitem__(key, value)

    def __setitem__(self, key, value):
        if self.has_key(key):
            raise ValueError('already have key %r' % key)
        super(type(self), self).__setitem__(key, value)

    def asdict(self):
        return dict(self)

def safe_dict(*args, **kwargs):
    return NoClobberDict(*args, **kwargs).asdict()

# ------------------------------------------------------------------------------

def error(message):
    print >> sys.stderr, message
    sys.exit(1)


def expect_blank_line(stream):
    line = next(stream)
    if not isblank(line):
        error('expected a blank line but found %r' % line)
    return line


def isblank(line):
    return bool(BLANKLINE_RE.match(line))


def isresultsline(line):
    return bool(RESULTSLINE_RE.match(line))


def iscrcline(line):
    return bool(CRCLINE_RE.match(line))


def cleanup(rawtext):
    text0 = rawtext
    text1 = EMPTY_RE.sub('', text0)
    text2 = TRAILINGCOMMA_RE.sub(lambda m: m.group(1), text1)
    return text2


def split_chunks(rawtext, _nonempty=lambda s: len(s) > 0):
    text = cleanup(rawtext)
    return filter(_nonempty, SEPARATOR_RE.split(text))

# -----------------------------------------------------------------------------

def parse_csv_line(line):
    stream = cStringIO.StringIO(cleanup(line))
    return next(csv.reader(stream))

# -----------------------------------------------------------------------------

def parse_metadata(text):
    stream = cStringIO.StringIO(text)
    data = list(csv.reader(stream))

    seen = set()
    def to_key_value_pair(sequence):
        nitems = len(sequence)
        assert nitems > 1

        key = sequence[0]
        assert key not in seen
        seen.add(key)

        if nitems == 2:
            value = sequence[1]
        else:
            value = sequence[1:]
        return [key, value]

    return safe_dict([to_key_value_pair(sequence) for sequence in data])


def parse_calibration(text,
                      _record=collections.namedtuple(typename='calibration',
                                                     field_names=['info', 'data'])):

    stream = cStringIO.StringIO(text)

    first_row = parse_csv_line(next(stream))
    if len(first_row) == 1:
        info = first_row[0].strip(':')
    else:
        info = first_row

    row_iterator = csv.reader(stream)
    if info == 'Most Recent Calibration and Verification Results':
        data = safe_dict(row_iterator)
    elif info == 'CALInfo':
        calinfo = NoClobberDict()
        accumulator = None
        try:
            while True:
                row = next(row_iterator)
                if row[0] == 'Lot':
                    keys = row
                    values = next(row_iterator)
                    datum = safe_dict(zip(keys, values))
                    accumulator.append(datum)
                else:
                    assert len(row) == 1
                    calinfo[row[0]] = accumulator = []
        except StopIteration:
            pass

        data = calinfo.asdict()
    else:
        data = list(row_iterator)

    return _record(info, data)


def parse_result(text,
                 _record=collections.namedtuple(typename='result',
                                                field_names=['info', 'data'])):
    stream = cStringIO.StringIO(text)

    info = parse_csv_line(next(stream))
    data = pandas.read_csv(stream)

    return _record(info, data)

# -----------------------------------------------------------------------------

def get_metadata(stream):
    lines = [next(stream) for _ in range(3)]
    expect_blank_line(stream)
    while True:
        line = next(stream)
        if isblank(line):
            break
        lines.append(line)
    text = ''.join(lines)
    return text


def get_calibration(stream):
    lines = []
    while True:
        line = cleanup(next(stream))
        if isresultsline(line):
            expect_blank_line(stream)
            break
        lines.append(line)
    text = ''.join(lines)
    return [parse_calibration(chunk) for chunk in split_chunks(text)]


def get_results(stream):
    lines = []
    while True:
        line = next(stream)
        if iscrcline(line):
            break
        lines.append(line)
    text = ''.join(lines)
    return [parse_result(chunk) for chunk in split_chunks(text)]

# -----------------------------------------------------------------------------

def parse(stream,
          _record=collections.namedtuple(typename='parsedcontents',
                                         field_names=['metadata',
                                                      'calibration',
                                                      'results'])):
    metadata = get_metadata(stream)
    calibration = get_calibration(stream)
    results = get_results(stream)
    return _record(metadata, calibration, results)

# -----------------------------------------------------------------------------

def make_basename(string_):
    return NONWORDCHARS_RE.sub('_', string_).lower()


def dump_results(results, dirpath):
    os.makedirs(dirpath)

    seen = set()
    for result in results:
        info = result.info
        assert len(info) == 2 and info[0] == 'DataType:'

        basename = '%s.tsv' % make_basename(info[1])
        assert basename not in seen
        seen.add(basename)

        resultspath = os.path.join(dirpath, basename)
        with open(resultspath, 'w') as stream:
            dataframe = result.data
            dataframe.to_csv(stream, sep='\t', index=False)


def dump(sections, dirpath):
    os.makedirs(dirpath)
    metadatapath = os.path.join(dirpath, 'metadata.yaml')
    with open(metadatapath, 'w') as stream:
        yaml.dump(sections.metadata, stream=stream)

    calibrationpath = os.path.join(dirpath, 'calibration.yaml')
    with open(calibrationpath, 'w') as stream:
        items = [list(item) for item in sections.calibration]
        yaml.dump(items, stream=stream)

    resultsdir = os.path.join(dirpath, 'results')
    dump_results(sections.results, resultsdir)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    inputpath = sys.argv[1]
    outputdir = sys.argv[2]
    sections = parse(open(inputpath))
    dump(sections, outputdir)
