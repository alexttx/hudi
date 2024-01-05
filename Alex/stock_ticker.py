#!/bin/env python3

import re
import os
import sys
import errno
import time
import argparse
import subprocess
import datetime
import fnmatch
import itertools
import collections
import pprint

# okay if compression libraries are missing
try:
    import gzip
    import bz2
    import lzma
    import lz4.frame
except:
    pass

# open a possibly compressed file
def myopen(fname):
    kw = { 'mode': 'rt',
           'encoding': 'ascii',
           'errors': 'backslashreplace' }
    if fname == "-":
        fh = sys.stdin
    elif fnmatch.fnmatch(fname,"*.gz"):
        fh = gzip.open(fname, **kw)
    elif fnmatch.fnmatch(fname,"*.bz2"):
        fh = bz2.open(fname, **kw)
    elif fnmatch.fnmatch(fname,"*.lzma") or fnmatch.fnmatch(fname,"*.xz"):
        fh = lzma.open(fname, **kw)
    elif fnmatch.fnmatch(fname,"*.lz4"):
        fh = lz4.frame.open(fname, **kw)
    else:
        fh = open(fname, **kw)
        pass
    return fh

def exception_msg():
    return str(sys.exc_info()[1])

def emsg(*lines, **kwargs):
    print(*lines,
          sep=kwargs.get('sep','\n'),
          file=kwargs.get('file',sys.stderr))

def fatal(*lines, **kwargs):
    emsg(*lines, **kwargs)
    sys.exit(1)

def syntax(*lines):
    emsg(*lines)
    emsg("USe -h for help.")
    sys.exit(1)
    pass

def help(argparser):
    argparser.print_help()
    pass

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_FILE = os.path.join(SCRIPT_DIR,"stock_ticker_template.json")
YYYY = 2020
MM = 1
DD = 1
HH = 9

def make_argparser(cmd):
    ap = argparse.ArgumentParser(
        prog=cmd,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
        description="Generate stock ticker data",
        epilog="")

    ap.add_argument("-h", dest="help", help="show help", action="store_true")

    ap.add_argument("-y", dest="year",  type=int, default=YYYY)
    ap.add_argument("-m", dest="month", type=int, default=MM)
    ap.add_argument("-d", dest="day",   type=int, default=DD)
    ap.add_argument("-t", dest="hour",  type=int, default=HH)
    ap.add_argument("-l", dest="limit", type=int, default=-1, help="stop after LIMIT entries")

    return ap

def main(args):
    global op
    cmd = os.path.basename(__file__)
    ap = make_argparser(cmd)
    op = ap.parse_args(args[1:])

    if op.help:
        help(ap)
        return 0

    yyyy = f"{op.year:04}"
    mm = f"{op.month:02}"
    dd = f"{op.day:02}"
    hh = f"{op.hour:02}"

    count = 0
    with myopen(TEMPLATE_FILE) as fh:
        for line in fh:
            if count == op.limit:
                break
            print(line.replace("=YYYY",yyyy).replace("=MM",mm).replace("=DD",dd).replace("=HH",hh))
            count += 1
            pass
        pass
    return 0

if (__name__ == "__main__"):
    try:
        rc = main(sys.argv)
    except KeyboardInterrupt:
        rc = 1
        pass
    except BrokenPipeError:
        rc = 1
        pass
    sys.exit(rc)

