#!/bin/env python3

import re
import os
import sys
import io
#import errno
import time
import argparse
#import subprocess
#import datetime
import fnmatch
#import itertools
#import collections
import pprint
import numpy as np
import pandas as pd

# okay if compression libraries are missing
try:
    import gzip
    import bz2
    import lzma
    import lz4.frame
except:
    pass

DEFAULT_INTERVAL = 15
DEFAULT_DIGITS = 2


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

def warn(*lines, **kwargs):
    print(*lines,
          sep=kwargs.get('sep','\n'),
          file=kwargs.get('file',sys.stderr))

def fatal(*lines, **kwargs):
    warn(*lines, **kwargs)
    sys.exit(1)

def syntax(*lines):
    warn(*lines)
    warn("Use -h for help.")
    sys.exit(1)
    pass

def help(argparser):
    argparser.print_help()
    pass


EXAMPLE_RECORD = {
    "volume":  483951,
    "symbol": "MSFT",
    "ts":     "2018-08-31 09:30:00",
    "month":  "08",
    "high":    111.74,
    "low":     111.55,
    "open":    111.55,
    "close":   111.72,
    "key":     "MSFT_2018-08-31 09",
    "year":    2018,
    "date":   "2018/08/31",
    "day":    "31",
}

# Example DataFrame as read from CSV file:
#
#           Date        Open        High         Low       Close   Adj Close   Volume
#  0  2019-01-02  164.330002  172.250000  163.350006  172.029999  152.752411  3999400
#  1  2019-01-03  170.660004  171.770004  168.289993  169.509995  150.514740  4060200
#  2  2019-01-04  172.990005  176.000000  171.100006  175.050003  155.433975  3788300
#  3  2019-01-07  175.229996  177.830002  172.270004  176.020004  156.295288  3152100
#
class Stock:
    def __init__(self, name, fname):
        self.name = name
        self.filename = fname
        seed = 17
        for i in name:
            seed = seed * ord(i)
            pass
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        # Load w/ index column set to Date so we can check the dates are unique
        # and monotonicially increasing, then reset the index.
        df = pd.read_csv(fname, index_col='Date')
        self.suspect = False
        if not df.index.is_unique:
            warn(f"Dates in {fname} are not unique")
            self.suspect = True
            pass
        if not df.index.is_monotonic_increasing:
            warn(f"Dates in {fname} are not monotonically increasing")
            self.suspect = True
            pass
        self.df = df.reset_index()
        pass

    def rows(self):
        return len(self.df.index)

    def start_date(self):
        return self.df.index[0]

    def end_date(self):
        return self.df.index[-1]

    def expand(self, date, interval, rounding):
        if not date in self.df.index:
            return [False, False]

        series = self.df.loc[date]

        # These vars hold the values in row 0 of the expanded DataFrame.
        op = series['Open']
        hi = series['High']
        lo = series['Low']
        cl = series['Close']

        price_base = lo
        price_range = hi - lo

        minutes_start =  9 * 60 + 30  #  9:30
        minutes_end   = 16 * 60       # 16:00

        def fmt_index(date, minutes):
            hh = int(minutes / 60)
            mm = minutes % 60
            return f"{date} {hh:02}:{mm:02}:00"

        index = [fmt_index(date,m) for m in range(minutes_start, minutes_end, interval)]
        entries = len(index)

        op_values = [ round(price_base + price_range * self.rng.random(), rounding) for i in range(entries) ]
        hi_values = [ round(price_base + price_range * self.rng.random(), rounding) for i in range(entries) ]
        lo_values = [ round(price_base + price_range * self.rng.random(), rounding) for i in range(entries) ]
        cl_values = [ round(price_base + price_range * self.rng.random(), rounding) for i in range(entries) ]

        # This is kind of a cheat, but has the nice property that the original
        # input row is preserved in the first output row of the expanded dataframe.
        op_values[0] = round(op, rounding)
        hi_values[0] = round(hi, rounding)
        lo_values[0] = round(lo, rounding)
        cl_values[0] = round(cl, rounding)

        df = pd.DataFrame(index=index,
                          data={'symbol':[self.name]*entries,
                                'open': op_values,
                                'high': hi_values,
                                'low': lo_values,
                                'close': cl_values})
        df.index.name = 'ts'
        return [True, df]

class StockIterator:
    def __init__(self, stock):
        self.s = stock
        self.offset = 0
        pass
    def eof(self):
        assert self.offset <= self.s.rows()
        return self.offset == self.s.rows()
    def next(self):
        if not self.eof():
            self.offset += 1
            pass
        return
    def seek(self, date):
        pandas_index_object = self.s.df[self.s.df['Date'] >= date].index
        if len(pandas_index_object) == 0:
            self.offset = self.s.rows()
        else:
            self.offset = pandas_index_object[0]
            pass
        return
    def item(self):
        assert not self.eof()
        return self.s.df.iloc[self.offset]
    pass


class Catalog:
    def __init__(self, directory):
        self.stocks = dict()
        for fname in os.listdir(directory):
            symbol, ext = os.path.splitext(fname)
            if ext == ".csv" or ext == ".CSV":
                self.stocks[symbol] = Stock(symbol, os.path.join(directory, fname))
                pass
            pass
        pass
    def get_stock(self, name):
        return self.stocks[name]
    def stock_names(self):
        return sorted(self.stocks.keys())
    def valid_name(self, name):
        return name in self.stocks
    pass

def make_argparser(cmd):
    ap = argparse.ArgumentParser(
        prog=cmd,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
        description="Generate stock ticker data",
        epilog="\n".join([
            "The output data contains one quote for each stock every INTERVAL minutes.",
            "If INTERVAL is 1, the each stock will have quotes for every minute.",
            "If INTERVAL is 30, the each stock will have quotes every half-hour.",
            "There are 390 minutes in a trading day, so INTERVAL >= 390 results",
            "in one quote per day.",
            "",
            "Defaults:",
            f"  INTERVAL = {DEFAULT_INTERVAL}",
            f"  DIGITS   = {DEFAULT_DIGITS}",
        ]))

    grp = ap.add_argument_group(title="Help/Debug")
    grp.add_argument("-h", dest="help", action="store_true", help="show help")
    grp.add_argument("-H", action="store_true", dest="head", help="output first 5 rows")
    grp.add_argument("-T", action="store_true", dest="tail", help="output last 5 rows")
    grp.add_argument("--show-stocks", action='store_true', help="show available stocks")
    grp.add_argument("--show-source-data", action='store_true', help="show source stocks + data")

    grp = ap.add_argument_group(title="Select stock symbols")
    grp.add_argument("-s", nargs="+", metavar="SYM", dest="names", help="pick stocks")

    grp = ap.add_argument_group(title="Quote interval")
    grp.add_argument("-i", type=int, dest="interval", default=DEFAULT_INTERVAL,
                     help="minute interval (see below)")

    grp = ap.add_argument_group(title="Selecting which rows to emit")
    grp.add_argument("-b", dest="base_date",
                     help="set base date (YYYY, YYYY-MM, or YYYY-MM-DD)")
    grp.add_argument("-m",action='store_true', dest="count_by_months", help="count by months (instead of days)")
    grp.add_argument("-o", type=int, dest="offset", default=0,
                     help="number of days (or months) to skip from base date")
    grp.add_argument("-c", type=int, dest="count", default=1,
                     help="number of days (or months) to output")

    grp = ap.add_argument_group(title="Output row order")
    grp.add_argument("-k", nargs="*", metavar="KEY", dest="sort_keys",
                     help="set sort keys (use column names of output table)")

    grp = ap.add_argument_group(title="Output formats")
    grp.add_argument("-r", type=int, dest="digits", default=DEFAULT_DIGITS,
                     help="number of digits after decimal for dollar amounts")
    grp.add_argument("-f", dest="format", choices=['json','csv','pretty'], default='pretty',
                     help="set output format")

    return ap

def main(args):
    global op
    ap = make_argparser(args[0])
    op = ap.parse_args(args[1:])

    if op.help:
        help(ap)
        return 0

    cat = Catalog('data/stocks')
    all_stocks = [cat.get_stock(name) for name in sorted(cat.stock_names())]

    if op.names != None and len(op.names) > 0:
        for name in op.names:
            if not cat.valid_name(name):
                fatal(f"No such stock symbol: {name}")
                pass
            pass
        selected_stocks = [cat.get_stock(name) for name in op.names]
    else:
        selected_stocks = all_stocks

    if op.show_stocks:
        for i, name in enumerate(all_stocks):
            s = cat.get_stock(name)
            if i == 0:
                headers = ["Symbol", "Start", "End", "Rows", "File"]
                print(f"{headers[0]:8} {headers[1]:12}  {headers[2]:12}  {headers[3]:>5}  {headers[4]}")
                pass
            sdate = s.start_date()
            edate = s.end_date()
            print(f"{s.name:8} {sdate:12}  {edate:12}  {s.rows:>5}  {s.filename}")
            pass
        return 0

    if op.show_source_data:
        for s in selected_stocks:
            print("="*80)
            print(f"Symbol: {s.name}")
            print(f"Rows:   {s.rows}")
            print(f"File:   {s.filename}")
            print(f"Data:")
            print(s.df.iloc[:3])
            print("---")
            print(s.df.iloc[-3:])
            pass
        return 0

    # Use pandas to parse date. It will convert 2020 to 2020-01-01, and 2020-04
    # to 2020-04-01.  Then compute start and end date. If counting by months,
    # insist that start date is a first day of month, otherwise it gets messy
    # because not all months have the same number of days (e.g., what is 1/31 +
    # 1 month?).
    base_date_ts = pd.Timestamp(op.base_date).floor(freq="D")
    if op.count_by_months:
        if base_date_ts.day != 1:
            fatal("Base date must on the first day of a month when counting by months")
            pass
        # Can't use Timedelta for month-based math.
        start_date_ts = pd.Timestamp(
            year  = base_date_ts.year + int(op.offset / 12),
            month = base_date_ts.month + op.offset % 12,
            day   = base_date_ts.day)
        end_date_ts = pd.Timestamp(
            year  = start_date_ts.year + int(op.count / 12),
            month = start_date_ts.month + op.count % 12,
            day   = start_date_ts.day)
    else:
        start_date_ts = base_date_ts + pd.Timedelta(days=op.offset)
        end_date_ts = start_date_ts + pd.Timedelta(days=op.count)
        pass

    start_date = f"{start_date_ts.year}-{start_date_ts.month:02}-{start_date_ts.day:02}"
    end_date = f"{end_date_ts.year}-{end_date_ts.month:02}-{end_date_ts.day:02}"
    #print(f"Start: {start_date}")
    #print(f"End:   {end_date}")

    first_start_date = min([s.start_date() for s in selected_stocks])
    last_end_date = max([s.end_date() for s in selected_stocks])

    df_list = []
    for s in selected_stocks:
        it = StockIterator(s)
        it.seek(start_date)
        while not it.eof() and it.item()['Date'] < end_date:
            #print(f"expand {s.name:4} {it.item().tolist()}")
            have_data, df = s.expand(s.start_date(), op.interval, op.digits)
            if have_data:
                df_list.append(df)
                pass
            it.next()
            pass
        pass

    # concat the list into one df
    df = pd.concat([df.reset_index() for df in df_list])

    # add a column of random values
    rng = np.random.default_rng(314159)
    df['random'] = rng.random(size=len(df.index))

    # sort
    if op.sort_keys == None or len(op.sort_keys) == 0:
        op.sort_keys = ['ts','symbol']
        pass
    for k in op.sort_keys:
        if not k in df.columns:
            fatal("Invalid sort key: {k}",
                  "Must be one of: ",
                  " ".join(df.columns))
            pass
        pass
    df = df.sort_values(by=op.sort_keys, ignore_index=True)

    show = []
    if op.head:
        show.append(df.head())
        pass
    if op.tail:
        show.append(df.tail())
        pass
    if len(show) == 0:
        show.append(df)
        pass

    if op.format == "json":
        for df in show:
            out = io.StringIO()
            df.to_json(out, orient="records", lines=True, index=False)
            print(out.getvalue())
            pass
        pass
        pass
    elif op.format == "csv":
        for df in show:
            out = io.StringIO()
            df.to_csv(out, index=False)
            print(out.getvalue())
            pass
        pass
    else:
        assert op.format == "pretty"
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 10000):
            for df in show:
                print(df)
                pass
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
