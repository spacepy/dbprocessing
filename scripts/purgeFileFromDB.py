#!/usr/bin/env python

from __future__ import print_function

import argparse

from dbprocessing import DButils


if __name__ == '__main__':
    usage = "usage: %prog -m database filename [filename [filename] ...]"
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--recursive", action="store_true",
                        help="Recursive removal", default=False)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose", default=False)
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database", default=None)
    parser.add_argument("filename", nargs='+',
                        help="file(s) to purge", default=None)
    options = parser.parse_args()

    dbu = DButils.DButils(options.mission)
    dbu._purgeFileFromDB(options.filename, recursive=options.recursive, verbose=options.verbose)
    dbu.closeDB()
