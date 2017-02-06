#!/usr/bin/env python

from __future__ import print_function

from optparse import OptionParser

from dbprocessing import DButils


if __name__ == '__main__':
    usage = "usage: %prog -m database filename [filename [filename] ...]"
    parser = OptionParser(usage=usage)
    parser.add_option("-r", "--recursive", dest="recursive", action="store_true",
                      help="Recursive removal", default=False)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      help="Verbose", default=False)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("incorrect number of arguments")

    dbu = DButils.DButils(options.mission)
    dbu._purgeFileFromDB(args, recursive=options.recursive, verbose=options.verbose)
    dbu.closeDB()