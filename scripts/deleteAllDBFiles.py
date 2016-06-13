#!/usr/bin/env python

from __future__ import print_function

from optparse import OptionParser

from dbprocessing import DButils

if __name__ == "__main__":
    usage = \
        """
        Usage: %prog -m
            -m -> selects mission
        """
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    a = DButils.DButils(options.mission)
    f = a.getAllFilenames()
    for ff in f:
        a.purgeFileFromDB(ff[0])
    print('deleted {0} files'.format(len(f)))
