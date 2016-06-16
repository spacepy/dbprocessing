#!/usr/bin/env python2.6

from __future__ import print_function

import sys
from optparse import OptionParser

from dbprocessing import DButils

if __name__ == "__main__":
    usage = '%prog file_id [file_id[file_id[...]]]'
    parser = OptionParser()
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("incorrect number of arguments")

    if options.mission is None:
        parser.error("No mission specified")

    a = DButils.DButils(options.mission)
    n_del = 0
    for ff in args:
        try:
            a._purgeFileFromDB(ff)
            print(' File {0} was removed from DB'.format(ff))
            n_del += 1
        except DButils.DBNoData:
            print(' File {0} was not in the DB'.format(ff))
    print('deleted {0} files'.format(n_del))


