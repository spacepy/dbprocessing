#!/usr/bin/env python2.6

from __future__ import print_function

import itertools
import glob
import os
from optparse import OptionParser
import re
import sys
import traceback
import warnings

from dbprocessing import DButils
from dbprocessing import Version


if __name__ == '__main__':
    usage = "usage: %prog -m database filename [filename [filename] ...]"
    parser = OptionParser(usage=usage)
    parser.add_option("-r", "--recursive", dest="recursive", action="store_true",
                      help="Recursive removal", default=False)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("incorrect number of arguments")

    if options.recursive:
        raise(NotImplementedError("Recursive removal not implemented in DButils yet"))
        
    dbu = DButils.DButils(options.mission)

    for f in args:
        try:
            f_id = dbu.getFileID(f)
        except DButils.DBNoData:
            print("WARNING: File {1} not in db".format(f), file=sys.stderr)
        else:
            dbu._purgeFileFromDB(f)
            print("  File {0}:{1} removed from DB".format(f_id, f))

    
    dbu.closeDB()
        
