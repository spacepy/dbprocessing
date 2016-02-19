#!/usr/bin/env python2.6

import sys

from dbprocessing import DBUtils

if len(sys.argv) != 3:
    print("Usage: {0} database message".format(sys.argv[0]))
    print("    clears the processing flag from a processing that has crashed")
    sys.exit(-1)

if __name__ == "__main__":
    a = DBUtils.DBUtils(sys.argv[1])
    a.resetProcessingFlag(sys.argv[2])
    print('Database lock removed')
