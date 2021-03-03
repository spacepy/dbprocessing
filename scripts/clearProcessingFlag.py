#!/usr/bin/env python

import sys

from dbprocessing import DButils

if len(sys.argv) != 3:
    print("Usage: {0} database message".format(sys.argv[0]))
    print("    clears the processing flag from a processing that has crashed")
    sys.exit(-1)

if __name__ == "__main__":
    a = DButils.DButils(sys.argv[1])
    a.resetProcessingFlag(sys.argv[2])
    a.closeDB()
    print('Database lock removed')
