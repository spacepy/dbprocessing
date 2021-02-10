#!/usr/bin/env python

from __future__ import print_function

import sys

from dbprocessing import DButils

if len(sys.argv) != 2:
    print("Usage: {0} database".format(sys.argv[0]))
    sys.exit(-1)

if __name__ == "__main__":
    a = DButils.DButils(sys.argv[1])
    n_items = a.ProcessqueueLen()
    a.ProcessqueueFlush()
    print('Flushed {0} items from queue'.format(n_items))

