#!/usr/bin/env python2.6

import sys

from dbprocessing import DBUtils

if len(sys.argv) != 2:
    print('Usage: {0} database'.format(sys.argv[0]))
    sys.exit(-1)

if __name__ == "__main__":
    a = DBUtils.DBUtils(sys.argv[1])
    a._openDB()
    a._createTableObjects()
    n_items = a.Processqueue.len()
    items = a.Processqueue.getAll()
    print('There are a total of {0} files'.format(n_items))
    for v in items:
        tb = a.getFileTraceback(v)
        print('{0}\t{1}\t{2}'.format(v, tb['file'].filename, tb['product'].product_name))

