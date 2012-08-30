#!/usr/bin/env python2.6

from dbprocessing import DBUtils

if __name__ == "__main__":
    a = DBUtils.DBUtils('rbsp')
    a._openDB()
    a._createTableObjects()
    n_items = a.processqueueLen()
    a.processqueueFlush()
    print 'Flushed {0} items from queue'.format(n_items)

