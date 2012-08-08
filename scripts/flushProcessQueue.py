#!/usr/bin/env python2.6

from dbprocessing import DBUtils2

if __name__ == "__main__":
    a = DBUtils2.DBUtils2('rbsp')
    a._openDB()
    a._createTableObjects()
    n_items = a.processqueueLen()
    a.processqueueFlush()
    print 'Flushed {0} items from queue'.format(n_items)

