#!/usr/bin/env python2.6

from dbprocessing import DBUtils2

if __name__ == "__main__":
    a = DBUtils2.DBUtils2('rbsp')
    a._openDB()
    a._createTableObjects()
    n_items = a.processqueueLen()
    items = a.processqueueGetAll()
    for v in items:
        tb = a.getFileTraceback(v)
        print('{0}\t{1}\t{2}'.format(v, tb['file'].filename, tb['product'].product_name))

