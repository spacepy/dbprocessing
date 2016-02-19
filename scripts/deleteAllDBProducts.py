#!/usr/bin/env python2.6

from __future__ import print_function

from dbprocessing import DBUtils


if __name__ == "__main__":
    a = DBUtils.DBUtils('rbsp')
    a._openDB()
    a._createTableObjects()
    prod_ids = zip(*a.getProductNames())[4]

    for ff in f:
        a.purgeFileFromDB(ff[0])
    print('deleted {0} files'.format(len(f)))

