#!/usr/bin/env python2.6

from __future__ import print_function

from dbprocessing import DButils


if __name__ == "__main__":
    a = DButils.DButils('rbsp')
    a.openDB()
    a._createTableObjects()
    prod_ids = zip(*a.getProductNames())[4]

    for ff in f:
        a._purgeFileFromDB(ff[0])
    print('deleted {0} files'.format(len(f)))

