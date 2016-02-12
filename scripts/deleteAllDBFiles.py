#!/usr/bin/env python2.6


from dbprocessing import DBUtils
from __future__ import print_function


if __name__ == "__main__":
    a = DBUtils.DBUtils('rbsp')
    a._openDB()
    a._createTableObjects()
    f = a.getAllFilenames()
    for ff in f:
        a._purgeFileFromDB(ff[0])
    print('deleted {0} files'.format(len(f)))

