#!/usr/bin/env python2.6




if __name__ == "__main__":
    from dbprocessing import DBUtils2
    a = DBUtils2.DBUtils2('rbsp')
    a._openDB()
    a._createTableObjects()
    f = a.getAllFilenames()
    for ff in f:
        a._purgeFileFromDB(ff[0])
    print 'deleted {0} files'.format(len(f))

