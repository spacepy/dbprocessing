#!/usr/bin/env python2.6

import sys

from dbprocessing import DBUtils2

def usage():
    """
    print the usage message out
    """
    print "Usage: {0} [file id to delete] ... ".format(sys.argv[0])
    print "   -> specify as many file_ids or filenames as should be deleted from the db"
    return

if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
        sys.exit(2)
    a = DBUtils2.DBUtils2('rbsp')
    a._openDB()
    a._createTableObjects()
    n_del = 0
    for ff in sys.argv[1:]:
        try:
            a._purgeFileFromDB(ff)
            print(' File {0} was removed from DB'.format(ff))
            n_del += 1
        except DBUtils2.DBNoData:
            print(' File {0} was not in the DB'.format(ff))
    print 'deleted {0} files'.format(n_del)

