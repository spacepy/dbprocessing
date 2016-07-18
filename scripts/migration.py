#!/usr/bin/env python

from __future__ import print_function
from optparse import OptionParser

from sqlalchemy import types

from dbprocessing import DButils

def add_column(db, table, column, datatype):
    db.execute("ALTER TABLE {0} ADD {1} {2}".format(table, column, datatype))

if __name__ == '__main__':
    usage = "usage: %prog [options] -m mission_db filename"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission", type="string",
                      help="mission to connect to", default='')

    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    dbu = DButils.DButils(options.mission)

    # Add trigger support
    add_column(dbu.session, "process", "trigger", types.String(250))