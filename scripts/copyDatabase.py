import sys
import os

import collections
import datetime
import functools
import hashlib
import itertools
import math
import numpy
import os
import os.path
import struct
import warnings
import pkgutil

import sqlalchemy
import sqlalchemy.engine.reflection
import sqlalchemy.sql

from dbprocessing import DButils
from optparse import OptionParser

def copyDB(DBname = None):


    usage = \
        """
        Usage: %prog -m
            -m -> selects mission
        """
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    DButils.CreateDB(copiedDB_test.db)
    #a = DButils.DButils('/home/natejm/dbprocessing/tests/testDB/testDB.sqlite')
    #a.CreateDB(copiedDB_test.db)


"""
    copyName = '/home/natejm/dbprocessing/tests/testDB/testDB_COPY.sqlite'
    db_conn = sqlite3.connect(DBname)
    cp_conn = sqlite3.connect(copyName)
    c = db_conn.cursor()
    cpy = cp_conn.cursor()
    
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = c.fetchall()
    
    for table in tables:
        c.execute("SELECT * FROM %s" %table)
        rows = c.fetchall()
        #print("testing: %s this is some %s testing you know.." %(rows, table))
        tableStr = 'CREATE TABLE IF NOT EXISTS %s' %table
        tableStr= tableStr +'(None)'
        cpy.execute(tableStr)
        #print(rows)
        for row in rows:
            r = str(row)
            s = 'ALTER TABLE %s' %table
            s = s + ' ADD COLUMN %s' %r
            #cpy.execute(s)
            print('\n')
            print(row)

    cp_conn.commit()
    c.close()
    cpy.close()
    db_conn.close()


"""
copyDB('/home/natejm/dbprocessing/tests/testDB/testDB.sqlite')
