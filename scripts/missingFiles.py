#!/usr/bin/env python2.6


"""
go through the DB and print put a list of dates that do not have files for a given database

"""

import datetime
import fnmatch
import os
from optparse import OptionParser
import subprocess

from dateutil import parser as dup

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing
from dbprocessing import DBUtils
from dbprocessing import inspector


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-s", "--startDate", dest="startDate", type="string",
                      help="Date to start search (e.g. 2012-10-02 or 20121002)", default=None)
    parser.add_option("-e", "--endDate", dest="endDate", type="string",
                      help="Date to end search (e.g. 2012-10-25 or 20121025)", default=None)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database **required**", default=None)

    
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    if options.startDate is not None:
        startDate = dup.parse(options.startDate)
    else:
        startDate = datetime.datetime(2012, 8, 30)
    if options.endDate is not None:
        endDate = dup.parse(options.endDate)
    else:
        endDate = datetime.datetime.now()

    if endDate < startDate:
        parser.error("endDate must be >= to startDate")

    dbu = DBUtils.DBUtils(options.mission)

    # get the product tree:
    tree = dbu.getProductParentTree()
    for t1 in tree:
        for t2 in t1[1]:
            cmd = [ os.path.expanduser('~/dbUtils/missingFilesByProduct.py'),
                '-m', options.mission,
                '-s', startDate.date().isoformat(),
                '-e', endDate.date().isoformat(),
                '-p', '--parent={0}'.format(t1[0]), str(t2) ]
            print("Running {0}".format(cmd))
            subprocess.call(cmd)
            


