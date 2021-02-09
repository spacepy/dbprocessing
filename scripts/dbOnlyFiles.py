#!/usr/bin/env python

"""
this code searches the database and disk for files that are in the db but not on disk
files that are such should have thier exists_on_disk flag set to false
"""
from __future__ import division

import argparse
import bisect
import datetime
import os

from dateutil import parser as dup
from dateutil.relativedelta import relativedelta

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing
from dbprocessing.Utils import progressbar

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--startDate", type=str,
                        help="Date to start reprocessing (e.g. 2012-10-02)", default=None)
    parser.add_argument("-e", "--endDate", type=str,
                        help="Date to end reprocessing (e.g. 2012-10-25)", default=None)
    parser.add_argument("-f", "--fix", action='store_true',
                        help="Fix the database exists_on_disk field ", default=False)
    parser.add_argument("-m", "--mission",
                        help="selected mission database", required=True)
    parser.add_argument("--echo", action='store_true',
                        help="echo sql queries for debugging", default=False)
    parser.add_argument("-n", "--newest", action='store_true',
                        help="Only check the newest files", default=False)
    parser.add_argument("--startID", type=int,
                        help="The File id to start on", default=1)
    parser.add_argument("-v", "--verbose", action='store_true',
                        help="Print out each file as it is checked", default=False)
    parser.add_argument("-p", "--path", action="store_true",
                        help="Print full file path of missing files", default=False)

    
    options = parser.parse_args()

    if options.startDate is not None:
        startDate = dup.parse(options.startDate)
    else:
        startDate = None
    if options.endDate is not None:
        try:
            endDate = datetime.datetime.strptime(options.endDate, "%Y%m") # yyyymm
            endDate += relativedelta(months=1)
            endDate -= datetime.timedelta(days=1)
        except ValueError:
            endDate = dup.parse(options.endDate)
    else:
        endDate = None

    dbu = dbprocessing.DButils.DButils(options.mission, echo=options.echo)

    files = sorted(dbu.getAllFileIds(startDate=startDate, endDate=endDate, newest_version=options.newest))
    files = files[bisect.bisect_left(files, options.startID):]

    for f in files:
        if options.verbose:
            print("{0} is being checked".format(f))
        if not dbu.checkDiskForFile(f, options.fix):
            if options.path:
                print("{0} is missing".format(dbu.getFileFullPath(f)))
            else:
                print("{0} is missing".format(f))


    dbu.closeDB()
