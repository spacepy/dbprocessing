#!/usr/bin/env python2.6

"""
this code searches the database and disk for files that are in the db but not on disk
files that are such should have thier exists_on_disk flag set to false

additionally, this can remove files from the db that are only in the db
"""
from __future__ import division

import bisect
import datetime
import math
import os
from optparse import OptionParser

from dateutil import parser as dup
from dateutil.relativedelta import relativedelta

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing
from dbprocessing.Utils import progressbar

if __name__ == "__main__":
    usage = "%prog -m mission"
    parser = OptionParser(usage=usage)
#    parser.add_option("-s", "--startDate", dest="startDate", type="string",
#                      help="Date to start reprocessing (e.g. 2012-10-02)", default=None)
#    parser.add_option("-e", "--endDate", dest="endDate", type="string",
#                      help="Date to end reprocessing (e.g. 2012-10-25)", default=None)
#    parser.add_option("", "--force", dest="force", type="int",
#                      help="Force the reprocessing, speicify which version number {0},{1},{2}", default=None)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    parser.add_option("", "--echo", dest="echo", action='store_true',
                      help="echo sql queries for debugging", default=False)
#    parser.add_option("-n", "--newest", dest="newest", action='store_true',
#                      help="Only check the newest files", default=False)
    parser.add_option("", "--startID", dest="startID", type="int",
                      help="The File id to start on", default=1)

    
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

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

    dbu = dbprocessing.DBUtils.DBUtils(options.mission, echo=options.echo)

    # getFileFullPath is really slow

    files = sorted(dbu.getAllFileIds(newest_version=options.newest))
    files = files[bisect.bisect_left(files, options.startID):]

    try:
        for i, f in enumerate(files):
            # get the name and see if it is there
            ff = dbu.getEntry('File', f)
            exists = ff.exists_on_disk

            fullpath = dbu.getFileFullPath(f)
            isfile = os.path.isfile(fullpath)
            if not exists and isfile:
                ff.exists_on_disk = True
                dbu.session.add(ff)
                extra = 'Fixed1'
            elif exists and not isfile:
                ff.exists_on_disk = True
                dbu.session.add(ff)
                extra = 'Fixed2'                
            else:
                extra = ''
            print("{0:6} {1:7} {2} {3}".format(f, str(isfile), fullpath, extra ))
            if i % 100 == 0:
                dbu._commitDB()

    finally:
        dbu._commitDB()
        dbu._closeDB()






    
