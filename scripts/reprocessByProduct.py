#!/usr/bin/env python2.6


"""
go through the DB and add all the files that are a certain product and put then onto the
processqueue so that the next ProcessQueue -p will run them
"""



import datetime
from optparse import OptionParser

from dateutil import parser as dup
from dateutil.relativedelta import relativedelta

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing


if __name__ == "__main__":
    usage = "%prog [-s yyyymmdd] [-e yyyymmdd] -m product_id [[product_id] ...]"
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--startDate", dest="startDate", type="string",
                      help="Date to start reprocessing (e.g. 2012-10-02)", default=None)
    parser.add_option("-e", "--endDate", dest="endDate", type="string",
                      help="Date to end reprocessing (e.g. 2012-10-25)", default=None)
    parser.add_option("", "--force", dest="force", type="int",
                      help="Force the reprocessing, speicify which version number {0},{1},{2}", default=None)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)

    
    (options, args) = parser.parse_args()
    if len(args) < 1:
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

    if options.force not in [None, 0, 1, 2]:
        parser.error("invalid force option [0,1,2]")

    db = dbprocessing.ProcessQueue(options.mission,)

    print startDate, endDate

    for prod in args:
        num = db.reprocessByProduct(prod, startDate=startDate, endDate=endDate, incVersion=options.force)
        if num is None:
            num = 0
        print('Added {0} files to be reprocessed for product {1}'.format(num, prod))
        DBlogging.dblogger.info('Added {0} files to be reprocessed for product {1}'.format(num, prod))


