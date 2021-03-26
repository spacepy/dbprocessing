#!/usr/bin/env python


"""
go through the DB and add all the files that are a certain product and put then onto the
processqueue so that the next ProcessQueue -p will run them
"""
from __future__ import print_function


import argparse
import datetime

from dateutil import parser as dup
from dateutil.relativedelta import relativedelta

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing


if __name__ == "__main__":
    usage = "%prog [-s yyyymmdd] [-e yyyymmdd] -m product_id [[product_id] ...]"
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--startDate", type=str,
                      help="Date to start reprocessing (e.g. 2012-10-02)", default=None)
    parser.add_argument("-e", "--endDate", type=str,
                      help="Date to end reprocessing (e.g. 2012-10-25)", default=None)
    parser.add_argument("--force", type=int,
                      help="Force the reprocessing, specify which version number {0},{1},{2}", default=None)
    parser.add_argument("-m", "--mission", required=True,
                      help="selected mission database", default=None)
    parser.add_argument("--echo", dest="echo", action='store_true',
                      help="echo sql queries for debugging", default=False)
    parser.add_argument('product', action='store', nargs='+',
                        help='Name or ID of product.')
    
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

    if options.force not in [None, 0, 1, 2]:
        parser.error("invalid force option [0,1,2]")

    if startDate is not None and endDate is not None and startDate > endDate:
        parser.error("startDate > endDate, nothing will be added")

        
    db = dbprocessing.ProcessQueue(options.mission, echo=options.echo)

    print(startDate, endDate)

    for prod in options.product:
        num = db.reprocessByProduct(prod, startDate=startDate, endDate=endDate, incVersion=options.force)
        if num is None:
            num = 0
        print('Added {0} files to be reprocessed for product {1}'.format(num, prod))
        DBlogging.dblogger.info('Added {0} files to be reprocessed for product {1}'.format(num, prod))
    del db

