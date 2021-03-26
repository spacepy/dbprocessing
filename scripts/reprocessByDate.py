#!/usr/bin/env python


"""
go through the DB and add all the files that are in a date range and put them into the
processqueue so that the next ProcessQueue -p will run them
"""
from __future__ import print_function

import argparse

from dateutil import parser as dup

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--startDate", dest="startDate", type=str, required=True,
                      help="Date to start reprocessing (e.g. 2012-10-02)", default=None)
    parser.add_argument("-e", "--endDate", dest="endDate", type=str, required=True,
                        help="Date to end reprocessing (e.g. 2012-10-25)", default=None)
    parser.add_argument("--force", dest="force", type=int,
                        help="Force the reprocessing, specify which version number {0},{1},{2}", default=None)
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database", default=None)
    parser.add_argument("--echo", dest="echo", action='store_true',
                        help="echo sql queries for debugging", default=False)
    parser.add_argument("--level", dest="level", type=float,
                        help="The data level to reprocess", default=None)

    options = parser.parse_args()

    startDate = dup.parse(options.startDate)

    endDate = dup.parse(options.endDate)

    if options.force not in [None, 0, 1, 2]:
        parser.error("invalid force option [0,1,2]")

    db = dbprocessing.ProcessQueue(options.mission, echo=options.echo)

    print(startDate, endDate)

    num = db.reprocessByDate(startDate=startDate, endDate=endDate, incVersion=options.force, level=options.level)
    del db
    if num is None:
        num = 0
    print('Added {0} files to be reprocessed'.format(num))
    DBlogging.dblogger.info('Added {0} files to be reprocessed'.format(num))
