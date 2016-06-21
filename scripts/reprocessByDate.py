#!/usr/bin/env python2.6


"""
go through the DB and add all the files that are in a date range and put them into the
processqueue so that the next ProcessQueue -p will run them
"""
from __future__ import print_function

from optparse import OptionParser

from dateutil import parser as dup

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing

if __name__ == "__main__":
    usage = "%prog -s yyyymmdd -e yyyymmdd -m database"
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--startDate", dest="startDate", type="string",
                      help="Date to start reprocessing (e.g. 2012-10-02)", default=None)
    parser.add_option("-e", "--endDate", dest="endDate", type="string",
                      help="Date to end reprocessing (e.g. 2012-10-25)", default=None)
    parser.add_option("", "--force", dest="force", type="int",
                      help="Force the reprocessing, speicify which version number {0},{1},{2}", default=None)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    parser.add_option("", "--echo", dest="echo", action='store_true',
                      help="echo sql queries for debugging", default=False)

    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    if options.startDate is not None:
        startDate = dup.parse(options.startDate)
    else:
        parser.error("-s must be specified")

    if options.endDate is not None:
        endDate = dup.parse(options.endDate)
    else:
        parser.error("-e must be specified")

    if options.force not in [None, 0, 1, 2]:
        parser.error("invalid force option [0,1,2]")

    db = dbprocessing.ProcessQueue(options.mission, echo=options.echo)

    print(startDate, endDate)

    num = db.reprocessByDate(startDate=startDate, endDate=endDate, incVersion=options.force)
    if num is None:
        num = 0
    print('Added {0} files to be reprocessed'.format(num))
    DBlogging.dblogger.info('Added {0} files to be reprocessed'.format(num))
