#!/usr/bin/env python2.6


"""
go through the DB and print put a list of dates that do not have files for a given product

"""

import datetime
import fnmatch
from optparse import OptionParser

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
    parser.add_option("-f", "--filter", dest='filter',
                      help="Filter to use on filename (space separated globs)", default=None)
    parser.add_option("-p", "--process", dest='process', action='store_true', default=False,
                      help="Add those dates to the process queue for the specified DB")
    parser.add_option("", "--parent", dest='parent', type='int', default=None,
                      help="The parent product ID  to enable processing of the missing files")

    
    (options, args) = parser.parse_args()
    if len(args) != 1:
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

    db = dbprocessing.ProcessQueue(options.mission,)

    dbu = DBUtils.DBUtils(options.mission)

    dates = [startDate + datetime.timedelta(days=v) for v in range((endDate-startDate).days)]
    if not dates: # only one day
        dates = [startDate]

    # this is the possible dates for a product
    dates = set(dates)

    files = dbu.getAllFilenames(fullPath=False, product=args[0]) 
    if options.filter is not None:
        if not hasattr('__iter__', options.filter):
            options.filter = [options.filter]
        for ff in options.filter:
            files = [v for v in files if fnmatch.fnmatch(v, ff)]

    filedates = set([inspector.extract_YYYYMMDD(v) for v in files])
    # get a list of the missing dates
    missing = sorted(list(dates.difference(filedates)))
    for m in missing:
        print("{0}".format(m.strftime('%Y%m%d')))

    if options.process:
        if options.parent is None:
            parser.error("Cannot process without a parent product id specified")
#        parents = dbu.getAllFilenames(fullPath=False, product=options.parent)
#        pdates = [inspector.extract_YYYYMMDD(v) for v in parents]
        print("Adding to process queue:")
        for m in missing:
            print("   {0}".format(m))
            num = db.reprocessByProduct(options.parent, startDate=m, endDate=m)
            print('Added {0} files to be reprocessed for product {1}'.format(num, args[0]))
            DBlogging.dblogger.info('Added {0} files to be reprocessed for product {1}'.format(num, args[0]))


