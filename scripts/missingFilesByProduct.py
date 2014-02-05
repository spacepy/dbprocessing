#!/usr/bin/env python2.6


"""
go through the DB and print put a list of dates that do not have files for a given product

"""

import datetime
import fnmatch
from optparse import OptionParser
import sys
import warnings

from dateutil import parser as dup

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing
from dbprocessing import DBUtils
from dbprocessing import inspector

usage = "%prog -m mission product_id [-s startDate] [-e endDate] [-f filter] [-p] [--parent=parent_id]"
warnings.filterwarnings("ignore")

if __name__ == "__main__":
    parser = OptionParser(usage=usage)
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

    startDate = startDate.date()

    if options.endDate is not None:
        endDate = dup.parse(options.endDate)
    else:
        endDate = datetime.datetime.now()

    endDate = endDate.date()

    if endDate < startDate:
        parser.error("endDate must be >= to startDate")

    db = dbprocessing.ProcessQueue(options.mission,)

    dbu = DBUtils.DBUtils(options.mission)

    dates = [startDate + datetime.timedelta(days=v) for v in range((endDate-startDate).days +1)]
    if not dates: # only one day
        dates = [startDate]

    # this is the possible dates for a product
    dates = sorted(dates)

    product_id = args[0]

    dbfiles = dbu.getFilesByProductDate(product_id,
                                        [startDate, endDate],
                                        newest_version=True)
    
    # this is then file objects for all the newest files in the date range for this product
    # find the missing days
    missing_dates = []
    for d in dates:
        tmp = [f.filename for f in dbfiles if f.utc_file_date == d]
        if not tmp:
            missing_dates.append(d)
    if not missing_dates:
        print("No missing files")
        sys.exit(0)
       
    print("Missing files for product {0} for these dates:".format(product_id))
    for d in missing_dates:
        print("\t{0}".format(d.isoformat()))

    if options.process:
        if options.parent is None:
            parser.error("Cannot process without a parent product id specified")
        print("Adding to process queue:")
        for m in missing_dates:
            num = db.reprocessByProduct(options.parent, startDate=m, endDate=m)
            print("   {0} -- Added {1} files to be reprocessed for product {2}".format(m, num, options.parent))
            DBlogging.dblogger.info('Added {0} files to be reprocessed for product {1}'.format(num, options.parent))

