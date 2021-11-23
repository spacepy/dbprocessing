#!/usr/bin/env python


"""
go through the DB and print put a list of dates that do not have files for a given product

"""

import argparse
import datetime
import fnmatch
from operator import itemgetter
import sys
import warnings

from dateutil import parser as dup

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing
from dbprocessing import DButils
from dbprocessing import inspector

warnings.filterwarnings("ignore")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--startDate", type=str,
                        help="Date to start search (e.g. 2012-10-02 or 20121002)", default=None)
    parser.add_argument("-e", "--endDate", type=str,
                        help="Date to end search (e.g. 2012-10-25 or 20121025)", default=None)
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database **required**", default=None)
    parser.add_argument("-f", "--filter",
                        help="Filter to use on filename (space separated globs)", default=None)
    parser.add_argument("-p", "--process", action='store_true', default=False,
                        help="Add those dates to the process queue for the specified DB")
    parser.add_argument("--parent", type=int, default=None,
                        help="The parent product ID  to enable processing of the missing files")
    parser.add_argument("--echo", default=False, action='store_true',
                        help="Put the database in echo mode")
    parser.add_argument('product_id', action='store', type=int,
                        help='Product ID to check.')

    
    options = parser.parse_args()

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

    dbu = DButils.DButils(options.mission, echo=options.echo)

    dates = [startDate + datetime.timedelta(days=v) for v in range((endDate-startDate).days +1)]
    if not dates: # only one day
        dates = [startDate]

    # this is the possible dates for a product
    dates = sorted(dates)

    product_id = options.product_id

    dbfiles = dbu.getFilesByProductDate(product_id,
                                        [startDate, endDate],
                                        newest_version=True)
    # this returns filenames
    # make it file objects
    #dbfiles = [dbu.getEntry('File', v) for v in dbfiles]
    
    # this is then file objects for all the newest files in the date range for this product
    # find the missing days
    missing_dates = []
    for d in dates:
        tmp = [f.filename for f in dbfiles if f.utc_file_date == d]
        if not tmp:
            missing_dates.append(d)
    if not missing_dates:
        print("No missing files")
        del dbu
        sys.exit(0)
       
    print("Missing files for product {0} for these dates:".format(product_id))
    tmp = [d.isoformat() for d in missing_dates]
    print("{0}".format(' '.join(tmp)))

    if options.process:
        if options.parent is None:
            parser.error("Cannot process without a parent product id specified")
        # do a custom query here to get all the file ids in one sweep
        files = dbu.session.query(dbu.File.file_id).filter_by(product_id=options.parent).filter(dbu.File.utc_file_date.in_(missing_dates)).all()
        files = list(map(itemgetter(0), files))
        added = dbu.ProcessqueuePush(files)
        print("   -- Added {0} files to be reprocessed for product {1}".format(len(added), options.parent))
        DBlogging.dblogger.info('Added {0} files to be reprocessed for product {1}'.format(len(added), options.parent))
    del dbu
