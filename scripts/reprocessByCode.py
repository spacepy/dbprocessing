#!/usr/bin/env python


"""
go through the DB and add all the files that went into this code onto the
processqueue so that the next ProcessQueue -p will run them
"""




#==============================================================================
# INPUTS
#==============================================================================
# code_id (or code name)
## startDate - date to start the reprocess
## endDate - date to end the reprocess

import argparse

from dateutil import parser as dup

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--startDate", type=str,
                        help="Date to start reprocessing (e.g. 2012-10-02)", default=None)
    parser.add_argument("-e", "--endDate", type=str,
                        help="Date to end reprocessing (e.g. 2012-10-25)", default=None)
    parser.add_argument("--force", type=int,
                      help="Force the reprocessing, specify which version number {0},{1},{2}", default=None)
    parser.add_argument("-m", "--mission", required=True,
                      help="selected mission database", default=None)
    parser.add_argument('code', action='store',
                        help='Name or ID of code.')

    options = parser.parse_args()

    if options.startDate is not None:
        startDate = dup.parse(options.startDate)
    else:
        startDate = None
    if options.endDate is not None:
        endDate = dup.parse(options.endDate)
    else:
        endDate = None

    db = dbprocessing.ProcessQueue(options.mission)

    if options.force not in [None, 0, 1, 2]:
        parser.error("invalid force option [0,1,2]")
    num = db.reprocessByCode(options.code, startDate=startDate, endDate=endDate, incVersion=options.force)

    del db
    print('Added {0} files to be reprocessed for code {1}'.format(num, options.code))
    DBlogging.dblogger.info('Added {0} files to be reprocessed for code {1}'.format(num, options.code))
