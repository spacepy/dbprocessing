#!/usr/bin/env python


"""
go through the DB and add all the files that are a certain instrument (and level) and put then onto the
processqueue so that the next ProcessQueue -p will run them
"""



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
    parser.add_argument("-l", "--level", type=float,
                        help="The level to reprocess for the given instrument", default=None)
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database", default=None)
    parser.add_argument('instrument', action='store',
                        help='Name or ID of instrument.')

    options = parser.parse_args()

    if options.startDate is not None:
        startDate = dup.parse(options.startDate)
    else:
        startDate = None
    if options.endDate is not None:
        endDate = dup.parse(options.endDate)
    else:
        endDate = None

    db = dbprocessing.ProcessQueue(options.mission,)

    if options.force not in [None, 0, 1, 2]:
        parser.error("invalid force option [0,1,2]")

    num = db.reprocessByInstrument(options.instrument, level=options.level,
                                   startDate=startDate, endDate=endDate,
                                   incVersion=options.force)

    del db
    print('Added {0} files to be reprocessed for instrument {1}'.format(num, options.instrument))
    DBlogging.dblogger.info('Added {0} files to be reprocessed for instrument {1}'.format(num, options.instrument))


