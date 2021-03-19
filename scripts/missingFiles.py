#!/usr/bin/env python


"""
go through the DB and print put a list of dates that do not have files for a given database

"""

import argparse
import datetime
import fnmatch
import os
import subprocess
import sys

from dateutil import parser as dup

import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing
from dbprocessing import DButils
from dbprocessing import inspector


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--startDate", dest="startDate", type=str,
                        help="Date to start search (e.g. 2012-10-02 or 20121002)", default=None)
    parser.add_argument("-e", "--endDate", dest="endDate", type=str,
                        help="Date to end search (e.g. 2012-10-25 or 20121025)", default=None)
    parser.add_argument("-m", "--mission", dest="mission", required=True,
                        help="selected mission database **required**", default=None)

    
    options = parser.parse_args()

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

    dbu = DButils.DButils(options.mission)

    # get the product tree:
    tree = dbu.getProductParentTree()
    del dbu
    for t1 in tree:
        for t2 in t1[1]:
            cmd = [
                sys.executable,
                os.path.join(os.path.dirname(__file__),
                             'missingFilesByProduct.py'),
                '-m', options.mission,
                '-s', startDate.date().isoformat(),
                '-e', endDate.date().isoformat(),
                '-p', '--parent={0}'.format(t1[0]), str(t2) ]
            print("Running {0}".format(cmd))
            subprocess.call(cmd)
            


