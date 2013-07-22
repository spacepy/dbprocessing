#!/usr/bin/env python2.6

import datetime
import glob
import os
import sys
import re
import bisect
import glob
from optparse import OptionParser
import os
import re

import dateutil.parser as dup
import numpy as np

import rbsp #rbsp.mission_day_to_UTC

from dbprocessing import DBUtils, Utils, inspector
from rbsp import Version



def noNewestVersion(dbu, fix=False):
    """
    print out a list of dates and files where there are files but there is not newest version
    this should be sorted by product
    """
    products = dbu.getAllProducts()
    prod_ids = [v.product_id for v in products]
    for prod_id in prod_ids:
        files_all = dbu.getFilesByProduct(prod_id, newest_version=False)
        files_newest = dbu.getFilesByProduct(prod_id, newest_version=True)
        dates_all = set([v.utc_file_date for v in files_all])
        dates_newest = set([v.utc_file_date for v in files_newest])
        dates_missing =  dates_all.difference(dates_newest)
        if dates_missing:
            dates_missing = sorted(list(dates_missing))
            for f in dates_missing:
                print('{0}, product {1}, no files are newest version'.format(f, prod_id))
                if fix:
                    tmp = dbu.getFiles_product_utc_file_date(prod_id, f)
                    f_tmp = dbu.getEntry('File', max(tmp, key=lambda x: x[1])[0])
                    f_tmp.newest_version = 1
                    print(' ** Changed {0} to be newest version'.format(f_tmp.filename))
            dbu._commitDB() # commit for each product



if __name__ == "__main__":
    usage = "%prog"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission", default=None)
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    if options.mission is None:
        parser.error("-m must be specified")

                
    dbu = DBUtils.DBUtils(options.mission)

    noNewestVersion(dbu)
