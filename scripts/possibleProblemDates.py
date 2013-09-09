#!/usr/bin/env python2.6

import bisect
import datetime
import glob
import itertools
import os
import sys
import re
from optparse import OptionParser
import re

import dateutil.parser as dup
import numpy as np
from sqlalchemy import func

import rbsp #rbsp.mission_day_to_UTC

from dbprocessing import DBUtils, Utils, Version, inspector
from rbsp import Version

from dbprocessing import DBlogging
DBlogging.dblogger.setLevel(DBlogging.LEVELS['info'])

def wrongNewestVersion(dbu, fix=False):
    """
    loop over products and dates and make sure the correct version is marked latest

    do this by:
    1) get all products
    2) get all dates
    3) loop over all products and dates (itertools.product)
    4) get the versions for the files
    5) find the largest
    6) make sure it is marked newest and others are not
    """
    #firstDate = dbu.session.query(func.min(dbu.File.utc_file_date)).first()[0]
    #lastDate  = dbu.session.query(func.max(dbu.File.utc_file_date)).first()[0]
    #dates = Utils.expandDates(firstDate, lastDate)
    prods = dbu.getAllProducts()

    for p in prods:
        files = dbu.getFilesByProduct(p.product_id, newest_version=False)
        dates = sorted(list(set([v.utc_file_date for v in files])))

        for date in dates:
            ftmp = [v for v in files if v.utc_file_date == date]
            versions = [dbu.getVersion(v) for v in ftmp]
            mx = max(versions)
            commit = False
            for f in ftmp:
                if dbu.getVersion(f) != mx and f.newest_version:
                    print('File {0}:{1} is marked newest, it should not be'.format(f.file_id, f.filename))
                    if fix:
                        f.newest_version = False
                        dbu.session.add(f)
                        commit = True
                if dbu.getVersion(f) == mx and not f.newest_version:
                    print('File {0}:{1} is not marked newest, it should be'.format(f.file_id, f.filename))
                    if fix:
                        f.newest_version = True
                        dbu.session.add(f)
                        commit = True
            if commit:
                dbu._commitDB()



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
            fixes = []
            for f in dates_missing:
                print('{0}, product {1}, no files are newest version'.format(f, prod_id))
                if fix:
                    tmp = dbu.getFiles_product_utc_file_date(prod_id, f)
                    fixes.append(dbu.getEntry('File', max(tmp, key=lambda x: x[1])[0]))
                    fixes[-1].newest_version = 1
                    print(' ** Changed {0} to be newest version'.format(fixes[-1].filename))
                    dbu.session.add(fixes[-1])
            dbu._commitDB() # commit for each product



if __name__ == "__main__":
    usage = "%prog"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission", default=None)
    parser.add_option("", "--fix", dest="fix", action='store_true',
                      help="Fix the issues (make a backup first)", default=False)    
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    if options.mission is None:
        parser.error("-m must be specified")

                
    dbu = DBUtils.DBUtils(options.mission)

    print("Running noNewestVersion()")
    noNewestVersion(dbu, options.fix)
    print("Running wrongNewestVersion()")
    wrongNewestVersion(dbu, options.fix)
