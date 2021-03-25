#!/usr/bin/env python

from __future__ import division

import argparse
import datetime

from sqlalchemy import func

from dbprocessing import DButils, Utils, Version, inspector
from dbprocessing.runMe import ProcessException

from dbprocessing import DBlogging
DBlogging.dblogger.setLevel(DBlogging.LEVELS['info'])

def missingInstrumentproductlink(dbu, fix=False):
    """
    loop over all the products and make sure they have an instrumentproductlink
    """
    prods = dbu.getAllProducts()
    prod_ids = [v.product_id for v in prods]
    for prod_id in prod_ids:
        num = dbu.session.query(dbu.Instrumentproductlink).filter_by(product_id=prod_id).count() 
        if num == 0:
            print('Product {0}:{1} does not have an instrument link'.format(prod_id, dbu.getEntry('Product', prod_id).product_name))
        if num > 1:
            print('Product {0}:{1} has more than one instrument link'.format(prod_id, dbu.getEntry('Product', prod_id).product_name))
            
def _timedelta2days(inval):
    return inval.days + inval.seconds/60/60/24
            
def suspiciousDateRanges(dbu, fix=False):
    """
    check all the files makeing sure that the date ranges look reasonable, they should be
    something like, 1 day, 1 week, 1 month, or 1 year
    """
    files = dbu.session.query(dbu.File).filter_by(newest_version=1).all()
    t_range = [(v.file_id, v.filename, _timedelta2days(v.utc_stop_time - v.utc_start_time)) for v in files]
    for fid, f, tr in t_range:
        if tr < (1/24):  # 1 hour  1/3600
            print("File: {0}:{1} has short duration {2} days".format(fid, f, tr))
        elif tr > 2 and tr < 6: # more than 2 days but less that week
            print("File: {0}:{1} has odd week duration {2} days".format(fid, f, tr))
        elif tr > 8 and tr < 27: # more than 1 week but less than a month
            print("File: {0}:{1} has odd month duration {2} days".format(fid, f, tr))
        elif tr > 35 and tr < 364: # more than 1 month but less than a year
            print("File: {0}:{1} has odd year duration {2} days".format(fid, f, tr))
        elif tr > 368: # more than 1 year
            print("File: {0}:{1} has odd year duration {2} days".format(fid, f, tr))

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
            versions = [dbu.getFileVersion(v) for v in ftmp]
            mx = max(versions)
            commit = False
            for f in ftmp:
                if dbu.getFileVersion(f) != mx and f.newest_version:
                    print('File {0}:{1} is marked newest, it should not be'.format(f.file_id, f.filename))
                    if fix:
                        f.newest_version = False
                        dbu.session.add(f)
                        commit = True
                if dbu.getFileVersion(f) == mx and not f.newest_version:
                    print('File {0}:{1} is not marked newest, it should be'.format(f.file_id, f.filename))
                    if fix:
                        f.newest_version = True
                        dbu.session.add(f)
                        commit = True
            if commit:
                dbu.commitDB()



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
                    try:
                        fixes.append(dbu.getEntry('File', max(tmp, key=lambda x: x[1])[0]))
                    except ValueError:
                        continue
                    fixes[-1].newest_version = 1
                    print(' ** Changed {0} to be newest version'.format(fixes[-1].filename))
                    dbu.session.add(fixes[-1])
            dbu.commitDB() # commit for each product



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission", default=None)
    parser.add_argument("--fix", action='store_true',
                        help="Fix the issues (make a backup first)", default=False)
    parser.add_argument("--echo", action='store_true',
                        help="enable sqlalchemy echo mode for debugging", default=False)
    options = parser.parse_args()
                
    dbu = DButils.DButils(options.mission, echo=options.echo)

    # If we will be editing the DB we have to have lock
    if options.fix:
        # check currently processing
        curr_proc = dbu.currentlyProcessing()
        if curr_proc:  # returns False or the PID
            # check if the PID is running
            if Utils.processRunning(curr_proc):
                # we still have an instance processing, don't start another
                dbu.closeDB()
                DBlogging.dblogger.error( "There is a process running, can't start another: PID: %d" % (curr_proc))
                raise ProcessException("There is a process running, can't start another: PID: %d" % (curr_proc))
            else:
                # There is a processing flag set but it died, don't start another
                dbu.closeDB()
                DBlogging.dblogger.error( "There is a processing flag set but it died, don't start another" )
                raise ProcessException("There is a processing flag set but it died, don't start another")
        
    print("Running noNewestVersion()")
    noNewestVersion(dbu, options.fix)
    print("Running wrongNewestVersion()")
    wrongNewestVersion(dbu, options.fix)
    print("Running missingInstrumentproductlink()")
    missingInstrumentproductlink(dbu, options.fix)
    print("Running suspiciousDateRanges()")
    suspiciousDateRanges(dbu, options.fix)
    del dbu
