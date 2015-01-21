#!/usr/bin/env python2.6

from __future__ import division

import bisect
import datetime
import glob
import itertools
import os
import sys
import re
from operator import itemgetter, attrgetter
from optparse import OptionParser
import re

import dateutil.parser as dup
import numpy as np
from sqlalchemy import func

import rbsp #rbsp.mission_day_to_UTC

from dbprocessing import DBUtils, Utils, Version, inspector
from dbprocessing.runMe import ProcessException
from rbsp import Version

from dbprocessing import DBlogging
DBlogging.dblogger.setLevel(DBlogging.LEVELS['info'])


if __name__ == "__main__":
    usage = "%prog"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission", default=None)
    parser.add_option("", "--fix", dest="fix", action='store_true',
                      help="Fix the issues (make a backup first)", default=False)    
    parser.add_option("", "--echo", dest="echo", action='store_true',
                      help="enable sqlalchemy echo mode for debugging", default=False)    
    (options, args) = parser.parse_args()


    if len(args) != 0:
        parser.error("incorrect number of arguments")

    if options.mission is None:
        parser.error("-m must be specified")

                
    dbu = DBUtils.DBUtils(options.mission, echo=options.echo)

    # If we will be editing the DB we have to have lock
    if options.fix:
        # check currently processing
        curr_proc = dbu._currentlyProcessing()
        if curr_proc:  # returns False or the PID
            # check if the PID is running
            if Utils.processRunning(curr_proc):
                # we still have an instance processing, don't start another
                dbu._closeDB()
                DBlogging.dblogger.error( "There is a process running, can't start another: PID: %d" % (curr_proc))
                raise(ProcessException("There is a process running, can't start another: PID: %d" % (curr_proc)))
            else:
                # There is a processing flag set but it died, don't start another
                dbu._closeDB()
                DBlogging.dblogger.error( "There is a processing flag set but it died, don't start another" )
                raise(ProcessException("There is a processing flag set but it died, don't start another"))
        
    print("Looping over each database file ebtry to be sure it exists on disk")
    print("  if --fix was set db entries will be removed for those that are not on disk")

    products = [v.product_id for v in dbu.getAllProducts()]
    plen = len(products)

    lisfile = os.path.isfile
    lbasename = os.path.basename
    ldirname  = os.path.dirname
    ljoin = os.path.join
    lgetEntry = dbu.getEntry
    lgetFileFullPath = dbu.getFileFullPath
    for ii, pid in enumerate(products):
        files = dbu.getAllFilenames(fullPath=False, product=pid)
        if not files:
            continue
        one_file = dbu.session.query(dbu.File).filter_by(product_id = pid).first().filename
        prod_path = ldirname(lgetFileFullPath(one_file))
        print("Processing product {0} it is {1} of {2}  {3}".format(pid, ii+1, plen, prod_path))

        diskfiles = os.listdir(prod_path)
        
        print("    Found {0} diskfiles and {1} dbfiles, checking".format(len(diskfiles), len(files)))
        files = set(files)
        diskfiles = set(diskfiles)
        delfiles = files.difference(diskfiles)
        for f in delfiles:
            if not lisfile(f):
                print("    ** {0} not found on disk".format(f))
                if options.fix:
                    fentry = lgetEntry('File', lbasename(f))
                    if fentry.exists_on_disk:
                        dbu._purgeFileFromDB(ljoin(prod_path, f))
                        print("        ** {0} removed from DB".format(ljoin(prod_path, f)))
                    else:
                        print("        ** was already not marked exists_on_disk")
    
    dbu._closeDB()
