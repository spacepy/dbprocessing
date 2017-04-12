#!/usr/bin/env python2.6

import datetime
import os
import operator
from optparse import OptionParser
import traceback
import subprocess

import dateutil.parser as dup
import spacepy.toolbox as tb

from dbprocessing import dbprocessing
from dbprocessing.runMe import ProcessException
from dbprocessing import runMe, Utils, DButils

"""
This code, Runner.py, is used to demo run codes for certain dates out of the database

THis primarily used in testing can also be used to reprocess files as needed

"""


if __name__ == "__main__":
    usage = \
    """
    Usage: %prog -m db process_id
    """
    parser = OptionParser(usage=usage)
    parser.add_option("-d", "--dryrun", dest="dryrun", action="store_true",
                      help="dryrun, only print what would be done", default=False)
    parser.add_option("-v", "--version", dest="version", type='str', 
                      help="NOTIMPLEMENTED set output version", default='1.0.0')
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    parser.add_option("", "--echo", dest="echo", action="store_true",
                      help="Start sqlalchemy with echo in place for debugging", default=False)
    parser.add_option("-s", "--startDate", dest="startDate", type="string",
                      help="Date to start search (e.g. 2012-10-02 or 20121002)", default=None)
    parser.add_option("-e", "--endDate", dest="endDate", type="string",
                      help="Date to end search (e.g. 2012-10-25 or 20121025)", default=None)
    parser.add_option("", "--nooptional", dest="optional", action="store_false",
                      help="Do not include optional inputs", default=True) # logic is backwards
    parser.add_option("-n", "--num-proc", dest="numproc", type='int',
                      help="Number of processes to run in parallel", default=1)

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

    pq = dbprocessing.ProcessQueue(options.mission, dryrun=options.dryrun, echo=options.echo)

    dates = Utils.expandDates(startDate, endDate)
    print('dates', list(dates))

    inproc = args[0]

    # get the input products for a process
    products = pq.dbu.getInputProductID(inproc)
    print('products', products)

    runme = []
    for d in dates:
        print("Processing date: {0}".format(d))
        # we need a file_id that goes into the process
        input_files = []
        for p, opt in products:
            prod_ = pq.dbu.getEntry('Product', p)
            print("    Processing product: {0} : {1}".format((p,opt), prod_.product_name))
            files = pq.dbu.getFilesByProductDate(p, [d]*2, newest_version=True)
            if files:
                fnames = [v.filename for v in files]
            else:
                fnames = []
            print("        Found files: {0}".format(list(fnames)))

            #files = [v.file_id for v in files]
            if files:
                input_files.extend([v.file_id for v in files])
            if not files:
                print("No process to run for {0}".format(d.isoformat()))
                continue
            #files, input_prods = pq._getRequiredProducts(inproc, files[0].filename, d)
        if not input_files:
            print("No files to run for process ({0}) {1} on {2}".format(inproc,
                                                                      pq.dbu.getEntry('Process', inproc).process_name,
                                                                      d.isoformat()))
        else:
            runme.append(runMe.runMe(pq.dbu, d, inproc, input_files, pq, force=True))
    runMe.runner(runme, pq.dbu, MAX_PROC=options.numproc, rundir='.')
                
