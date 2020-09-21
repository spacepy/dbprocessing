#!/usr/bin/env python2.6

import datetime
import os
import operator
from optparse import OptionParser
import traceback
import subprocess

import dateutil.parser as dup

from dbprocessing import dbprocessing
from dbprocessing.runMe import ProcessException
from dbprocessing import runMe, Utils, DButils

"""
This code, Runner.py, is used to demo run codes for certain dates out of the database

THis primarily used in testing can also be used to reprocess files as needed

"""

def parse_args(argv=None):
    """Parse arguments for this script

    Parameters
    ==========
    argv : list
        Argument list, default from sys.argv

    Returns
    =======
    args : list
        positional arguments from command line
    options : optparse.Values
        Arguments to command line flags
    """
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

    (options, args) = parser.parse_args(argv)
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    if options.startDate is not None:
        options.startDate = dup.parse(options.startDate)
    else:
        options.startDate = datetime.datetime(2012, 8, 30)
    if options.endDate is not None:
        options.endDate = dup.parse(options.endDate)
    else:
        options.endDate = datetime.datetime.now()

    if options.endDate < options.startDate:
        parser.error("endDate must be >= to startDate")
    return args, options


def calc_runme(pq, startDate, endDate, inproc):
    """Find all processes that can be run given the inputs

    Parameters
    ==========
    pq : dbprocessing.dbprocessing.ProcessQueue
        Process queue to use in calculation
    startDate : datetime.datetime
        First date to process (inclusive)
    endDate : datetime.datetime
        Last date to process (inclusive)
    inproc : int
        Process ID to run

    Returns
    =======
    runme : list
        All commands that can be run.
    """
    dates = [Utils.datetimeToDate(d)
             for d in Utils.expandDates(startDate, endDate)]
    print('dates', dates)

    timebase = pq.dbu.getProcessTimebase(inproc)

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
            filegetter = pq.dbu.getFilesByProductTime \
                         if timebase in ('DAILY',) \
                         else pq.dbu.getFilesByProductDate
            files = filegetter(p, [d]*2, newest_version=True)
            fnames = [v.filename for v in files]
            print("        Found files: {0}".format(list(fnames)))

            input_files.extend([v.file_id for v in files])
        if not input_files:
            print("No files to run for process ({0}) {1} on {2}".format(inproc,
                                                                      pq.dbu.getEntry('Process', inproc).process_name,
                                                                      d.isoformat()))
        else:
            runme.append(runMe.runMe(pq.dbu, d, inproc, input_files, pq, force=True))
    return runme


if __name__ == "__main__":
    args, options = parse_args()
    inproc = args[0]
    pq = dbprocessing.ProcessQueue(options.mission, dryrun=options.dryrun, echo=options.echo)
    runme = calc_runme(pq, options.startDate, options.endDate, inproc)
    runMe.runner(runme, pq.dbu, MAX_PROC=options.numproc, rundir='.')
                
