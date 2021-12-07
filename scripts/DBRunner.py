#!/usr/bin/env python

import datetime
import os
import operator
import argparse
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
    options : argparse.Values
        Arguments from command line, from flags and non-flag arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dryrun", action="store_true", default=False,
                        help="dryrun, only print what would be done")
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database")
    parser.add_argument("--echo", action="store_true", default=False,
                        help="Start sqlalchemy with echo in place for"
                        " debugging")
    parser.add_argument("-s", "--startDate", default=None,
                        help="Date to start search (e.g. 2012-10-02 or"
                        " 20121002)")
    parser.add_argument("-e", "--endDate", default=None,
                        help="Date to end search (e.g. 2012-10-25 or"
                        " 20121025)")
    howtorun = parser.add_mutually_exclusive_group()
    howtorun.add_argument("--force", type=int, default=None, choices=[0, 1, 2],
                          help="Always process and bump version; specify"
                          " which version to bump (0: interface; 1: quality;"
                          " 2: revision). Mutually exclusive with -u, -v."
                          " (Default: version 1.0.0.)")
    howtorun.add_argument("-u", "--update", action="store_true", default=False,
                          help="Only run files that have not yet been created"
                          " or have updated codes. Mutually exclusive with"
                          " --force, -v. (Default: run all.)")
    howtorun.add_argument("-v", "--version",  default='1.0.0',
                          help="NOTIMPLEMENTED set output version."
                          " Mutually exclusive with --force, -u.")
    parser.add_argument("-i", "--ingest", action="store_true", default=False,
                        help="Ingest the created files. Requires -u or --force."
                        " (Default: create file in current directory.)")
    parser.add_argument(
        "--nooptional", dest="optional", action="store_false", default=True,
        help="Do not include optional inputs") # logic is backwards
    parser.add_argument("-n", "--num-proc", dest="numproc", type=int, default=1,
                        help="Number of processes to run in parallel")
    parser.add_argument('process_id', action='store',
                        help="Process ID or name of process to run")

    options = parser.parse_args(argv)
    if options.ingest and options.force is None and not options.update:
        parser.error("argument -i/--ingest: requires --force or --update")

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
    return options


def calc_runme(pq, startDate, endDate, inproc,
               version_bump=None, update=False):
    """Find all processes that can be run given the inputs

    Parameters
    ==========
    pq : dbprocessing.dbprocessing.ProcessQueue
        Process queue to use in calculation
    startDate : datetime.datetime
        First date to process (inclusive)
    endDate : datetime.datetime
        Last date to process (inclusive)
    inproc : int or str
        Process ID or name to run
    version_bump : int
        Which component of version to bump (0-2, 0 for interface).
        Cannot combine with `update`. Default: do not bump version.
    update : bool
        Only run for updated files, and increment version appropriately.
        Cannot combine with `version_bump`.

    Returns
    =======
    runme : list
        All commands that can be run.
    """
    if update and version_bump is not None:
        raise ValueError('Cannot specify both update and version_bump.')
    dates = [Utils.datetimeToDate(d)
             for d in Utils.expandDates(startDate, endDate)]
    print('dates', dates)

    # Get the full Process entry for the process ID
    inproc = pq.dbu.getEntry('Process', inproc)
    timebase = pq.dbu.getProcessTimebase(inproc.process_id)

    # get the input products for a process
    products = pq.dbu.getInputProductID(inproc.process_id)
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
            print("{3} ({0}) {1} on {2}".format(
                inproc.process_id, inproc.process_name,
                d.isoformat(),
                "No files to run for" if products
                else "No input product, always check"))
            if products: # Skip the run.
                continue
        r = runMe.runMe(
            pq.dbu, d, inproc.process_id, input_files, pq,
            version_bump=version_bump,
            # "force" flag means "force even if version conflict"; no
            # version conflicts if bumping version or only running out-of-date
            force=(version_bump is None and not update))
        if r.ableToRun or version_bump is not None or not update:
            runme.append(r)
        else:
            print("Up to date, not running.")
    return runme


if __name__ == "__main__":
    options = parse_args()
    inproc = options.process_id
    pq = dbprocessing.ProcessQueue(options.mission, dryrun=options.dryrun, echo=options.echo)
    runme = calc_runme(pq, options.startDate, options.endDate, inproc,
                       version_bump=options.force, update=options.update)
    runMe.runner(runme, pq.dbu, MAX_PROC=options.numproc,
                 rundir=None if options.ingest else '.')
    # Close database by removing all references
    del runme  # All runMe objects w/references to pq and its DButils
    del pq  # pq and reference to its DButils
