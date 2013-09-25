#!/usr/bin/env python2.6

import datetime
import os
import operator
from optparse import OptionParser
import traceback
import subprocess

from dbprocessing import DBlogging, dbprocessing
from dbprocessing.runMe import ProcessException
from dbprocessing import runMe

__version__ = '2.0.3'


if __name__ == "__main__":
    usage = \
    """
    Usage: %prog [-i] [-p] [-d] [-m Test]
        -i -> import
        -p -> process
        -m -> selects mission
        -d -> dryrun
    """
    parser = OptionParser(usage=usage)
    parser.add_option("-i", "", dest="i", action="store_true",
                      help="ingest mode", default=False)
    parser.add_option("-p", "", dest="p", action="store_true",
                      help="process mode", default=False)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    parser.add_option("-d", "--dryrun", dest="dryrun", action="store_true",
                      help="only do a dryrun processing or ingesting", default=False)
    parser.add_option("-r", "--report", dest="report", action="store_true",
                      help="Make the html report", default=False)
    parser.add_option("-l", "--log-level", dest="loglevel",
                      help="Set the logging level", default="debug")

    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    if options.i and options.p:
        parser.error("options -i and -p are mutually exclusive")
    if not options.i and not options.p:
        parser.error("either -i or -p must be specified")

    if options.loglevel not in DBlogging.LEVELS:
        parser.error("invalid --log-level specified")

    DBlogging.dblogger.setLevel(DBlogging.LEVELS[options.loglevel])
        
    pq = dbprocessing.ProcessQueue(options.mission, dryrun=options.dryrun)

    # check currently processing
    curr_proc = pq.dbu._currentlyProcessing()
    if curr_proc:  # returns False or the PID
        # check if the PID is running
        if pq.dbu.processRunning(curr_proc):
            # we still have an instance processing, don't start another
            pq.dbu._closeDB()
            DBlogging.dblogger.error( "There is a process running, can't start another: PID: %d" % (curr_proc))
            raise(ProcessException("There is a process running, can't start another: PID: %d" % (curr_proc)))
        else:
            # There is a processing flag set but it died, don't start another
            pq.dbu._closeDB()
            DBlogging.dblogger.error( "There is a processing flag set but it died, don't start another" )
            raise(ProcessException("There is a processing flag set but it died, don't start another"))
    # start logging as a lock
    pq.dbu._startLogging()


    if options.i: # import selected
        try:
            start_len = pq.dbu.Processqueue.len()
            pq.checkIncoming()
            if not options.dryrun:
                while len(pq.queue) != 0:
                    pq.importFromIncoming()
            else:
                pq.importFromIncoming()

        except:
            #Generic top-level error handler, because otherwise people freak if
            #they see an exception thrown.
            print('Error in running processing chain; debugging details follow:')
            tbstring = traceback.format_exc()
            print tbstring
            print('This probably indicates a programming error. Please pass '
                  'this debugging\ninformation to the developer, along with '
                  'any information on what was\nhappening at the time.')
            DBlogging.dblogger.critical(tbstring)
            pq.dbu._stopLogging('Abnormal exit on exception')
        else:
            pq.dbu._stopLogging('Nominal Exit')
        pq.dbu._closeDB()
        print("Import finished: {0} files added".format(pq.dbu.Processqueue.len()-start_len))

    if options.p: # process selected
        number_proc = 0

        def do_proc(file_id):
            DBlogging.dblogger.debug("popped {0} from pq.dbu.Processqueue.get(), {1} left".format(file_id, pq.dbu.Processqueue.len()-1))
            children = pq.dbu.getChildrenProducts(file_id) # returns process
            if not children:
                DBlogging.dblogger.debug("No children found for {0}".format(file_id))
                return None
            ## right here we have a list of processes that should run
            # loop through the children and see which to build
            for child_process in children:
                ## are all the required inputs available? For the dates we are doing
                pq.buildChildren(child_process, [file_id])

        try:
            DBlogging.dblogger.debug("pq.dbu.Processqueue.len(): {0}".format(pq.dbu.Processqueue.len()))
            # this loop does everything, both make the runMe objects and then
            #   do all the actuall running
            while pq.dbu.Processqueue.len() > 0:
                # clean the queue every 10 precesses (and the first)
                if (number_proc % 10 ==0):
                    print('Cleaning Processqueue')
                    pq.dbu.Processqueue.clean(options.dryrun)  # get rid of duplicates
                # this loop makes all the runMe objects for all the files in the processqueue

                if not options.dryrun:
                    while pq.dbu.Processqueue.len() > 0:
                        print('Deciding what can run')
                        # instead of getAll() here lets pop 10 and run those in this batch
                        #  this should be more robust against network hiccups
                        queue_f = [pq.dbu.Processqueue.pop() for v in range(10)] # this is empty queue safe, gives None
                        # for f in pq.dbu.Processqueue.getAll():
                        for f in queue_f:
                            if f is None:
                                continue
                            do_proc(f)
                            number_proc += 1
                else:
                    print('')
                    for f in pq.dbu.Processqueue.getAll():
                        print('.'),
                        if f is None:
                            break
                        do_proc(f)
                # now do all the running

#==============================================================================
#                 this all moved to inside clean()
#                 # sort them so that we run the lowest level first, don't want to process in any other order
#                 pq.runme_list = sorted(pq.runme_list, key=lambda val: pq.dbu.getEntry('Product', pq.dbu.getEntry('Process', val.process_id).output_product).level)
#==============================================================================

                # lets sort the runme_list so that they process in order, kinda nice
                # level then date
                print('Sorting runMe list')
                # we might was well go through the runme_list and get rid of all processes
                #   that cannot run by checking the ableToRun attribitute
                pq.runme_list = [v for v in pq.runme_list if v.ableToRun]
                try:
                    pq.runme_list = sorted(pq.runme_list, key=lambda x: (x.data_level, x.utc_file_date))
                except AttributeError: # this seems unneeded, maybe a DB error caused this...
                    pq.runme_list = sorted(pq.runme_list, key=lambda x: (x.utc_file_date))
                run_num = 0
                print('Running processes')
                # pass the whole runme list off to the runMe module function
                #  it will go through and decide what can be run in parrallel
                if options.dryrun:
                    print('<dryrun> Process: {0} Date: {1} Outname: {2} '\
                          .format(v.process_id, v.utc_file_date, v.filename))
                else:
                    n_good, n_bad = runMe.runner(pq.runme_list)
                    print("{0} of {1} processes were successful".format(n_good, n_bad+n_good))
                    DBlogging.dblogger.info("{0} of {1} processes were successful".format(n_good, n_good+n_bad))

        except:
            #Generic top-level error handler, because otherwise people freak if
            #they see an exception thrown.
            print('Error in running processing chain; debugging details follow:')
            tbstring = traceback.format_exc()
            print tbstring
            print('This probably indicates a programming error. Please pass '
                  'this debugging\ninformation to the developer, along with '
                  'any information on what was\nhappening at the time.')
            DBlogging.dblogger.critical(tbstring)
            pq.dbu._stopLogging('Abnormal exit on exception')
        else:
            pq.dbu._stopLogging('Nominal Exit')
        pq.dbu._closeDB()

