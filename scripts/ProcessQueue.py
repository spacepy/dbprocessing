#!/usr/bin/env python
from __future__ import print_function

import argparse
import datetime
import os
import operator
import traceback
import subprocess

from dbprocessing import DBlogging, dbprocessing
from dbprocessing.runMe import ProcessException
from dbprocessing import runMe, Utils
from dbprocessing.Utils import dateForPrinting as DFP


from dbprocessing import __version__


if __name__ == "__main__":
    usage = \
    """
    Usage: %prog [-i|-p [-d] [-s] [-o process[,process...]]] -m database
        -i -> import
        -p -> process
        -m -> selects mission
        -d -> dryrun
        -s -> skip run timebase
        -o -> only run listed processes
    """
    parser = argparse.ArgumentParser()
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument("-i", action="store_true",
                              help="ingest mode", default=False)
    action_group.add_argument("-p", action="store_true",
                              help="process mode", default=False)
    parser.add_argument("-s", dest="s", action="store_true",
                        help="Skip run timebase processes", default=False)
    parser.add_argument("-o", "--only", dest="o", type=str,
                        help='Run only listed processes (either id or name)', default = None)
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database", default=None)
    parser.add_argument("-d", "--dryrun", action="store_true",
                        help="only do a dryrun processing or ingesting", default=False)
    parser.add_argument("-r", "--report", action="store_true",
                        help="Make the html report", default=False)
    parser.add_argument("-l", "--log-level", dest="loglevel",
                        help="Set the logging level", default="debug")
    parser.add_argument("-n", "--num-proc", dest="numproc", type=int,
                        help="Number of processes to run in parallel", default=2)
    parser.add_argument("--echo", action="store_true",
                        help="Start sqlalchemy with echo in place for debugging", default=False)
    parser.add_argument("--glb", dest="glob", type=str,
                        help='Glob to use when reading files from incoming: default "*"', default="*")

    options = parser.parse_args()

    if options.s and not options.p:
        parser.error('-s requires -p')
    if options.o and not options.p:
        parser.error('-o requires -p')

    logname = os.path.basename(options.mission).replace('.', '_')
    DBlogging.change_logfile(logname)

    if options.loglevel not in DBlogging.LEVELS:
        parser.error("invalid --log-level specified")

    DBlogging.dblogger.setLevel(DBlogging.LEVELS[options.loglevel])

    pq = dbprocessing.ProcessQueue(options.mission, dryrun=options.dryrun, echo=options.echo)

    # check currently processing
    curr_proc = pq.dbu.currentlyProcessing()
    if curr_proc:  # returns False or the PID
        # check if the PID is running
        if Utils.processRunning(curr_proc):
            # we still have an instance processing, don't start another
            pq.dbu.closeDB()
            DBlogging.dblogger.error( "There is a process running, can't start another: PID: %d" % (curr_proc))
            raise ProcessException("There is a process running, can't start another: PID: %d" % (curr_proc))
        else:
            # There is a processing flag set but it died, don't start another
            pq.dbu.closeDB()
            DBlogging.dblogger.error( "There is a processing flag set but it died, don't start another" )
            raise ProcessException("There is a processing flag set but it died, don't start another")
    # start logging as a lock
    pq.dbu.startLogging()


    if options.i: # import selected
        try:
            start_len = pq.dbu.ProcessqueueLen()
            print("{0} Currently {1} entries in process queue".format(DFP(), start_len))
            pq.checkIncoming(glb=options.glob) 
            if not options.dryrun:
                while len(pq.queue) != 0:
                    pq.importFromIncoming()
            else:
                pq.importFromIncoming()

        except RuntimeError:
            #Generic top-level error handler, because otherwise people freak if
            #they see an exception thrown.
            print('{0} Error in running processing chain; debugging details follow:'.format(DFP()))
            tbstring = traceback.format_exc()
            print(tbstring)
            print('This probably indicates a programming error. Please pass '
                  'this debugging\ninformation to the developer, along with '
                  'any information on what was\nhappening at the time.')
            DBlogging.dblogger.critical(tbstring)
            pq.dbu.stopLogging('Abnormal exit on exception')
        except KeyboardInterrupt:
            print('Shutting down processing chain')
            DBlogging.dblogger.error('Ctrl-C issued, quiting')
            pq.dbu.stopLogging('Ctrl-C Exit')

        else:
            pq.dbu.stopLogging('Nominal Exit')
        pq.dbu.closeDB()
        print("{0} Import finished: {1} files added".format(DFP(), pq.dbu.ProcessqueueLen()-start_len))

    if options.p: # process selected
        number_proc = 0

        try:
            DBlogging.dblogger.debug("pq.dbu.ProcessqueueLen(): {0}".format(pq.dbu.ProcessqueueLen()))
            # this loop does everything, both make the runMe objects and then
            #   do all the actuall running
            while pq.dbu.ProcessqueueLen() > 0:
                # BAL 30 Mar 2017, no need to clean here as buildChildren() will clean
                # print('{0} Cleaning Processes queue'.format(DFP()))
                # # clean the queue
                # pq.dbu.ProcessqueueClean(options.dryrun)  # get rid of duplicates and sort
                # if not pq.dbu.ProcessqueueLen():
                #     print("{0} Process queue is empty".format(DFP()))
                #     break

                # this loop makes all the runMe objects for all the files in the processqueue
                run_num = 0
                n_good  = 0
                n_bad   = 0
                
                print('{0} Building commands for {1} items in the queue'.format(DFP(), pq.dbu.ProcessqueueLen()))
         
                # make the cpommand lines for all the files in tehj processqueue
                totalsize = pq.dbu.ProcessqueueLen()
                tmp_ind = 0
                Utils.progressbar(tmp_ind, 1, totalsize, text='Command Build Progress:')
                while pq.dbu.ProcessqueueLen() > 0:
                    # do smarter pop that sorts at the db level
                    #f = (pq.dbu.session.query(pq.dbu.Processqueue.file_id)
                    #     .join((pq.dbu.File, pq.dbu.Processqueue.file_id==pq.dbu.File.file_id))
                    #     .order_by(pq.dbu.File.data_level, pq.dbu.File.utc_file_date).first())
                    #if hasattr(f, '__iter__') and len(f) == 1:
                    #    f = f[0]
                    #pq.dbu.ProcessqueueRemove(f) # remove by file_id
                    f = pq.dbu.ProcessqueuePop()
                    DBlogging.dblogger.debug("popped {0} from pq.dbu.ProcessqueueGet(), {1} left".format(f, pq.dbu.ProcessqueueLen()))
                    #                    f = pq.dbu.ProcessqueuePop() # this is empty queue safe, gives None
                    #if f is None:
                    #    continue
                    pq.buildChildren(f, skip_run=options.s,
                                     run_procs=options.o)
                    tmp_ind += 1
                    Utils.progressbar(tmp_ind, 1, totalsize, text='Command Build Progress: {0}:{1}'.format(tmp_ind, totalsize))

                    #pq.runme_list.extend(sorted([v for v in pq.runme_list if v.ableToRun], key=lambda x: x.utc_file_date))
                    
                # pass the whole runme list off to the runMe module function
                #  it will go through and decide what can be run in parrallel
                
                n_good_t, n_bad_t = runMe.runner(pq.runme_list, pq.dbu, options.numproc)
                n_good += n_good_t
                n_bad  += n_bad_t
                print("{0} {1} of {2} processes were successful".format(DFP(), n_good, n_bad+n_good))
                DBlogging.dblogger.info("{0} of {1} processes were successful".format(n_good, n_good+n_bad))

        except RuntimeError:
            #Generic top-level error handler, because otherwise people freak if
            #they see an exception thrown.
            print('{0} Error in running processing chain; debugging details follow:'.format(DFP()))
            tbstring = traceback.format_exc()
            print(tbstring)
            print('This probably indicates a programming error. Please pass '
                  'this debugging\ninformation to the developer, along with '
                  'any information on what was\nhappening at the time.')
            DBlogging.dblogger.critical(tbstring)
            pq.dbu.stopLogging('Abnormal exit on exception')
        except KeyboardInterrupt:
            print('Shutting down processing chain')
            DBlogging.dblogger.error('Ctrl-C issued, quiting')
            pq.dbu.stopLogging('Ctrl-C Exit')
        else:
            pq.dbu.stopLogging('Nominal Exit')
        finally: 
            pq.dbu.closeDB()
        del pq
