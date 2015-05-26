#!/usr/bin/env python2.6

import datetime
import os
import operator
from optparse import OptionParser
import traceback
import subprocess

import spacepy.toolbox as tb

from dbprocessing import DBlogging, dbprocessing
from dbprocessing.runMe import ProcessException
from dbprocessing import runMe, Utils

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
    parser.add_option("-n", "--num-proc", dest="numproc", type='int',
                      help="Number of processes to run in parallel", default=2)
    parser.add_option("", "--echo", dest="echo", action="store_true",
                      help="Start sqlalchemy with echo in place for debugging", default=False)
    parser.add_option("", "--glb", dest="glob", type="string",
                      help='Glob to use when reading files from incoming: defualt "*"', default="*")


    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    if options.i and options.p:
        parser.error("options -i and -p are mutually exclusive")
    if not options.i and not options.p:
        parser.error("either -i or -p must be specified")

    logname = os.path.basename(options.mission).replace('.', '_')
    DBlogging.change_logfile(logname)

    if options.loglevel not in DBlogging.LEVELS:
        parser.error("invalid --log-level specified")

    DBlogging.dblogger.setLevel(DBlogging.LEVELS[options.loglevel])

    pq = dbprocessing.ProcessQueue(options.mission, dryrun=options.dryrun, echo=options.echo)

    # check currently processing
    curr_proc = pq.dbu._currentlyProcessing()
    if curr_proc:  # returns False or the PID
        # check if the PID is running
        if Utils.processRunning(curr_proc):
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
            print("Currently {0} entries in process queue".format(start_len))
            pq.checkIncoming(glb=options.glob) 
            if not options.dryrun:
                while len(pq.queue) != 0:
                    pq.importFromIncoming()
            else:
                pq.importFromIncoming()

        except RuntimeError:
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

        try:
            DBlogging.dblogger.debug("pq.dbu.Processqueue.len(): {0}".format(pq.dbu.Processqueue.len()))
            # this loop does everything, both make the runMe objects and then
            #   do all the actuall running
            while pq.dbu.Processqueue.len() > 0:
                print('Cleaning Processes queue')
                # clean the queue
                pq.dbu.Processqueue.clean(options.dryrun)  # get rid of duplicates and sort
                if not pq.dbu.Processqueue.len():
                    print("Process queue is empty")
                    break
                # this loop makes all the runMe objects for all the files in the processqueue

                run_num = 0
                n_good  = 0
                n_bad   = 0
                
                print('Building commands for {0} items in the queue'.format(pq.dbu.Processqueue.len()))
         
                # make the cpommand lines for all the files in tehj processqueue
                totalsize = pq.dbu.Processqueue.len()
                tmp_ind = 0
                tb.progressbar(tmp_ind, 1, totalsize, text='Command Build Progress:')
                while pq.dbu.Processqueue.len() > 0:
                    # do smarter pop that sorts at the db level
                    #f = (pq.dbu.session.query(pq.dbu.Processqueue.file_id)
                    #     .join((pq.dbu.File, pq.dbu.Processqueue.file_id==pq.dbu.File.file_id))
                    #     .order_by(pq.dbu.File.data_level, pq.dbu.File.utc_file_date).first())
                    #if hasattr(f, '__iter__') and len(f) == 1:
                    #    f = f[0]
                    #pq.dbu.Processqueue.remove(f) # remove by file_id
                    f = pq.dbu.Processqueue.pop()
                    DBlogging.dblogger.debug("popped {0} from pq.dbu.Processqueue.get(), {1} left".format(f, pq.dbu.Processqueue.len()))
                    #                    f = pq.dbu.Processqueue.pop() # this is empty queue safe, gives None
                    #if f is None:
                    #    continue
                    pq.buildChildren(f)
                    tmp_ind += 1
                    tb.progressbar(tmp_ind, 1, totalsize, text='Command Build Progress:')

                    #pq.runme_list.extend(sorted([v for v in pq.runme_list if v.ableToRun], key=lambda x: x.utc_file_date))
                    
                # pass the whole runme list off to the runMe module function
                #  it will go through and decide what can be run in parrallel
                
                n_good_t, n_bad_t = runMe.runner(pq.runme_list, pq.dbu, options.numproc)
                n_good += n_good_t
                n_bad  += n_bad_t
                print("{0} of {1} processes were successful".format(n_good, n_bad+n_good))
                DBlogging.dblogger.info("{0} of {1} processes were successful".format(n_good, n_good+n_bad))

        except RuntimeError:
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
        finally: 
            pq.dbu._closeDB()
        del pq
