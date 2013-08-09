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
            if file_id is None:
                return 'break'
            children = pq.dbu.getChildrenProducts(file_id) # returns process
            if not children:
                DBlogging.dblogger.debug("No children found for {0}".format(file_id))
                if not options.dryrun:
                    pq.dbu.Processqueue.pop() # done in two steps for crashes
                return None
            ## right here we have a list of processes that should run
            # loop through the children and see which to build
            for child_process in children:
                ## are all the required inputs available? For the dates we are doing
                pq.buildChildren(child_process, [file_id])
                if not options.dryrun:
                        pq.dbu.Processqueue.pop()
        try:
            DBlogging.dblogger.debug("pq.dbu.Processqueue.len(): {0}".format(pq.dbu.Processqueue.len()))
            # this loop does everything, both make the runMe objects and then
            #   do all the actuall running
            while pq.dbu.Processqueue.len() > 0:
                # clean the queue every 10 precesses (and the first)
                if (number_proc % 10 ==0):
                    pq.dbu.Processqueue.clean(options.dryrun)  # get rid of duplicates
                # this loop makes all the runMe objects for all the files in the processqueue

                if not options.dryrun:
                    while pq.dbu.Processqueue.len() > 0:
                        for f in pq.dbu.Processqueue.getAll():
                            retval = do_proc(f)
                            if retval == 'break':
                                break
                            else:
                                number_proc += 1
                else:
                    print('')
                    for f in pq.dbu.Processqueue.getAll():
                        print('.'),
                        retval = do_proc(f)
                        if retval == 'break':
                            break
                # now do all the running

#==============================================================================
#                 this all moved to inside clean()
#                 # sort them so that we run the lowest level first, don't want to process in any other order
#                 pq.runme_list = sorted(pq.runme_list, key=lambda val: pq.dbu.getEntry('Product', pq.dbu.getEntry('Process', val.process_id).output_product).level)
#==============================================================================

                # lets sort the runme_list so that they process in order, kinda nice
                # level then date
                
                pq.runme_list = sorted(pq.runme_list, key=lambda x: (x.data_level, x.utc_file_date))
                print len(pq.runme_list), pq.runme_list
                run_num = 0
                while pq.runme_list:
                    run_num += 1
                    v = pq.runme_list.pop(0)
                    ## TODO if one wanted to add smarts do it here, like running in parrallel
                    DBlogging.dblogger.info("Running file {0} there are {1} left".format(run_num, len(pq.runme_list)))
                    if not options.dryrun:
                        runMe.runner(v)
                    else:
                        print('<dryrun> Process: {0} Date: {1} Outname: {2} '\
                            .format(v.process_id, v.utc_file_date, v.filename))
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

        ## at the end of processing create a weekly report
        ## do this for the last 7 days if we did anything

        if number_proc > 0 and not options.dryrun and options.report:
            if os.path.isfile(pq.mission):
                miss_name = os.path.splitext(os.path.basename(pq.mission))[0]
            else:
                miss_name = pq.mission
            today = datetime.datetime.utcnow().date()
            outname = os.path.expanduser(os.path.join('~', 'dbprocessing_logs', 'SOCreport_{0}_{1}.html'.format(today.isoformat(), miss_name)))
            command_line = ['nice', '-n 2',
                '/u/ectsoc/dbUtils/weeklyReport.py',
                os.path.expanduser(os.path.join('~', 'dbprocessing_logs')),
                today.strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'), outname]
            subprocess.check_call(command_line)

