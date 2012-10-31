#!/usr/bin/env python2.6

from optparse import OptionParser
import traceback

from dbprocessing import DBlogging, dbprocessing
from dbprocessing.runMe import ProcessException
from dbprocessing import runMe

__version__ = '2.0.3'

def usage():
    """
    print the usage messag out
    """
    print "Usage: {0} [-i] [-p] [-m Test]".format("ProcessQueue")
    print "   -i -> import"
    print "   -p -> process"
    print "   -m -> selects mission"
    return

if __name__ == "__main__":
    usage = \
    """
    Usage: {0} [-i] [-p] [-m Test]".format("ProcessQueue")
        -i -> import
        -p -> process
        -m -> selects mission
    """
    parser = OptionParser(usage)
    parser.add_option("-i", "", dest="i", action="store_true",
                      help="ingest mode", default=False)
    parser.add_option("-p", "", dest="p", action="store_true",
                      help="process mode", default=False)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission", default=None)
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")

    if options.i and options.p:
        parser.error("options -i and -p are mutually exclusive")
    if not options.i and not options.p:
        parser.error("either -i or -p must be specified")

    pq = dbprocessing.ProcessQueue(options.mission)

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
            while len(pq.queue) != 0:
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
        try:
            DBlogging.dblogger.debug("pq.dbu.Processqueue.len(): {0}".format(pq.dbu.Processqueue.len()))
            # this loop does everything, both make the runMe objects and then
            #   do all the actuall running
            while pq.dbu.Processqueue.len() > 0:
                pq.dbu.Processqueue.clean()  # get rid of duplicates
                # this loop makes all the runMe objects for all the files in the processqueue
                while pq.dbu.Processqueue.len() > 0:
                    file_id = pq.dbu.Processqueue.get(version_bump=True)
                    DBlogging.dblogger.debug("popped {0} from pq.dbu.Processqueue.get()".format(file_id))
                    if file_id is None:
                        break
                    children = pq.dbu.getChildrenProducts(file_id[0]) # returns process
                    if not children:
                        DBlogging.dblogger.debug("No children found for {0}".format(file_id))
                        pq.dbu.Processqueue.pop() # done in two steps for crashes
                        continue
                    ## right here we have a list of processes that should run
                    # loop through the children and see which to build
                    for child_process in children:
                        ## are all the required inputs available? For the dates we are doing
                        pq.buildChildren(child_process, file_id)
                        pq.dbu.Processqueue.pop()
                # now do all the running
                # sort them so that we run the oldest date first, cuts down on reprocess
                pq.runme_list = sorted(pq.runme_list, key=lambda val: val.utc_file_date)
                print len(pq.runme_list), pq.runme_list
                for v in pq.runme_list:
                    ## TODO if one wanted to add smarts do it here, like running in parrallel
                    runMe.runner(v)

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
