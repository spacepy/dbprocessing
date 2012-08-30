#!/usr/bin/env python2.6

import getopt
import sys
import traceback

from dbprocessing import DBlogging, dbprocessing
from dbprocessing.dbprocessing import ProcessException
from DBUtils import processRunning

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
    # TODO decide if we really want to run this way, works for now
#    s = '--mission=Test --import --process'
#    args = s.split()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "pim:")
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    try:
        if not '-i' in zip(*opts)[0] and not '-p' in zip(*opts)[0]:
            usage()
            sys.exit(2)
    except IndexError:
        usage()
        sys.exit(2)

    if '-m' in zip(*opts)[0]:
        for o in opts:
            if o[0] == '-m':
                pq = dbprocessing.ProcessQueue(o[1])
    else:
        pq = dbprocessing.ProcessQueue('Test')

    # check currently processing
    curr_proc = pq.dbu._currentlyProcessing()
    if curr_proc:  # returns False or the PID
        # check if the PID is running
        if processRunning(curr_proc):
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

    if '-i' in zip(*opts)[0]: # import selected
        try:
            start_len = pq.dbu.processqueueLen()
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
        print("Import finished: {0} files added".format(pq.dbu.processqueueLen()-start_len))

    if '-p' in zip(*opts)[0]: # process selected
        try:
            DBlogging.dblogger.debug("pq.dbu.processqueueLen(): {0}".format(pq.dbu.processqueueLen()))
            while pq.dbu.processqueueLen() > 0:
                pq.dbu.processqueueClean()  # get rid of duplicates
                file_id = pq.dbu.processqueueGet()
                DBlogging.dblogger.debug("popped {0} from pq.dbu.processqueueGet()".format(file_id))
                if file_id is None:
                    break
                children = pq.dbu.getChildrenProducts(file_id) # returns process
                if not children:
                    DBlogging.dblogger.debug("No children found for {0}".format(file_id))
                    pq.dbu.processqueuePop() # done in two steps for crashes
                    continue
                ## right here we have a list of processes that should run
                # loop through the children and see which to build
                for child_process in children:
                    ## are all the required inputs available? For the dates we are doing
                    pq.buildChildren(child_process, file_id)
                    pq.dbu.processqueuePop()

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
