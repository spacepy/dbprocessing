#!/usr/bin/env python
from __future__ import division

import os
from optparse import OptionParser

from dbprocessing import DButils, Utils, DBlogging
from dbprocessing.runMe import ProcessException

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

                
    dbu = DButils.DButils(options.mission, echo=options.echo)

    # If we will be editing the DB we have to have lock
    if options.fix:
        # check currently processing
        curr_proc = dbu.currentlyProcessing()
        if curr_proc:  # returns False or the PID
            # check if the PID is running
            dbu.closeDB()
            if Utils.processRunning(curr_proc):
                # we still have an instance processing, don't start another
                raise(ProcessException("There is a process running, can't start another: PID: %d" % (curr_proc)))
            else:
                # There is a processing flag set but it died, don't start another
                raise(ProcessException("There is a processing flag set but it died, don't start another"))

    result = dbu.checkFiles()

    for res in result:
        if res[1] == 2:
            print("{0} not found on disk".format(res[0]))
            if options.fix:
                dbu._purgeFileFromDB(res[0])
                print("{0} removed from DB".format(res[0]))
    
    dbu.closeDB()