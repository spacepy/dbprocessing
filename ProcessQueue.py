#!/usr/bin/env python2.6

import getopt
import imp
import os
import os.path
import shutil
import subprocess
import sys
import tempfile
import traceback

import DBfile
import DBlogging
import DBqueue
import DBUtils2
import Diskfile


__version__ = '2.0.3'


class ProcessException(Exception):
    """Class for errors in ProcessQueue"""
    pass


class ForException(Exception):
    """Cheezy but separate excpetion for breaking out of a nested loop"""
    pass


class ProcessQueue(object):
    """
    Main code used to process the Queue, looks in incioming and builds all
    possible files

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 02-Dec-2010 (BAL)
    """
    def __init__(self,
                 mission):

        dbu = DBUtils2.DBUtils2(mission)
        dbu._openDB()
        dbu._createTableObjects()
        self.tempdir = None
        self.dbu = dbu
        self.childrenQueue = DBqueue.DBqueue()
        self.moved = DBqueue.DBqueue()
        self.depends = DBqueue.DBqueue()
        self.queue = DBqueue.DBqueue()
        self.findChildren = DBqueue.DBqueue()
        DBlogging.dblogger.info("Entering ProcessQueue")

#    def __del__(self):
#        """
#        attempt a bit of cleanup
#        """
#        self.rm_tempdir()

    def rm_tempdir(self):
        if self.tempdir != None:
            print 'rm_tempdir', self.tempdir
            shutil.rmtree(self.tempdir)
            self.tempdir = None
            DBlogging.dblogger.debug("Temp dir deleted: {0}".format(self.tempdir))

    def depDict(self, infile, file=None, code=None):
        """
        Dict to keep track of dependencies
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """

        DBlogging.dblogger.debug("Entered depDict:")

        # maybe this should be a class
        if not isinstance(file, (list, tuple)):
            file = [file]
        if not isinstance(code, (list, tuple)):
            code = [code]
        return {'infile':infile, 'code':code, 'file':file}


    def checkIncoming(self):
        """
        Goes out to incoming and grabs all files there adding them to self.queue

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entered checkIncoming:")

        self.queue.extendleft(self.dbu._checkIncoming())
        # step through and remove duplicates
        # if python 2.7 deque has a .count() otherwise have to use
        #  this workaropund
        for i in range(len(self.queue )):
            try:
                if list(self.queue).count(self.queue[i]) != 1:
                    self.queue.remove(self.queue[i])
            except IndexError:
                pass   # this means it was shortened
        DBlogging.dblogger.debug("Queue contains (%d): %s" % (len(self.queue),
                                                              self.queue))


    ## def doProcess(self):
    ##     DBlogging.dblogger.info("Entering doProcess()")
    ##     self.process()

    def moveToError(self, fname):
        """
        Moves a file from incoming to error

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entered moveToError:")

        path = self.dbu.getErrorPath()
        if os.path.isfile(os.path.join(path, os.path.basename(fname) ) ):
        #TODO do I realy want to remove old version:?
            os.remove( os.path.join(path, os.path.basename(fname) ) )
            DBlogging.dblogger.warning("removed {0}, as it was under a copy".format(os.path.join(path, os.path.basename(fname) )))
        shutil.move(fname, path + os.sep)
        DBlogging.dblogger.warning("**ERROR** {0} moved to {1}".format(fname, path))

    def moveToIncoming(self, fname):
        """
        Moves a file from location to incoming

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 18-Apr-2012 (BAL)
        """
        DBlogging.dblogger.debug("Entered moveToIncoming: {0}".format(fname))
        inc_path = self.dbu.getIncomingPath()
        if os.path.isfile(os.path.join(inc_path, os.path.basename(fname))):
        #TODO do I realy want to remove old version:?
            os.remove( os.path.join(inc_path, os.path.basename(fname)) )
        shutil.move(fname, inc_path + os.sep)

    def importFromIncoming(self):
        """
        Import a file from incoming into the database

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entering importFromIncoming, {0} to import".format(len(self.queue)))

        for val in self.queue.popleftiter() :
            self.current_file = val
            DBlogging.dblogger.debug("popped '%s' from the queue" % (self.current_file))
            df = self.figureProduct()
            if df is None:
                DBlogging.dblogger.info("Found no product moving to error, {0}".format(self.current_file))
                self.moveToError(self.current_file)
                continue

            if df.mission[0] == self.dbu.mission:
                # if the file is the wrong mission skip it
                dbf = DBfile.DBfile(df, self.dbu)
                try:
                    f_id = dbf.addFileToDB()
                    DBlogging.dblogger.info("File {0} entered in DB, f_id={1}".format(df.filename, f_id))
                except (DBUtils2.DBInputError, DBUtils2.DBError) as errmsg:
                    DBlogging.dblogger.warning("Except adding file to db so" + \
                                               " moving to error: %s" % (errmsg))
                    self.moveToError(val)
                    continue
                # move the file to the its correct home
                dbf.move()
                # add to processqueue for later processing
                self.dbu.processqueuePush(f_id)

            else:  # wrong mission for this processing
                DBlogging.dblogger.info("File is not the active mission ({1}), skipping: {0}".format(df.filename, df.mission))

    def figureProduct(self):
        """
        This funtion imports the inspectors and figures out whcih inspectors claim the file
        """
        act_insp = self.dbu.getActiveInspectors()
        claimed = []
        for code, arg in act_insp:
            try:
                inspect = imp.load_source('inspect', code)
            except IOError, msg:
                DBlogging.dblogger.error("Inspector: {0} not found: {1}".format(code, msg))
                continue
            if arg is not None:
                df = inspect.Inspector(self.current_file, self.dbu,  **arg)
            else:
                df = inspect.Inspector(self.current_file, self.dbu, )
            if df is not None:
                claimed.append(df)
                DBlogging.dblogger.debug("Match found: {0}: {1}".format(self.current_file, code, ))

        if len(claimed) == 0: # no match
            DBlogging.dblogger.info("File {0} found no inspector match".format(self.current_file))
            return None
        if len(claimed) > 1:
            DBlogging.dblogger.error("File {0} matched more than one product, there is a DB error".format(self.current_file))
            raise(DBUtils2.DBError("File {0} matched more than one product, there is a DB error".format(self.current_file)))

        return claimed[0]  # return the diskfile

    def buildChildren(self):
        """
        given the values in self.childrenQueue build them is possible

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        # TODO maybe this should be broken up
        DBlogging.dblogger.debug("Entered buildChildren:")
        for val in self.childrenQueue.popleftiter():
            # val is a dbfile, product id tuple
            DBlogging.dblogger.debug("popleft %s from self.childrenQueue" % \
                                     (str(val)))

            # check to see if there is a process defined on this output product
            proc_id = self.getProcessFromOutputProduct([val[1]])[0]
            if proc_id == []:  #TODO should this be a None?
                continue # there is no process defined


            # since we have a process do we have a code that does it?
            code_id = self.getCodeFromProcess([proc_id])
            try:
                code_id = code_id[0]
            except IndexError:
                continue  # The code_id was []
            # figure out the codes name and path
            codep = self.getCodePath([code_id])[0]

            sq1 = self.dbu.session.query(self.dbu.Product).filter_by(product_id = val[1])[0]
            product = sq1.product_id
            root_path = self.dbu.session.query(self.dbu.Mission.rootdir).filter_by(mission_name = self.dbu.mission)[0][0]  # should only have one value

            date = val[0].diskfile.params['utc_file_date']
            version = val[0].diskfile.params['version']
            self.tempdir = tempfile.mkdtemp('_dbprocessing')
            DBlogging.dblogger.debug("Created temp directory: {0}".format(self.tempdir))

            out_path = os.path.join(self.tempdir, val[0].diskfile.makeProductFilename(product, date, version))
            # now we have everything it takes to build the file

            arg_subs = {'datetime': date,
                        'BASEDIR': root_path,
                        'OUTPATH': out_path,
                        }
            # ####### get all the input_product_id and filenames
            #    make sure they all exist before we build the child.
            # from the process get all the input_product_id
            products = self.dbu._getInputProductID(proc_id)
            # query for the files that match the products for the right date
            # TODO this is another place that is one day to one day limited
            try:
                for pval in products:
                    sq1 = self.dbu.session.query(self.dbu.File).filter_by(product_id = pval).filter_by(utc_file_date = date)
                    if sq1.count() == 0:
                        DBlogging.dblogger.debug("Skipping file since " + \
                                                 "requirement not available" + \
                                                 "(sq1.count)")
                        raise(ForException())
                    DBlogging.dblogger.debug("<>Looking for product %d for date %s" % (pval, date))
                    # get an in_path for exe
                    in_path = self.dbu._getFileFullPath(val[0].diskfile.makeProductFilename(pval, date, version))
                    arg_subs['INPATH_{:d}'.format(pval)] = in_path
                    if in_path == None:
                        DBlogging.dblogger.debug("Skipping file since " + \
                                                 "requirement not available" + \
                                                 "(in_path)")
                        raise(ForException())
            except ForException:
                self.rm_tempdir()
                continue

            args = self.getCodeArgs([code_id])[0]
            # TODO fix this
            cmd = codep + ' ' + self.dbu.format(args, **arg_subs)
            cmd = cmd.split(' ')
            DBlogging.dblogger.debug('Executing: %s' % ' '.join(cmd))
            print cmd
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode != 0 and proc.returncode is not None:
                DBlogging.dblogger.error("Non-zero return code ({1}) from: {0}".format(cmd[0], proc.returncode))
            (stdoutdata, stderrdata) = proc.communicate()
            if stderrdata != '':
                DBlogging.dblogger.error("Code {0} stderr messages\n{1}".format(cmd[0], stderrdata))
            if stdoutdata != '':
                DBlogging.dblogger.debug("Code {0} stdout messages\n{1}".format(cmd[0], stdoutdata))


            self.moveToIncoming(out_path)
            1/0
            inc_path = self.dbu.getIncomingPath()
            self.importFile(os.path.join(inc_path, os.path.sbaename(out_path)))
            1/0
            # done with the temp file, clean it up
            self.rm_tempdir()
#            self.importFromIncoming()  # we added something it is time to import it


            # we have all the info needed to add the links
            # filefilelink is out_path in_path
            try:
                self.dbu._addFilefilelink(self.dbu._getFileID(os.path.basename(val[0].diskfile.filename)),
                                          self.dbu._getFileID(os.path.basename(out_path)) )
            except DBUtils2.DBError:
                DBlogging.dblogger.error("Could not create file_file_link due to error with created file: {0}".format(out_path))
            # TODO, think here if this is really ok to do
            try:
                self.dbu._addFilecodelink(self.dbu._getFileID(os.path.basename(out_path)),
                                          self.dbu._getCodeID(os.path.basename(codep)) )
            except DBUtils2.DBError:
                DBlogging.dblogger.error("Could not create file_code_link due to error with created file: {0}".format(out_path))

    def findChildrenProducts(self):
        """
        from the queue self.findChildren go though and find all the products of
        the children

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        # childern is a sub sub query
        # select * from product where product_id = (SELECT output_product from
        #   process where process_id
        #  = (select product_id from productprocesslink where product_id = 16));
        DBlogging.dblogger.debug( "Entered findChildrenProducts()" )

        for val in self.findChildren.popleftiter():
            DBlogging.dblogger.debug("popleftiter %s from self.findChildren" % \
                                     (str(val)))

            proc_ids = self.dbu.session.query(self.dbu.Productprocesslink.process_id).filter_by(input_product_id = val.diskfile.params['product_id'])

            for pval in proc_ids:
                sq2 = self.dbu.session.query(self.dbu.Process.output_product).filter_by(process_id = pval[0]).subquery()

                sq_ans = self.dbu.session.query(self.dbu.Product).filter_by(product_id = sq2)
                for val2 in sq_ans:
                    self.childrenQueue.append( (val, val2.product_id) )
                    DBlogging.dblogger.debug( "found products for children: %s: %s" % \
                                             (val, val2.product_id))


def processRunning(pid):
    """
    given a PID see if it is currently running

    @param pid: a pid
    @type pid: long

    @return: True if pid is running, False otherwise
    @rtype: bool

    @author: Brandon Craig Rhodes
    @organization: Stackoverflow
    http://stackoverflow.com/questions/568271/check-if-pid-is-not-in-use-in-python

    @version: V1: 02-Dec-2010 (BAL)
    """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


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
                pq = ProcessQueue(o[1])
    else:
        pq = ProcessQueue('Test')

    # check currently processing
    curr_proc = pq.dbu._currentlyProcessing()
    if curr_proc:  # returns False or the PID
        # check if the PID is running
        if processRunning(curr_proc):
            # we still have an instance processing, dont start another
            pq.dbu._closeDB()
            DBlogging.dblogger.error( "There is a process running, can't start another: PID: %d" % (curr_proc))
            raise(ProcessException("There is a process running, can't start another: PID: %d" % (curr_proc)))
        else:
            # There is a processing flag set but it died, dont start another
            pq.dbu._closeDB()
            DBlogging.dblogger.error( "There is a processing flag set but it died, dont start another" )
            raise(ProcessException("There is a processing flag set but it died, dont start another"))
    # start logging as a lock
    pq.dbu._startLogging()

    if '-i' in zip(*opts)[0]: # import selected
        try:
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

    if '-p' in zip(*opts)[0]: # process selected
        try:
            while len(pq.childrenQueue) != 0:
                pq.buildChildren()
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
