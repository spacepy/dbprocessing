#!/usr/bin/env python2.6

import datetime
import getopt
import imp
import itertools
import os
import os.path
import shutil
import subprocess
import sys
import tempfile
import traceback

import numpy as np

import DBfile
import DBlogging
import DBqueue
import DBUtils2
import Version

try: # new version changed this annoyingly
    from sqlalchemy.exceptions import IntegrityError
except ImportError:
    from sqlalchemy.exc import IntegrityError

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

        self.mission = mission
        dbu = DBUtils2.DBUtils2(self.mission)
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

    def __del__(self):
        """
        attempt a bit of cleanup
        """

    def rm_tempdir(self):
        if self.tempdir != None:
            name = self.tempdir
            shutil.rmtree(self.tempdir)
            self.tempdir = None
            DBlogging.dblogger.debug("Temp dir deleted: {0}".format(name))

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
        if path[-1] != os.sep:
            path = path+os.sep
        shutil.move(fname, path)
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
        """
        DBlogging.dblogger.debug("Entering importFromIncoming, {0} to import".format(len(self.queue)))

        for val in self.queue.popleftiter() :
            self.current_file = val
            DBlogging.dblogger.debug("popped '{0}' from the queue: {1} left".format(self.current_file, len(self.queue)))
            df = self.figureProduct()
            if df is None:
                DBlogging.dblogger.info("Found no product moving to error, {0}".format(self.current_file))
                self.moveToError(self.current_file)
                continue

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
            # set files in the db of the same product and same utc_file_date to not be newest version
            files = self.dbu.getFiles_product_utc_file_date(dbf.diskfile.params['product_id'], dbf.diskfile.params['utc_file_date'])
            print "dbf.diskfile.params['product_id']", dbf.diskfile.params['product_id']
            print "dbf.diskfile.params['utc_file_date']", dbf.diskfile.params['utc_file_date']
            print 'files', files
            print zip(*files)[1]
            mx = max(zip(*files)[1])
            for f in files:
                print "f[1] != mx", f[1], mx, f[1] != mx
                if f[1] != mx: # this is not the max, newest_version should be False
                    self.dbu.session.query(self.dbu.File).filter_by(file_id = f[0]).update({self.dbu.File.newest_version: False})
                    DBlogging.dblogger.debug("set {0}.newest_version=False".format(f[0]))
            try:
                self.dbu.session.commit()
            except IntegrityError as IE:
                self.session.rollback()
                raise(DBUtils2.DBError(IE))
            # add to processqueue for later processing
            self.dbu.processqueuePush(f_id)

    def _strargs_to_args(self, strargs):
        """
        read in the arguments string forn te db and change to a dict
        """
        kwargs = {}
        for val in strargs.split():
            tmp = val.split('=')
            kwargs[tmp[0]] = tmp[1]
        return kwargs

    def figureProduct(self):
        """
        This funtion imports the inspectors and figures out whcih inspectors claim the file
        """
        act_insp = self.dbu.getActiveInspectors()
        claimed = []
        for code, arg, product in act_insp:
            try:
                inspect = imp.load_source('inspect', code)
            except IOError, msg:
                DBlogging.dblogger.error("Inspector: {0} not found: {1}".format(code, msg))
                continue
            if arg is not None:
                kwargs = self._strargs_to_args(arg)
                df = inspect.Inspector(self.current_file, self.dbu, product, **kwargs)
            else:
                df = inspect.Inspector(self.current_file, self.dbu, product, )
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

    def buildChildren(self, process_id, file_id):
        """
        go through and run the process if possible
        """
        DBlogging.dblogger.debug("Entered buildChildren: process_id={0}".format(process_id))

        ## get all the input products for that process, and if they are optional
        input_product_id = self.dbu.getInputProductID(process_id) # this is a list

        utc_file_date = self.dbu.getFileUTCfileDate(file_id)
        DBlogging.dblogger.debug("Finding input files for {0}".format(utc_file_date))

        ## here decide how we build putput and do it.
        timebase = self.dbu.getProcessTimebase(process_id)
        if timebase == 'FILE': # taking one file to te next file
            DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
            files = []
            for val, opt in input_product_id:
                tmp = self.dbu.getFiles_product_utc_file_date(val, utc_file_date)
                if tmp != []:
                    files.extend(tmp)
            DBlogging.dblogger.debug("buildChildren files: ".format(str(files)))
            files = self.dbu.file_id_Clean(files)

        elif timebase == 'DAILY':
            DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
            raise(NotImplemented('Not implented yet (1001)'))
            ## from the input file see what the timebase is and grab all files that go into process

            ########## this is giving bad answers!!!  getFiles_product_utc_file_daterange
            files = [self.dbu.getFiles_product_utc_file_daterange(val,
                                                                   [datetime.datetime(utc_file_date.year, utc_file_date.month, utc_file_date.day),
                                                                   datetime.datetime(utc_file_date.year, utc_file_date.month, utc_file_date.day+1) -
                                                                   datetime.timedelta(microseconds=1)]) for val, opt in input_product_id ]

        else:
            DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
            raise(ValueError('Bad timebase for product: {0}'.format(process_id)))

#==============================================================================
# do we have the required files to do the build?
#==============================================================================
        # get the products of the input files
        ## need to go through the input_product_id and make sure we have a file for each required product
        for prod, opt in input_product_id:
            if not opt:
                if not prod in zip(*files)[2]: # the product ID
                    DBlogging.dblogger.info("Required products not found, continuing.  Process:{0}, product{1}".format(process_id, prod))
                    return None

        input_files = zip(*files)[0]
        print "input_files", input_files

#==============================================================================
# setup and do the processing
#==============================================================================

        # since we have a process do we have a code that does it?
        code_id = self.dbu.getCodeFromProcess(process_id)
        print "code_id", code_id

        # figure out the code path so that it can be called
        codepath = self.dbu.getCodePath(code_id)
        print "codepath", codepath
        DBlogging.dblogger.debug("Going to run code: {0} located at {1}".format(code_id, codepath))



        self.tempdir = tempfile.mkdtemp('_dbprocessing')
        DBlogging.dblogger.debug("Created temp directory: {0}".format(self.tempdir))


        1/0

        date=val[0].diskfile.params['utc_file_date']
        version = val[0].diskfile.params['version']

        out_path = os.path.join(self.tempdir, val[0].diskfile.makeProductFilename(product, date, version))
        # now we have everything it takes to build the file
        print "out_path",out_path
        1/0

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

    def queueClean(self):
        """
        go through the process queue and clear out lower versions of the same files
        this is determined by product and utc_file_date
        """
        # TODO this might break with weekly input files
        DBlogging.dblogger.debug("Entering in queueClean(), there are {0} entries".format(self.dbu.processqueueLen()))
        try:
            pqdata = self.dbu.processqueueGetAll()
        except DBUtils2.DBError:
            return None
        # build up a list of tuples file_id, product_id, utc_file_date, version
        data = []
        for val in pqdata:
            file_id = val
            sq = self.dbu.session.query(self.dbu.File).filter_by(file_id = val)
            try:
                product_id = sq[0].product_id
                utc_file_date = sq[0].utc_file_date
                version = Version.Version(sq[0].interface_version, sq[0].quality_version, sq[0].revision_version)
                data.append( (file_id, product_id, utc_file_date, version) )
            except IndexError: # None return, maybe off the end
                pass
        ## think here on better, but a dict makes for easy del
        data2 = {}
        for ii, val in enumerate(data):
            data2[ii] = val
        for k1, k2 in itertools.product(range(len(data2)), range(len(data2))):
            if k1 == k2:
                continue
            try:
                if data2[k1][1] == data2[k2][1] and data2[k1][2] == data2[k2][2]: # same product an date
                    # drop the one with the lower version
                    if data2[k1][3] > data2[k2][3]:
                        del data2[k2]
                        DBlogging.dblogger.info("Removed {0} from the process queue".format(data2[k2]))
                    else:
                        del data2[k1]
                        DBlogging.dblogger.info("Removed {0} from the process queue".format(data2[k1]))
            except KeyError: # we deleted one of them
                continue
        ## now we have a dict of just the unique files
        self.dbu.processqueueFlush()
        for key in data2:
            self.dbu.processqueuePush(data2[key][0]) # the file_id goes back on
        DBlogging.dblogger.debug("Done in queueClean(), there are {0} entries left".format(self.dbu.processqueueLen()))


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
        pq.queueClean()  # get rid of duplicates
        try:
            DBlogging.dblogger.debug("pq.dbu.processqueueLen(): {0}".format(pq.dbu.processqueueLen()))
            while pq.dbu.processqueueLen() > 0:
                print 'processqueueLen', pq.dbu.processqueueLen()
                file_id = pq.dbu.processqueueGet()
                DBlogging.dblogger.debug("popped {0} from pq.dbu.processqueueGet()".format(file_id))
                print 'file_id', file_id
                if file_id is None:
                    break
                children = pq.dbu.getChildrenProducts(file_id) # returns process
                if not children:
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
