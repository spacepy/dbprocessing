#!/usr/bin/env python2.6

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

from dbprocessing import DBfile
from dbprocessing import DBlogging
from dbprocessing import DBStrings
from dbprocessing import DBqueue
from dbprocessing import DBUtils2
from dbprocessing import Version

try: # new version changed this annoyingly
    from sqlalchemy.exceptions import IntegrityError
except ImportError:
    from sqlalchemy.exc import IntegrityError

__version__ = '2.0.3'


class ProcessException(Exception):
    """Class for errors in ProcessQueue"""
    pass


class ForException(Exception):
    """Cheezy but separate exception for breaking out of a nested loop"""
    pass


class ProcessQueue(object):
    """
    Main code used to process the Queue, looks in incoming and builds all
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
        self.current_file = None
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
        #TODO do I really want to remove old version:?
            os.remove( os.path.join(path, os.path.basename(fname) ) )
            DBlogging.dblogger.warning("removed {0}, as it was under a copy".format(os.path.join(path, os.path.basename(fname) )))
        if path[-1] != os.sep:
            path = path+os.sep
        try:
            shutil.move(fname, path)
        except IOError:
            DBlogging.dblogger.warning("file {0} was not successfully moved to error".format(os.path.join(path, os.path.basename(fname) )))
        else:
            DBlogging.dblogger.warning("**ERROR** {0} moved to {1}".format(fname, path))

    def moveToIncoming(self, fname):
        """
        Moves a file from location to incoming

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 18-Apr-2012 (BAL)
        """
        inc_path = self.dbu.getIncomingPath()
        DBlogging.dblogger.debug("Entered moveToIncoming: {0} {1}".format(fname, inc_path))
        if os.path.isfile(os.path.join(inc_path, os.path.basename(fname))):
        #TODO do I really want to remove old version:?
            os.remove( os.path.join(inc_path, os.path.basename(fname)) )
        shutil.move(fname, inc_path + os.sep)

    def diskfileToDB(self, df):
        """
        given a diskfile go through and do all the steps to add it into the db
        """
        if df is None:
            DBlogging.dblogger.info("Found no product moving to error, {0}".format(self.current_file))
            self.moveToError(self.current_file)
            return None

        # if the file is the wrong mission skip it
        dbf = DBfile.DBfile(df, self.dbu)
        try:
            f_id = dbf.addFileToDB()
            DBlogging.dblogger.info("File {0} entered in DB, f_id={1}".format(df.filename, f_id))
        except (ValueError, DBUtils2.DBError) as errmsg:
            DBlogging.dblogger.warning("Except adding file to db so" + \
                                       " moving to error: %s" % (errmsg))
            self.moveToError(os.path.join(df.path, df.filename))
            return None

        # move the file to the its correct home
        dbf.move()
        # set files in the db of the same product and same utc_file_date to not be newest version
        files = self.dbu.getFiles_product_utc_file_date(dbf.diskfile.params['product_id'], dbf.diskfile.params['utc_file_date'])
        mx = max(zip(*files)[1])
        for f in files:
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
        return f_id

    def importFromIncoming(self):
        """
        Import a file from incoming into the database
        """
        DBlogging.dblogger.debug("Entering importFromIncoming, {0} to import".format(len(self.queue)))

        for val in self.queue.popleftiter() :
            self.current_file = val
            DBlogging.dblogger.debug("popped '{0}' from the queue: {1} left".format(self.current_file, len(self.queue)))
            df = self.figureProduct()
            self.diskfileToDB(df)

    def _strargs_to_args(self, strargs):
        """
        read in the arguments string forn the db and change to a dict
        """
        kwargs = {}
        if isinstance(strargs, (list, tuple)): # we have multiple to deal with
            for val in strargs:
                tmp = self._strargs_to_args(val)
                for key in tmp:
                    kwargs[key] = tmp[key]
            return kwargs
        try:
            for val in strargs.split():
                tmp = val.split('=')
                kwargs[tmp[0]] = tmp[1]
        except AttributeError: # it was None
            pass
        return kwargs

    def figureProduct(self):
        """
        This function imports the inspectors and figures out which inspectors claim the file
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
                break # lets call it done after we find one

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

        daterange = self.dbu.getFileDaterange(file_id)
        dates = self.dbu.daterange_to_dates(daterange)

        for utc_file_date in dates:

            ## get all the input products for that process, and if they are optional
            input_product_id = self.dbu.getInputProductID(process_id) # this is a list

            DBlogging.dblogger.debug("Finding input files for {0}".format(utc_file_date))

            ## here decide how we build output and do it.
            timebase = self.dbu.getProcessTimebase(process_id)
            if timebase == 'FILE': # taking one file to te next file
                DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
                files = []
                for val, opt in input_product_id:
                    # TODO there is a suspect danger here that multiple files have the same date with different start stop
                    tmp = self.dbu.getFiles_product_utc_file_date(val, utc_file_date)
                    if tmp != []:
                        files.extend(tmp)
                DBlogging.dblogger.debug("buildChildren files: ".format(str(files)))
                files = self.dbu.file_id_Clean(files)

            elif timebase == 'DAILY':
                DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
                ## from the input file see what the timebase is and grab all files that go into process
                DBlogging.dblogger.debug("Finding input files for {0}".format(daterange))

                ########## this is giving bad answers!!!  getFiles_product_utc_file_daterange
                files = []
                for val, opt in input_product_id:
                    # TODO there is a suspect danger here that multiple files have the same date with different start stop
                    tmp = self.dbu.getFiles_product_utc_file_daterange(val, daterange)
                    if tmp != []:
                        files.extend(tmp)
                DBlogging.dblogger.debug("buildChildren files: ".format(str(files)))
                files = self.dbu.file_id_Clean(files)

            else:
                DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
                raise(NotImplementedError('Not implented yet (1001)'))
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
            DBlogging.dblogger.debug("Input files found, {0}".format(input_files))

    #==============================================================================
    # setup and do the processing
    #==============================================================================

            # since we have a process do we have a code that does it?
            code_id = self.dbu.getCodeFromProcess(process_id)

            # figure out the code path so that it can be called
            codepath = self.dbu.getCodePath(code_id)
            DBlogging.dblogger.debug("Going to run code: {0}:{1}".format(code_id, codepath))

            out_prod = self.dbu.getOutputProductFromProcess(process_id)
            format_str = self.dbu._getProductFormats(out_prod)
            # get the process_keywords from the file if there are any
            process_keywords = self._strargs_to_args([self.dbu.getFileProcess_keywords(fid) for fid in input_files])
            for key in process_keywords:
                format_str = format_str.replace('{'+key+'}', process_keywords[key])

            # get the format string
            mission, satellite, instrument, product, product_id = self.dbu._getProductNames(out_prod)

            ## need to build a version string for the output file
            version = self.dbu.getCodeVersion(code_id)

            fmtr = DBStrings.DBFormatter()
            output_file_version = Version.Version(version.interface, 0, 0)

            incCode = True
            incFiles = True
            while True:
                filename = fmtr.expand_format(format_str, {'SPACECRAFT':satellite,
                                                             'PRODUCT':product,
                                                             'VERSION':str(output_file_version),
                                                             'datetime':utc_file_date})
                DBlogging.dblogger.debug("Filename: %s created" % (filename))
                # if this filename is already in the DB we have to figure out which version number to increment
                try:
                    f_id_db = self.dbu._getFileID(filename)
                    DBlogging.dblogger.debug("Filename: {0} is in the DB, have to make different version".format(filename))
                    # the file is in the DB, has the code changed version?
                    ## we are planning to use code_id code was this used before?
                    db_code_id = self.dbu.getFilecodelink_byfile(f_id_db)
                    DBlogging.dblogger.debug("f_id_db: {0}   db_code_id: {1}".format(f_id_db, db_code_id))
                    if db_code_id is None:
                        DBlogging.dblogger.error("Database inconsistency found!! A generate file {0} does not have a filecodelink".format(filename))
                    if incCode:
                        if db_code_id != code_id: # did the code change
                            DBlogging.dblogger.debug("code_id: {0}   db_code_id: {1}".format(code_id, db_code_id))
                            ver_diff = (self.dbu.getCodeVersion(code_id) - self.dbu.getCodeVersion(db_code_id))
                            # order matters here by the way these reset each other
                            ## did the revision change?
                            if ver_diff[2] > 0:
                                output_file_version.incRevision()
                                DBlogging.dblogger.debug("Filename: {0} incRevision()".format(filename))
                            ## did the quality change?
                            if ver_diff[1] > 0:
                                output_file_version.incQuality()
                                DBlogging.dblogger.debug("Filename: {0} incQuality()".format(filename))
                            ## did the interface change?
                            if ver_diff[0] > 0:
                                output_file_version.incInterface()
                                DBlogging.dblogger.debug("Filename: {0} incInterface()".format(filename))
                            incCode = False
                    db_files = self.dbu.getFilefilelink_byresult(f_id_db)
                    did_inc = False
                    for in_file in input_files:
                        DBlogging.dblogger.debug("in_file: {0}, db_files: {1}  in_file in db_files or len(db_files) != len(input_files):{2}".format(in_file, db_files, in_file not in db_files or len(db_files) != len(input_files)))
                        if in_file not in db_files or len(db_files) != len(input_files):
                            output_file_version.incQuality()
                            DBlogging.dblogger.debug("Filename: {0} found a file that was not in the original version".format(filename))
                            did_inc = True
                            break # out of the for loop to try again
                    if did_inc:
                        continue # another loop of the while
                    if incFiles:
                        DBlogging.dblogger.debug("db_files: {0}  f_id_db: {1}".format(db_files, f_id_db))
                        DBlogging.dblogger.debug("input_files: {0}  ".format(input_files))
                        # go through and see if all the same files are present,
                        ## if not quality is incremented
                        file_vers = [0,0,0]
                        for in_file in input_files:
                            # the file is there is it the same version
                            input_file_version = self.dbu.getFileVersion(in_file)
                            DBlogging.dblogger.debug("self.dbu.getFileVersion(in_file): {0}".format(self.dbu.getFileVersion(in_file)))
                            db_file_version = self.dbu.getFileVersion(db_files[db_files.index(in_file)])
                            DBlogging.dblogger.debug("self.dbu.getFileVersion(input_files[input_files.index(in_file)]): {0}".format(self.dbu.getFileVersion(input_files[input_files.index(in_file)])))
                            ver_diff =  input_file_version - db_file_version
                            DBlogging.dblogger.debug("ver_diff: {0}".format(ver_diff))
                            ## did the revision change?
                            if ver_diff[2] > 0:
                                file_vers[2] += 1
                            ## did the quality change?
                            if ver_diff[1] > 0:
                                file_vers[1] += 1
                            ## did the interface change?
                            if ver_diff[0] > 0:
                                file_vers[0] += 1
                            DBlogging.dblogger.debug("file_vers: {0}".format(file_vers))
                            if file_vers[0] > 0:
                                output_file_version.incInterface()
                                break # out of the for loop
                            elif file_vers[1] > 0:
                                output_file_version.incQuality()
                                break # out of the for loop
                            elif file_vers[0] > 0:
                                output_file_version.incRevision()
                                break # out of the for loop
                            else: # this file would be the same as what we already have, don't process it
                                DBlogging.dblogger.debug("Filename: {0} found all the same files".format(filename))
                                return None

                except DBUtils2.DBError: # this is for self.dbu._getFileID(filename)
                    DBlogging.dblogger.debug("Filename: {0} is not in the DB, can process".format(filename))
                    break # leave the while loop and do the processing

            # make a directory to run the code
            self.tempdir = tempfile.mkdtemp('_dbprocessing')
            DBlogging.dblogger.debug("Created temp directory: {0}".format(self.tempdir))

            ## build the command line we are to run
            cmdline = [codepath]
            ## build the command line we are to run
            cmdline = [codepath]

            ## figure out how to put the arguments together
            args = self.dbu.getCodeArgs(code_id)
            if args is not None:
                args = args.split()
                for arg in args:
                    if 'input' not in arg and 'output' not in arg:
                        cmdline.append(arg)

            for i_fid in input_files:
                if args is not None:
                   for arg in args:
                       if 'input' in arg:
                           cmdline.append(arg.split('=')[1])
                cmdline.append(self.dbu._getFileFullPath(i_fid))
            if args is not None:
                for arg in args:
                   if 'output' in arg:
                       cmdline.append(arg.split('=')[1])
            cmdline.append(os.path.join(self.tempdir, filename))

            DBlogging.dblogger.info("running command: {0}".format(' '.join(cmdline)))

            # TODO, think here on how to grab the output
            try:
                subprocess.check_call(cmdline, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError:
                # TODO figure out how to print what the return code was
                DBlogging.dblogger.error("Command returned a non-zero return code")
                # assume the file is bad and move it to error
                self.moveToError(filename)
                self.rm_tempdir() # clean up
                continue
            DBlogging.dblogger.info("command finished")

            # the code worked and the file should not go to incoming (it had better have an inspector)
            self.moveToIncoming(os.path.join(self.tempdir, filename))
            self.rm_tempdir() # clean up
            ## TODO several additions have to be made here
            # -- after the process is run we have to somehow keep track of the filefilelink and the foldcodelink so they can be added
            #    -- maybe instead of moving this to incoming we add it to the db now with the links

            # need to add the current file to the DB so that we have the filefilelink and filecodelink info
            current_file = self.current_file # so we can put it back
            self.current_file = os.path.join(self.dbu.getIncomingPath(), filename)
            df = self.figureProduct()
            df.params['verbose_provenance'] = ' '.join(cmdline)
            f_id = self.diskfileToDB(df)
            ## here the file is in the DB so we can add the filefilelink an filecodelinks
            if f_id is not None: # None comes back if the file goes to error
                self.dbu.addFilecodelink(f_id, code_id)
                for val in input_files: # add a link for each input file
                    self.dbu.addFilefilelink(f_id, val)
            self.current_file = current_file # so we can put it back


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
                pq.queueClean()  # get rid of duplicates
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
