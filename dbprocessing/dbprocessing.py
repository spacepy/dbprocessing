#!/usr/bin/env python2.6

import datetime
import imp
import os
import shutil
import sys
import tempfile
import traceback

import DBfile
import DBlogging
import DBqueue
import DBUtils
import runMe
import Utils
from Utils import strargs_to_args

try: # new version changed this annoyingly
    from sqlalchemy.exceptions import IntegrityError
except ImportError:
    from sqlalchemy.exc import IntegrityError

__version__ = '2.0.4'


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
                 mission, dryrun=False):

        self.dryrun = dryrun
        self.mission = mission
        dbu = DBUtils.DBUtils(self.mission)
        self.tempdir = None
#        self.current_file = None
        self.runme_list = []
        self.dbu = dbu
        self.childrenQueue = DBqueue.DBqueue()
        self.moved = DBqueue.DBqueue()
        self.depends = DBqueue.DBqueue()
        self.queue = DBqueue.DBqueue()
        self.findChildren = DBqueue.DBqueue()
        DBlogging.dblogger.info("Entering ProcessQueue")

    def __del__(self):
        """
        attempt a bit of up
        """

    def rm_tempdir(self):
        """
        remove the temp directory
        """
        if self.tempdir != None:
            name = self.tempdir
            shutil.rmtree(self.tempdir)
            self.tempdir = None
            DBlogging.dblogger.debug("Temp dir deleted: {0}".format(name))

    def mk_tempdir(self, suffix='_dbprocessing'):
        """
        create a secure temp directory
        """
        self.tempdir = tempfile.mkdtemp(suffix)

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
        #  this workaround
        for i in range(len(self.queue )):
            try:
                if list(self.queue).count(self.queue[i]) != 1:
                    self.queue.remove(self.queue[i])
            except IndexError:
                pass   # this means it was shortened
        DBlogging.dblogger.debug("Queue contains (%d): %s" % (len(self.queue),
                                                              self.queue))

    def moveToError(self, fname):
        """
        Moves a file from incoming to error
        """
        DBlogging.dblogger.debug("Entered moveToError: {0}".format(fname))

        path = self.dbu.getErrorPath()
        # if the file is a link then don;t move it to incoming just delete the link
        if os.path.islink(fname):
            os.unlink(fname) # Remove a file (same as remove(path)).
            DBlogging.dblogger.info("moveToError file {0} was a link, so link removed not moved to error".format(fname))
        else:
            try:
                shutil.move(fname, os.path.join(path, os.path.basename(fname)))
            except IOError:
                DBlogging.dblogger.error("file {0} was not successfully moved to error".format(os.path.join(path, os.path.basename(fname) )))
            else:
                DBlogging.dblogger.info("moveToError {0} moved to {1}".format(fname, path))

    def diskfileToDB(self, df):
        """
        given a diskfile go through and do all the steps to add it into the db
        """
        if df is None:
            DBlogging.dblogger.info("Found no product moving to error, {0}".format(self.filename))
            if not self.dryrun:
                self.moveToError(self.filename)
            else:
                print('<dryrun> Found no product moving to error, {0}'.format(self.filename))
            return None

        # create the DBfile
        dbf = DBfile.DBfile(df, self.dbu)
        try:
            if not self.dryrun:
                f_id = dbf.addFileToDB()
                DBlogging.dblogger.info("File {0} entered in DB, f_id={1}".format(df.filename, f_id))
            else:
                print('<dryrun> File {0} entered in DB'.format(df.filename))
        except (ValueError, DBUtils.DBError) as errmsg:
            if not self.dryrun:
                DBlogging.dblogger.warning("Except adding file to db so" + \
                                           " moving to error: %s" % (errmsg))
                self.moveToError(os.path.join(df.path, df.filename))
            else:
                print('<dryrun> Except adding file to db so' +
                                           ' moving to error: %s' % (errmsg))
            return None

        # move the file to the its correct home
        if not self.dryrun:
            dbf.move()
        # set files in the db of the same product and same utc_file_date to not be newest version
        files = self.dbu.getFiles_product_utc_file_date(dbf.diskfile.params['product_id'], dbf.diskfile.params['utc_file_date'])
        if files:
            mx = max(zip(*files)[1]) # max on version
        for f in files:
            if f[1] != mx: # this is not the max, newest_version should be False
                fle = self.dbu.getEntry('File', f[0])
                fle.newest_version = False
                if not self.dryrun:
                    self.dbu.session.add(fle)
                    DBlogging.dblogger.debug("set file: {0}.newest_version=False".format(f[0]))
        if not self.dryrun:
            try:
                self.dbu.session.commit()
            except IntegrityError as IE:
                self.session.rollback()
                raise(DBUtils.DBError(IE))
            # add to processqueue for later processing
            self.dbu.Processqueue.push(f_id)
            return f_id
        else:
            return None

    def importFromIncoming(self):
        """
        Import a file from incoming into the database
        """
        DBlogging.dblogger.debug("Entering importFromIncoming, {0} to import".format(len(self.queue)))

        if not self.dryrun:
            vals = self.queue.popleftiter()
        else:
            vals = self.queue

        for val in vals:
            self.filename = val
            DBlogging.dblogger.debug("popped '{0}' from the queue: {1} left".format(self.filename, len(self.queue)))
            df = self.figureProduct()
            if df != []:
                self.diskfileToDB(df)

    def figureProduct(self, filename=None):
        """
        This function imports the inspectors and figures out which inspectors claim the file
        """
        if filename is None:
            filename = self.filename
        act_insp = self.dbu.getActiveInspectors()
        claimed = []
        for code, arg, product in act_insp:
            try:
                inspect = imp.load_source('inspect', code)
            except IOError, msg:
                DBlogging.dblogger.error('Inspector: "{0}" not found: {1}'.format(code, msg))
                if os.path.isfile(code + ' '):
                    DBlogging.dblogger.info('---> However inspector: "{0}" was found'.format(code+' '))
                    print('---> However inspector: "{0}" was found.'.format(code+' '))
                continue
            if arg is not None:
                kwargs = strargs_to_args(arg)
                try:
                    df = inspect.Inspector(filename, self.dbu, product, **kwargs)
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    DBlogging.dblogger.error("File {0} inspector threw an exception: {1} {2} {3}".format(filename, str(exc_type), exc_value, traceback.print_tb(exc_traceback))) #exc_traceback.tb_lineno))
                    continue # try the next inspector
            else:
                try:
                    df = inspect.Inspector(filename, self.dbu, product, )
                except:
                    DBlogging.dblogger.error("File {0} inspector threw an exception".format(filename))
                    continue # try the next inspector
            if df is not None:
                claimed.append(df)
                DBlogging.dblogger.debug("Match found: {0}: {1}".format(filename, code, ))
                break # lets call it done after we find one

        if len(claimed) == 0: # no match
            DBlogging.dblogger.info("File {0} found no inspector match".format(filename))
            return None
        if len(claimed) > 1:
            DBlogging.dblogger.error("File {0} matched more than one product, there is a DB error".format(filename))
            raise(DBUtils.DBError("File {0} matched more than one product, there is a DB error".format(filename)))

        return claimed[0]  # return the diskfile


    def _getRequiredProducts(self, process_id, file_id, utc_file_date):
        #####################################################
        ## get all the input products for that process, and if they are optional
        input_product_id = self.dbu.getInputProductID(process_id) # this is a list of tuples (id, optional)

        DBlogging.dblogger.debug("Finding input files for file_id:{0} process_id:{1} date:{2}".format(file_id, process_id, utc_file_date))

        ## here decide how we build output and do it.
        timebase = self.dbu.session.query(self.dbu.Process.output_timebase).get(process_id)[0] # faster

        if timebase == 'FILE': # taking one file to the next file
            # for file based processing we are going to look to the "process_keywords" and cull the
            #   retuned files based on making sure they are all the same
            #   If process_keywords is none it will fall back to current behavior (since they will all be the same)
            DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
            files = []
            # get all the possible files based on dates that we might want to put into the process now
            for val, opt in input_product_id:
                files.extend(self.dbu.getFiles_product_utc_file_date(val, utc_file_date))

            DBlogging.dblogger.debug("buildChildren files: ".format(str(files)))
            # remove all the files that are not the newest version
            files = self.dbu.file_id_Clean(files)
            # grab the process_keywords column for the file_id and all the possible other files
            # infile_process_keywords = self.dbu.getEntry('File', file_id).process_keywords # below is faster
            infile_process_keywords = self.dbu.session.query(self.dbu.File.process_keywords).get(file_id)[0]
            files_process_keywords = [self.dbu.getEntry('File', v[0]).process_keywords for v in files]
            # now if the process_keywords in files_process_keywords does not match that in infile_process_keywords
            #   drop it
            files_out = []
            for ii, v in enumerate(files_process_keywords):
                if v == infile_process_keywords:
                    files_out.append(files[ii])
            # and give it the right name
            files = files_out

        elif timebase == 'DAILY':
            DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
            ## from the input file see what the timebase is and grab all files that go into process
            DBlogging.dblogger.debug("Finding input files for {0}".format(utc_file_date))

            files = []
            for val, opt in input_product_id:
                tmp = self.dbu.getFiles_product_utc_file_date(val, utc_file_date)
                if tmp:  # != []
                    files.extend(tmp)
            DBlogging.dblogger.debug("buildChildren files: ".format(str(files)))
            files = self.dbu.file_id_Clean(files)

        else:
            DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
            raise(NotImplementedError('Not implemented yet: {0} based processing'.format(timebase)))
            raise(ValueError('Bad timebase for product: {0}'.format(process_id)))
        return files, input_product_id

    def _requiredFilesPresent(self, files, input_product_id, process_id):
        #==============================================================================
        # do we have the required files to do the build?
        #==============================================================================
        # get the products of the input files
        ## need to go through the input_product_id and make sure we have a file for each required product
        if not files:
            return False
        for prod, opt in input_product_id:
            if not opt:
                if not prod in zip(*files)[2]: # the product ID
                    DBlogging.dblogger.debug("Required products not found, continuing.  Process:{0}, product{1}".format(process_id, prod))
                    return False
        return True

    def buildChildren(self, process_id, file_id):
        """
        go through and all all the runMe's to the runme_list variable
        """
        DBlogging.dblogger.debug("Entered buildChildren: process_id={0}".format(process_id))

        daterange = self.dbu.getFileDates(file_id[0]) # this is the dates that this product spans

        # iterate over all the days between the start and stop date from above (including stop date)
        for utc_file_date in Utils.expandDates(*daterange):

            files, input_product_id = self._getRequiredProducts(process_id, file_id[0], utc_file_date)

            #==============================================================================
            # do we have the required files to do the build?
            #==============================================================================
            if not self._requiredFilesPresent(files, input_product_id, process_id):
                DBlogging.dblogger.debug("For file: {0} date: {1} required files not present".format(file_id[0], utc_file_date))
                continue # go on to the next file

            input_files = zip(*files)[0] # this is the file_id
            DBlogging.dblogger.debug("Input files found, {0}".format(input_files))

            runme = runMe.runMe(self.dbu, utc_file_date, process_id, input_files, )
            self.runme_list.append(runme)

    def onStartup(self):
        """
        Processes can be defined as output timebase "STARTUP" which means to run
        them each time to processing chain is run
        """
        proc = self.dbu.getAllProcesses(timebase='STARTUP')
        #TODO just going to run there here for now.  This shold move to runMe
        for p in proc:  # run them all
            code = self.dbu.getEntry('Code', p.process_id)
            # print code.codename

        # need to call a "runner" with these processes
        ######
        ##
        # not sure how to deal with having to specify a filename and handle that in the DB
        # things made here will also have to have inspectors
        raise(NotImplementedError('Not yet implemented'))

    def _reprocessBy(self, id_in, code=False, prod=False, inst=False, level=None, startDate=None, endDate=None, incVersion=2):
        """
        given a code_id (or name) add all files that this code touched to processqueue
            so that next -p run they will be reprocessed
        If one adds a new code run this on the code that this is replacing
        Force moves the file back to incoming and then removes it from the db
        incVersion sets which of the version numbers to increment {0}.{1}.{2}
        ** this ends up being a little more aggressive as the input files are put
           on the processqueue so all products associated with them are remade
           ** If we want to change this then several steps need to occur:
               1) need to have a way to tell buildchildren or _runner to only use
                   certain codes
        """
        # 1) get all the files made by this code
        # 2) get all the parents of the 1) files
        # 3) add all these back to the processqueue (use set as duplicates breaks things)
        if code:
            code_id = self.dbu.getCodeID(id_in) # allows name or id
            files = self.dbu.getFilesByCode(code_id)
        elif prod:
            prod_id = self.dbu.getProductID(id_in)
            files = self.dbu.getFilesByProduct(prod_id)
        elif inst:
            inst_id = self.dbu.getInstrumentID(id_in)
            prods = self.dbu.getProductsByInstrument(inst_id)
            if level is not None: # cull the list by level
                prods2 = []
                for prod in prods:
                    ptb = self.dbu.getTraceback('Product', prod)
                    if ptb['product'].level == level:
                        prods2.append(ptb['product'].product_id)
                prods = prods2
            for prod in prods: # add them all to be reprocessed
                self.reprocessByProduct(prod, startDate=startDate, endDate=endDate, incVersion=incVersion)
        else:
            raise(ValueError('No reprocess by specified'))
        # files before this date are removed from the list
        if startDate is not None:
            files = [val for val in files if val.utc_file_date >= startDate]
        # files after this date are removed from the list
        if endDate is not None:
            files = [val for val in files if val.utc_file_date <= endDate]
        f_ids = [val.file_id for val in files]
        parents = [self.dbu.getFileParents(val, id_only=True) for val in f_ids]
        filesToReprocess = set(Utils.flatten(parents))
        for f in filesToReprocess:
            try:
                self.dbu.Processqueue.push(f, incVersion)
            except filesToReprocess:
                pass
        return len(filesToReprocess)

    # TODO can functools.partial help here?
    def reprocessByCode(self, id_in, startDate=None, endDate=None, incVersion=2):
        return(self._reprocessBy(id_in, code=True, prod=False, startDate=startDate, endDate=endDate, incVersion=incVersion))

    def reprocessByProduct(self, id_in, startDate=None, endDate=None, incVersion=2):
        if isinstance(startDate, datetime.datetime):
            startDate = startDate.date()
        if isinstance(endDate, datetime.datetime):
            endDate = endDate.date()
        try:
            prod_id = self.dbu.getProductID(id_in)
        except DBUtils.DBNoData:
            print('No product_id {0} found in the DB'.format(id_in))
            return None

        files = self.dbu.getFilesByProduct(prod_id)
        # files before this date are removed from the list
        if startDate is not None:
            files = [val for val in files if val.utc_file_date >= startDate]
        # files after this date are removed from the list
        if endDate is not None:
            files = [val for val in files if val.utc_file_date <= endDate]
        f_ids = [val.file_id for val in files]
        filesToReprocess = set(f_ids)
        added = 0
        for f in filesToReprocess:
            try:
                self.dbu.Processqueue.push(f, incVersion)
                added += 1
            except DBUtils.DBError:
                print("File {0} failed to add, was already there".format(f))
        return added

    def reprocessByInstrument(self, id_in, level=None, startDate=None, endDate=None, incVersion=2):
        files = self.dbu.getFilesByInstrument(id_in, level=level, id_only=False)
        # files before this date are removed from the list
        if startDate is not None:
            files = [val for val in files if val.utc_file_date >= startDate]
        # files after this date are removed from the list
        if endDate is not None:
            files = [val for val in files if val.utc_file_date <= endDate]
        f_ids = [val.file_id for val in files]
        filesToReprocess = set(f_ids)
        for f in filesToReprocess:
            try:
                self.dbu.Processqueue.push(f, incVersion)
            except filesToReprocess:
                pass
        return len(filesToReprocess)

    def reprocessByAll(self, level=None, startDate=None, endDate=None):
        """
        this is a raw call into the db meant to be fast and all every file
        between the dates into the process queue
        - there is no version incremnt allowed
        """
        if startDate is not None and endDate is not None and level is None:
            files = self.dbu.session.query(self.dbu.File.file_id).filter(self.dbu.File.utc_file_date >= startDate).filter(self.dbu.File.utc_file_date <= endDate).all()
        elif startDate is not None and endDate is not None and level is not None:
            files = self.dbu.session.query(self.dbu.File.file_id).filter(self.dbu.File.utc_file_date >= startDate).filter(self.dbu.File.utc_file_date <= endDate).filter(self.dbu.File.data_level == level).all()
        elif startDate is None and endDate is not None and level is not None:
            files = self.dbu.session.query(self.dbu.File.file_id).filter(self.dbu.File.utc_file_date <= endDate).filter(self.dbu.File.data_level == level).all()
        elif startDate is None and endDate is None and level is not None:
             files = self.dbu.session.query(self.dbu.File.file_id).filter(self.dbu.File.data_level == level).all()
        elif startDate is None and endDate is None and level is None:
             files = self.dbu.session.query(self.dbu.File.file_id).all()
        elif startDate is not None and endDate is None and level is None:
            files = self.dbu.session.query(self.dbu.File.file_id).filter(self.dbu.File.utc_file_date >= startDate).all()
        elif startDate is None and endDate is not None and level is None:
            files = self.dbu.session.query(self.dbu.File.file_id).filter(self.dbu.File.utc_file_date <= endDate).all()
        elif startDate is not None and endDate is None and level is not None:
             files = self.dbu.session.query(self.dbu.File.file_id).filter(self.dbu.File.utc_file_date >= startDate).filter(self.dbu.File.data_level == level).all()

        else:
            raise(NotImplementedError("Sorry combination is not implemented"))

        try:
            ids = zip(*files)[0]
        except IndexError:
            ids = []
            n_added = 0

        if ids:
            n_added = self.dbu.Processqueue.rawadd(ids)

        return n_added
