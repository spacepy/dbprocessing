#!/usr/bin/env python
"""High-level implementation of the dbprocessing processing queue."""

from __future__ import absolute_import
from __future__ import print_function

import datetime
import imp
import os
import shutil
import sys
import tempfile
import time
import traceback
from operator import itemgetter

from . import DBfile
from . import DBlogging
from . import DBqueue
from . import DButils
from . import Utils
from . import runMe
from .Utils import strargs_to_args

from sqlalchemy.exc import IntegrityError


class ProcessQueue(object):
    """Main code used to process the Queue.

    Looks in incoming and builds all possible files

    Warnings
    --------
    As this object holds a reference to
    :class:`~dbprocessing.DButils.DButils`, that database should be
    closed before the program terminates. Deleting this object will
    ordinarily suffice.
    """

    def __init__(self,
                 mission, dryrun=False, echo=False):
        """Initializes the process queue

        Parameters
        ----------
        mission : str
            Mission database, as in :class:`.DButils`. May also be
            an existing instance of :class:`.DButils`.
        dryrun : bool, default False
            Treat all functionality as "dry run", make no changes.
        echo : bool, default False
            Log all SQL statements (as in
            :class:`~dbprocessing.DButils.DButils`).
        """
        self.dryrun = dryrun
        self.filename = None
        """Full path to file currently being processed (:class:`str`)"""
        if isinstance(mission, DButils.DButils):
            dbu = mission
            self.mission = dbu.mission
        else:
            self.mission = mission
            dbu = DButils.DButils(self.mission, echo=echo)
        self.tempdir = None
        self.runme_list = []
        self.dbu = dbu
        self.childrenQueue = DBqueue.DBqueue()
        self.moved = DBqueue.DBqueue()
        self.depends = DBqueue.DBqueue()
        self.queue = DBqueue.DBqueue()
        self.findChildren = DBqueue.DBqueue()
        DBlogging.dblogger.debug("Entering ProcessQueue")

    def __del__(self):
        """Clean up (remove temporary files and close database)"""
        self.rm_tempdir()
        try:
            del self.dbu
        except AttributeError:
            pass

    def set_filename(self, filename):
        """
        Setter for filename, this is cleaner than just random sets

        Parameters
        ----------
        filename : :class:`str`
            filename to set to :data:`filename`
        """
        self.filename = filename
        self.basename = os.path.basename(self.filename)

    def rm_tempdir(self):
        """
        remove the temp directory
        """
        try:
            if self.tempdir != None:
                name = self.tempdir
                shutil.rmtree(self.tempdir)
                self.tempdir = None
                DBlogging.dblogger.debug("Temp dir deleted: {0}".format(name))
        except AttributeError:
            pass

    def mk_tempdir(self, suffix='_dbprocessing_{0}'.format(os.getpid())):
        """
        create a secure temp directory
        """
        self.tempdir = tempfile.mkdtemp(suffix)

    def checkIncoming(self, glb='*'):
        """
        Goes out to incoming and grabs all files there adding them to self.queue

        Parameters
        ----------
        glb : :class:`str`, optional
            Glob pattern that files must match.
        """
        DBlogging.dblogger.debug("Entered checkIncoming:")

        self.queue.extendleft(self.dbu.checkIncoming(glb=glb))
        # step through and remove duplicates
        # if python 2.7 deque has a .count() otherwise have to use
        #  this workaround
        for i in range(len(self.queue)):
            try:
                if list(self.queue).count(self.queue[i]) != 1:
                    self.queue.remove(self.queue[i])
            except IndexError:
                pass  # this means it was shortened
        DBlogging.dblogger.debug("Queue contains (%d): %s" % (len(self.queue),
                                                              self.queue))

    def moveToError(self, fname):
        """
        Moves a file from incoming to error

        Parameters
        ----------
        fname : :class:`str`
            Full path to file to move to error.
        """
        DBlogging.dblogger.debug("Entered moveToError: {0}".format(fname))

        path = self.dbu.getErrorPath()
        # if the file is a link then don;t move it to incoming just delete the link
        if os.path.islink(fname):
            os.unlink(fname)  # Remove a file (same as remove(path)).
            DBlogging.dblogger.info("moveToError file {0} was a link, so link removed not moved to error".format(fname))
        else:
            try:
                shutil.move(fname, os.path.join(path, os.path.basename(fname)))
            except IOError:
                DBlogging.dblogger.error(
                    "file {0} was not successfully moved to error".format(os.path.join(path, os.path.basename(fname))))
            else:
                DBlogging.dblogger.info("moveToError {0} moved to {1}".format(fname, path))

    def diskfileToDB(self, df):
        """
        given a diskfile go through and do all the steps to add it into the db

        Parameters
        ----------
        df : :class:`.Diskfile`
            File to add to database.
        """
        if df is None:
            DBlogging.dblogger.info("Found no product moving to error, {0}".format(self.basename))
            if not self.dryrun:
                self.moveToError(self.filename)
            else:
                print('<dryrun> Found no product moving to error, {0}'.format(self.basename))
            return None

        # create the DBfile
        dbf = DBfile.DBfile(df, self.dbu)
        try:
            if not self.dryrun:
                f_id = dbf.addFileToDB()
                DBlogging.dblogger.info("File {0} entered in DB, f_id={1}".format(df.filename, f_id))
            else:
                print('<dryrun> File {0} entered in DB'.format(df.filename))
        except (ValueError, DButils.DBError) as errmsg:
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

        if not self.dryrun:
            try:
                self.dbu.session.commit()
            except IntegrityError as IE:
                self.session.rollback()
                raise DButils.DBError(IE)
            # add to processqueue for later processing
            self.dbu.ProcessqueuePush(f_id)
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

        T0 = time.time()
        for ii, val in enumerate(vals, 1):
            self.set_filename(val)
            DBlogging.dblogger.debug("popped '{0}' from the queue: {1} left".format(self.basename, len(self.queue)))
            # see if the file is in the db, if so then don't call the inspectors
            try:
                id = self.dbu.getFileID(self.basename)
                DBlogging.dblogger.info(
                    'File {0}:{1} was already in DB, not inspecting'.format(id, self.basename))
                self.moveToError(self.filename)
                T1 = time.time() - T0
                print('{1}:{2} Removed from incoming: {0} - already present  {3:.2f}s'.format(self.basename, ii, len(self.queue), T1))
                T0 = time.time()
                continue
            except DButils.DBNoData:
                DBlogging.dblogger.info('File {0} was not in DB, inspecting'.format(self.basename))
            df = self.figureProduct()
            if df != []:
                self.diskfileToDB(df)
                T1 = time.time() - T0
                print('{1}:{2} Removed from incoming: {0} - ingested   {3:.2f}s'.format(self.basename, ii, len(self.queue), T1))
                T0 = time.time()

    def figureProduct(self, filename=None):
        """Imports inspectors and figures out which inspectors claim the file

        Parameters
        ----------
        filename : :class:`str`, optional
            Full path to file to check, default :data:`filename`.
        """
        if filename is None:
            filename = self.filename
        act_insp = self.dbu.getActiveInspectors()
        claimed = []
        for code, desc, arg, product in act_insp:
            try:
                inspect = imp.load_source('inspect', code)
            except IOError as msg:
                DBlogging.dblogger.error('Inspector: "{0}" not found: {1}'.format(code, msg))
                if os.path.isfile(code + ' '):
                    DBlogging.dblogger.info('---> However inspector: "{0}" was found'.format(code + ' '))
                    print('---> However inspector: "{0}" was found.'.format(code + ' '))
                continue
            if arg is not None:
                kwargs = strargs_to_args(arg)
                try:
                    df = inspect.Inspector(filename, self.dbu, product, **kwargs)()
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    DBlogging.dblogger.error(
                        "File {0} inspector threw an exception: {1} {2} {3}".format(filename, str(exc_type), exc_value,
                                                                                    traceback.print_tb(
                                                                                        exc_traceback)))  # exc_traceback.tb_lineno))
                    continue  # try the next inspector
            else:
                try:
                    df = inspect.Inspector(filename, self.dbu, product, )()
                except:
                    DBlogging.dblogger.error("File {0} inspector threw an exception".format(filename))
                    continue  # try the next inspector
            if df is not None:
                claimed.append(df)
                DBlogging.dblogger.debug("Match found: {0}: {1}".format(filename, code, ))
                break  # lets call it done after we find one

        if len(claimed) == 0:  # no match
            DBlogging.dblogger.info("File {0} found no inspector match".format(filename))
            return None
        if len(claimed) > 1:
            DBlogging.dblogger.error("File {0} matched more than one product, there is a DB error".format(filename))
            raise DButils.DBError("File {0} matched more than one product, there is a DB error".format(filename))

        return claimed[0]  # return the diskfile

    def _getRequiredProducts(self, process_id, file_id, utc_file_date, debug=False):
        #####################################################
        ## get all the input products for that process, and if they are optional
        T0 = time.time()
        input_product_id = self.dbu.getInputProductID(process_id, True)  # this is a list of tuples (id, optional, yesterday, tommorow)
        if debug: print("21:    {0}: self.dbu.getInputProductID: {1}".format(time.time() - T0, input_product_id))
        T0 = time.time()

        DBlogging.dblogger.debug(
            "Finding input files for file_id:{0} process_id:{1} date:{2}".format(file_id, process_id, utc_file_date))

        ## here decide how we build output and do it.

        timebase = self.dbu.getProcessTimebase(process_id)
        if debug: print("22:    {0}: self.dbu.getProcessTimebase: {1}".format(time.time() - T0, timebase))
        T0 = time.time()


        DBlogging.dblogger.debug("Doing {0} based processing".format(timebase))
        if timebase in ['FILE', 'DAILY', 'RUN']:  # taking one file to the next file
            # for file based processing we are going to look to the "process_keywords" and cull the
            #   returned files based on making sure they are all the same
            #   If process_keywords is none it will fall back to current behavior (since they will all be the same)
            files = []
            # get all the possible files based on dates that we might want to put into the process now

            for iprod_id, opt, y, t in input_product_id:
                # accept a datetime.datetime or datetime.date
                dt = Utils.datetimeToDate(utc_file_date)
                start = dt - datetime.timedelta(days=y)
                end = dt + datetime.timedelta(days=t)

                kwargs = {'startTime': start, 'endTime': end} \
                         if timebase in ('DAILY',) \
                         else {'startDate': start, 'endDate': end}
                tmp_files = self.dbu.getFiles(
                    product=iprod_id, newest_version=True, exists=True,
                    **kwargs)
                if debug: print("23:    {0}: self.dbu.getFiles, {1} {2} {3}".format(time.time() - T0, iprod_id, dt, tmp_files))
                T0 = time.time()


                if not tmp_files and not opt:
                    return None, input_product_id
                else:
                    files.extend(tmp_files)

            DBlogging.dblogger.debug("buildChildren files: ".format(str(files)))

            ###############
            # BAL 30 March 2017, dropping this clean step as they should all be newest version per above
            # remove all the files that are not the newest version, they all should be
            # files = self.dbu.file_id_Clean(files)
            ###############

            if timebase == 'FILE':  # taking one file to the next file
                files_out = []
                # grab the process_keywords column for the file_id and all the possible other files
                #   they have to match in order for the file to be the same
                infile_process_keywords = self.dbu.getEntry('File', file_id).process_keywords
                try:
                    files_process_keywords = [v.process_keywords for v in files]
                except AttributeError:
                    files_process_keywords = []
                # now if the process_keywords in files_process_keywords does not match that in infile_process_keywords
                #   drop it
                for ii, v in enumerate(files_process_keywords):
                    if v == infile_process_keywords:
                        files_out.append(files[ii])
                # and give it the right name
                files = files_out
        else:
            raise NotImplementedError('Not implemented yet: {0} based processing'.format(timebase))
            raise ValueError('Bad timebase for product: {0}'.format(process_id))
        return files, input_product_id

    def buildChildren(self, file_id, debug=False, skip_run=False, run_procs=None):
        """
        go through and all the runMe's and add to the runme_list variable

        Parameters
        ----------
        file_id : :class:`int`
            file ID of the file for which children will be built
        skip_run : :class:`bool`, default False
            Skip RUN timebase processes if True
        run_procs : :class:`str`, optional
            If provided, comma-separated list of process IDs
            or process names to run; other processes are
            ignored. (Default: all possible processes).
        """

        # if processes to run specified, turn into list of IDs
        # getProcessID accepts either ID or name and returns ID
        if run_procs is not None:
            run_procs = [self.dbu.getProcessID(rp)
                         for rp in run_procs.split(',')]

        T0 = time.time()
        DBlogging.dblogger.debug("Entered buildChildren: file_id={0}".format(file_id))
        if debug: print("Entered buildChildren: file_id={0}".format(file_id))
        # if this file is not a newest_version we do not ant to run
        #print("{1}: Entered buildChildren: file_id={0}".format(file_id, time.time()-T0))
        T0 = time.time()
        if not self.dbu.fileIsNewest(file_id[0]):
            DBlogging.dblogger.debug("Was not newest version in buildChildren: file_id={0}".format(file_id))
            print("    Was not newest version in buildChildren: file_id={0}".format(file_id))
            return  # do nothing
            if debug: print("    {1}: was newest moving on in buildChildren: file_id={0}".format(file_id, time.time()-T0))
        T0 = time.time()

        children = self.dbu.getChildrenProcesses(file_id[0])  # returns process
        if debug: print("11:   {1}: done self.dbu.getChildrenProcesses buildChildren: file_id={0} : {2}".format(file_id, time.time()-T0, children))
        T0 = time.time()
        daterange = self.dbu.getFileDates(file_id[0])  # this is the dates that this file spans
        if debug: print("12:   {1}: done self.dbu.getFileDates  buildChildren: file_id={0} : {2}".format(file_id, time.time()-T0, daterange))
        T0 = time.time()

        if debug: print("children: {0}".format(children))
        for child_process in children:

            # iterate over all the days between the start and stop date from above (including stop date)
            for utc_file_date in Utils.expandDates(*daterange):
                if debug: print("    utc_file_date: {0}".format(utc_file_date))
                files, input_product_id = self._getRequiredProducts(child_process, file_id[0], utc_file_date)
                if debug: print("13:   {0}: self._getRequiredProducts   {1} {2}".format(time.time()-T0, files, input_product_id))
                T0 = time.time()
                if not files:
                    # figure out the missing products
                    DBlogging.dblogger.debug("For file: {0} date: {1} required files not present {2}"
                                             .format(file_id[0], utc_file_date, input_product_id))
                    continue  # go on to the next file

                    # ==============================================================================
                    # do we have the required files to do the build?
                    # ==============================================================================
                    ##             if not self._requiredFilesPresent(files, input_product_id, process_id):
                    ##                 DBlogging.dblogger.debug("For file: {0} date: {1} required files not present".format(file_id[0], utc_file_date))
                    ##                 continue # go on to the next file

                try:
                    input_files = [v.file_id for v in files]
                except AttributeError:
                    continue
                DBlogging.dblogger.debug("Input files found, {0}".format(input_files))

                if skip_run \
                   and self.dbu.getProcessTimebase(child_process) == 'RUN':
                    DBlogging.dblogger.info(
                        "Process: {} skipping because RUN timebase"
                        .format(self.dbu.getEntry('Process', child_process)
                                .process_name))
                    continue
                if run_procs is not None and child_process not in run_procs:
                    DBlogging.dblogger.info(
                        "Process: {} skipping because not in run-only list"
                        .format(self.dbu.getEntry('Process', child_process)
                                .process_name))
                    continue
                runme = runMe.runMe(self.dbu, utc_file_date, child_process, input_files, self, file_id[1])
                #print("{0}:  runMe.runMe".format(time.time()-T0))
                #T0 = time.time()
                # only add to runme list if it can be run
                if runme.ableToRun and (runme not in self.runme_list):
                    self.runme_list.append(runme)
                    DBlogging.dblogger.info("Filename: {0} is not in the DB, can process".format(runme.filename))

    def onStartup(self):
        """
        Processes can be defined as output timebase "STARTUP" which means to run
        them each time to processing chain is run
        """
        proc = self.dbu.getAllProcesses(timebase='STARTUP')
        # TODO just going to run there here for now.  This should move to runMe
        for p in proc:  # run them all
            code = self.dbu.getEntry('Code', p.process_id)
            # print code.codename

        # need to call a "runner" with these processes
        ######
        ##
        # not sure how to deal with having to specify a filename and handle that in the DB
        # things made here will also have to have inspectors
        raise NotImplementedError('Not yet implemented')

    def _reprocessBy(self,
                     startDate=None,
                     endDate=None,
                     level=None,
                     product=None,
                     code=None,
                     instrument=None,
                     incVersion=None):
        """
        Given parameters, add all files to processqueue so that next
        -p run they will be reprocessed

        All parameters are optional; if not specified, default is "all".

        Parameters
        ----------
        startDate : :class:`~datetime.datetime`, optional
            First date to add to process queue
        endDate : :class:`~datetime.datetime`, optional
            Last date to add to process queue (inclusive)
        level : :class:`float`, optional
            Only add files of this level.
        product : :class:`int`, optional
            :sql:column:`~product.product_id` of files to add
        code : :class:`int`, optional
            Only add files created by code with ID of
            :sql:column:`~code.code_id` 
        instrument : :class:`int`, optional
            Only add files with instrument
            :sql:column:`~instrument.instrument_id`
        incVersion : :class:`int`, optional
            Force processing and increment this version number, {0}.{1}.{2}
        """
        startDate = Utils.datetimeToDate(startDate)
        endDate = Utils.datetimeToDate(endDate)
        f_ids = [val.file_id for val in self.dbu.getFiles(startDate=startDate,
                                                          endDate=endDate,
                                                          level=level,
                                                          product=product,
                                                          code=code,
                                                          instrument=instrument,
                                                          newest_version=True)]

        return self.dbu.ProcessqueueRawadd(f_ids, incVersion)

    def reprocessByCode(self, id_in, startDate=None, endDate=None, incVersion=None):
        """Add files made by a code to the queue for reprocessing.

        Parameters
        ----------
        id_in : :class:`str` or :class:`int`
            ID or filename of code to reprocess
        startDate : :class:`~datetime.datetime`, optional
            First date to reprocess (default all)
        endDate : :class:`~datetime.datetime`, optional
            Last date to reprocess (default all)
        incVersion : :class:`int` {0, 1, 2}, optional
            Which version number to increment: major (0), minor (1),
            subminor (2). Forces reprocessing. (default do not force).
        """
        try:
            code_id = self.dbu.getCodeID(id_in)
            return self._reprocessBy(code=code_id, startDate=startDate, endDate=endDate,
                                     incVersion=incVersion)
        except DButils.DBNoData:
            DBlogging.dblogger.error('No code_id {0} found in the DB'.format(id_in))

    def reprocessByProduct(self, id_in, startDate=None, endDate=None, incVersion=None):
        """Add files of a particular product to the queue for reprocessing.

        Parameters
        ----------
        id_in : :class:`str` or :class:`int`
            ID or name of code to reprocess
        startDate : :class:`~datetime.datetime`, optional
            First date to reprocess (default all)
        endDate : :class:`~datetime.datetime`, optional
            Last date to reprocess (default all)
        incVersion : :class:`int` {0, 1, 2}, optional
            Which version number to increment: major (0), minor (1),
            subminor (2). Forces reprocessing (default do not force).
        """
        try:
            prod_id = self.dbu.getProductID(id_in)
            return self._reprocessBy(product=prod_id, startDate=startDate, endDate=endDate,
                                     incVersion=incVersion)
        except DButils.DBNoData:
            DBlogging.dblogger.error('No product_id {0} found in the DB'.format(id_in))

    def reprocessByDate(self, startDate=None, endDate=None, incVersion=None, level=None):
        """Add files to the queue for reprocessing, by file date.

        Parameters
        ----------
        startDate : :class:`~datetime.datetime`, optional
            First date to reprocess (default all)
        endDate : :class:`~datetime.datetime`, optional
            Last date to reprocess (default all)
        incVersion : :class:`int` {0, 1, 2}, optional
            Which version number to increment: major (0), minor (1),
            subminor (2). Forces reprocessing (default do not force).
        level : :class:`float`, optional
            Only reprocess files of this level (default all)
        """
        return self._reprocessBy(startDate=startDate, endDate=endDate,
                                 incVersion=incVersion, level=level)

    def reprocessByInstrument(self, id_in, level=None, startDate=None, endDate=None, incVersion=None):
        """Add files for an instrument to the queue for reprocessing

        Parameters
        ----------
        id_in : :class:`str` or :class:`int`
            ID or name of instrument to reprocess
        level : :class:`int`, optional
            Only reprocess files of this level (default all)
        startDate : :class:`~datetime.datetime`, optional
            First date to reprocess (default all)
        endDate : :class:`~datetime.datetime`, optional
            Last date to reprocess (default all)
        incVersion : :class:`int` {0, 1, 2}, optional
            Which version number to increment: major (0), minor (1),
            subminor (2). Forces reprocessing (default do not force).
        """
        try:
            inst_id = self.dbu.getInstrumentID(id_in)
            return self._reprocessBy(instrument=inst_id, level=level, startDate=startDate, endDate=endDate,
                                     incVersion=incVersion)
        except DButils.DBNoData:
            DBlogging.dblogger.error('No inst_id {0} found in the DB'.format(id_in))

