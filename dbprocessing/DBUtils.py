import itertools
import sqlalchemy
import glob
from sqlalchemy.orm import sessionmaker
import os.path
import datetime
import numpy as np
from sqlalchemy import Table #Column, Integer, String, DateTime, BigInteger, Boolean, Date, Float, Table
from sqlalchemy.orm import mapper # sessionmaker
try: # new version changed this annoyingly
    from sqlalchemy.exceptions import IntegrityError
except ImportError:
    from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import asc #, desc
from sqlalchemy import or_
import DBlogging
import socket # to get the local hostname
import sys

import DBStrings
import Version
from Diskfile import calcDigest, DigestError

## This goes in the processing comment field in the DB, do update it
__version__ = '2.0.3'



#########################################################
## NOTES, read these if new to this module
#########################################################
# - functions are in transition from returning the thing the name says e.g. getFileID returens a number to
#      instead returning the sqlalcheml object that meets the criteria so getFileID would return a File instance
#      and the user would then have to get the ID by using File.file_id.  This makes for fewer functions and is
#      significantly cleaner in a few spots



class DBError(Exception):
    pass
class DBProcessingError(Exception):
    pass
class FilenameParse(Exception):
    pass
class DBNoData(Exception):
    pass


class DBUtils(object):
    """
    @summary: DBUtils - utility routines for the DBProcessing class, all of these may be user called but are meant to
    be internal routines for DBProcessing
    """

    def __init__(self, mission='Test', db_var=None):
        """
        @summary: Initialize the DBUtils class, default mission is 'Test'
        """
        self.dbIsOpen = False
        if mission == None:
            raise(DBError("Must input mission name to create DBUtils instance"))
        self.mission = mission
        #Expose the format/regex routines of DBFormatter
        fmtr = DBStrings.DBFormatter()
        self.format = fmtr.format
        self.re = fmtr.re
        self._openDB(db_var)
        self._createTableObjects()
        self._patchProcessQueue()

    def __del__(self):
        """
        try and clean up a little bit
        """
        self._closeDB()

    def __repr__(self):
        """
        @summary: Print out something useful when one prints the class instance

        @return: DBProcessing class instance for mission <mission name>
        """
        return 'DBProcessing class instance for mission ' + self.mission + ', version: ' + __version__

    @classmethod
    def _test_SQLAlchemy_version(self, version= sqlalchemy.__version__):
        """This tests the version to be sure that it is compatible"""
        expected = '0.7'
        if version[0:len(expected)] != expected:
            raise DBError(
                "SQLAlchemy version %s was not expected, expected %s.x" %
                (version, expected))
        return True

    def _patchProcessQueue(self):
        self.Processqueue.flush = self._processqueueFlush
        self.Processqueue.remove = self._processqueueRemoveItem
        self.Processqueue.getAll = self. _processqueueGetAll
        self.Processqueue.push = self._processqueuePush
        self.Processqueue.len = self._processqueueLen
        self.Processqueue.pop = self._processqueuePop
        self.Processqueue.get = self._processqueueGet
        self.Processqueue.clean = self._processqueueClean

####################################
###### DB and Tables ###############
####################################

    def _openDB(self, db_var=None, verbose=False):
        """
        setup python to talk to the database, this is where it is, name and password.

        @keyword verbose: (optional) - print information out to the command line

        @todo: change the user form owner to ops as DB permnissons are fixed

        >>>  pnl._openDB()
        """
        if self.dbIsOpen == True:
            return
        try:

            if self.mission in  ['Test', 'rbsp']:
                engine = sqlalchemy.create_engine('postgresql+psycopg2://rbsp_owner:rbsp_owner@edgar:5432/rbsp', echo=False)

            elif self.mission == 'unittest':
                if db_var is None:
                    engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=False)
                else:
                    engine = db_var.engine

            DBlogging.dblogger.info("Database Connection opened: {0}".format(str(engine)))

        except DBError:
            (t, v, tb) = sys.exc_info()
            raise(DBError('Error creating engine: ' + str(v)))
        try:
            metadata = sqlalchemy.MetaData(bind=engine)
            # a session is what you use to actually talk to the DB, set one up with the current engine
            Session = sessionmaker(bind=engine)
            session = Session()
            self.engine = engine
            self.metadata = metadata
            self.session = session
            self.dbIsOpen = True
            if verbose: print("DB is open: %s" % (engine))
            return
        except Exception, msg:
            raise(DBError('Error opening database: %s'% (msg)))


    def _createTableObjects(self, verbose = False):
        """
        cycle through the database and build classes for each of the tables

        @keyword verbose: (optional) - print information out to the command line

        >>>  pnl._createTableObjects()
        """
        DBlogging.dblogger.debug("Entered _createTableObjects()")

## ask for the table names form the database (does not grab views)
        table_names = self.engine.table_names()

## create a dictionary of all the table names that will be used as class names.
## this uses the db table name as the table name and a cap 1st letter as the class
## when interacting using python use the class
        table_dict = {}
        for val in table_names:
            table_dict[val.title()] = val
##  dynamically create all the classes (c1)
##  dynamically create all the tables in the db (c2)
##  dynamically create all the mapping between class and table (c3)
## this just saves a lot of typing and is equivalent to:
##     class Missions(object):
##         pass
##     missions = Table('missions', metadata, autoload=True)
##     mapper(Missions, missions)
#        try:
        for val in table_dict:
            if verbose: print val
            if not hasattr(self, val):  # then make it
                c1 = compile("""class %s(object):\n\tpass""" % (val), '', 'exec')
                c2 = compile("%s = Table('%s', self.metadata, autoload=True)" % (str(table_dict[val]), table_dict[val]) , '', 'exec')
                c3 = compile("mapper(%s, %s)" % (val, str(table_dict[val])), '', 'exec')
                c4 = compile("self.%s = %s" % (val, val), '', 'exec' )
                exec(c1)
                exec(c2)
                exec(c3)
                exec(c4)
                if verbose: print("Class %s created" % (val))
                DBlogging.dblogger.debug("Class %s created" % (val))


#####################################
####  Do processing and input to DB
#####################################

    def _currentlyProcessing(self):
        """
        Checks the db to see if it is currently processing, don't want to do 2 at the same time

        @return: false or the pid
        @rtype: (bool, long)

        >>>  pnl._currentlyProcessing()
        """
        DBlogging.dblogger.info("Checking currently_processing")

        sq = self.session.query(self.Logging).filter_by(currently_processing = True).all()
        if len(sq) == 1:
            DBlogging.dblogger.warning("currently_processing is set.  PID: {0}".format(sq[0].pid))
            return sq[0].pid
        elif len(sq) == 0:
            return False
        else:
            DBlogging.dblogger.error("More than one currently_processing flag set, fix the DB" )
            raise(DBError("More than one currently_processing flag set, fix the DB"))

    def _resetProcessingFlag(self, comment=None):
        """
        Query the db and reset a processing flag

        @keyword comment: the comment to enter into the processing log DB
        @return: True - Success, False - Failure

        >>>  pnl._resetProcessingFlag()
        """
        if comment == None:
            raise(ValueError("Must enter a comment to override DB lock"))
        sq = self.session.query(self.Logging).filter_by(currently_processing = True)
        for val in sq:
            val.currently_processing = False
            val.processing_end = datetime.datetime.utcnow()
            val.comment = 'Overridden:' + comment + ':' + __version__
            DBlogging.dblogger.error( "Logging lock overridden: %s" % ('Overridden:' + comment + ':' + __version__) )
            self.session.add(val)
        self._commitDB()

    def _startLogging(self):
        """
        Add an entry to the logging table in the DB, logging

        >>>  pnl._startLogging()
        """
        # this is the logging of the processing, no real use for it yet but maybe we will in the future
        # helps to know is the process ran and if it succeeded
        if self._currentlyProcessing():
            raise(DBError('A Currently Processing flag is still set, cannot process now'))
        # save this class instance so that we can finish the logging later
        self.__p1 = self._addLogging(True,
                              datetime.datetime.utcnow(),
                              self.getMissionID(self.mission),
                              os.getlogin(),
                              socket.gethostname(),
                              pid = os.getpid() )
        DBlogging.dblogger.info( "Logging started: %d: %s, PID: %s, M_id: %s, user: %s, hostmane: %s" %
                                 (self.__p1.logging_id, self.__p1.processing_start_time, self.__p1.pid,
                                  self.__p1.mission_id, self.__p1.user, self.__p1.hostname) )

    def _addLogging(self,
                    currently_processing,
                    processing_start_time,
                    mission_id,
                    user,
                    hostname,
                    pid=None,
                    processing_end_time= None,
                    comment=None):
        """
        add an entry to the logging table

        @param currently_processing: is the db currently processing?
        @type currently_processing: bool
        @param processing_start_time: the time the processing started
        @type processing_start_time: datetime.datetime
        @param mission_id: the mission id the processing if for
        @type mission_id: int
        @param user: the user doing the processing
        @type user: str
        @param hostname: the hostname that initiated the processing
        @type hostname: str

        @keyword pid: the process id that id the processing
        @type pid: int
        @keyword processing_end_time: the time the processing stopped
        @type processing_end_time: datetime.datetime
        @keyword comment: comment about the processing run
        @type comment: str

        @return: instance of the Logging class
        @rtype: Logging

        """
        l1 = self.Logging()
        l1.currently_processing = currently_processing
        l1.processing_start_time = processing_start_time
        l1.mission_id = mission_id
        l1.user = user
        l1.hostname = hostname
        l1.pid = pid
        l1.processing_end_time = processing_end_time
        l1.comment = comment
        self.session.add(l1)
        self._commitDB()
        return l1    # so we can use the same session to stop the logging

    def _stopLogging(self, comment):
        """
        Finish the entry to the processing table in the DB, logging

        @param comment: (optional) a comment to insert into he DB
        @type param: str

        >>>  pnl._stopLogging()
        """
        try: self.__p1
        except:
            DBlogging.dblogger.warning( "Logging was not started, can't stop")
            raise(DBProcessingError("Logging was not started"))
        # clean up the logging, we are done processing and we can release the lock (currently_processing) and
        # put in the complete time

        self.__p1.processing_end = datetime.datetime.utcnow()
        self.__p1.currently_processing = False
        self.__p1.comment = comment+':' + __version__
        self.session.add(self.__p1)
        self._commitDB()
        DBlogging.dblogger.info( "Logging stopped: %s comment '%s' " % (self.__p1.processing_end, self.__p1.comment) )
        del self.__p1

    def _checkDiskForFile(self, file_id, fix=False):
        """
        Check the filesystem to see if the file exits or not as it says in the db

        return true is consistent, False otherwise

        @keyword fix: (optional) set to have the DB fixed to match the filesystem
           this is **NOT** sure to be safe
        """
        file_id = self.getFileID(file_id)
        sq = self.getEntry('File', file_id)
        if sq.exists_on_disk:
            file_path = self.getFileFullPath(file_id)
            if not os.path.exists(file_path):
                if fix:
                    sq.exists_on_disk = False
                    self.session.add(sq)
                    self._commitDB()
                    return self._checkDiskForFile(file_id) # call again to get the True
                else:
                    return False
            else:
                return True
        else:
            return True

    def _processqueueFlush(self):
        """
        remove everything from he process queue
        """
        length = self.Processqueue.len()
        self.session.query(self.Processqueue).delete()
        self._commitDB()
        DBlogging.dblogger.info( "Processqueue was cleared")
        return length

    def _processqueueRemoveItem(self, item):
        """
        remove a file from the queue by name or number
        """
        item = self.getFileID(item)
        contents = self.Processqueue.getAll()
        try:
            ind = contents.index(item)
            self.Processqueue.pop(ind)
        except ValueError:
            raise(DBNoData("No Item ID={0} found".format(item)))

    def _processqueueGetAll(self):
        """
        return the entire contents of the process queue
        """
        try:
            pqdata = zip(*self.session.query(self.Processqueue.file_id).all())[0]
        except IndexError:
            pqdata = self.session.query(self.Processqueue.file_id).all()
        DBlogging.dblogger.debug( "Entire Processqueue was read: {0} elements returned".format(len(pqdata)))
        return pqdata

    def _processqueuePush(self, fileid):
        """
        push a file onto the process queue (onto the right)

        Parameters
        ==========
        fileid : (int, string)
            the file id (or name to put on the process queue)

        Returns
        =======
        file_id : int
            the file_id that was passed in, but grabbed from the db
        """
        if hasattr(fileid, '__iter__'):
            ans = []
            for v in fileid:
                ans.extend(self.Processqueue.push(v))
            return ans
        fileid = self.getFileID(fileid)
        pq1 = self.Processqueue()
        pq1.file_id = fileid
        self.session.add(pq1)
        DBlogging.dblogger.info( "File added to process queue {0}:{1}".format(fileid, self.getEntry('File', fileid).filename ) )
        self._commitDB()
        pqid = self.session.query(self.Processqueue.file_id).all()
        return pqid[-1]

    def _processqueueLen(self):
        """
        return the number of files in the process queue
        """
        return self.session.query(self.Processqueue).count()

    def _processqueuePop(self, index=0):
        """
        pop a file off the process queue (from the left)

        Other Parameters
        ================
        index : int
            the index in the queue to pop

        Returns
        =======
        file_id : int
            the file_id of the file popped from the queue
        """
        num = self.Processqueue.len()
        if num == 0:
            return None
        elif index >= num:
            return None
        else:
            for ii, fid in enumerate(self.session.query(self.Processqueue)):
                if ii == index:
                    self.session.delete(fid)
                    fid_ret = fid.file_id
                    break # there can be only one
            self._commitDB()
            return fid_ret

    def _processqueueGet(self, index=0):
        """
        get the file at the head of the queue (from the left)

        Returns
        =======
        file_id : int
            the file_id of the file popped from the queue
        """
        num = self.Processqueue.len()
        if num == 0:
            DBlogging.dblogger.info( "processqueueGet() returned: None (empty queue)")
            return None
        elif index >= num:
            DBlogging.dblogger.info( "processqueueGet() returned: None (requested index larger than size)")
            return None
        else:
            for ii, fid in enumerate(self.session.query(self.Processqueue)):
                if ii == index:
                    fid_ret = fid.file_id
                    break # there can be only one
            DBlogging.dblogger.info( "processqueueGet() returned: {0}".format(fid_ret) )
            return fid_ret

    def _processqueueClean(self):
        """
        go through the process queue and clear out lower versions of the same files
        this is determined by product and utc_file_date
        """
        # TODO this might break with weekly input files
        DBlogging.dblogger.debug("Entering in queueClean(), there are {0} entries".format(self.Processqueue.len()))
        pqdata = self.Processqueue.getAll()
        if len(pqdata) <= 1: # can't clean just one (or zero) entries
            return

        ans = [] # this will hold the unique file_id's to put back on the queue

        file_entries = [self.getEntry('File', val) for val in pqdata]
        # setup a tuple of (product_id, utc_file_date)
        dat = [(val.product_id, val.utc_file_date) for val in file_entries]
        # now we want just the unique enetries
        uniq_dat = list(set(dat))
        # step through the uniq ones and if there is more than one drop
        for uval in uniq_dat:
            # should be albe t do a bailout here, TODO
            # create a new list of just those
            tmp = [val for val in file_entries if val.product_id == uval[0] and val.utc_file_date == uval[1]]
            mx = max(tmp, key=lambda x: Version.Version(x.interface_version, x.quality_version, x.revision_version))
            ans.append(mx.file_id)

        ## now we have a list of just the unique file_id's
        self.Processqueue.flush()
        self.Processqueue.push(ans)
        DBlogging.dblogger.debug("Done in queueClean(), there are {0} entries left".format(self.Processqueue.len()))

    def _purgeFileFromDB(self, filename=None, recursive=False):
        """
        removes a file from the DB

        @keyword filename: name of the file to remove (or a list of names)
        @return: True - Success, False - Failure

        if recursive then it removes all files that depend on the one to remove

        >>>  pnl._purgeFileFromDB('Test-one_R0_evinst-L1_20100401_v0.1.1.cdf')

        """
        if not hasattr(filename, '__iter__'): # if not an iterable make it a iterable
            filename = [filename]
        for f in filename:
            f = self.getFileID(f)
            # we need to look in each table that could have a reference to this file and delete that
            ## processqueue
            try:
                self.Processqueue.remove(f)
            except DBNoData:
                pass
            ## filefilelink
            try:
                self.delFilefilelink(f)
            except DBNoData:
                pass
            ## filecodelink
            try:
                self.delFilecodelink(f)
            except DBNoData:
                pass
            ## file
            self.session.delete(self.getEntry('File', f))
            self._commitDB()

    def deleteAllEntries(self):
        """
        delete all entries from the DB (leaves mission, satellite, instrument)
        """
        # clean everything out
        self.session.query(self.Processqueue).delete()
        self.session.query(self.Filefilelink).delete()
        self.session.query(self.Filecodelink).delete()
        self.session.query(self.File).delete()
        self.session.query(self.Code).delete()
        self.session.query(self.Inspector).delete()
        self.session.query(self.Instrumentproductlink).delete()
        self.session.query(self.Productprocesslink).delete()
        self.session.query(self.Process).delete()
        self.session.query(self.Product).delete()
        self.session.query(self.Logging).delete()
        self._commitDB()

    def getAllFilenames(self):
        """
        return all the filenames in the database
        """
        ans = []
        sq = self.session.query(self.File.filename).all()
        for v in sq:
            ans.append( (v[0], self.getFileFullPath(v[0])) )
        return ans

    def addMission(self,
                    mission_name,
                    rootdir):
        """ add a mission to the database

        @param mission_name: the name of the mission
        @type mission_name: str
        @param rootdir: the root directory of the mission
        @type rootdir: str

        """
        if not isinstance(mission_name, str):
            raise(ValueError("Mission name has to  a string"))
        if not isinstance(rootdir, str):
            raise(ValueError("Rootdir must be a string"))
        try:
            m1 = self.Mission()
        except AttributeError:
            raise(DBError("Class Mission not found was it created?"))

        m1.mission_name = mission_name
        m1.rootdir = rootdir
        self.session.add(m1)
        self._commitDB()
        return m1.mission_id

    def addSatellite(self,
                    satellite_name,):
        """ add a satellite to the database

        @param satellite_name: the name of the mission
        @type satellite_name: str
        """
        if not isinstance(satellite_name, str):
            raise(ValueError("Satellite name has to  a string"))

        try:
            s1 = self.Satellite()
        except AttributeError:
            raise(DBError  ("Class Satellite not found was it created?"))
        s1.mission_id = self.getMissionID(self.mission)
        s1.satellite_name = satellite_name
        self.session.add(s1)
        self._commitDB()
        return self.getSatelliteID(satellite_name)

    def addProcess(self,
                    process_name,
                    output_product,
                    output_timebase,
                    extra_params=None,
                    super_process_id=None):
        """ add a process to the database

        @param process_name: the name of the process
        @type process_name: str
        @param output_product: the output product id
        @type output_product: int
        @keyword extra_params: extra parameters to pass to the code
        @type extra_params: str
        @keyword super_process_id: the process id of the superprocess for this process
        @type super_process_id: int
        """
        if output_timebase not in ['ORBIT', 'DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY', 'FILE']:
            raise(ValueError("output_timebase invalid choice"))

        p1 = self.Process()
        p1.output_product = output_product
        p1.process_name = process_name
        p1.extra_params = extra_params
        p1.output_timebase = output_timebase
        p1.super_process_id = super_process_id
        self.session.add(p1)
        self._commitDB()
        self.updateProcessSubs(p1.process_id)
        return p1.process_id

    def addProduct(self,
                    product_name,
                    instrument_id,
                    relative_path,
                    super_product_id,
                    format,
                    level):
        """ add a product to the database

        @param product_name: the name of the product
        @type product_name: str
        @param instrument_id: the instrument   the product is from
        @type instrument_id: int
        @param relative_path:relative path for the product
        @type relative_path: str
        @param super_product_id: the product id of the super product for this product
        @type super_product_id: int
        @param format: the format of the product files
        @type super_product_id: str
        """
        p1 = self.Product()
        p1.instrument_id = instrument_id
        p1.product_name = product_name
        p1.relative_path = relative_path
        p1.super_product_id = super_product_id
        p1.format = format
        p1.level = level
        self.session.add(p1)
        self._commitDB()
        return p1.product_id

    def updateProductSubs(self, product_id):
        """
        update an existing product performing the {} replacements
        """
        # need to do {} replacement, have to do it as a modification
        p1 = self.getEntry('Product', product_id)

        product_id = p1.product_id
        product_name = self._nameSubProduct(p1.product_name, product_id)
        p1.product_name = product_name
        relative_path = self._nameSubProduct(p1.relative_path, product_id)
        p1.relative_path = relative_path
        fmt = self._nameSubProduct(p1.format, product_id)
        p1.format = fmt
        self.session.add(p1)
        self._commitDB()

    def updateProcessSubs(self, proc_id):
        """
        update an existing product performing the {} replacements
        """
        # need to do {} replacement, have to do it as a modification
        p1 = self.getEntry('Process', proc_id)
        proc_id = p1.process_id
        process_name = self._nameSubProcess(p1.process_name, proc_id)
        p1.process_name = process_name
        extra_params = self._nameSubProcess(p1.extra_params, proc_id)
        p1.extra_params = extra_params
        self.session.add(p1)
        self._commitDB()

    def addproductprocesslink(self,
                    input_product_id,
                    process_id,
                    optional):
        """ add a product process link to the database

        @param input_product_id: id of the product to link
        @type input_product_id: int
        @param process_id: id of the process to link
        @type process_id: int

        """
        ppl1 = self.Productprocesslink()
        ppl1.input_product_id = input_product_id
        ppl1.process_id = process_id
        ppl1.optional = optional
        self.session.add(ppl1)
        self._commitDB()
        return ppl1.input_product_id, ppl1.process_id

    def addFilecodelink(self,
                     resulting_file_id,
                     source_code):
        """ add a file code  link to the database

        @param resulting_file_id: id of the product to link
        @type resulting_file_id: int
        @param source_code: id of the process to link
        @type source_code: int

        """
        fcl1 = self.Filecodelink()
        fcl1.resulting_file = resulting_file_id
        fcl1.source_code = source_code
        self.session.add(fcl1)
        self._commitDB()
        return fcl1.resulting_file, fcl1.source_code

    def delInspector(self, i):
        """
        removes an inspector form the db
        """
        insp = self.getEntry('Inspector', i)
        self.session.delete(insp)
        self._commitDB()

    def delFilefilelink(self, f):
        """
        remove entries from Filefilelink, it will remove if the file is in either column
        """
        f = self.getFileID(f) # change a name to a number
        n1 = self.session.query(self.Filefilelink).filter_by(source_file = f).delete()
        n2 = self.session.query(self.Filefilelink).filter_by(resulting_file = f).delete()
        if n1+n2 == 0:
            raise(DBNoData("No entry for ID={0} found".format(f)))
        else:
            self._commitDB()

    def delFilecodelink(self, f):
        """
        remove entries from Filecodelink fore a given file
        """
        f = self.getFileID(f) # change a name to a number
        n2 = self.session.query(self.Filecodelink).filter_by(resulting_file = f).delete()
        if n2 == 0:
            raise(DBNoData("No entry for ID={0} found".format(f)))
        else:
            self._commitDB()

    def addFilefilelink(self,
                     resulting_file_id,
                     source_file,):
        """ add a file file  link to the database

        @param source_file: id of the product to link
        @type source_file: int
        @param resulting_file_id: id of the process to link
        @type resulting_file_id: int

        """
        ffl1 = self.Filefilelink()
        ffl1.source_file = source_file
        ffl1.resulting_file = resulting_file_id
        self.session.add(ffl1)
        self._commitDB()
        return ffl1.source_file, ffl1.resulting_file

    def addInstrumentproductlink(self,
                     instrument_id,
                     product_id):
        """ add a instrument product  link to the database

        @param instrument_id: id of the instrument to link
        @type instrument_id: int
        @param product_id: id of the product to link
        @type product_id: int
        """
        ipl1 = self.Instrumentproductlink()
        ipl1.instrument_id = instrument_id
        ipl1.product_id = product_id
        self.session.add(ipl1)
        self._commitDB()
        return ipl1.instrument_id, ipl1.product_id

    def addInstrument(self,
                    instrument_name,
                    satellite_id):
        """ add a Instrument to the database

        @param instrument_name: the name of the mission
        @type instrument_name: str
        @param satellite_id: the root directory of the mission
        @type satellite_id: int
        """
        i1 = self.Instrument()

        i1.satellite_id = satellite_id
        i1.instrument_name = instrument_name
        self.session.add(i1)
        self._commitDB()
        return i1.instrument_id

    def addCode(self,
                   filename,
                   relative_path,
                   code_start_date,
                   code_stop_date,
                   code_description,
                   process_id,
                   version,
                   active_code,
                   date_written,
                   output_interface_version,
                   newest_version,
                   arguments=None):
        """
        Add an executable code to the DB

        @param filename: the filename of the code
        @type filename: str
        @param relative_path: the relative path (relative to mission base dir)
        @type relative_path: str
        @param code_start_date: start of validity of the code (datetime)
        @type code_start_date: datetime
        @param code_stop_date: end of validity of the code (datetime)
        @type code_stop_date: datetime
        @param code_description: description of the code (50 char)
        @type code_description: str
        @param process_id: the id of the process this code is part of
        @type process_id: int
        @param version: the version of the code
        @type version: Version.Version
        @param active_code: Boolean True means the code is active
        @type active_code: Boolean
        @param date_written: the date the cod was written
        @type date_written: date
        @param output_interface_version: the interface version of the output (effects the data file names)
        @type output_interface_version: int
        @param newest_version: is this code the newest version in the DB?
        @type newest_version: bool

        @return: the code_id of the newly inserted code
        @rtype: long
        """
        c1 = self.Code()
        c1.filename = filename
        c1.relative_path = relative_path
        c1.code_start_date = code_start_date
        c1.code_stop_date = code_stop_date
        c1.code_description = code_description
        c1.process_id = process_id
        c1.interface_version = version.interface
        c1.quality_version =version.quality
        c1.revision_version = version.revision
        c1.active_code = active_code
        c1.date_written = date_written
        c1.output_interface_version = output_interface_version
        c1.newest_version = newest_version
        c1.arguments = arguments

        self.session.add(c1)
        self._commitDB()
        return c1.code_id

    def addInspector(self,
                   filename,
                   relative_path,
                   description,
                   version,
                   active_code,
                   date_written,
                   output_interface_version,
                   newest_version,
                   product,
                   arguments=None):
        """
        Add an executable code to the DB

        @param filename: the filename of the code
        @type filename: str
        @param relative_path: the relative path (relative to mission base dir)
        @type relative_path: str
        @param description: description of the code (50 char)
        @type description: str
        @param product: the id of the product this inspector finds
        @type product: int
        @param version: the version of the code
        @type version: Version.Version
        @param active_code: Boolean True means the code is active
        @type active_code: Boolean
        @param date_written: the date the cod was written
        @type date_written: date
        @param output_interface_version: the interface version of the output (effects the data file names)
        @type output_interface_version: int
        @param newest_version: is this code the newest version in the DB?
        @type newest_version: bool

        @return: the inspector_id of the newly inserted code
        @rtype: long

        """
        c1 = self.Inspector()
        c1.filename = filename
        c1.relative_path = relative_path
        # need to do {} replacement
        description = self._nameSubProduct(description, product)
        c1.description = description
        c1.product = self.getProductID(product)
        c1.interface_version = version.interface
        c1.quality_version = version.quality
        c1.revision_version = version.revision
        c1.active_code = active_code
        c1.date_written = date_written
        c1.output_interface_version = output_interface_version
        c1.newest_version = newest_version
        c1.arguments = self._nameSubProduct(arguments, product)

        self.session.add(c1)
        self._commitDB()
        return c1.inspector_id

    def _nameSubProduct(self, inStr, product_id):
        """
        in inStr replace the standard {} with the names
        """
        if inStr is None:
            return inStr
        repl = ['{INSTRUMENT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}']
        ftb = self.getProductTraceback(product_id)
        if '{INSTRUMENT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{MISSION}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{MISSION}', ftb['mission'].mission_name)
        if '{PRODUCT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{PRODUCT}', ftb['product'].product_name)
        if '{LEVEL}' in inStr :
            inStr = inStr.replace('{LEVEL}', str(ftb['product'].level))
        if '{ROOTDIR}' in inStr :
            inStr = inStr.replace('{ROOTDIR}', str(ftb['mission'].rootdir))
        if any(val in inStr for val in repl): # call yourself again
            inStr = self._nameSubProduct(inStr, product_id)
        return inStr

    def _nameSubProcess(self, inStr, process_id):
        """
        in inStr replace the standard {} with the names
        """
        p_id = self.getProcessID(process_id)
        if inStr is None:
            return inStr
        repl = ['{INSTRUMENT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}']
        ftb = self.getProcessTraceback(p_id)
        if '{INSTRUMENT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{MISSION}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{MISSION}', ftb['mission'].mission_name)
        if '{PRODUCT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{PRODUCT}', ftb['input_product'][0][0].product_name)
        if '{LEVEL}' in inStr :
            inStr = inStr.replace('{LEVEL}', str(ftb['input_product'][0][0].level))
        if '{ROOTDIR}' in inStr :
            inStr = inStr.replace('{ROOTDIR}', str(ftb['mission'].rootdir))
        if any(val in inStr for val in repl): # call yourself again
            inStr = self._nameSubProcess(inStr, p_id)
        return inStr

    def _nameSubFile(self, inStr, file_id):
        """
        in inStr replace the standard {} with the names
        """
        if inStr is None:
            return inStr
        ftb = self.getFileTraceback(file_id)
        if '{INSTRUMENT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{MISSION}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{MISSION}', ftb['mission'].mission_name)
        if '{LEVEL}' in inStr :
            inStr = inStr.replace('{LEVEL}', str(ftb['product'].level))
        return inStr

    def _commitDB(self):
        """
        do the commit to the DB
        """
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))

    def _closeDB(self):
        """
        Close the database connection

        @keyword verbose: (optional) print information out to the command line

        >>>  pnl._closeDB()
        """
        if self.dbIsOpen == False:
            return
        try:
            self.session.close()
            self.dbIsOpen = False
            DBlogging.dblogger.info( "Database connection closed" )
        except DBError:
            DBlogging.dblogger.error( "Database connection could not be closed" )
            raise(DBError('could not close DB'))

    def addFile(self,
                filename = None,
                data_level = None,
                version = None,
                file_create_date = None,
                exists_on_disk = None,
                utc_file_date = None,
                utc_start_time = None,
                utc_stop_time = None,
                check_date = None,
                verbose_provenance = None,
                quality_comment = None,
                caveats = None,
                met_start_time = None,
                met_stop_time = None,
                product_id = None,
                newest_version = None,
                md5sum = None,
                process_keywords = None):
        """
        add a datafile to the database

        @param filename: filename to add
        @type filename: str
        @param data_level: the data level of the file
        @type data_level: float
        @param version: the version of te file to create
        @type version: Version.Version
        @param file_create_date: date the fie was created
        @type file_create_date: datetime.datetime
        @param exists_on_disk: does the file exist on disk?
        @type exists_on_disk: bool
        @param product_id: the product id of he product he file belongs to
        @type product_id: int

        @keyword utc_file_date: The UTC date of the file
        @type utc_file_date: datetime.date
        @keyword utc_start_time: utc start time of the file
        @type utc_start_time: datetime.datetime
        @keyword utc_end_time: utc end time of the file
        @type utc_end_time: datetime.datetime
        @keyword check_date: the date the file was quality checked
        @type check_date: datetime.datetime
        @keyword verbose_provenance: Verbose provenance of the file
        @type verbose_provenance: str
        @keyword quality_comment: comment on quality from quality check
        @type quality_comment: str
        @keyword caveats: caveats associated with the file
        @type caveates: str
        @keyword met_start_time: met start time of the file
        @type met_start_time: long
        @keyword met_stop_time: met stop time of the file
        @type met_stop_time: long

        @ return: file_id of the newly inserted file
        @rtype: long
        """
        d1 = self.File()

        self._createTableObjects()
        d1 = self.File()
        d1.filename = filename
        d1.utc_file_date = utc_file_date
        d1.utc_start_time = utc_start_time
        d1.utc_stop_time = utc_stop_time
        d1.data_level = data_level
        d1.check_date= check_date
        d1.verbose_provenance = verbose_provenance
        d1.quality_comment = quality_comment
        d1.caveats = caveats
        d1.interface_version = version.interface
        d1.quality_version = version.quality
        d1.revision_version = version.revision
        d1.file_create_date = file_create_date
        d1.product_id = product_id
        d1.met_start_time = met_start_time
        d1.met_stop_time = met_stop_time
        d1.exists_on_disk = exists_on_disk
        d1. newest_version  = newest_version
        d1.md5sum = md5sum
        d1.process_keywords = process_keywords
        self.session.add(d1)
        self._commitDB()
        return d1.file_id

    def _codeIsActive(self, ec_id, date):
        """
        Given a ec_id and a date is that code active for that date

        @param ec_id: executable code id to see if is active
        @param date: date object to use when checking

        @return: True - the code is active for that date, False otherwise

        """
        # can only be one here (sq)
        code = self.getEntry('Code', ec_id)
        if code.active_code == False:
            return False
        try:
            if code.code_start_date > date:
                return False
            if code.code_stop_date < date:
                return False
        except TypeError:
            if code.code_start_date > date.date():
                return False
            if code.code_stop_date < date.date():
                return False
        return True

    def getFileFullPath(self, filename):
        """
        return the full path to a file given the name or id
        (name or id is based on type)

        """
        filename = self.getEntry('File', filename).filename
        file_id = self.getFileID(filename)
        # need to know file product and mission to get whole path
        ftb = self.getFileTraceback(file_id)
        rel_path = ftb['product'].relative_path
        try:
            root_dir = ftb['mission'].rootdir
        except KeyError:
            raise(DBError("Mission root directory not set, fix the DB"))
        return os.path.join(root_dir, rel_path, filename)

    def getProcessFromInputProduct(self, product):
        """
        given an product name or id return all the processes that use that as an input
        """
        p_id = self.getProductID(product)
        sq = self.session.query(self.Productprocesslink).filter_by(input_product_id = p_id).all()
        ans = []
        for v in sq:
            ans.append(v.process_id)
        return ans

    def getProcessID(self, proc_name):
        """
        given a process name return its id
        """
        try:
           proc_id = long(proc_name)
           self.session.query(self.Process).get(proc_id)
        except ValueError: # it is not a number
            proc_id = self.session.query(self.Process.process_id).filter_by(process_name = proc_name).all()[0][0]
        return proc_id

    def getFileVersion(self, filename):
        """
        given a filename or fileid return a Version instance
        """
        fle = self.getEntry('File', filename)
        return Version.Version(fle.interface_version, fle.quality_version, fle.revision_version)

    def getFileMission(self, filename):
        """
        given an a file name or a file ID return the mission(s) that file is
        associated with
        """
        filename = self.getFileID(filename) # change a name to a number
        product_id = self.getEntry('File', filename).product_id
        # get all the instruments
        inst_id = self.getInstrumentFromProduct(product_id)
        # get all the satellites
        sat_id = self.getEntry('Instrument', inst_id).satellite_id
        # get the missions
        mission = self.getSatelliteMission(sat_id)
        return mission

    def getSatelliteMission(self, sat_name):
        """
        given a satellite or satellite id return the mission
        """
        s_id = self.getSatelliteID(sat_name) # is a name
        m_id = self.getEntry('Satellite', s_id).mission_id
        return self.getEntry('Mission', m_id)

    def getInstrumentFromProduct(self, product_id):
        """
        given a product ID get the instrument(s) id associated with it
        """
        sq = self.session.query(self.Instrumentproductlink.instrument_id).filter_by(product_id = product_id).all()
        inst_id = [v[0] for v in sq]
        if len(inst_id) == 1: # it should
            return inst_id[0]
        else:
            return inst_id

    def getInstrumentID(self, name, satellite_id=None):
        """
        Return the instrument_id for a given instrument

        @return: instrument_id - the instrument ID

        """
        try:
            i_id = long(name)
            sq = self.session.query(self.Instrument).get(i_id)
            if sq is None:
                raise(DBNoData("No instrument_id {0} found in the DB".format(i_id)))
            return sq.instrument_id
        except ValueError:
            sq = self.session.query(self.Instrument).filter_by(instrument_name = name).all()
            if len(sq) == 0:
                raise(DBNoData("No instrument_name {0} found in the DB".format(name)))
            if len(sq) > 1:
                if satellite_id == None:
                    raise(ValueError('Non unique instrument name and no satellite specified'))
                for v in sq:
                    if v.satellite_id == satellite_id:
                        return v.instrument_id
            return sq[0].instrument_id

    def getMissions(self):
        """return a list of all the missions"""
        sq = self.session.query(self.Mission.mission_name)
        return [val[0] for val in sq.all()]

    def getFileID(self, filename):
        """
        Return the fileID for the input filename

        @param filename: filename to return the fileid of
        @type filename: str

        @return: file_id: file_id of the input file
        @rtype: long
        """
        try:
            f_id = long(filename)
            sq = self.session.query(self.File).get(f_id)
            if sq is None:
                raise(DBNoData("No file_id {0} found in the DB".format(filename)))
            return sq.file_id
        except ValueError:
            sq = self.session.query(self.File).filter_by(filename = filename)
            try:
                return sq[0].file_id
            except IndexError: # no file_id found
                raise(DBNoData("No filename %s found in the DB" % (filename)))

    def getCodeID(self, codename):
        """
        Return the codeID for the input code

        @param codename: filename to return the fileid of
        @type filename: str

        @return: code_id: code_id of the input file
        @rtype: long
        """
        try:
            c_id = long(codename)
            code = self.session.query(self.Code).get(c_id)
            if code is None:
                raise(DBNoData("No code id {0} found in the DB".format(c_id)))
        except ValueError:
            sq = self.session.query(self.Code.code_id).filter_by(filename = codename).all()
            if len(sq) == 0:
                raise(DBNoData("No code name {0} found in the DB".format(codename)))
            c_id = sq[0].code_id
        return c_id

    def getFileDates(self, file_id):
        """
        given a file_id or name return the dates it spans
        """
        sq = self.getEntry('File', file_id)
        start_time = sq.utc_start_time.date()
        stop_time =  sq.utc_stop_time.date()
        retval = [start_time, stop_time]
        ans = np.unique(retval).tolist()
        DBlogging.dblogger.debug( "Found getFileDates():  file_id: {0}, dates: {1}".format(file_id, ans) )
        return ans

    def getFiles_product_utc_file_date(self, product_id, date):
        """
        given a product id and a utc_file_date return all the files that match [(file_id, Version, product_id, product_id, utc_file_date), ]
        """
        DBlogging.dblogger.debug( "Entered getFiles_product_utc_file_date():  product_id: {0} date: {1}".format(product_id, date) )
        if isinstance(date, datetime.datetime):
            date = date.date()
        retval = []
        try:
            for d in date:
                if isinstance(d, datetime.datetime):
                    d = d.date()
                sq = self.session.query(self.File).filter_by(product_id = product_id).filter(or_(self.File.utc_start_time.between(datetime.datetime.combine(d, datetime.time(0)), datetime.datetime.combine(d, datetime.time(0))+datetime.timedelta(days=1)), self.File.utc_stop_time.between(datetime.datetime.combine(d, datetime.time(0)), datetime.datetime.combine(d, datetime.time(0))+datetime.timedelta(days=1))))
                #sq = self.session.query(self.File).filter_by(product_id = product_id).filter(self.File.utc_start_time >= datetime.datetime.combine(d, datetime.time(0))).filter(self.File.utc_stop_time < datetime.datetime.combine(d, datetime.time(0))+datetime.timedelta(days=1) )
                sq = [(v.file_id, Version.Version(v.interface_version, v.quality_version, v.revision_version), self.getEntry('File', v.file_id).product_id, self.getEntry('File', v.file_id).utc_file_date ) for v in sq]
                retval.extend(sq)
        except TypeError:
            d = date
            sq = self.session.query(self.File).filter_by(product_id = product_id).filter(or_(self.File.utc_start_time.between(datetime.datetime.combine(d, datetime.time(0)), datetime.datetime.combine(d, datetime.time(0))+datetime.timedelta(days=1)), self.File.utc_stop_time.between(datetime.datetime.combine(d, datetime.time(0)), datetime.datetime.combine(d, datetime.time(0))+datetime.timedelta(days=1))))
            # sq = self.session.query(self.File).filter_by(product_id = product_id).filter(self.File.utc_start_time >= datetime.datetime.combine(d, datetime.time(0))).filter(self.File.utc_stop_time < datetime.datetime.combine(d, datetime.time(0))+datetime.timedelta(days=1) )
            sq = [(v.file_id, Version.Version(v.interface_version, v.quality_version, v.revision_version), self.getEntry('File', v.file_id).product_id, self.getEntry('File', v.file_id).utc_file_date ) for v in sq]
            retval.extend(sq)
        DBlogging.dblogger.debug( "Done getFiles_product_utc_file_date():  product_id: {0} date: {1} retval: {2}".format(product_id, date, retval) )
        return retval


    def file_id_Clean(self, invals):
        """
        given an input tuple (file_id, Version, product_id, utc_file_date) go through and clear out lower versions of the same files
        this is determined by utc_file_date
        """
        # TODO this might break with weekly input files
        # build up a list of tuples file_id, product_id, utc_file_date, version

        ## think here on better, but a dict makes for easy del
        data2 = dict((x, (y1, y2, y3)) for x, y1, y2, y3 in invals)

        for k1, k2 in itertools.combinations(data2, 2):
            ## TODO this can be done cleaner
            try: # if the key is gone, move on
                tmp = data2[k1]
                tmp = data2[k2]
            except KeyError:
                continue
            if data2[k1][1] != data2[k2][1]:   # not the same product
                continue
            if data2[k1][2] == data2[k2][2]: # same date
                # drop the one with the lower version
                if data2[k1][0] > data2[k2][0]:
                    del data2[k2]
                else:
                    del data2[k1]
        ## now we have a dict of just the unique files
        ans = [(key, data2[key][0], data2[key][1], data2[key][2]) for key in data2]
        return ans

    def getInputProductID(self, process_id):
        """
        Return the fileID for the input filename

        @param process_id: process_)id to return the input_product_id for
        @type process_id: long

        @return: list of input_product_ids
        @rtype: list
        """
        sq = self.session.query(self.Productprocesslink.input_product_id, self.Productprocesslink.optional).filter_by(process_id = process_id).all()
        return sq

    def getFilesByProductDate(self, product_id, daterange):
        """
        return the files in the db by product id that have data in the date specified
        """
        sq1 = self.session.query(self.File).filter_by(product_id = product_id).filter_by(exists_on_disk = True).filter_by(newest_version = True).filter(self.File.utc_start_time >= daterange[0]).all()
        sq1 = set([v.file_id for v in sq1])
        sq2 = self.session.query(self.File).filter_by(product_id = product_id).filter_by(exists_on_disk = True).filter_by(newest_version = True).filter(self.File.utc_stop_time <= daterange[1]).all()
        sq2 = set([v.file_id for v in sq2])
        ans = sq1.intersection(sq2)
        return list(ans)

    def getFilesByProduct(self, prod_id):
        """
        given a product_id or name return all the file instances associated with it
        """
        prod_id = self.getProductID(prod_id)
        sq = self.session.query(self.File).filter_by(product_id = prod_id)
        return sq.all()

    def getActiveInspectors(self):
        """
        query the db and return a list of all the active inspector filenames [(filename, arguments, product), ...]
        """
        sq = self.session.query(self.Inspector).filter(self.Inspector.active_code == True).all()
        basedir = self.getMissionDirectory()
        retval = [(os.path.join(basedir, ans.relative_path, ans.filename), ans.arguments, ans.product) for ans in sq]
        return retval

    def getChildrenProducts(self, file_id):
        """
        given a file ID return all the processes that use this as input
        """
        DBlogging.dblogger.debug( "Entered findChildrenProducts():  file_id: {0}".format(file_id) )
        product_id = self.getEntry('File', file_id).product_id

        # get all the process ids that have this product as an input
        proc_ids = self.getProcessFromInputProduct(product_id)
        return proc_ids

    def getProductID(self,
                     product_name):
        """
        Return the product ID for an input product name

        @param product_name: the name of the product to et the id of
        @type product_name: str

        @return: product_id -the product  ID for the input product name
        """
        try:
            product_name = long(product_name)
            sq = self.session.query(self.Product).get(product_name)
            if sq is not None:
                return sq.product_id
            else:
                raise(DBNoData("No product_id {0} found in the DB".format(product_name)))
        except ValueError:
            sq = self.session.query(self.Product).filter_by(product_name = product_name)
            try:
                return sq[0].product_id
            except IndexError: # no file_id found
                raise(DBNoData("No product_name %s found in the DB" % (product_name)))

    def getSatelliteID(self,
                        sat_name):
        """
        @param sat_name: the satellite name to look up the id
        @type sat_name: str

        @return: satellite_id - the requested satellite  ID
        """
        if isinstance(sat_name, (list, tuple)):
            s_id = []
            for v in sat_name:
                s_id.append(self.getSatelliteID(v))
            return s_id
        try:
            sat_id = long(sat_name)
            sq = self.session.query(self.Satellite).get(sat_id)
            return sq.satellite_id
        except ValueError: # it was a name
            sq = self.session.query(self.Satellite).filter_by(satellite_name=sat_name).all()
        return sq[0].satellite_id  # there can be only one of each name

    def getCodePath(self, code_id):
        """
        Given a code_id list return the full name (path and all) of the code
        """
        code = self.getEntry('Code', code_id)
        if not code.active_code: # not an active code
            return None
        mission_dir =  self.getMissionDirectory()
        return os.path.join(mission_dir, code.relative_path, code.filename)

    def getCodeVersion(self, code_id):
        """
        Given a code_id the code version
        """
        DBlogging.dblogger.debug("Entered getCodeVersion: {0}".format(code_id))
        sq =  self.session.query(self.Code.interface_version, self.Code.quality_version, self.Code.revision_version).filter_by(code_id = code_id)  # should only have one value
        try:
            return Version.Version(*sq[0])
        except IndexError:
            raise(DBNoData("No code number {0} in the db".format(code_id)))

    def getProcessFromOutputProduct(self, outProd):
        """
        Gets process from the db that have the output product
        """
        DBlogging.dblogger.debug("Entered getProcessFromOutputProduct: {0}".format(outProd))
        p_id = self.getProductID(outProd)
        sq1 =  self.session.query(self.Process).filter_by(output_product = p_id).all()  # should only have one value
        return sq1[0].process_id

    def getCodeFromProcess(self, proc_id):
        """
        given a process id return the code that makes performs that process
        """
        DBlogging.dblogger.debug("Entered getCodeFromProcess: {0}".format(proc_id))
        sq1 =  self.session.query(self.Code.code_id).filter_by(process_id = proc_id).all()  # should only have one value
        try:
            return sq1[0][0]
        except IndexError:
            return None

    def getMissionDirectory(self):
        """
        return the base directory for the current mission

        @return: base directory for current mission
        @rtype: str
        """
        DBlogging.dblogger.debug("Entered getMissionDirectory: ")
        sq = self.session.query(self.Mission.rootdir).filter_by(mission_name  = self.mission)
        return sq.first()[0]  # there can be only one of each name

    def _checkIncoming(self):
        """
        check the incoming directory for the current mission and add those files to the getting list

        @return: processing list of file ids
        @rtype: list
        """
        path = self.getIncomingPath()
        DBlogging.dblogger.debug("Looking for files in {0}".format(path))
        files = glob.glob(os.path.join(path, '*'))
        return files

    def getIncomingPath(self):
        """
        return the incoming path for the current mission
        """
        basedir = self.getMissionDirectory()
        path = os.path.join(basedir, 'incoming/')
        return path

    def getErrorPath(self):
        """
        return the error path for the current mission
        """
        basedir = self.getMissionDirectory()
        path = os.path.join(basedir, 'errors/')
        return path

    def getFilefilelink_byresult(self, file_id):
        """
        given a file_id return all the other file_ids that went into making it
        """
        DBlogging.dblogger.debug("Entered getFilefilelink_byresult: file_id={0}".format(file_id))
        f_id = self.getFileID(file_id)
        sq = self.session.query(self.Filefilelink.source_file).filter_by(resulting_file = f_id).all()
        try:
            return zip(*sq)[0]
        except IndexError:
            return None

    def getFilecodelink_byfile(self, file_id):
        """
        given a file_id return the code_id associated with it, or None
        """
        DBlogging.dblogger.debug("Entered getFilecodelink_byfile: file_id={0}".format(file_id))
        f_id = self.getFileID(file_id)
        sq = self.session.query(self.Filecodelink.source_code).filter_by(resulting_file = f_id).all() # can only be one
        try:
            return sq[0][0]
        except IndexError:
            return None

    @classmethod
    def daterange_to_dates(self, daterange):
        """
        given a daterange return the dat objects for all days in the range
        """
        DBlogging.dblogger.debug("Entered daterange_to_dates: daterange={0}".format(daterange))
        return [daterange[0] + datetime.timedelta(days=val) for val in xrange((daterange[1]-daterange[0]).days+1)]

    def getMissionID(self, mission_name):
        """
        given a misio name return its ID
        """
        try:
            m_id = long(mission_name)
            ms = self.session.query(self.Mission).get(m_id)
            if ms is None:
                raise(DBNoData('Invalid mission id {0}'.format(m_id)))
        except (ValueError, TypeError):
            sq = self.session.query(self.Mission.mission_id).filter_by(mission_name = mission_name).all()
            if len(sq) == 0:
                raise(DBNoData('Invalid mission name {0}'.format(mission_name)))
            m_id = sq[0].mission_id
        return m_id

    def getNewestFiles(self):
        """
        for the current mission get a tuple of all file ids that are marked newest version
        """
        sq = self.session.query(self.File.file_id).filter_by(newest_version = True).all()
        sq = zip(*sq)[0]
        return sq

    def tag_release(self, rel_num):
        """
        tag all the newest versions of files to a release number (integer)
        """
        newest_files = self.getNewestFiles()
        for f in newest_files:
            self.addRelease(f, rel_num, commit=False)
        self._commitDB()
        return len(newest_files)

    def addRelease(self, filename, release, commit=False):
        """
        given a filename or file_id add an entry to the release table
        """
        f_id = self.getFileID(filename)  # if a number
        rel = self.Release()
        rel.file_id = f_id
        rel.release_num = release
        self.session.add(rel)
        if commit: # so that if we are doing a lot it is faster
            self._commitDB()

    def list_release(self, rel_num, fullpath=True):
        """
        given a release number return a list of all the filenames with the release
        """
        sq = self.session.query(self.Release.file_id).filter_by(release_num = rel_num).all()
        sq = list(zip(*sq)[0])
        for i, v in enumerate(sq):
            if fullpath:
                sq[i] = self.getFileFullPath(v)
            else:
                sq[i] = self.getEntry('File', v).filename
        return sq

    def checkFileMD5(self, file_id):
        """
        given a file id or name check the db checksum and the file checksum
        """
        db_sha = self.getEntry('File', file_id).md5sum
        disk_sha = calcDigest(self.getFileFullPath(file_id))
        if str(disk_sha) == str(db_sha):
            return True
        else:
            return False

    def checkFiles(self):
        """
        check files in the DB, return inconsistent files and why
        """
        files = zip(*self.getAllFilenames())[0]
        ## check of existance and checksum
        bad_list = []
        for f in files:
            try:
                if not self.checkFileMD5(f):
                    bad_list.append((f, '(100) bad checksum'))
            except DigestError:
                bad_list.append((f, '(200) file not found'))
        return bad_list

    def getProductTraceback(self, prod_id):
        """
        given a product id return instances of all the tables it takes to define it
        mission, satellite, instrument, product, inspector, Instrumentproductlink
        """
        prod_id = self.getProductID(prod_id) # convert name to ID
        retval = {}
        # get the product instance
        retval['product'] = self.getEntry('Product', prod_id)
        # inspector
        retval['inspector'] = self.session.query(self.Inspector).filter_by(product = prod_id).first()
        # instrument
        inst_id = self.getInstrumentFromProduct(prod_id)
        retval['instrument'] = self.getEntry('Instrument', inst_id)
        # Instrumentproductlink
        retval['instrumentproductlink'] = self.session.query(self.Instrumentproductlink).get((inst_id, prod_id))
        # satellite
        sat_id = self.getEntry('Instrument', inst_id).satellite_id
        retval['satellite'] = self.getEntry('Satellite', sat_id)
        # mission
        mission = self.getSatelliteMission(sat_id)
        retval['mission'] = mission
        return retval

    def getFileTraceback(self, file_id):
        """
        given a product id return instances of all the tables it takes to define it
        mission, satellite, instrument, product, inspector, Instrumentproductlink
        """
        file_id = self.getFileID(file_id)
        prod_id = self.getEntry('File', file_id).product_id
        retval = self.getProductTraceback(prod_id)
        retval['file'] = self.getEntry('File', file_id)
        return retval

    def getProcessTraceback(self, proc_id):
        """
        given a process id return instances of all the tables it takes to define it
        mission, satellite, instrument, product, process, code, productprocesslink
        """
        retval = {}
        # get the product instance
        retval['process'] = self.getEntry('Process', proc_id)
        retval['output_product'] = self.getEntry('Product', retval['process'].output_product)
        # instrument
        inst_id = self.getInstrumentFromProduct(retval['output_product'].product_id)
        retval['instrument'] = self.getEntry('Instrument', inst_id)
        # satellite
        sat_id = self.getEntry('Instrument', inst_id).satellite_id
        retval['satellite'] = self.getEntry('Satellite', sat_id)
        # mission
        mission_id = self.getSatelliteMission(sat_id).mission_id
        retval['mission'] = self.getEntry('Mission', mission_id)
        # code
        code_id = self.getCodeFromProcess(retval['process'].process_id)
        if code_id is not None:
            retval['code'] = self.getEntry('Code', code_id)
        # input products
        retval['input_product'] = []
        in_prod_id = self.getInputProductID(retval['process'].process_id)
        for val, opt in in_prod_id:
            retval['input_product'].append((self.getEntry('Product', val), opt) )
        retval['productprocesslink'] = []
        ppl = self.session.query(self.Productprocesslink).filter_by(process_id = retval['process'].process_id)
        for val in ppl:
            retval['productprocesslink'].append(ppl)
        return retval

    def getProducts(self):
        """
        get all products for the given mission
        """
        outval = []
        prods = self.getAllProducts()
        mission_id = self.getMissionID(self.mission)
        for val in prods:
            if self.getProductTraceback(val.product_id)['mission'].mission_id == mission_id:
                outval.append(val)
        return outval

    def getAllProcesses(self):
        """
        get all processes for the given mission
        """
        outval = []
        procs = self.session.query(self.Process).all()
        mission_id = self.getMissionID(self.mission)
        for val in procs:
            if self.getProcessTraceback(val.process_id)['mission'].mission_id == mission_id:
                outval.append(val)
        return outval

    def getAllProducts(self):
        """
        return a list of all products as instaces
        """
        prods = self.session.query(self.Product).all()
        return prods

    def getEntry(self, table, *args):
        """
        master method to return a entry instance from any table in the db
        """
        if table not in dir(self):
            raise(ValueError('Invalid table specification: {0}'.format(table)))
        if ('get' + table + 'ID') in dir(self):
            cmd = 'get' + table + 'ID'
            pk = getattr(self, cmd)(args[0])
        else:
            try:
                pk = long(args[0])
            except ValueError:
                raise(ValueError('Invalid primary key, {1}, specified for table {0}'.format(table, args[0])))
        retval = self.session.query(getattr(self, table)).get(pk)
        if retval is None:
            raise(DBNoData('No entry {0} for table {1}'.format(args[0], table)))
        return retval

    @classmethod
    def processRunning(self, pid):
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




