import datetime
import glob
import itertools
import functools
import os.path
from operator import itemgetter
import pwd
import socket # to get the local hostname
import sys

import dateutil.rrule # do this long so where it is from is remembered

import numpy as np
import sqlalchemy
from sqlalchemy import Table
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sessionmaker
try: # new version changed this annoyingly
    from sqlalchemy.exceptions import IntegrityError
except ImportError:
    from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import asc
from sqlalchemy import or_, and_

from Diskfile import calcDigest, DigestError
import DBlogging
import DBStrings
import Version
import Utils

## This goes in the processing comment field in the DB, do update it
__version__ = '2.0.3'



#########################################################
## NOTES, read these if new to this module
#########################################################
# - functions are in transition from returning the thing the name says e.g. getFileID returns a number to
#      instead returning the sqlalchemy object that meets the criteria so getFileID would return a File instance
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

    def __init__(self, mission='Test', db_var=None, echo=False):
        """
        @summary: Initialize the DBUtils class, default mission is 'Test'
        """
        self.dbIsOpen = False
        if mission is None:
            raise(DBError("Must input database name to create DBUtils instance"))
        self.mission = mission
        #Expose the format/regex routines of DBFormatter
        fmtr = DBStrings.DBFormatter()
        self.format = fmtr.format
        self.re = fmtr.re
        self._openDB(db_var, echo=echo)
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

    @staticmethod
    def _test_SQLAlchemy_version(version= sqlalchemy.__version__):
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
        self.Processqueue.rawadd = self._processqueueRawadd

####################################
###### DB and Tables ###############
####################################

    def _openDB(self, db_var=None, verbose=False, echo=False):
        """
        setup python to talk to the database, this is where it is, name and password.
        """
        if self.dbIsOpen == True:
            return
        try:
            if self.mission == 'unittest':
                if db_var is None:
                    engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=echo)
                else:
                    engine = db_var.engine

            else: # assume we got a filename and use that
                if not os.path.isfile(os.path.expanduser(self.mission)):
                    raise(ValueError("DB file specified doesn't exist"))
                engine = sqlalchemy.create_engine('sqlite:///' + os.path.expanduser(self.mission), echo=echo)
                self.mission = os.path.realpath(os.path.expanduser(self.mission))

            DBlogging.dblogger.info("Database Connection opened: {0}  {1}".format(str(engine), self.mission))

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
                if verbose: DBlogging.dblogger.debug("Class %s created" % (val))


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
        """
        if comment is None:
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
        """
        # this is the logging of the processing, no real use for it yet but maybe we will in the future
        # helps to know is the process ran and if it succeeded
        if self._currentlyProcessing():
            raise(DBError('A Currently Processing flag is still set, cannot process now'))
        # save this class instance so that we can finish the logging later
        self.__p1 = self._addLogging(True,
                              datetime.datetime.utcnow(),
                              ## for now there is one mission only per DB
                              # self.getMissionID(self.mission),
                              self.session.query(self.Mission.mission_id).first()[0],
                              pwd.getpwuid(os.getuid())[0],
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
        sq = self.getEntry('File', file_id)
        if sq.exists_on_disk:
            file_path = self.getFileFullPath(sq.file_id)
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
        remove everything from the process queue
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

    def _processqueueGetAll(self, version_bump=None):
        """
        return the entire contents of the process queue
        """
        if version_bump is None:
            try:
                pqdata = self.session.query(self.Processqueue.file_id).all()
                pqdata = list(map(itemgetter(0), pqdata))
            except (IndexError, TypeError):
                pqdata = self.session.query(self.Processqueue.file_id).all()
            ans = pqdata
        else:
            try:
                pqdata1 = self.session.query(self.Processqueue.file_id).all()
                pqdata2 = self.session.query(self.Processqueue.version_bump).all()
                pqdata1 = list(map(itemgetter(0), pqdata1))
                pqdata2 = list(map(itemgetter(0), pqdata2))
            except (IndexError, TypeError):
                pqdata1 = self.session.query(self.Processqueue.file_id).all()
                pqdata2 = self.session.query(self.Processqueue.version_bump).all()
            ans = zip(pqdata1, pqdata2)
        DBlogging.dblogger.debug( "Entire Processqueue was read: {0} elements returned".format(len(ans)))
        return ans

    def _processqueuePush(self, fileid, version_bump=None):
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
            fileid = set(fileid)
            for v in fileid:
                ans.extend(self.Processqueue.push(v, version_bump))
            return ans
        fileid = self.getFileID(fileid)
        pq1 = self.Processqueue()
        pq1.file_id = fileid
        if hasattr(version_bump, '__iter__'):
            pq1.version_bump = version_bump[0]
        else:
            pq1.version_bump = version_bump
        self.session.add(pq1)
        DBlogging.dblogger.debug( "File added to process queue {0}:{1}".format(fileid, '---'))
        self._commitDB()
#        pqid = self.session.query(self.Processqueue.file_id).all()
        return pq1.file_id

    def _processqueueRawadd(self, fileid, version_bump=None):
        """
        raw add file ids to the process queue
        *** this might break things if an id is added that does not exist
        ***   meant to be fast and used after getting the ids
        *** IS safe against adding ids that are already in the queue

        Parameters
        ==========
        fileid : (int, listlike)
            the file id (or lisklike of file ids)

        Returns
        =======
        num : int
            the number of entries added to the processqueue
        """
        current_q = set(self._processqueueGetAll())

        if not hasattr(fileid, '__iter__'):
            fileid = [fileid]

        fileid = set(fileid)
        # drop all the values in the current_q from fileid
        files_to_add = fileid.difference(current_q)

        # are there any left?
        if len(files_to_add) != 0:
            for f in files_to_add:
                pq1 = self.Processqueue()
                pq1.file_id = f
                self.session.add(pq1)
                DBlogging.dblogger.debug( "File added to process queue {0}:{1}".format(fileid, '---'))
            self._commitDB() # commit once for all the adds
        return  len(files_to_add)

    def _processqueueLen(self):
        """
        return the number of files in the process queue
        """
        return self.session.query(self.Processqueue).count()

    def _processqueuePop(self, index=0, version_bump=None):
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
                    if version_bump is not None:
                        fid_ret = (fid.file_id, fid.version_bump)
                    else:
                        fid_ret = fid.file_id
                    self.session.delete(fid)
                    break # there can be only one
            self._commitDB()
            DBlogging.dblogger.debug( "File removed from process queue {0}:{1}".format(fid_ret, '---'))
            return fid_ret

    def _processqueueGet(self, index=0, version_bump=None):
        """
        get the file at the head of the queue (from the left)

        Returns
        =======
        file_id : int
            the file_id of the file popped from the queue
        """
        num = self.Processqueue.len()
        if num == 0:
            DBlogging.dblogger.debug( "processqueueGet() returned: None (empty queue)")
            return None
        elif index >= num:
            DBlogging.dblogger.debug( "processqueueGet() returned: None (requested index larger than size)")
            return None
        else:
            for ii, fid in enumerate(self.session.query(self.Processqueue)):
                if ii == index:
                    if version_bump is not None:
                        fid_ret = (fid.file_id, fid.version_bump)
                    else:
                        fid_ret = fid.file_id
                    break # there can be only one
            DBlogging.dblogger.debug( "processqueueGet() returned: {0}".format(fid_ret) )
            return fid_ret

    def _processqueueClean(self, dryrun=False):
        """
        go through the process queue and clear out lower versions of the same files
        this is determined by product and utc_file_date
        """
        # TODO this might break with weekly input files
        DBlogging.dblogger.debug("Entering in queueClean(), there are {0} entries".format(self.Processqueue.len()))
        pqdata = self.Processqueue.getAll(version_bump=True)
        if len(pqdata) <= 1: # can't clean just one (or zero) entries
            return

        file_entries = [(self.getEntry('File', val[0]), val[1]) for val in pqdata]
#        keep = [(val[0].file_id, val[1]) for val in file_entries if val[0].newest_version==True]
        keep = [(val[0], val[1]) for val in file_entries if val[0].newest_version==True]

#==============================================================================
#         # sort keep on dates, then sort keep on level
#==============================================================================
        # this should make them in order for each level
        keep = sorted(keep, key=lambda x: x[0].utc_file_date, reverse=1)
        keep = sorted(keep, key=lambda x: x[0].data_level)
        keep = [(val[0].file_id, val[1]) for val in keep]

        ## now we have a list of just the newest file_id's
        if not dryrun:
            self.Processqueue.flush()
            #        self.Processqueue.push(ans)
            for v in keep:
                self.Processqueue.push(*v)
        else:
            print('<dryrun> Queue cleaned leaving {0} of {1} entries'.format(len(keep), self.Processqueue.len()))

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
            DBlogging.dblogger.info( "File removed from db {0}".format(f) )

    def getAllSatellites(self):
        """
        return dictionaries of satellite, mission objects
        """
        ans = []
        sats = self.session.query(self.Satellite).all()
        for s in sats:
            ans.append(self.getTraceback('Satellite', s.satellite_id))
        return ans

    def getAllInstruments(self):
        """
        return dictionaries of instrument traceback dictionaries
        """
        ans = []
        sats = self.session.query(self.Instrument).all()
        for s in sats:
            ans.append(self.getTraceback('Instrument', s.instrument_id))
        return ans

    def getAllFilenames(self, fullPath=True, level=None, product=None):
        """
        return all the file names in the database

        if level==None get all filenames, otherwise only for a level

        I worked this for speed the zip(*names) is way too slow (this is about x18 faster)

        """
        if level is None and product is None:
            names = self.session.query(self.File.filename).all()
        elif product is None:
            names = self.session.query(self.File.filename).filter_by(data_level=level).all()
        elif level is None:
            names = self.session.query(self.File.filename).filter_by(product_id=product).all()
        else: # both specified
            names = self.session.query(self.File.filename).filter_by(product_id=product).filter_by(data_level=level).all()
        names = list(map(itemgetter(0), names))
        if fullPath:
            names = [ self.getFileFullPath(v) for v in names]
        return names

    def getAllFileIds(self):
        """
        return all teh file ids in the database

        the itemgetter method is a lot faster then zip(*) (x16)
        """
        ids = self.session.query(self.File.file_id).all()
        ids =  list(map(itemgetter(0), ids))
        return ids

    def addMission(self,
                    mission_name,
                    rootdir,
                    incoming_dir):
        """ add a mission to the database

        @param mission_name: the name of the mission
        @type mission_name: str
        @param rootdir: the root directory of the mission
        @type rootdir: str

        """
        mission_name = str(mission_name)
        rootdir = str(rootdir)
        try:
            m1 = self.Mission()
        except AttributeError:
            raise(DBError("Class Mission not found was it created?"))

        m1.mission_name = mission_name
        m1.rootdir = rootdir.replace('{MISSION}', mission_name)
        m1.incoming_dir = incoming_dir.replace('{MISSION}', mission_name)
        self.session.add(m1)
        self._commitDB()
        return m1.mission_id

    def addSatellite(self,
                    satellite_name, mission_id):
        """ add a satellite to the database

        @param satellite_name: the name of the mission
        @type satellite_name: str
        """
        satellite_name = str(satellite_name)
        s1 = self.Satellite()

        s1.mission_id = mission_id
        s1.satellite_name = satellite_name.replace('{MISSION}', self.getEntry('Mission', mission_id).mission_name)
        self.session.add(s1)
        self._commitDB()
        return s1.satellite_id

    def addProcess(self,
                    process_name,
                    output_product,
                    output_timebase,
                    extra_params=None):
        """ add a process to the database

        @param process_name: the name of the process
        @type process_name: str
        @param output_product: the output product id
        @type output_product: int
        @keyword extra_params: extra parameters to pass to the code
        @type extra_params: str
        """
        if output_timebase not in ['RUN', 'ORBIT', 'DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY', 'FILE']:
            raise(ValueError("output_timebase invalid choice"))

        p1 = self.Process()
        p1.output_product = output_product
        p1.process_name = process_name
        p1.extra_params = Utils.toNone(extra_params)
        p1.output_timebase = output_timebase
        self.session.add(p1)
        self._commitDB()
        # self.updateProcessSubs(p1.process_id)
        return p1.process_id

    def addProduct(self,
                    product_name,
                    instrument_id,
                    relative_path,
                    format,
                    level,
                    product_description):
        """ add a product to the database

        @param product_name: the name of the product
        @type product_name: str
        @param instrument_id: the instrument   the product is from
        @type instrument_id: int
        @param relative_path:relative path for the product
        @type relative_path: str
        @param format: the format of the product files
        @type format: str
        """
        p1 = self.Product()
        p1.instrument_id = instrument_id
        p1.product_name = product_name
        p1.relative_path = relative_path
        p1.format = format
        p1.level = level
        p1.product_description = product_description
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

    def updateInspectorSubs(self, insp_id):
        """
        update an existing inspector performing the {} replacements
        """
        # need to do {} replacement, have to do it as a modification
        p1 = self.getEntry('Inspector', insp_id)

        insp_id = p1.inspector_id
        relative_path = self._nameSubInspector(p1.relative_path, insp_id)
        p1.relative_path = relative_path
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
        ppl1.input_product_id = self.getProductID(input_product_id)
        ppl1.process_id = self.getProcessID(process_id)
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
        if '{MISSION}' in instrument_name:
            mission_id = self.getSatelliteMission(satellite_id)
            mission_name = self.getEntry('Mission', mission_id).mission_name
            instrument_name = instrument_name.replace('{MISSION}', mission_name)
        if '{SPACECRAFT}' in instrument_name:
            satellite_name = self.getEntry('Satellite', satellite_id).satellite_name
            instrument_name = instrument_name.replace('{SPACECRAFT}', satellite_name)

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
                   arguments=None,
                   cpu=1,
                   ram=1):
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
        if isinstance(version, (str, unicode)):
            version = Version.Version.fromString(version)

        c1 = self.Code()
        c1.filename = filename
        c1.relative_path = relative_path
        c1.code_start_date = Utils.parseDate(code_start_date)
        c1.code_stop_date = Utils.parseDate(code_stop_date)
        c1.code_description = code_description
        c1.process_id = process_id
        c1.interface_version = version.interface
        c1.quality_version =version.quality
        c1.revision_version = version.revision
        c1.active_code = Utils.toBool(active_code)
        c1.date_written = Utils.parseDate(date_written)
        c1.output_interface_version = output_interface_version
        c1.newest_version = Utils.toBool(newest_version)
        c1.arguments = Utils.toNone(arguments)
        c1.ram = ram
        c1.cpu = cpu

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
        if isinstance(version, (str, unicode)):
            version = Version.Version.fromString(version)
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
        c1.active_code = Utils.toBool(active_code)
        c1.date_written = Utils.parseDate(date_written)
        c1.output_interface_version = output_interface_version
        c1.newest_version = Utils.toBool(newest_version)
        c1.arguments = Utils.toNone(self._nameSubProduct(arguments, product))

        self.session.add(c1)
        self._commitDB()
        return c1.inspector_id

    def _nameSubProduct(self, inStr, product_id):
        """
        in inStr replace the standard {} with the names
        """
        if inStr is None:
            return inStr
        repl = ['{INSTRUMENT}', '{SPACECRAFT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}']
        ftb = self.getTraceback('Product', product_id)
        if '{INSTRUMENT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{SPACECRAFT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{SPACECRAFT}', ftb['satellite'].satellite_name)
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

    def _nameSubInspector(self, inStr, inspector_id):
        """
        in inStr replace the standard {} with the names
        """
        if inStr is None:
            return inStr
        repl = ['{INSTRUMENT}', '{SPACECRAFT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}']
        insp = self.getEntry('Inspector', inspector_id)
        ftb = self.getTraceback('Product', insp.product)
        if '{INSTRUMENT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{SPACECRAFT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{SPACECRAFT}', ftb['satellite'].satellite_name)
        if '{MISSION}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{MISSION}', ftb['mission'].mission_name)
        if '{PRODUCT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{PRODUCT}', ftb['product'].product_name)
        if '{LEVEL}' in inStr :
            inStr = inStr.replace('{LEVEL}', str(ftb['product'].level))
        if '{ROOTDIR}' in inStr :
            inStr = inStr.replace('{ROOTDIR}', str(ftb['mission'].rootdir))
        if any(val in inStr for val in repl): # call yourself again
            inStr = self._nameSubProduct(inStr, inspector_id)
        return inStr

    def _nameSubProcess(self, inStr, process_id):
        """
        in inStr replace the standard {} with the names
        """
        p_id = self.getProcessID(process_id)
        if inStr is None:
            return inStr
        repl = ['{INSTRUMENT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}']
        ftb = self.getTraceback('Process', p_id)
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
        ftb = self.getTraceback('File', file_id)
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
                shasum = None,
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
        d1.shasum = shasum
        d1.process_keywords = process_keywords
        self.session.add(d1)
        self._commitDB()
        return d1.file_id

    def _codeIsActive(self, ec_id, date):
        """
        Given a ec_id and a date is that code active for that date and is newest version

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
        if not code.newest_version:
            return False

        return True

    def getFileFullPath(self, filename):
        """
        return the full path to a file given the name or id
        (name or id is based on type)

        """
        file_entry = self.getEntry('File', filename)
        # need to know file product and mission to get whole path
        ftb = self.getTraceback('File', file_entry.file_id)
        rel_path = ftb['product'].relative_path
        if rel_path is None:
            raise(DBError("product {0} does not have a relative_path set, fix the DB".format(ftb['product'].product_id)))
        try:
            root_dir = ftb['mission'].rootdir
            if root_dir is None:
                raise(KeyError())
        except KeyError:
            raise(DBError("Mission {0} root directory not set, fix the DB".format(ftb['mission'].mission_id)))
        # perform anu required subitutions
        path = os.path.join(root_dir, rel_path, file_entry.filename)
        path = Utils.dirSubs(path,
                             file_entry.filename,
                             file_entry.utc_file_date,
                             file_entry.utc_start_time,
                             self.getFileVersion(file_entry.file_id)
                             )
        return path

    def getProcessFromInputProduct(self, product):
        """
        given a product id return all the processes that use that as an input

        Use getProductID if have a name (or not sure)
        """
        DBlogging.dblogger.debug("Entered getProcessFromInputProduct: {0}".format(product))
        sq = self.session.query(self.Productprocesslink.process_id).filter_by(input_product_id = product).all()
        return [v[0] for v in sq]

    def getProcessFromOutputProduct(self, outProd):
        """
        Gets process from the db that have the output product
        """
        DBlogging.dblogger.debug("Entered getProcessFromOutputProduct: {0}".format(outProd))
        p_id = self.getProductID(outProd)
        sq1 = self.session.query(self.Process).filter_by(output_product = p_id).all()  # should only have one value
        if not sq1:
            print('No Process has Product {0} as an output'.format(p_id))
            DBlogging.dblogger.info('No Process has Product {0} as an output'.format(p_id))
            return None
        return sq1[0].process_id

    def getRunProcess(self):
        """
        return a list of the processes who's output_timebase is "RUN"
        """
        proc = self.session.query(self.Process).filter_by(output_timebase = 'RUN').all()
        return proc

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
        fle = self.getFileID(filename)
        return self.getVersion(fle)

    def getFileMission(self, filename):
        """
        given an a file name or a file ID return the mission(s) that file is
        associated with
        """
        tb = self.getTraceback('File', filename)
        return tb['mission']

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
                if satellite_id is None:
                    raise(ValueError('Non unique instrument name and no satellite specified'))
                for v in sq:
                    if v.satellite_id == satellite_id:
                        return v.instrument_id
            return sq[0].instrument_id

    def getMissions(self):
        """return a list of all the missions"""
        sq = self.session.query(self.Mission.mission_name)
        return [val[0] for val in sq.all()]

    def renameFile(self, filename, newname):
        """
        rename a file in the db
        """
        f = self.getEntry('File', filename)
        f.filename = newname
        self.session.add(rel)
        self._commitDB()

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
        except TypeError: # came in as list or tuple
            return [self.getFileID(v) for v in filename]
        except ValueError:
            sq = self.session.query(self.File).filter_by(filename = filename).first()
            if sq is not None:
                return sq.file_id
            else: # no file_id found
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
        except TypeError: # came in as list or tuple
            return [self.getCodeID(v) for v in codename]
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
        # ans = np.unique(retval).tolist()
        DBlogging.dblogger.debug( "Found getFileDates():  file_id: {0}, dates: {1}".format(file_id, retval) )
        return retval

    def getFiles_product_utc_file_date(self, product_id, date):
        """
        given a product id and a utc_file_date return all the files that match
        [(file_id, Version, product_id, utc_file_date), ]
        """
        DBlogging.dblogger.debug( "Entered getFiles_product_utc_file_date(): " +
                                  "product_id: {0} date: {1}".format(product_id, date) )

        # get all the possible files:
        ## start date is before date and end date is after date
        if isinstance(date, (datetime.datetime)):
            date = date.date()

##         sq = self.session.query(self.File).filter_by(product_id = product_id).\
##              filter(and_(self.File.utc_start_time < datetime.datetime.combine(date + datetime.timedelta(1), datetime.time(0)),
##                          self.File.utc_stop_time >= datetime.datetime.combine(date, datetime.time(0))))
        sq = self.session.query(self.File).filter_by(product_id = product_id).\
             filter(and_(self.File.utc_start_time.between(datetime.datetime.combine(date, datetime.time(0)),
                                                          datetime.datetime.combine(date + datetime.timedelta(1), datetime.time(0))),
                         self.File.utc_stop_time.between(datetime.datetime.combine(date, datetime.time(0)), 
                                                         datetime.datetime.combine(date + datetime.timedelta(1), datetime.time(0)))))

        if not sq.count():  # there were none
            return None

        # if these files have met_start_time then that is the logic we want, otherwise we want simpler logic
#        if not sq[0].met_start_time and not sq[0].met_stop_time: # use logic only on utc_file_date
#            sq = self.session.query(self.File).filter_by(product_id = product_id).\
#                 filter_by(utc_file_date = date)

        ans = [(v.file_id, self.getVersion(v.file_id), v.product_id, v.utc_file_date ) for v in sq]
        DBlogging.dblogger.debug( "Done getFiles_product_utc_file_date():  product_id: {0} date: {1} retval: {2}".format(product_id, date, ans) )
        return ans


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

    def getFilesByProduct(self, prod_id, newest_version=False):
        """
        given a product_id or name return all the file instances associated with it

        if newest is set return only the newest files
        """
        prod_id = self.getProductID(prod_id)
        if newest_version:
            sq = self.session.query(self.File).filter_by(product_id = prod_id).filter_by(newest_version = True)
        else:
            sq = self.session.query(self.File).filter_by(product_id = prod_id)
        return sq.all()

    def getFilesByInstrument(self, inst_id, level=None, id_only=False):
        """
        given an instrument_if return all the file instances associated with it
        """
        prod_ids = self.session.query(self.Instrumentproductlink.product_id).filter_by(instrument_id=inst_id).all()
        prod_ids = list(map(itemgetter(0), prod_ids))
        prods_to_use = []
        if level is not None: # filter only on the level we want
            for p in prod_ids:
                tmp = self.getEntry('Product', p)
                if tmp.level == level:
                    prods_to_use.append(p)
        else:
            prods_to_use = list(prod_ids)
        ans = []
        for p in prods_to_use:
            ans.extend(self.getFilesByProduct(p))
        if id_only:
            return [v.file_id for v in ans]
        else:
            return ans

    def getActiveInspectors(self):
        """
        query the db and return a list of all the active inspector file names [(filename, arguments, product), ...]
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
        return self.getProcessFromInputProduct(product_id)

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
                # if two products have the same name always return the lower id one
                try:
                    return sorted(sq, key=lambda x: x.product_id)[0].product_id
                except TypeError:
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
        try:
            return sq[0].satellite_id  # there can be only one of each name
        except IndexError:
            raise(DBNoData("No satellite %s found in the DB" % (sat_name)))

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

    def getCodeFromProcess(self, proc_id, utc_file_date):
        """
        given a process id return the code id that makes performs that process

        Returns
        =======
        outval : int
            code id that performs the process
        """
        DBlogging.dblogger.debug("Entered getCodeFromProcess: {0}".format(proc_id))
        # will have as many values as there are codes for a process
        sq = self.session.query(self.Code.code_id).filter_by(process_id = proc_id).filter_by(newest_version = True).\
             filter_by(active_code = True).filter(self.Code.code_start_date <= utc_file_date).\
             filter(self.Code.code_stop_date >= utc_file_date)
        if sq.count() == 0:
            return None
        elif sq.count() > 1:
           raise(DBError('More than one code active for a given day'))
        return sq[0].code_id

    def getMissionDirectory(self, mission_id=None):
        """
        return the base directory for the current mission

        @return: base directory for current mission
        @rtype: str
        """
        if mission_id is None:
            mission_id = self.session.query(self.Mission.mission_id).all()
            if len(mission_id) > 1:
                raise(ValueError('No mission id specified and more than one mission present'))
            else:
                mission_id = mission_id[0][0]

        mission = self.getEntry('Mission',mission_id)
        return mission.rootdir

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

    def getIncomingPath(self, mission_id=None):
        """
        return the incoming path for the current mission
        """
        if mission_id is None:
            mission_id = self.session.query(self.Mission.mission_id).all()
            if len(mission_id) > 1:
                raise(ValueError('No mission id specified and more than one mission present'))
            else:
                mission_id = mission_id[0][0]

        mission = self.getEntry('Mission',mission_id)

        basedir = self.getMissionDirectory(mission_id=mission_id)
        inc_path = mission.incoming_dir
        return os.path.join(basedir, inc_path)

    def getErrorPath(self):
        """
        return the error path for the current mission
        """
        basedir = self.getMissionDirectory()
        path = os.path.join(basedir, 'errors/')
        return path

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

    @staticmethod
    def daterange_to_dates(daterange):
        """
        given a daterange return the dat objects for all days in the range
        """
        DBlogging.dblogger.debug("Entered daterange_to_dates: daterange={0}".format(daterange))
        return [daterange[0] + datetime.timedelta(days=val) for val in xrange((daterange[1]-daterange[0]).days+1)]

    def getMissionID(self, mission_name):
        """
        given a mission name return its ID
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

    def getNewestFiles(self, product=None, instrument=None):
        """
        for the current mission get a tuple of all file ids that are marked newest version
        """
        if product is None:
            sq = self.session.query(self.File.file_id).filter_by(newest_version = True).all()
        else:
            sq = self.session.query(self.File.file_id).filter_by(newest_version = True).filter_by(product = product).all()
        sq = list(map(itemgetter(0), sq))
        if instrument is not None:
            ans = []
            for s in sq:
                ptb = self.getTraceback('Product', self.getEntry('File', s).product_id)
                tmp = ptb['instrument'].instrument_name
                if tmp == self.getEntry('Instrument', tmp).instrument_name:
                    ans.append(tmp)
            sq = ans
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
        sq = list(map(itemgetter(0), sq))
        for i, v in enumerate(sq):
            if fullpath:
                sq[i] = self.getFileFullPath(v)
            else:
                sq[i] = self.getEntry('File', v).filename
        return sq

    def checkFileSHA(self, file_id):
        """
        given a file id or name check the db checksum and the file checksum
        """
        db_sha = self.getEntry('File', file_id).shasum
        disk_sha = calcDigest(self.getFileFullPath(file_id))
        if str(disk_sha) == str(db_sha):
            return True
        else:
            return False

    def checkFiles(self):
        """
        check files in the DB, return inconsistent files and why
        """
        files = self.getAllFilenames(fullPath=True)
        ## check of existence and checksum
        bad_list = []
        for f in files:
            try:
                if not self.checkFileSMA(f):
                    bad_list.append((f, '(100) bad checksum'))
            except DigestError:
                bad_list.append((f, '(200) file not found'))
        return bad_list

    def getTraceback(self, table, in_id, in_id2=None):
        """
        master routine for all te getXXXTraceback functions, this will make for less code

        this is some large select statements with joins in them, these are tested and do work
        """
        retval = {}
        if table.capitalize() == 'File':
            vars = ['file', 'product', 'inspector', 'instrument',
                    'instrumentproductlink', 'satellite', 'mission']

            sq = (self.session.query(self.File, self.Product,
                                    self.Inspector, self.Instrument,
                                    self.Instrumentproductlink, self.Satellite,
                                    self.Mission)
                  .filter_by(file_id=in_id)
                  .join((self.Product, self.File.product_id == self.Product.product_id))
                  .join((self.Inspector, self.Product.product_id == self.Inspector.product))
                  .join((self.Instrumentproductlink, self.Product.product_id == self.Instrumentproductlink.product_id))
                  .join((self.Instrument, self.Instrumentproductlink.instrument_id==self.Instrument.instrument_id))
                  .join((self.Satellite, self.Instrument.satellite_id==self.Satellite.satellite_id))
                  .join((self.Mission, self.Satellite.mission_id == self.Mission.mission_id)).all())
                  
            if len(sq) > 1:
                raise(DBError("Found multiple tracebacks for file {0}".format(in_id)))
            for ii, v in enumerate(vars):
                retval[v] = sq[0][ii]

        elif table.capitalize() == 'Code':
            vars = ['code', 'process', 'product', 'instrument',
                    'instrumentproductlink', 'satellite', 'mission']

            sq = (self.session.query(self.Code, self.Process,
                                     self.Product, self.Instrument,
                                     self.Instrumentproductlink, self.Satellite,
                                     self.Mission)
                  .filter_by(code_id=in_id)
                  .join((self.Process, self.Code.process_id == self.Process.process_id))
                  .join((self.Product, self.Product.product_id == self.Process.output_product))
                  .join((self.Inspector, self.Product.product_id == self.Inspector.product))
                  .join((self.Instrumentproductlink, self.Product.product_id == self.Instrumentproductlink.product_id))
                  .join((self.Instrument, self.Instrumentproductlink.instrument_id==self.Instrument.instrument_id))
                  .join((self.Satellite, self.Instrument.satellite_id==self.Satellite.satellite_id))
                  .join((self.Mission, self.Satellite.mission_id == self.Mission.mission_id)).all())
            
            if len(sq) > 1:
                raise(DBError("Found multiple tracebacks for code {0}".format(in_id)))
            for ii, v in enumerate(vars):
                retval[v] = sq[0][ii]

        elif table.capitalize() == 'Inspector':
            retval['inspector'] = self.getEntry(table.capitalize(), in_id)
            tmp = self.getTraceback('Product', retval['inspector'].product)
            retval = dict(retval.items() + tmp.items())

        elif table.capitalize() == 'Product':
            vars = ['product', 'inspector', 'instrument',
                    'instrumentproductlink', 'satellite', 'mission']

            sq = (self.session.query(self.Product,
                                    self.Inspector, self.Instrument,
                                    self.Instrumentproductlink, self.Satellite,
                                    self.Mission)
                  .filter_by(product_id=in_id)
                  .join((self.Inspector, self.Product.product_id == self.Inspector.product))
                  .join((self.Instrumentproductlink, self.Product.product_id == self.Instrumentproductlink.product_id))
                  .join((self.Instrument, self.Instrumentproductlink.instrument_id==self.Instrument.instrument_id))
                  .join((self.Satellite, self.Instrument.satellite_id==self.Satellite.satellite_id))
                  .join((self.Mission, self.Satellite.mission_id == self.Mission.mission_id)).all())
                  
            if len(sq) > 1:
                raise(DBError("Found multiple tracebacks for product {0}".format(in_id)))
            for ii, v in enumerate(vars):
                retval[v] = sq[0][ii]

        elif table.capitalize() == 'Process':

            vars = ['process', 'product', 'instrument',
                    'instrumentproductlink', 'satellite', 'mission']

            sq = (self.session.query(self.Process,
                                     self.Product, self.Instrument,
                                     self.Instrumentproductlink, self.Satellite,
                                     self.Mission)
                  .filter_by(process_id=in_id)
                  .join((self.Product, self.Product.product_id == self.Process.output_product))
                  .join((self.Inspector, self.Product.product_id == self.Inspector.product))
                  .join((self.Instrumentproductlink, self.Product.product_id == self.Instrumentproductlink.product_id))
                  .join((self.Instrument, self.Instrumentproductlink.instrument_id==self.Instrument.instrument_id))
                  .join((self.Satellite, self.Instrument.satellite_id==self.Satellite.satellite_id))
                  .join((self.Mission, self.Satellite.mission_id == self.Mission.mission_id)).all())
            
            if len(sq) > 1:
                raise(DBError("Found multiple tracebacks for process {0}".format(in_id)))
            for ii, v in enumerate(vars):
                retval[v] = sq[0][ii]

##             if 'file' in retval:
##                 code_id = self.getCodeFromProcess(retval['process'].process_id, retval['file'].utc_file_date)
##             else:
##                 code_id = self.getCodeFromProcess(retval['process'].process_id, datetime.date.today())

            code_id = self.getCodeFromProcess(retval['process'].process_id, datetime.date.today())
 
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

        elif table.capitalize() == 'Instrument':
            retval['instrument'] = self.getEntry('Instrument', in_id)
            tmp = self.getTraceback('Satellite', retval['instrument'].satellite_id)
            retval = dict(retval.items() + tmp.items())

        elif table.capitalize() == 'Satellite':
            retval['satellite'] = self.getEntry('Satellite', in_id)
            tmp = self.getTraceback('Mission', retval['satellite'].mission_id)
            retval = dict(retval.items() + tmp.items())

        elif table.capitalize() == 'Mission':
            retval['mission'] = self.getEntry('Mission', in_id)

        else:
            raise(NotImplementedError('The traceback or {0} is not implemented'.format(table)))

        return retval

    ######################
    # add in some helpers to match what we had
    # TODO figure ou how to do this!!
    ######################
#    getProductTraceback = functools.partial(getTraceback, 'Product')
#    getFileTraceback = functools.partial(getTraceback, 'File')
#    getCodeTraceback = functools.partial(getTraceback, 'Code')
#    getInspectorTraceback = functools.partial(getTraceback, 'Inspector')
#    getProcessTraceback = functools.partial(getTraceback, 'Process')
#    getInstrumentTraceback = functools.partial(getTraceback, 'Instrument')
#    getSatelliteTraceback = functools.partial(getTraceback, 'Satellite')
#    getMissionTraceback = functools.partial(getTraceback, 'Mission')

    def getProductsByInstrument(self, inst_id):
        """
        get all the products for a given instrument
        """
        inst_id = self.getInstrumentID(inst_id)
        sq = self.session.query(self.Instrumentproductlink.product_id).filter_by(instrument_id = inst_id).all()
        if sq:
            return  list(map(itemgetter(0), sq))
        else:
            return None

    def getAllProcesses(self, timebase='all'):
        """
        get all processes
        """
        if timebase == 'all':
            procs = self.session.query(self.Process).all()
        else:
            procs = self.session.query(self.Process).filter_by(output_timebase = timebase.upper()).all()
        return procs

    def getAllProducts(self):
        """
        return a list of all products as instances
        """
        prods = self.session.query(self.Product).all()
        return prods

    def getEntry(self, table, *args):
        """
        master method to return a entry instance from any table in the db
        """
        # just try and get the entry
        retval = self.session.query(getattr(self, table)).get(args[0])
        if retval is None: # either this was not a valid pk or not a pk that os in the db
            # see if it was a name
            if ('get' + table + 'ID') in dir(self):
                cmd = 'get' + table + 'ID'
                pk = getattr(self, cmd)(args[0])
                retval = self.session.query(getattr(self, table)).get(pk)
                if retval is None:
                    raise(DBNoData('No entry {0} for table {1}'.format(args[0], table)))
        return retval

    def getFilesByCode(self, code_id, id_only=False):
        """
        given a code_id (or name) return the files that were created using it
        """
        code_id = self.getCodeID(code_id)
        f_ids = self.session.query(self.Filecodelink.resulting_file).filter_by(source_code=code_id).all()
        f_ids = list(map(itemgetter(0), f_ids))
        files = [self.getEntry('File', val) for val in f_ids]
        if not id_only:
            return files
        else:
            return [val.file_id for val in files]

    def getFileParents(self, file_id, id_only=False):
        """
        given a file_id (or filename) return the files that went into making it
        """
        file_id = self.getFileID(file_id)
        f_ids = self.session.query(self.Filefilelink.source_file).filter_by(resulting_file=file_id).all()
        if not f_ids:
            return []
        f_ids = list(map(itemgetter(0), f_ids))
        files = [self.getEntry('File', val) for val in f_ids]
        if not id_only:
            return files
        else:
            return [val.file_id for val in files]

    @staticmethod
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

    def getVersion(self, fileid):
        """
        return the version instance for a file
        """
        if not isinstance(fileid, self.File):
            fileid = self.getEntry('File', fileid)
        return Version.Version(fileid.interface_version,
                               fileid.quality_version,
                               fileid.revision_version)


    def _childTree(self, inprod):
        """
        given an input product return a dict of its output prods
        """
        out_proc = self.getProcessFromInputProduct(inprod)
        return [self.getEntry('Process', op).output_product for op in out_proc]

    def getProductParentTree(self):
        """
        go through the db and return a tree of all products are thier parents

        This will allow for a run all the non done files script
        """
        prods = self.getAllProducts()
        prods = sorted(prods, key=lambda x: x.level)
        tree = []
        # for each of the level 0 products add a base tree then iterate through them with dbu.getProcessFromInputProduct
        #  then get the output for that process
        for p in prods:
            tree.append( [p.product_id, self._childTree(p.product_id)] )
        return tree
