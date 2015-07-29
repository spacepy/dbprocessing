from collections import namedtuple
import datetime
import glob
import itertools
import functools
import os.path
from operator import itemgetter, attrgetter
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
    from sqlalchemy.orm.exceptions import NoResultFound
except ImportError:
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import asc, desc
from sqlalchemy.sql import func
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
        self.MissionDirectory = self.getMissionDirectory()

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

        # TODO to do thus cleaner and allow for [] to work on the classes
        # ...info here...
        # metaclass that is dbutils aware
        # get __getitem__ to be a class mo


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
        for val in table_dict:
            if verbose: print val
            if not hasattr(self, val):  # then make it
                myclass = type(str(val), (object,), dict())
                tableobj = Table(table_dict[val], self.metadata, autoload=True)
                mapper(myclass, tableobj)
                setattr(self, str(val), myclass)
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
        sq2 = self.session.query(self.Logging).filter_by(currently_processing = True).count()
        if sq2 and comment is None:
            raise(ValueError("Must enter a comment to override DB lock"))
        sq = self.session.query(self.Logging).filter_by(currently_processing = True)
        for val in sq:
            val.currently_processing = False
            val.processing_end = datetime.datetime.utcnow()
            val.comment = 'Overridden:' + comment + ':' + __version__
            DBlogging.dblogger.error( "Logging lock overridden: %s" % ('Overridden:' + comment + ':' + __version__) )
            self.session.add(val)
        if sq2:
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
        This is as optimized as it can be
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
        # if the input is a filename need to handle that
        if not hasattr(item, '__iter__'): 
            item = [item]
        for ii, v in enumerate(item):
            try:
                int(v)
            except ValueError: # it was name
                item[ii] = self.getFileID(v)
        sq = self.session.query(self.Processqueue).filter(self.Processqueue.file_id.in_(item))
        for v in sq:
            self.session.delete(v)
        if sq:
            self._commitDB()

    def _processqueueGetAll(self, version_bump=None):
        """
        return the entire contents of the process queue
        """
        if version_bump is None:
            try:
                pqdata = self.session.query(self.Processqueue.file_id).all()
                pqdata = map(itemgetter(0), pqdata)
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
        if not hasattr(fileid, '__iter__'):
            fileid = [fileid]
        else:
            # do this in chunks as too many entries breaks things
            MAX_ADD = 150
            if len(fileid) > MAX_ADD:
                outval = []
                for v in Utils.chunker(fileid, MAX_ADD):
                    outval.extend(self._processqueuePush(v, version_bump=version_bump))
                return outval
        
        # first filter() takes care of putting in values that are not in the DB.  It is silent
        # second filter() takes care of not readding files that are alereadhy in the queue
        subq  = self.session.query(self.Processqueue.file_id).subquery()
        
        fileid = (self.session.query(self.File.file_id)
                  .filter(self.File.file_id.in_(fileid))
                  .filter(~self.File.file_id.in_(subq))).all()

        fileid = list(map(itemgetter(0), fileid)) # nested tuples to list

        pq = set(self.Processqueue.getAll())
        fileid = set(fileid).difference(pq)

        outval = []
        objs = []
        for f in fileid:
            pq1 = self.Processqueue()
            pq1.file_id = f
            pq1.version_bump = version_bump
            objs.append(pq1)
            outval.append(pq1.file_id)
        DBlogging.dblogger.debug( "File added to process queue {0}:{1}".format(fileid, '---'))
        if fileid:
            self.session.add_all(objs)
            self._commitDB()
#        pqid = self.session.query(self.Processqueue.file_id).all()
        return outval

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
        val = self._processqueueGet(index=index, version_bump=version_bump, instance=True)
        self.session.delete(val)
        self._commitDB()
        return (val.file_id, val.version_bump)

    def _processqueueGet(self, index=0, version_bump=None, instance=False):
        """
        get the file at the head of the queue (from the left)

        Returns
        =======
        file_id : int
            the file_id of the file popped from the queue
        """
        if index < 0:  # emable the python from the end indexing
            index = self.Processqueue.len() + index

        sq = self.session.query(self.Processqueue).offset(index).first()
        if instance:
            ans = sq
        else:
            ans = (sq.file_id, sq.version_bump)
        return ans

    def _processqueueClean(self, dryrun=False):
        """
        go through the process queue and clear out lower versions of the same files
        this is determined by product and utc_file_date
        """
        # TODO this might break with weekly input files
        DBlogging.dblogger.debug("Entering in queueClean(), there are {0} entries".format(self.Processqueue.len()))
        pqdata = self.Processqueue.getAll(version_bump=True)

        file_ids = list(map(itemgetter(0), pqdata))
        version_bumps = list(map(itemgetter(1), pqdata))
        
        # speed this up using a sql in_ call not looping over getEntry for each one
        # this gets all the file objects for the processqueue file_ids
        subq = self.session.query(self.Processqueue.file_id).subquery()
        file_entries = self.session.query(self.File).filter(self.File.file_id.in_(subq)).all()

        file_entries2 = self.file_id_Clean(file_entries)
        inds = (file_entries.index(v) for v in file_entries2)
        version_bumps2 = (version_bumps[i] for i in inds)
        #==============================================================================
        #         # sort keep on dates, then sort keep on level
        #==============================================================================
        # this should make them in order for each level
        file_entries2 = sorted(file_entries2, key=lambda x: x.utc_file_date, reverse=1)
        file_entries2 = sorted(file_entries2, key=lambda x: x.data_level)
        file_entries2 = [val.file_id for val in file_entries2]
        mixed_entries = itertools.izip(file_entries2, version_bumps2)

        ## now we have a list of just the newest file_id's
        if not dryrun:
            self.Processqueue.flush()
            #        self.Processqueue.push(ans)
            if not any(version_bumps2):
                self.Processqueue.push(file_entries2)
            else:
                itertools.starmap(self.Processqueue.push, mixed_entries)
#                for v in mixed_entries:
#                    itertools.startmap(self.Processqueue.push, v)
        else:
            print('<dryrun> Queue cleaned leaving {0} of {1} entries'.format(len(file_entries2), self.Processqueue.len()))

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
            try:
                f = self.getFileID(f)
            except DBNoData:
                pass            
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
            try:
                self.session.delete(self.getEntry('File', f))
            except DBNoData:
                pass            
            DBlogging.dblogger.info( "File removed from db {0}".format(f) )

        self._commitDB()

    def getAllSatellites(self):
        """
        return dictionaries of satellite, mission objects
        """
        ans = []
        sats = self.session.query(self.Satellite).all()
        ans = map(lambda x: self.getTraceback('Satellite', x.satellite_id), sats)
        return ans

    def getAllInstruments(self):
        """
        return dictionaries of instrument traceback dictionaries
        """
        ans = []
        insts = self.session.query(self.Instrument).all()
        ans = map(lambda x: self.getTraceback('Instrument', x.instrument_id), insts)
        return ans

    def getAllCodes(self, active=True):
        """
        return a list of all codes
        """
        ans = []
        if active:
            codes = self.session.query(self.Code).filter(and_(self.Code.newest_version, self.Code.active_code)).all()
        else:
            codes = self.session.query(self.Code).all()
        ans = map(lambda x: self.getTraceback('Code', x.code_id), codes)
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
        names = map(itemgetter(0), names)
        if fullPath:
            names = map(self.getFileFullPath, names)
        return names

    def getAllFileIds(self, newest_version=False):
        """
        return all the file ids in the database

        the itemgetter method is a lot faster then zip(*) (x16)
        """
        if not newest:
            ids = self.session.query(self.File.file_id).all()
            ids =  map(itemgetter(0), ids)
        else:
            # get all the product ids
            p_ids = self.getAllProducts()
            p_ids =  map(attrgetter('product_id'), p_ids)
            ids = []
            for p in p_ids:
                print p
                ids.extend(self.getFilesByProduct(p, newest_version=True))
            ids =  map(attrgetter('product_id'), ids)
        return ids

    def addMission(self,
                    mission_name,
                    rootdir,
                    incoming_dir):
        """
        add a mission to the database

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
        """
        add a product process link to the database

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
        """
        add a file code  link to the database

        @param resulting_file_id: id of the product to link
        @type resulting_file_id: int
        @param source_code: id of the code
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

        # are there any repalcements to do?  If not we are done
        match = False
        for r in repl:
            if r in inStr:
                match = True
        if not match:
            return inStr

        try:
            ftb = self.getTraceback('Product', product_id)
        except DBError: # during the addFromConfig process the full traceback is not yet there
            ftb = {}
            # fill in as much as we can know manually
            if '{PRODUCT}' in inStr :
                ftb['product'] = self.getEntry('Product', product_id)

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
        if '{ROOTDIR}' in inStr:
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
        if '{PRODUCT}' in inStr : # need to replace with the instrument name
            inStr = inStr.replace('{PRODUCT}', ftb['product'].product_name)
        if '{ROOTDIR}' in inStr:
            inStr = inStr.replace('{ROOTDIR}', str(ftb['mission'].rootdir))
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

        TODO, this is really slow, this query made it a lot faster but I bet it can get better

        """
        if hasattr(filename, 'upper'):
            filename = self.getFileID(filename)
        sq = self.session.query(self.File.filename, self.Product.relative_path).filter(self.File.file_id == filename).join((self.Product, self.File.product_id == self.Product.product_id)).one()
        path =  os.path.join(self.MissionDirectory, *sq[::-1])
        if '{' in path:
            file_entry = self.getEntry('File', filename)
            path = Utils.dirSubs(path,
                                 file_entry.filename,
                                 file_entry.utc_file_date,
                                 file_entry.utc_start_time,
                                 self.getVersion(file_entry.file_id)
                                 )
        return path


    def getProcessFromInputProduct(self, product):
        """
        given a product id return all the processes that use that as an input

        Use getProductID if have a name (or not sure)
        """
        DBlogging.dblogger.debug("Entered getProcessFromInputProduct: {0}".format(product))
        sq = self.session.query(self.Productprocesslink.process_id).filter_by(input_product_id = product).all()
        return map(itemgetter(0), sq)

    def getProcessFromOutputProduct(self, outProd):
        """
        Gets process from the db that have the output product
        """
        DBlogging.dblogger.debug("Entered getProcessFromOutputProduct: {0}".format(outProd))
        p_id = self.getProductID(outProd)
        sq1 = self.session.query(self.Process).filter_by(output_product = p_id).all()  # should only have one value
        if not sq1:
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
           proc_name = self.session.query(self.Process).get(proc_id)
           if proc_name is None:
               raise(NoResultFound('No row was found for id={0}'.format(proc_id)))
        except ValueError: # it is not a number
            proc_id = self.session.query(self.Process.process_id).filter_by(process_name = proc_name).one()[0]
        return proc_id

    def getSatelliteMission(self, sat_name):
        """
        given a satellite or satellite id return the mission
        """
        s_id = self.getSatelliteID(sat_name) # is a name
        m_id = self.getEntry('Satellite', s_id).mission_id
        return self.getEntry('Mission', m_id)

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
                sat_id = self.getSatelliteID(satellite_id)
                for v in sq:
                    if v.satellite_id == sat_id:
                        return v.instrument_id
                raise(ValueError("No matching instrument, satellite found. {0}:{1}".format(name, satellite_id)))
            return sq[0].instrument_id

    def getMissions(self):
        """return a list of all the missions"""
        sq = self.session.query(self.Mission.mission_name)
        return map(itemgetter(0), sq.all())

    def renameFile(self, filename, newname):
        """
        rename a file in the db
        """
        f = self.getEntry('File', filename)
        f.filename = newname
        self.session.add(f)
        self._commitDB()

    def getFileID(self, filename):
        """
        Return the fileID for the input filename

        @param filename: filename to return the fileid of
        @type filename: str

        @return: file_id: file_id of the input file
        @rtype: long
        """
        if isinstance(filename, self.File):
            return filename.file_id
        try:
            f_id = long(filename)
            sq = self.session.query(self.File).get(f_id)
            if sq is None:
                raise(DBNoData("No file_id {0} found in the DB".format(filename)))
            return sq.file_id
        except TypeError: # came in as list or tuple
            return map(self.getFileID, filename) 
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
            return map(self.getCodeID, codename)
        except ValueError:
            sq = self.session.query(self.Code.code_id).filter_by(filename = codename).all()
            if len(sq) == 0:
                raise(DBNoData("No code name {0} found in the DB".format(codename)))
            c_id = map(itemgetter(0), sq)
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

    def file_id_Clean(self, invals):
        """
        given a list of file objects clean out older versions of matching files
        matching is defined as same product_id and smae utc_file_date      
        """
        tmp = []
        for i in invals:
            if isinstance(i, (str, unicode)):
                tmp.append(self.getEntry('File', i))
            else:
                tmp.append(i)
        invals = tmp
        newest = list((v for fe in invals
                      for v in self.getFilesByProductDate(fe.product_id, [fe.utc_file_date]*2, newest_version=True)))
        newest = set([self.getEntry('File', v) for v in newest])
        return list(newest.intersection(invals))
        
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

    def getFilesByProductDate(self, product_id, daterange, newest_version=False):
        """
        return the files in the db by product id that have data in the date specified
        """
        dates = []
        for d in daterange:
            try:
                dates.append(d.date())
            except AttributeError:
                dates.append(d)

        if newest_version:
            # don't trust that the db has this correct
            # create a tabel populated with
            #   versionnum, file_id, utc_file_date


            # BUG DISCOVERED 2014-12-6 BAL
            # the logic in these queries does not use the max version for each utc_file_date
            # independently but instead the max in a range
            # this workaround fixes this but could be better

            aa = (self.session.query( (self.File.interface_version*1000  
                                           + self.File.quality_version*100 
                                           + self.File.revision_version).label('versionnum'), 
                                          self.File.file_id,
                                           self.File.filename, self.File, 
                                          self.File.utc_file_date )
                       .filter(self.File.utc_file_date.between(*dates))
                       .filter(self.File.product_id == product_id)
                       #.order_by(self.File.filename.asc())
                       .group_by(self.File.utc_file_date, 'versionnum')).all()

            
            sq = []
            for ele in aa:
                tmp = [v for v in aa if v[4] == ele[4]]
                t2 = max(tmp, key=lambda x: x[0])
                sq.append(t2[2])
            sq = sorted(list(set(list(sq))))


##             version = (self.session.query( (self.File.interface_version*1000  
##                                            + self.File.quality_version*100 
##                                            + self.File.revision_version).label('versionnum'), 
##                                           self.File.file_id,
##                                            self.File.filename,
##                                           self.File.utc_file_date )
##                        .filter(self.File.utc_file_date.between(*dates))
##                        .filter(self.File.product_id == product_id)
## #                       .group_by(self.File.file_id).subquery())
##                        .order_by(self.File.filename.asc())
##                        .group_by(self.File.utc_file_date)
##                        .order_by(self.File.filename.asc())
##                        .subquery())

##             subq = (self.session.query(func.max(version.c.versionnum))
##                   .group_by(version.c.utc_file_date)).order_by(version.c.utc_file_date).all()

##             sq = [self.session.query(version.c.file_id).filter(version.c.versionnum == v[0]).all() for v in subq]
##             sq = self.session.query(version.c.file_id).filter(version.c.versionnum == subq).all()

##             sq = list(map(itemgetter(0), sq))
##             sq = self.session.query(self.File).filter(self.File.file_id.in_(sq)).all()
           
        else: 
            sq = self.session.query(self.File).filter_by(product_id = product_id).\
                filter(self.File.utc_file_date.between(dates[0], dates[1])).all()       
        return sq

    def getFilesByDate(self, daterange, newest_version=False):
        """
        return the files in the db that have data in the date specified
        """
        dates = []
        for d in daterange:
            try:
                dates.append(d.date())
            except AttributeError:
                dates.append(d)

        if newest_version:
            raise(NotImplementedError("There is an error in this query, do not use!"))
            # don't trust that the db has this correct
            # create a tabel populated with
            #   versionnum, file_id, utc_file_date
            version = (self.session.query( (self.File.interface_version*1000  
                                           + self.File.quality_version*100 
                                           + self.File.revision_version).label('versionnum'), 
                                          self.File.file_id,
                                          self.File.utc_file_date, 
                                           self.File.product_id)
                       .filter(self.File.utc_file_date.between(*dates))
                       .group_by(self.File.product_id)).subquery()

            subq = (self.session.query(func.max(version.c.versionnum))
                  .group_by(version.c.product_id)).subquery()

            sq = self.session.query(version.c.file_id).filter(version.c.versionnum == subq).all()

            sq = list(map(itemgetter(0), sq))
            sq = self.session.query(self.File).filter(self.File.file_id.in_(sq)).all()
           
        else: 
            sq = self.session.query(self.File).\
                filter(self.File.utc_file_date.between(dates[0], dates[1])).all()       
        return sq

    def getFilesByProduct(self, prod_id, newest_version=False):
        """
        given a product_id or name return all the file instances associated with it

        if newest is set return only the newest files
        """
        prod_id = self.getProductID(prod_id)
        if newest_version:
            # don't trust that the db has this correct
            # create a tabel populated with
            #   versionnum, file_id, utc_file_date
            version = (self.session.query( (self.File.interface_version*1000  
                                           + self.File.quality_version*100 
                                           + self.File.revision_version).label('versionnum'), 
                                          self.File.file_id,
                                          self.File.utc_file_date )
                       .filter(self.File.product_id == prod_id)
                       .group_by(self.File.file_id).subquery())

            subq = (self.session.query(func.max(version.c.versionnum))
                  .group_by(version.c.utc_file_date)).subquery()

            sq = self.session.query(version.c.file_id).filter(version.c.versionnum == subq).all()

            sq = list(map(itemgetter(0), sq))
            sq = self.session.query(self.File).filter(self.File.file_id.in_(sq))

        else:
            sq = self.session.query(self.File).filter_by(product_id = prod_id)
        return sq.all()
    
    def getFilesByInstrument(self, inst_id, level=None, id_only=False):
        """
        given an instrument_if return all the file instances associated with it
        """
        inst_id = self.getInstrumentID(inst_id) # name or number
        subq = (self.session.query(self.Instrumentproductlink.product_id)
                .filter(self.Instrumentproductlink.instrument_id == inst_id).subquery())

        getme = (self.File.file_id if id_only else self.File)
        
        if level is None:
            files = (self.session.query(getme)
                     .filter(self.File.product_id.in_(subq))
                     .all())
        else:   
            files = (self.session.query(getme)
                     .filter(self.File.data_level==level)
                     .filter(self.File.product_id.in_(subq))
                     .all())
        if id_only:
            files = map(itemgetter(0), files)
        return files
    
    def getFilesByLevel(self, level, id_only=False, newest_version=False):
        """
        given a level return all the file instances associated with it
        """
        # get all the product ids of that level
        p_ids = self.getProductsByLevel(level)
        p_ids =  map(attrgetter('product_id'), p_ids)
        ids = []
        for p in p_ids:
            ids.extend(self.getFilesByProduct(p, newest_version=newest_version))
        if id_only:
            ids = map(attrgetter('file_id'), ids)
        return files

    def getAllFileIds(self, newest_version=False):
        """
        return all the file ids in the database

        the itemgetter method is a lot faster then zip(*) (x16)
        """
        if not newest_version:
            ids = self.session.query(self.File.file_id).all()
            ids =  map(itemgetter(0), ids)
        else:
            # get all the product ids
            p_ids = self.getAllProducts()
            p_ids =  map(attrgetter('product_id'), p_ids)
            ids = []
            for p in p_ids:
                print p
                ids.extend(self.getFilesByProduct(p, newest_version=True))
            ids =  map(attrgetter('product_id'), ids)
        return ids
    
    def getActiveInspectors(self):
        """
        query the db and return a list of all the active inspector file names [(filename, arguments, product), ...]
        """
        activeInspector = namedtuple('activeInspector', 'path arguments product_id')
        sq = self.session.query(self.Inspector).filter(self.Inspector.active_code == True).all()
        basedir = self.getMissionDirectory()
        retval = [activeInspector(os.path.join(basedir, ans.relative_path, ans.filename), ans.arguments, ans.product) for ans in sq]
        return retval

    def getChildrenProcesses(self, file_id):
        """
        given a file ID return all the processes that use this as input
        """
        DBlogging.dblogger.debug( "Entered findChildrenProducts():  file_id: {0}".format(file_id) )
        product_id = self.getEntry('File', file_id).product_id

        # get all the process ids that have this product as an input
        return self.getProcessFromInputProduct(product_id)

    def getProductID(self, product_name):
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
            s_id = map(self.getSatelliteID, sat_name)
            return s_id
        try:
            sat_id = long(sat_name)
            sq = self.session.query(self.Satellite).get(sat_id)
            if sq is None:
                raise(NoResultFound("No satellite id={0} found".format(sat_id)))
            return sq.satellite_id
        except ValueError: # it was a name
            sq = self.session.query(self.Satellite).filter_by(satellite_name=sat_name).one()
            return sq.satellite_id  # there can be only one of each name

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
        code = self.getEntry('Code', code_id)
        return Version.Version(code.interface_version, code.quality_version, code.revision_version)

    def getAllCodesFromProcess(self, proc_id):
        """
        given a process id return the code ids that performs that process and the valid dates

        Returns
        =======
        outval : (int, datetime.date, datetime.date)
            code id and dates that perform a process
        """
        DBlogging.dblogger.debug("Entered getAllCodesFromProcess: {0}".format(proc_id))
        # will have as many values as there are codes for a process
        sq = (self.session.query(self.Code).filter_by(process_id = proc_id)
              .filter_by(newest_version = True)
              .filter_by(active_code = True))
        ans = []
        for s in sq:
            ans.append((s.code_id, s.code_start_date, s.code_stop_date))
        return ans

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
        sq = (self.session.query(self.Code.code_id).filter_by(process_id = proc_id)
              .filter_by(newest_version = True)
              .filter_by(active_code = True).filter(self.Code.code_start_date <= utc_file_date)
              .filter(self.Code.code_stop_date >= utc_file_date))
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
            if len(mission_id) == 0:
                return None
            elif len(mission_id) > 1:
                raise(ValueError('No mission id specified and more than one mission present'))
            else:
                try:
                    mission_id = mission_id[0][0]
                except IndexError:
                    pass

        mission = self.getEntry('Mission',mission_id)
        return mission.rootdir

    def _checkIncoming(self, glb='*'):
        """
        check the incoming directory for the current mission and add those files to the getting list

        @return: processing list of file ids
        @rtype: list
        """
        path = self.getIncomingPath()
        DBlogging.dblogger.debug("Looking for files in {0}".format(path))
        files = glob.glob(os.path.join(path, glb))
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

    def tag_release(self, rel_num):
        """
        tag all the newest versions of files to a release number (integer)
        """
        newest_files = []
        prod_ids = [v.product_id for v in self.getAllProducts()]
        for prod in prods:
            newest_files.extend(self.getFilesByProduct(prod_ids, newest_version=True))

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
        sq = map(itemgetter(0), sq)
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
        master routine for all the getXXXTraceback functions, this will make for less code

        this is some large select statements with joins in them, these are tested and do work
        """       
        retval = {}
        if table.capitalize() == 'File':
            vars = ['file', 'product', 'inspector', 'instrument',
                    'instrumentproductlink', 'satellite', 'mission']

            in_id = self.getFileID(in_id)

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
            
            if not sq: # did not find a matchm this is a dberror
                raise(DBError("file {0} did not have a traceback, this is a problem, fix it".format(in_id)))
            
            if len(sq) > 1:
                raise(DBError("Found multiple tracebacks for file {0}".format(in_id)))
            for ii, v in enumerate(vars):
                retval[v] = sq[0][ii]

        elif table.capitalize() == 'Code':
            vars = ['code', 'process', 'product', 'instrument',
                    'instrumentproductlink', 'satellite', 'mission']

            in_id = self.getCodeID(in_id)

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
            
            if not sq: # did not find a match this is a dberror
                raise(DBError("code {0} did not have a traceback, this is a problem, fix it".format(in_id)))

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

            in_id = self.getProductID(in_id)

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
             
            if not sq: # did not find a match this is a dberror
                raise(DBError("product {0} did not have a traceback, this is a problem, fix it".format(in_id)))
                 
            if len(sq) > 1:
                raise(DBError("Found multiple tracebacks for product {0}".format(in_id)))
            for ii, v in enumerate(vars):
                retval[v] = sq[0][ii]

        elif table.capitalize() == 'Process':

            vars = ['process', 'product', 'instrument',
                    'instrumentproductlink', 'satellite', 'mission']

            in_id = self.getProcessID(in_id)

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

            if not sq: # did not find a match this is a dberror
                raise(DBError("process {0} did not have a traceback, this is a problem, fix it".format(in_id)))
            
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
            return  map(itemgetter(0), sq)
        else:
            return None

    def getProductsByLevel(self, level):
        """
        get all the products for a given level
        """
        sq = self.session.query(self.product).filter_by(level = level).all()
        if sq:
            return  map(itemgetter(0), sq)
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

    def getProcessTimebase(self, process_id):
        """
        return the timebase for a product
        """
        # this is two queries but allows for name or id as input
        process_id = self.getProcessID(process_id)
        return self.session.query(self.Process.output_timebase).get(process_id)[0] 

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
        return retval

    def getFilesByCode(self, code_id, id_only=False):
        """
        given a code_id (or name) return the files that were created using it
        """
        code_id = self.getCodeID(code_id)
        f_ids = self.session.query(self.Filecodelink.resulting_file).filter_by(source_code=code_id).all()
        f_ids = map(itemgetter(0), f_ids)
        files = map(lambda x: self.getEntry('File', x), f_ids)
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
        f_ids = map(itemgetter(0), f_ids)
        files = [self.getEntry('File', val) for val in f_ids]
        if not id_only:
            return files
        else:
            return map(attrgetter('file_id'), files)

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
