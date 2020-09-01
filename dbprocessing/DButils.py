from __future__ import absolute_import
from __future__ import print_function

import collections
import datetime
import pdb
import glob
import itertools
import os.path
import pwd
import socket  # to get the local hostname
import sys
from collections import namedtuple
from operator import itemgetter, attrgetter

import sqlalchemy
import sqlalchemy.sql.expression
from sqlalchemy import Table
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sessionmaker
import sqlalchemy.orm.exc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import func
from sqlalchemy import and_

from .Diskfile import calcDigest, DigestError
from . import DBlogging
from . import DBstrings
from . import Version
from . import Utils
from . import __version__


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


class DButils(object):
    """
    Utility routines for the DBProcessing class, all of these may be user called but are meant to
    be internal routines for DBProcessing
    """

    def __init__(self, mission='Test', db_var=None, echo=False, engine='sqlite'):
        """
        Initialize the DButils class

        :param mission: Name of the mission
        :type mission: str
        :param db_var: Does nothing.
        :param echo: if True, the Engine will log all statements as well as a repr() of their parameter lists to the logger
        :type echo: bool
        :param engine: DB engine to connect to
        :type engine: str
        """
        self.dbIsOpen = False
        if mission is None:
            raise (DBError("Must input database name to create DButils instance"))
        self.mission = mission
        # Expose the format/regex routines of DBformatter
        fmtr = DBstrings.DBformatter()
        self.format = fmtr.format
        self.re = fmtr.re
        self.openDB(db_var=db_var, engine=engine, echo=echo)
        self._createTableObjects()
        try:
            self._patchProcessQueue()
        except AttributeError:
            raise (AttributeError('{0} is not a valid database'.format(mission)))

        self.MissionDirectory = self.getMissionDirectory()
        self.CodeDirectory = self.getCodeDirectory()
        self.InspectorDirectory = self.getInspectorDirectory()

    def __del__(self):
        """
        try and clean up a little bit
        """
        self.closeDB()

    def __repr__(self):
        """
        @summary: Print out something useful when one prints the class instance

        :return: DBProcessing class instance for mission <mission name>
        """
        return 'DBProcessing class instance for mission ' + self.mission + ', version: ' + __version__

    def _patchProcessQueue(self):
        self.Processqueue.flush = self._processqueueFlush
        self.Processqueue.remove = self._processqueueRemoveItem
        self.Processqueue.getAll = self._processqueueGetAll
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

    def openDB(self, engine, db_var=None, verbose=False, echo=False):
        """
        Setup python to talk to the database, this is where it is, name and password.

        :param engine: DB engine to connect to
        :type engine: str
        :param db_var: Does nothing.
        :param echo: if True, the Engine will log all statements as well as a repr() of their parameter lists to the logger
        :type echo: bool
        :param verbose: if True, will print out extra debugging
        :type verbose: bool
        """
        if self.dbIsOpen == True:
            return
        try:
            if not os.path.isfile(os.path.expanduser(self.mission)):
                raise (ValueError("DB file specified doesn't exist"))
            engineIns = sqlalchemy.create_engine('{0}:///{1}'.format(engine, os.path.expanduser(self.mission)),
                                                 echo=echo)
            self.mission = os.path.abspath(os.path.expanduser(self.mission))

            DBlogging.dblogger.info("Database Connection opened: {0}  {1}".format(str(engineIns), self.mission))

        except (DBError, ArgumentError):
            (t, v, tb) = sys.exc_info()
            raise (DBError('Error creating engine: ' + str(v)))
        try:
            metadata = sqlalchemy.MetaData(bind=engineIns)
            # a session is what you use to actually talk to the DB, set one up with the current engine
            Session = sessionmaker(bind=engineIns)
            session = Session()
            self.engine = engineIns
            self.metadata = metadata
            self.session = session
            self.dbIsOpen = True
            if verbose: print("DB is open: %s" % (engineInsR))
            return
        except Exception as msg:
            raise (DBError('Error opening database: %s' % (msg)))

    def _createTableObjects(self, verbose=False):
        """
        cycle through the database and build classes for each of the tables
        """
        DBlogging.dblogger.debug("Entered _createTableObjects()")

        ## ask for the table names form the database (does not grab views)
        table_names = self.engine.table_names()

        ## create a dictionary of all the table names that will be used as class names.
        ## this uses the db table name as the table name and a cap 1st letter as the class
        ## when interacting using python use the class
        table_dict = { }
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
            if verbose: print(val)
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

    def currentlyProcessing(self):
        """
        Checks the db to see if it is currently processing, don't want to do 2 at the same time

        :return: false or the pid
        :rtype: bool or long

        >>>  pnl.currentlyProcessing()
        """
        DBlogging.dblogger.info("Checking currently_processing")

        sq = self.session.query(self.Logging).filter_by(currently_processing=True).all()
        if len(sq) == 1:
            DBlogging.dblogger.warning("currently_processing is set.  PID: {0}".format(sq[0].pid))
            return sq[0].pid
        elif len(sq) == 0:
            return False
        else:
            DBlogging.dblogger.error("More than one currently_processing flag set, fix the DB")
            raise (DBError("More than one currently_processing flag set, fix the DB"))

    def resetProcessingFlag(self, comment):
        """
        Query the db and reset a processing flag

        :param comment: the comment to enter into the processing log DB
        :type comment: str
        :return: True - Success, False - Failure
        :rtype: bool
        """
        sq2 = self.session.query(self.Logging).filter_by(currently_processing=True).count()
        if sq2 and comment is None:
            raise (ValueError("Must enter a comment to override DB lock"))
        sq = self.session.query(self.Logging).filter_by(currently_processing=True)
        for val in sq:
            val.currently_processing = False
            val.processing_end = datetime.datetime.utcnow()
            val.comment = 'Overridden:' + comment + ':' + __version__
            DBlogging.dblogger.error("Logging lock overridden: %s" % ('Overridden:' + comment + ':' + __version__))
            self.session.add(val)
        if sq2:
            self.commitDB()

    def startLogging(self):
        """
        Add an entry to the logging table in the DB, logging
        """
        # this is the logging of the processing, no real use for it yet but maybe we will in the future
        # helps to know is the process ran and if it succeeded
        if self.currentlyProcessing():
            raise (DBError('A Currently Processing flag is still set, cannot process now'))
        # save this class instance so that we can finish the logging later
        self.__p1 = self.addLogging(True,
                                    datetime.datetime.utcnow(),
                                    ## for now there is one mission only per DB
                                    # self.getMissionID(self.mission),
                                    self.session.query(self.Mission.mission_id).first()[0],
                                    pwd.getpwuid(os.getuid())[0],
                                    socket.gethostname(),
                                    pid=os.getpid())
        DBlogging.dblogger.info("Logging started: %d: %s, PID: %s, M_id: %s, user: %s, hostname: %s" %
                                (self.__p1.logging_id, self.__p1.processing_start_time, self.__p1.pid,
                                 self.__p1.mission_id, self.__p1.user, self.__p1.hostname))

    def addLogging(self,
                   currently_processing,
                   processing_start_time,
                   mission_id,
                   user,
                   hostname,
                   pid=None,
                   processing_end_time=None,
                   comment=None):
        """
        Add an entry to the logging table

        :param currently_processing: is the db currently processing?
        :type currently_processing: bool
        :param processing_start_time: the time the processing started
        :type processing_start_time: datetime.datetime
        :param mission_id: the mission id the processing if for
        :type mission_id: int
        :param user: the user doing the processing
        :type user: str
        :param hostname: the hostname that initiated the processing
        :type hostname: str

        :keyword pid: the process id that id the processing
        :type pid: int
        :keyword processing_end_time: the time the processing stopped
        :type processing_end_time: datetime.datetime
        :keyword comment: comment about the processing run
        :type comment: str

        :return: instance of the Logging class
        :rtype: Logging

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
        self.commitDB()
        return l1  # so we can use the same session to stop the logging

    def stopLogging(self, comment):
        """
        Finish the entry to the processing table in the DB, logging

        :param comment: (optional) a comment to insert into he DB
        :type param: str
        """
        try:
            self.__p1
        except:
            DBlogging.dblogger.warning("Logging was not started, can't stop")
            raise (DBProcessingError("Logging was not started"))
        # clean up the logging, we are done processing and we can release the lock (currently_processing) and
        # put in the complete time

        self.__p1.processing_end = datetime.datetime.utcnow()
        self.__p1.currently_processing = False
        self.__p1.comment = comment + ':' + __version__
        self.session.add(self.__p1)
        self.commitDB()
        DBlogging.dblogger.info("Logging stopped: %s comment '%s' " % (self.__p1.processing_end, self.__p1.comment))
        del self.__p1

    def checkDiskForFile(self, file_id, fix=False):
        """
        Check the file system to see if the file exits or not as it says in the db

        :param file_id: id of the file to check
        :type file_id: int

        :keyword fix: (optional) set to have the DB fixed to match the file system
           this is **NOT** sure to be safe
        :type fix: bool

        :returns: Return true is consistent, False otherwise
        :rtype: bool
        """
        sq = self.getEntry('File', file_id)
        if sq.exists_on_disk:
            file_path = self.getFileFullPath(sq.file_id)
            if not os.path.exists(file_path):
                if fix:
                    sq.exists_on_disk = False
                    self.session.add(sq)
                    self.commitDB()
                    return self.checkDiskForFile(file_id)  # call again to get the True
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
        self.commitDB()
        DBlogging.dblogger.info("Processqueue was cleared")
        return length

    def _processqueueRemoveItem(self, item, commit = True):
        """
        remove a file from the queue by name or number
        """
        # if the input is a file name need to handle that
        if not hasattr(item, '__iter__'):
            item = [item]
        for ii, v in enumerate(item):
            item[ii] = self.getFileID(v)
        sq = self.session.query(self.Processqueue).filter(self.Processqueue.file_id.in_(item))
        for v in sq:
            self.session.delete(v)
        if sq and commit:
            self.commitDB()

    def _processqueueGetAll(self, version_bump=False):
        """
        Return the entire contents of the process queue
        """
        pqdata = self.session.query(self.Processqueue).all()

        if version_bump:
            ans = zip(map(attrgetter('file_id'), pqdata), map(attrgetter('version_bump'), pqdata))
        else:
            ans = map(attrgetter('file_id'), pqdata)

        DBlogging.dblogger.debug("Entire Processqueue was read: {0} elements returned".format(len(ans)))
        return ans

    def _processqueuePush(self, fileid, version_bump=None, MAX_ADD=150):
        """
        Push a file onto the process queue (onto the right)

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
            if len(fileid) > MAX_ADD:
                outval = []
                for v in Utils.chunker(fileid, MAX_ADD):
                    outval.extend(self._processqueuePush(v, version_bump=version_bump))
                return outval

        # first filter() takes care of putting in values that are not in the DB.  It is silent
        # second filter() takes care of not reading files that are already in the queue
        subq = self.session.query(self.Processqueue.file_id).subquery()

        fileid = (self.session.query(self.File.file_id)
                  .filter(self.File.file_id.in_(fileid))
                  .filter(~self.File.file_id.in_(subq))).all()

        fileid = list(map(itemgetter(0), fileid))  # nested tuples to list

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
        DBlogging.dblogger.debug("File added to process queue {0}:{1}".format(fileid, '---'))
        if fileid:
            self.session.add_all(objs)
            self.commitDB()
        #        pqid = self.session.query(self.Processqueue.file_id).all()
        return outval

    def _processqueueRawadd(self, fileid, version_bump=None, commit=True):
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
                pq1.version_bump = version_bump
                self.session.add(pq1)
                DBlogging.dblogger.debug("File added to process queue {0}:{1}".format(fileid, '---'))
                
            if commit:
                self.commitDB()  # commit once for all the adds
        return len(files_to_add)

    def _processqueueLen(self):
        """
        Return the number of files in the process queue
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
        val = self._processqueueGet(index=index, instance=True)
        self.session.delete(val)
        self.commitDB()
        return (val.file_id, val.version_bump)

    def _processqueueGet(self, index=0, instance=False):
        """
        Get the file at the head of the queue (from the left)

        Returns
        =======
        file_id : int
            the file_id of the file popped from the queue
        """
        if index < 0:  # enable the python from the end indexing
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
        also sorts by level, date
        """

        # BAL 30 March 2017 Trying a different method here that might be cleaner

        # # TODO this might break with weekly input files
        # DBlogging.dblogger.debug("Entering _processqueueClean(), there are {0} entries".format(self.Processqueue.len()))
        # pqdata = self.Processqueue.getAll(version_bump=True)
        #
        # file_ids = list(map(itemgetter(0), pqdata))
        # version_bumps = list(map(itemgetter(1), pqdata))
        #
        # # speed this up using a sql in_ call not looping over getEntry for each one
        # # this gets all the file objects for the processqueue file_ids
        # subq = self.session.query(self.Processqueue.file_id).subquery()
        # file_entries = self.session.query(self.File).filter(self.File.file_id.in_(subq)).all()
        #
        # file_entries2 = self.file_id_Clean(file_entries)
        #
        # # ==============================================================================
        # #         # sort keep on dates, then sort keep on level
        # # ==============================================================================
        # # this should make them in order for each level
        # file_entries2 = sorted(file_entries2, key=lambda x: x.utc_file_date, reverse=1)
        # file_entries2 = sorted(file_entries2, key=lambda x: x.data_level)
        # # apply same sort/filter to version_bumps
        # version_bumps2 = (version_bumps[file_entries.index(v)] for v in file_entries2)
        #
        # file_entries2 = [val.file_id for val in file_entries2]
        # mixed_entries = itertools.izip(file_entries2, version_bumps2)
        #
        # ## now we have a list of just the newest file_id's
        # if not dryrun:
        #     self.Processqueue.flush()
        #     #        self.Processqueue.push(ans)
        #     if not any(version_bumps2):
        #         self.Processqueue.push(file_entries2)
        #     else:
        #         itertools.starmap(self.Processqueue.push, mixed_entries)
        #     #                for v in mixed_entries:
        #     #                    itertools.startmap(self.Processqueue.push, v)
        # else:
        #     print(
        #         '<dryrun> Queue cleaned leaving {0} of {1} entries'.format(len(file_entries2), self.Processqueue.len()))
        #
        # DBlogging.dblogger.debug(
        #     "Done in _processqueueClean(), there are {0} entries left".format(self.Processqueue.len()))

        # # BAL 30 March 2017 new version
        # # get all the files from the process queue
        DBlogging.dblogger.debug("Entering _processqueueClean(), there are {0} entries".format(self.Processqueue.len()))
        pqdata = self.Processqueue.getAll(version_bump=True)
        # all we need to do is look at each file and see if it is latest version or not, if it is not drop it
        entries = []
        version_bump = False
        for i, pq in enumerate(pqdata):
            # print(i, len(pqdata))
            if pq[1] is not None:
                entries.append(pq)  # if the version_bump is these it needs to stay in the queue
                version_bump = True
            else:
                if self.fileIsNewest(pq[0]):
                    entries.append(pq)  # if the file is newest_version than we keep it
        if not dryrun:
            self.Processqueue.flush()
            if not version_bump:
                self.Processqueue.rawadd(zip(*entries)[0])
            else:
                for f in pqdata:
                    self.Processqueue.add(f)
        else:
            print(
                '<dryrun> Queue cleaned leaving {0} of {1} entries'.format(len(file_entries2), self.Processqueue.len()))
        DBlogging.dblogger.debug(
            "Done in _processqueueClean(), there are {0} entries left".format(self.Processqueue.len()))


    def fileIsNewest(self, filename, debug=False):
        """
        quesry the database, is this filename or file_id newest version?

        @param filename: filename or file_id
        @return: Ture is file is lastest_version, False is not
        """
        file = self.getEntry('File', filename)
        product_id = file.product_id
        if debug: print('product_id', product_id )
        date = file.utc_file_date
        if debug: print('date', date)
        file_id = file.file_id
        if debug: print('file_id', file_id, file.filename)
        latest = self.getFilesByProductDate(product_id, [date]*2, newest_version=True)
        if len(latest) > 1:
            raise(DBError("More than one latest for a product date"))
        latest_id = latest[0].file_id
        if debug: print('latest_id', latest_id)
        return file_id == latest_id

    def _purgeFileFromDB(self, filename=None, recursive=False, verbose=False, trust_id=False, commit=True):
        """
        removes a file from the DB

        :param filename: name of the file to remove (or a list of names)
        :param recursive: remove all files that depend on the Given

        if recursive then it removes all files that depend on the one to remove

        >>>  pnl._purgeFileFromDB('Test-one_R0_evinst-L1_20100401_v0.1.1.cdf')
        """
        if not hasattr(filename, '__iter__'):  # if not an iterable make it a iterable
            filename = [filename]

        for ii, f in enumerate(filename):
            if not trust_id:
                try:
                    f = self.getFileID(f)
                except DBNoData:
                    pass
            else:
                pass  # just use the id without a lookup

            if recursive:
                ids = self.session.query(self.Filefilelink.resulting_file).filter_by(source_file=f).all()
                for fid in ids:
                    self._purgeFileFromDB(filename=fid.resulting_file, recursive=True, verbose=verbose)

            if verbose:
                print(ii, len(filename), f)

            # we need to look in each table that could have a reference to this file and delete that
            try:  ## processqueue
                self.Processqueue.remove(f)
            except DBNoData:
                pass

            try:  ## filefilelink
                self.delFilefilelink(f)
            except DBNoData:
                pass

            try:  ## filecodelink
                self.delFilecodelink(f)
            except DBNoData:
                pass

            try:  ## file
                self.session.delete(self.getEntry('File', f))
            except DBNoData:
                pass

            DBlogging.dblogger.info("File removed from db {0}".format(f))

        if commit:
            self.commitDB()

    def getAllSatellites(self):
        """
        Return dictionaries of satellite, mission objects

        :return: dictionaries of satellite, mission objects
        :rtype: dict
        """
        ans = []
        sats = self.session.query(self.Satellite).all()
        ans = map(lambda x: self.getTraceback('Satellite', x.satellite_id), sats)
        return ans

    def getAllInstruments(self):
        """
        Return dictionaries of instrument traceback dictionaries

        :return: dictionaries of instrument traceback dictionaries
        :rtype: dict
        """
        ans = []
        insts = self.session.query(self.Instrument).all()
        ans = map(lambda x: self.getTraceback('Instrument', x.instrument_id), insts)
        return ans

    def getAllCodes(self, active=True):
        """
        Return a list of all codes
        """
        ans = []
        if active:
            codes = self.session.query(self.Code).filter(and_(self.Code.newest_version, self.Code.active_code)).all()
        else:
            codes = self.session.query(self.Code).all()
        ans = map(lambda x: self.getTraceback('Code', x.code_id), codes)
        return ans

    def getAllFilenames(self,
                        fullPath=True,
                        startDate=None,
                        endDate=None,
                        level=None,
                        product=None,
                        code=None,
                        instrument=None,
                        exists=None,
                        newest_version=False,
                        limit=None):
        """
        Return all the file names in the database

        :param bool fullPath: Return the fullPath or just filename
        :param int level: Filter by given level
        :param int product: Filter by given product
        :param int limit: Limit number of results

        :return: List of strs with the filename
        :rtype: list
        """

        files = self.getFiles(startDate, endDate, level, product, code, instrument, exists, newest_version, limit)

        if fullPath:
            # Get file_id instead, saves time since getFileFullPath gets the ID anyway
            names = [d.file_id for d in files]
            # This is probobly slow, but hopfully not slow enough to be an issue
            return map(self.getFileFullPath, names)
        else:
            return [d.filename for d in files]

    def addMission(self,
                   mission_name,
                   rootdir,
                   incoming_dir,
                   codedir=None,
                   inspectordir=None,
                   errordir=None):
        """
        Add a mission to the database

        Optional directories which are not specified will be inserted
        into the database as nulls, and the default will be determined
        at runtime.

        :param mission_name: the name of the mission
        :type mission_name: str
        :param rootdir: the root directory of the mission
        :type rootdir: str
        :param str incoming_dir: directory for incoming files
        :param str codedir: directory containing codes (optional; see
                            :meth:`getCodeDirectory`)
        :param str inspectordir: directory containing product inspectors
                                 (optional; see :meth:`getInspectorDirectory`)
        :param str errordir: directory to contain error files (optional;
                             see :meth:`getErrorPath`)
        """
        mission_name = str(mission_name)
        rootdir = str(rootdir)
        try:
            m1 = self.Mission()
        except AttributeError:
            raise (DBError("Class Mission not found was it created?"))

        m1.mission_name = mission_name
        m1.rootdir = rootdir.replace('{MISSION}', mission_name)
        m1.incoming_dir = incoming_dir.replace('{MISSION}', mission_name)
        m1.codedir = None if codedir is None \
                     else codedir.replace('{MISSION}', mission_name)
        m1.inspectordir = None if inspectordir is None\
                          else inspectordir.replace('{MISSION}', mission_name)

        if hasattr(m1, 'newest_version'):
            # Old DBs will not have this, new ones will
            m1.errordir = None if errordir is None \
                          else errordir.replace('{MISSION}', mission_name)

        self.session.add(m1)
        self.commitDB()
        return m1.mission_id

    def addSatellite(self,
                     satellite_name, mission_id):
        """
        Add a satellite to the database

        :param satellite_name: the name of the mission
        :type satellite_name: str
        """
        satellite_name = str(satellite_name)
        s1 = self.Satellite()

        s1.mission_id = mission_id
        s1.satellite_name = satellite_name.replace('{MISSION}', self.getEntry('Mission', mission_id).mission_name)
        self.session.add(s1)
        self.commitDB()
        return s1.satellite_id

    def addProcess(self,
                   process_name,
                   output_product,
                   output_timebase,
                   extra_params=None,
                   trigger=None):
        """
        Add a process to the database

        :param process_name: the name of the process
        :type process_name: str
        :param output_product: the output product id
        :type output_product: int
        :keyword extra_params: extra parameters to pass to the code
        :type extra_params: str
        """
        if output_timebase not in ['RUN', 'ORBIT', 'DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY', 'FILE']:
            raise (ValueError("output_timebase invalid choice"))

        p1 = self.Process()
        p1.output_product = output_product
        p1.process_name = process_name
        p1.extra_params = Utils.toNone(extra_params)
        p1.output_timebase = output_timebase
        self.session.add(p1)
        self.commitDB()
        # self.updateProcessSubs(p1.process_id)
        return p1.process_id

    def addProduct(self,
                   product_name,
                   instrument_id,
                   relative_path,
                   format,
                   level,
                   product_description):
        """
        Add a product to the database

        :param product_name: the name of the product
        :type product_name: str
        :param instrument_id: the instrument   the product is from
        :type instrument_id: int
        :param relative_path: relative path for the product
        :type relative_path: str
        :param format: the format of the product files
        :type format: str
        """
        p1 = self.Product()
        p1.instrument_id = instrument_id
        p1.product_name = product_name
        p1.relative_path = relative_path
        p1.format = format
        p1.level = level
        p1.product_description = product_description
        self.session.add(p1)
        self.commitDB()
        return p1.product_id

    def updateProductSubs(self, product_id):
        """
        Update an existing product performing the {} replacements
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
        self.commitDB()

    def updateInspectorSubs(self, insp_id):
        """
        Update an existing inspector performing the {} replacements
        """
        # need to do {} replacement, have to do it as a modification
        p1 = self.getEntry('Inspector', insp_id)

        insp_id = p1.inspector_id
        relative_path = self._nameSubInspector(p1.relative_path, insp_id)
        p1.relative_path = relative_path
        self.session.add(p1)
        self.commitDB()

    def updateProcessSubs(self, proc_id):
        """
        Update an existing product performing the {} replacements
        """
        # need to do {} replacement, have to do it as a modification
        p1 = self.getEntry('Process', proc_id)
        proc_id = p1.process_id
        process_name = self._nameSubProcess(p1.process_name, proc_id)
        p1.process_name = process_name
        extra_params = self._nameSubProcess(p1.extra_params, proc_id)
        p1.extra_params = extra_params
        self.session.add(p1)
        self.commitDB()

    def addproductprocesslink(self,
                              input_product_id,
                              process_id,
                              optional,
                              yesterday=0,
                              tomorrow=0):
        """
        Add a product process link to the database

        :param input_product_id: id of the product to link
        :type input_product_id: int
        :param process_id: id of the process to link
        :type process_id: int
        :param optional: if the input product is necessary
        :type optional: boolean
        :param yesterday: How many extra days back do you need
        :type yesterday: int
        :param tomorrow: How many extra days forward do you need
        :type tomorrow: int
        """
        ppl1 = self.Productprocesslink()
        ppl1.input_product_id = self.getProductID(input_product_id)
        ppl1.process_id = self.getProcessID(process_id)
        ppl1.optional = optional
        #Backwards compatability with old databases
        if hasattr(ppl1, 'yesterday'):
            ppl1.yesterday = yesterday;
            ppl1.tomorrow = tomorrow;
        self.session.add(ppl1)
        self.commitDB()
        return ppl1.input_product_id, ppl1.process_id

    def addFilecodelink(self,
                        resulting_file_id,
                        source_code):
        """
        Add a file code  link to the database

        :param resulting_file_id: id of the product to link
        :type resulting_file_id: int
        :param source_code: id of the code
        :type source_code: int
        """
        fcl1 = self.Filecodelink()
        fcl1.resulting_file = resulting_file_id
        fcl1.source_code = source_code
        self.session.add(fcl1)
        self.commitDB()
        return fcl1.resulting_file, fcl1.source_code

    def delInspector(self, i):
        """
        Removes an inspector form the db
        """
        insp = self.getEntry('Inspector', i)
        self.session.delete(insp)
        self.commitDB()

    def delFilefilelink(self, f, commit = True):
        """
        Remove entries from Filefilelink, it will remove if the file is in either column
        """
        f = self.getFileID(f)  # change a name to a number
        n1 = self.session.query(self.Filefilelink).filter_by(source_file=f).delete()
        n2 = self.session.query(self.Filefilelink).filter_by(resulting_file=f).delete()
        if n1 + n2 == 0:
            raise (DBNoData("No entry for ID={0} found".format(f)))
        elif commit:
            self.commitDB()

    def delFilecodelink(self, f, commit = True):
        """
        Remove entries from Filecodelink fore a Given file
        """
        f = self.getFileID(f)  # change a name to a number
        n2 = self.session.query(self.Filecodelink).filter_by(resulting_file=f).delete()
        if n2 == 0:
            raise (DBNoData("No entry for ID={0} found".format(f)))
        elif commit:
            self.commitDB()

    def delProduct(self, pp):
        """
        Removes a product from the db
        Note: untested!
        """
        prod = self.getEntry('Product', pp)
        self.session.delete(prod)
        self.commitDB()

    def delProductProcessLink(self, ll):
        """
        Removes a product from the db
        :param list ll: two element list: process_id, product_id
        Note: untested!
        """
        link = self.getEntry('Productprocesslink', ll)
        self.session.delete(link)
        self.commitDB()

    def purgeProcess(self, proc, commit = True):
        """
        Remove process and productprocesslink
        :param DButils.Process proc: ID for process to be deleted.
        """
        sq=self.session.query(self.Productprocesslink.input_product_id)\
                       .filter_by(process_id=proc.process_id)
        prod_ids = [ii for ii, in sq]
        for prod_id in prod_ids:
            link = self.getEntry('Productprocesslink',[proc.process_id, prod_id])
            self.session.delete(link)

        self.session.delete(proc)
        if commit:
            self.commitDB()
        

    def addFilefilelink(self,
                        resulting_file_id,
                        source_file, ):
        """
        Add a file file  link to the database

        :param source_file: id of the product to link
        :type source_file: int
        :param resulting_file_id: id of the process to link
        :type resulting_file_id: int

        """
        ffl1 = self.Filefilelink()
        ffl1.source_file = source_file
        ffl1.resulting_file = resulting_file_id
        self.session.add(ffl1)
        self.commitDB()
        return ffl1.source_file, ffl1.resulting_file

    def addInstrumentproductlink(self,
                                 instrument_id,
                                 product_id):
        """
        Add a instrument product  link to the database

        :param instrument_id: id of the instrument to link
        :type instrument_id: int
        :param product_id: id of the product to link
        :type product_id: int
        """
        ipl1 = self.Instrumentproductlink()
        ipl1.instrument_id = instrument_id
        ipl1.product_id = product_id
        self.session.add(ipl1)
        self.commitDB()
        return ipl1.instrument_id, ipl1.product_id

    def addInstrument(self,
                      instrument_name,
                      satellite_id):
        """
        Add a Instrument to the database

        :param instrument_name: the name of the mission
        :type instrument_name: str
        :param satellite_id: the root directory of the mission
        :type satellite_id: int
        """
        i1 = self.Instrument()

        i1.satellite_id = satellite_id
        if '{MISSION}' in instrument_name:
            mission_name = self.getSatelliteMission(satellite_id).mission_name
            instrument_name = instrument_name.replace('{MISSION}', mission_name)
        if '{SPACECRAFT}' in instrument_name:
            satellite_name = self.getEntry('Satellite', satellite_id).satellite_name
            instrument_name = instrument_name.replace('{SPACECRAFT}', satellite_name)

        i1.instrument_name = instrument_name
        self.session.add(i1)
        self.commitDB()
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

        :param filename: the filename of the code
        :type filename: str
        :param relative_path: the relative path (relative to mission base dir)
        :type relative_path: str
        :param code_start_date: start of validity of the code (datetime)
        :type code_start_date: datetime
        :param code_stop_date: end of validity of the code (datetime)
        :type code_stop_date: datetime
        :param code_description: description of the code (50 char)
        :type code_description: str
        :param process_id: the id of the process this code is part of
        :type process_id: int
        :param version: the version of the code
        :type version: Version.Version
        :param active_code: Boolean True means the code is active
        :type active_code: Boolean
        :param date_written: the date the cod was written
        :type date_written: date
        :param output_interface_version: the interface version of the output (effects the data file names)
        :type output_interface_version: int
        :param newest_version: is this code the newest version in the DB?
        :type newest_version: bool

        :return: the code_id of the newly inserted code
        :rtype: long
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
        c1.quality_version = version.quality
        c1.revision_version = version.revision
        c1.active_code = Utils.toBool(active_code)
        c1.date_written = Utils.parseDate(date_written)
        c1.output_interface_version = output_interface_version
        c1.newest_version = Utils.toBool(newest_version)
        c1.arguments = Utils.toNone(arguments)
        c1.ram = ram
        c1.cpu = cpu

        self.session.add(c1)
        self.commitDB()
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

        :param filename: the filename of the code
        :type filename: str
        :param relative_path: the relative path (relative to mission base dir)
        :type relative_path: str
        :param description: description of the code (50 char)
        :type description: str
        :param product: the id of the product this inspector finds
        :type product: int
        :param version: the version of the code
        :type version: Version.Version
        :param active_code: Boolean True means the code is active
        :type active_code: Boolean
        :param date_written: the date the cod was written
        :type date_written: date
        :param output_interface_version: the interface version of the output (effects the data file names)
        :type output_interface_version: int
        :param newest_version: is this code the newest version in the DB?
        :type newest_version: bool

        :return: the inspector_id of the newly inserted code
        :rtype: long

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
        self.commitDB()
        return c1.inspector_id

    def _nameSubProduct(self, inStr, product_id):
        """
        In inStr replace the standard {} with the names
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
        except DBError:  # during the addFromConfig process the full traceback is not yet there
            ftb = { }
            # fill in as much as we can know manually
            if '{PRODUCT}' in inStr:
                ftb['product'] = self.getEntry('Product', product_id)

        if '{INSTRUMENT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{SPACECRAFT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{SPACECRAFT}', ftb['satellite'].satellite_name)
        if '{MISSION}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{MISSION}', ftb['mission'].mission_name)
        if '{PRODUCT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{PRODUCT}', ftb['product'].product_name)
        if '{LEVEL}' in inStr:
            inStr = inStr.replace('{LEVEL}', str(ftb['product'].level))
        if '{ROOTDIR}' in inStr:
            inStr = inStr.replace('{ROOTDIR}', str(ftb['mission'].rootdir))
        if any(val in inStr for val in repl):  # call yourself again
            inStr = self._nameSubProduct(inStr, product_id)
        return inStr

    def _nameSubInspector(self, inStr, inspector_id):
        """
        In inStr replace the standard {} with the names
        """
        if inStr is None:
            return inStr
        repl = ['{INSTRUMENT}', '{SPACECRAFT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}']
        insp = self.getEntry('Inspector', inspector_id)
        ftb = self.getTraceback('Product', insp.product)
        if '{INSTRUMENT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{SPACECRAFT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{SPACECRAFT}', ftb['satellite'].satellite_name)
        if '{MISSION}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{MISSION}', ftb['mission'].mission_name)
        if '{PRODUCT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{PRODUCT}', ftb['product'].product_name)
        if '{LEVEL}' in inStr:
            inStr = inStr.replace('{LEVEL}', str(ftb['product'].level))
        if '{ROOTDIR}' in inStr:
            inStr = inStr.replace('{ROOTDIR}', str(ftb['mission'].rootdir))
        if any(val in inStr for val in repl):  # call yourself again
            inStr = self._nameSubProduct(inStr, inspector_id)
        return inStr

    def _nameSubProcess(self, inStr, process_id):
        """
        In inStr replace the standard {} with the names
        """
        p_id = self.getProcessID(process_id)
        if inStr is None:
            return inStr
        repl = ['{INSTRUMENT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}']
        ftb = self.getTraceback('Process', p_id)
        if '{INSTRUMENT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{MISSION}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{MISSION}', ftb['mission'].mission_name)
        if '{PRODUCT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{PRODUCT}', ftb['input_product'][0][0].product_name)
        if '{LEVEL}' in inStr:
            inStr = inStr.replace('{LEVEL}', str(ftb['input_product'][0][0].level))
        if '{ROOTDIR}' in inStr:
            inStr = inStr.replace('{ROOTDIR}', str(ftb['mission'].rootdir))
        if any(val in inStr for val in repl):  # call yourself again
            inStr = self._nameSubProcess(inStr, p_id)
        return inStr

    def _nameSubFile(self, inStr, file_id):
        """
        In inStr replace the standard {} with the names
        """
        if inStr is None:
            return inStr
        ftb = self.getTraceback('File', file_id)
        if '{INSTRUMENT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{MISSION}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{MISSION}', ftb['mission'].mission_name)
        if '{LEVEL}' in inStr:
            inStr = inStr.replace('{LEVEL}', str(ftb['product'].level))
        if '{PRODUCT}' in inStr:  # need to replace with the instrument name
            inStr = inStr.replace('{PRODUCT}', ftb['product'].product_name)
        if '{ROOTDIR}' in inStr:
            inStr = inStr.replace('{ROOTDIR}', str(ftb['mission'].rootdir))
        return inStr

    def commitDB(self):
        """
        Do the commit to the DB
        """
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise (DBError(IE))

    def closeDB(self):
        """
        Close the database connection

        :keyword verbose: (optional) print information out to the command line

        >>>  pnl.closeDB()
        """
        if self.dbIsOpen == False:
            return
        try:
            self.session.close()
            self.dbIsOpen = False
            DBlogging.dblogger.info("Database connection closed")
        except DBError:
            DBlogging.dblogger.error("Database connection could not be closed")
            raise (DBError('could not close DB'))

    def addFile(self,
                filename=None,
                data_level=None,
                version=None,
                file_create_date=None,
                exists_on_disk=None,
                utc_file_date=None,
                utc_start_time=None,
                utc_stop_time=None,
                check_date=None,
                verbose_provenance=None,
                quality_comment=None,
                caveats=None,
                met_start_time=None,
                met_stop_time=None,
                product_id=None,
                shasum=None,
                process_keywords=None,
                quality_checked=None):
        """
        Add a datafile to the database

        :param filename: filename to add
        :type filename: str
        :param data_level: the data level of the file
        :type data_level: float
        :param version: the version of te file to create
        :type version: Version.Version
        :param file_create_date: date the fie was created
        :type file_create_date: datetime.datetime
        :param exists_on_disk: does the file exist on disk?
        :type exists_on_disk: bool
        :param product_id: the product id of he product he file belongs to
        :type product_id: int

        :keyword utc_file_date: The UTC date of the file
        :type utc_file_date: datetime.date
        :keyword utc_start_time: utc start time of the file
        :type utc_start_time: datetime.datetime
        :keyword utc_end_time: utc end time of the file
        :type utc_end_time: datetime.datetime
        :keyword check_date: the date the file was quality checked
        :type check_date: datetime.datetime
        :keyword verbose_provenance: Verbose provenafnce of the file
        :type verbose_provenance: str
        :keyword quality_comment: comment on quality from quality check
        :type quality_comment: str
        :keyword caveats: caveats associated with the file
        :type caveates: str
        :keyword met_start_time: met start time of the file
        :type met_start_time: long
        :keyword met_stop_time: met stop time of the file
        :type met_stop_time: long

        :return: file_id of the newly inserted file
        :rtype: long
        """
        d1 = self.File()
        d1.filename = filename
        d1.utc_file_date = utc_file_date
        d1.utc_start_time = utc_start_time
        d1.utc_stop_time = utc_stop_time
        d1.data_level = data_level
        d1.check_date = check_date
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
        d1.shasum = shasum
        d1.process_keywords = process_keywords
        d1.quality_checked = quality_checked

        if hasattr(d1, 'newest_version'):
            # This field is no longer used, but old databases may still have it.
            d1.newest_version = False

        self.session.add(d1)
        self.commitDB()
        return d1.file_id

    def codeIsActive(self, ec_id, date):
        """
        Given a ec_id and a date is that code active for that date and is newest version

        :param ec_id: executable code id to see if is active
        :param date: date object to use when checking

        :return: True if the code is active for that date, False otherwise

        """
        # can only be one here (sq)
        code = self.getEntry('Code', ec_id)
        if not code.active_code:
            return False

        if not code.newest_version:
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
        Return the full path to a file Given the name or id
        (name or id is based on type)

        TODO, this is really slow, this query made it a lot faster but I bet it can get better

        """
        if isinstance(filename, (str, unicode)):
            filename = self.getFileID(filename)
        sq = self.session.query(self.File.filename, self.Product.relative_path).filter(
            self.File.file_id == filename).join((self.Product, self.File.product_id == self.Product.product_id)).one()
        path = os.path.join(self.MissionDirectory, *sq[::-1])
        if '{' in path:
            file_entry = self.getEntry('File', filename)
            path = Utils.dirSubs(path, file_entry.filename, file_entry.utc_file_date, file_entry.utc_start_time,
                                 self.getFileVersion(file_entry.file_id))
        return path

    def getProcessFromInputProduct(self, product):
        """
        Given a product id return all the processes that use that as an input

        Use getProductID if have a name (or not sure)
        """
        DBlogging.dblogger.debug("Entered getProcessFromInputProduct: {0}".format(product))
        sq = self.session.query(self.Productprocesslink.process_id).filter_by(input_product_id=product).all()
        return map(itemgetter(0), sq)

    def getProcessFromOutputProduct(self, outProd):
        """
        Gets process from the db that have the output product
        """
        DBlogging.dblogger.debug("Entered getProcessFromOutputProduct: {0}".format(outProd))
        p_id = self.getProductID(outProd)
        sq1 = self.session.query(self.Process).filter_by(output_product=p_id).all()  # should only have one value
        if not sq1:
            DBlogging.dblogger.info('No Process has Product {0} as an output'.format(p_id))
            return None
        return sq1[0].process_id

    def getRunProcess(self):
        """
        Return a list of the processes who's output_timebase is "RUN"
        """
        return self.session.query(self.Process).filter_by(output_timebase='RUN').all()

    def getProcessID(self, proc_name):
        """
        Given a process name return its id
        """
        try:
            proc_id = long(proc_name)
            proc_name = self.session.query(self.Process).get(proc_id)
            if proc_name is None:
                raise (NoResultFound('No row was found for id={0}'.format(proc_id)))
        except ValueError:  # it is not a number
            proc_id = self.session.query(self.Process.process_id).filter_by(process_name=proc_name).one()[0]
        return proc_id

    def getSatelliteMission(self, sat_name):
        """
        Given a satellite or satellite id return the mission
        """
        return self.getTraceback('Satellite', sat_name)['mission']

    def getInstrumentID(self, name, satellite_id=None):
        """
        Return the instrument_id for a Given instrument

        :return: instrument_id - the instrument ID
        """
        try:
            i_id = long(name)
            sq = self.session.query(self.Instrument).get(i_id)
            if sq is None:
                raise (DBNoData("No instrument_id {0} found in the DB".format(i_id)))
            return sq.instrument_id
        except ValueError:
            sq = self.session.query(self.Instrument).filter_by(instrument_name=name).all()
            if len(sq) == 0:
                raise (DBNoData("No instrument_name {0} found in the DB".format(name)))
            if len(sq) > 1:
                if satellite_id is None:
                    raise (ValueError('Non unique instrument name and no satellite specified'))
                sat_id = self.getSatelliteID(satellite_id)
                for v in sq:
                    if v.satellite_id == sat_id:
                        return v.instrument_id
                # I do not believe this can be reached, BAL 2-12-2016
                raise (ValueError("No matching instrument, satellite found. {0}:{1}".format(name, satellite_id)))
            return sq[0].instrument_id

    def getMissions(self):
        """Return a list of all the missions"""
        sq = self.session.query(self.Mission.mission_name)
        return map(itemgetter(0), sq.all())

    def renameFile(self, filename, newname):
        """
        Rename a file in the db
        """
        f = self.getEntry('File', filename)
        f.filename = newname
        self.session.add(f)
        self.commitDB()

    def getFileID(self, filename):
        """
        Return the fileID for the input filename

        :param filename: filename to return the fileid of
        :type filename: str

        :return: file_id: file_id of the input file
        :rtype: long
        """
        if isinstance(filename, self.File):
            return filename.file_id
        try:
            f_id = long(filename)
            sq = self.session.query(self.File).get(f_id)
            if sq is None:
                raise (DBNoData("No file_id {0} found in the DB".format(filename)))
            return sq.file_id
        except TypeError:  # came in as list or tuple
            return map(self.getFileID, filename)
        except ValueError:
            sq = self.session.query(self.File).filter_by(filename=filename).first()
            if sq is not None:
                return sq.file_id
            else:  # no file_id found
                raise (DBNoData("No filename %s found in the DB" % (filename)))

    def getCodeID(self, codename):
        """
        Return the codeID for the input code

        :param codename: filename to return the fileid of
        :type filename: str

        :return: code_id: code_id of the input file
        :rtype: long
        """
        try:
            c_id = long(codename)
            code = self.session.query(self.Code).get(c_id)
            if code is None:
                raise (DBNoData("No code id {0} found in the DB".format(c_id)))
        except TypeError:  # came in as list or tuple
            return map(self.getCodeID, codename)
        except ValueError:
            sq = self.session.query(self.Code.code_id).filter_by(filename=codename).all()
            if len(sq) == 0:
                raise (DBNoData("No code name {0} found in the DB".format(codename)))
            c_id = map(itemgetter(0), sq)
        return c_id

    def getFileDates(self, file_id):
        """
        Given a file_id or name return the dates it spans
        """
        sq = self.getEntry('File', file_id)
        start_time = sq.utc_start_time.date()
        stop_time = sq.utc_stop_time.date()
        return [start_time, stop_time]

    def file_id_Clean(self, invals):
        """
        Given a list of file objects clean out older versions of matching files
        matching is defined as same product_id and same utc_file_date
        """
        tmp = []
        for i in invals:
            if isinstance(i, (str, unicode)):
                tmp.append(self.getEntry('File', i))
            else:
                tmp.append(i)
        invals = tmp
        newest = set(v for fe in invals
                       for v in self.getFilesByProductDate(fe.product_id, [fe.utc_file_date] * 2, newest_version=True))
        return list(newest.intersection(invals))

    def getInputProductID(self, process_id, range=False):
        """
        Return the fileID for the input filename

        :param process_id: process_id to return the input_product_id for
        :type process_id: long

        :return: list of input_product_ids
        :rtype: list
        """
        columns = [self.Productprocesslink.input_product_id,
                   self.Productprocesslink.optional]
        if range:
            columns.extend(
                [self.Productprocesslink.yesterday,
                 self.Productprocesslink.tomorrow]
                if hasattr(self.Productprocesslink, 'yesterday') else
                [sqlalchemy.sql.expression.literal(0).label('yesterday'),
                 sqlalchemy.sql.expression.literal(0).label('tomorrow')]
            )
        sq = self.session.query(*columns).filter_by(process_id=process_id).all()
        return sq

    def getFiles(self,
                 startDate=None,
                 endDate=None,
                 level=None,
                 product=None,
                 code=None,
                 instrument=None,
                 exists=None,
                 newest_version=False,
                 limit=None,
                 startTime=None,
                 endTime=None):
        # if a datetime.datetime comes in this does not work, make them datetime.date
        startDate = Utils.datetimeToDate(startDate)
        endDate = Utils.datetimeToDate(endDate)
        
        files = self.session.query(self.File)

        if product is not None:
            files = files.filter_by(product_id=product)
        
        if level is not None:
            files = files.filter_by(data_level=level)

        if exists is not None:
            files = files.filter_by(exists_on_disk=exists)

        if code is not None:
            files = files.join(self.Filecodelink, self.File.file_id == self.Filecodelink.resulting_file) \
                .filter_by(source_code=code)

        if instrument is not None:
            files = files.join(self.Instrumentproductlink,
                                self.File.product_id == self.Instrumentproductlink.product_id) \
                .filter_by(instrument_id=instrument)

        if startDate is not None:
            if endDate is not None:
                files = files.filter(self.File.utc_file_date.between(
                    startDate, endDate))
            else: # Start date only
                files = files.filter(self.File.utc_file_date >= startDate)
        elif endDate is not None: # End date only
            files = files.filter(self.File.utc_file_date <= endDate)

        if startTime is not None:
            files = files.filter(self.File.utc_stop_time >= Utils.toDatetime(startTime))
        if endTime is not None:
            files = files.filter(self.File.utc_start_time <= Utils.toDatetime(endTime, end=True))

        if newest_version:
            files = files.order_by(self.File.interface_version, self.File.quality_version, self.File.revision_version)
            x = files.limit(limit).all()
            
            # Last item wins. https://stackoverflow.com/questions/39678672/is-a-python-dict-comprehension-always-last-wins-if-there-are-duplicate-keys
            out = dict([((i.product_id, i.utc_file_date), i) for i in x])
            return list(out.values())
        else:
            return files.limit(limit).all()

    def getFilesByProductDate(self, product_id, daterange, newest_version=False):
        """
        Return the files in the db by product id with utc_file_date in range specified
        """
        return self.getFiles(startDate=min(daterange),
                             endDate=max(daterange),
                             product=product_id,
                             newest_version=newest_version)

    def getFilesByProductTime(self, product_id, daterange, newest_version=False):
        """
        Return the files in the db by product id with any data in range specified
        """
        return self.getFiles(startTime=min(daterange),
                             endTime=max(daterange),
                             product=product_id,
                             newest_version=newest_version)

    def getFilesByDate(self, daterange, newest_version=False):
        """
        Return files in the db with utc_file_date in the range specified
        """
        return self.getFiles(startDate=min(daterange),
                             endDate=max(daterange),
                             newest_version=newest_version)

    def getFilesByProduct(self, prod_id, newest_version=False):
        """
        Given a product_id or name return all the file instances associated with it

        if newest is set return only the newest files
        """

        return self.getFiles(product=self.getProductID(prod_id), newest_version=newest_version)

    def getFilesByInstrument(self, inst_id, level=None, newest_version=False, id_only=False):
        """
        Given an instrument_if return all the file instances associated with it
        """
        inst_id = self.getInstrumentID(inst_id)  # name or number
        files = self.getFiles(instrument=inst_id, level=level, newest_version=newest_version)

        if id_only:
            files = map(attrgetter('file_id'), files)  # this is faster than a list comprehension
        return files

    def getFilesByCode(self, code_id, newest_version=False, id_only=False):
        """
        Given a code_id (or name) return the files that were created using it
        """
        files = self.getFiles(code=code_id, newest_version=newest_version)

        if id_only:
            files = map(attrgetter('file_id'), files)  # this is faster than a list comprehension
        return files

    def getAllFileIds(self,
                      fullPath=True,
                      startDate=None,
                      endDate=None,
                      level=None,
                      product=None,
                      code=None,
                      instrument=None,
                      exists=None,
                      newest_version=False,
                      limit=None):
        """
        Return all the file ids in the database
        """
        files = self.getFiles(startDate=startDate, endDate=endDate, level=level, product=product, code=code,
                              instrument=instrument, exists=exists, newest_version=newest_version, limit=limit)

        return map(attrgetter('file_id'), files)  # this is faster than a list comprehension

    def getActiveInspectors(self):
        """
        Query the db and return a list of all the active inspector file names [(filename, description, arguments, product), ...]
        """
        activeInspector = namedtuple('activeInspector', 'path description arguments product_id')
        sq = self.session.query(self.Inspector).filter(self.Inspector.active_code == True).all()
        return [activeInspector(os.path.join(self.InspectorDirectory, ans.relative_path, ans.filename), ans.description,
                                ans.arguments, ans.product) for ans in sq]

    def getChildrenProcesses(self, file_id):
        """
        Given a file ID return all the processes that use this as input
        """
        DBlogging.dblogger.debug("Entered getChildrenProcesses():  file_id: {0}".format(file_id))
        product_id = self.getEntry('File', file_id).product_id

        # get all the process ids that have this product as an input
        return self.getProcessFromInputProduct(product_id)

    def getProductID(self, product_name):
        """
        Return the product ID for an input product name

        :param product_name: the name of the product to et the id of
        :type product_name: str

        :return: product_id -the product  ID for the input product name
        """
        try:
            product_name = long(product_name)
            sq = self.session.query(self.Product).get(product_name)
            if sq is not None:
                return sq.product_id
            else:
                raise (DBNoData("No product_id {0} found in the DB".format(product_name)))
        except TypeError:  # came in as list or tuple
            return map(self.getProductID, product_name)
        except ValueError:
            sq = self.session.query(self.Product).filter_by(product_name=product_name)
            try:
                # if two products have the same name always return the lower id one
                return (sorted([x.product_id for x in sq])[0])
            except IndexError:  # no file_id found
                raise (DBNoData("No product_name %s found in the DB" % (product_name)))

    def getSatelliteID(self,
                       sat_name):
        """
        Returns the satellite ID for an input satellite name
        :param sat_name: the satellite name to look up the id
        :type sat_name: str

        :return: satellite_id - the requested satellite  ID
        """
        try:
            sat_id = long(sat_name)
            sq = self.session.query(self.Satellite).get(sat_id)
            if sq is None:
                raise (NoResultFound("No satellite id={0} found".format(sat_id)))
            return sq.satellite_id
        except TypeError:  # came in as list or tuple
            return map(self.getSatelliteID, sat_name)
        except ValueError:  # it was a name
            sq = self.session.query(self.Satellite).filter_by(satellite_name=sat_name).one()
            return sq.satellite_id  # there can be only one of each name

    def getCodePath(self, code_id):
        """
        Given a code_id list return the full name (path and all) of the code
        """
        code = self.getEntry('Code', code_id)
        if not code.active_code:  # not an active code
            return None
        return os.path.join(self.CodeDirectory, code.relative_path, code.filename)

    def getCodeVersion(self, code_id):
        """
        Given a code_id the code version
        """
        code = self.getEntry('Code', code_id)
        return Version.Version(code.interface_version, code.quality_version, code.revision_version)

    def getAllCodesFromProcess(self, proc_id):
        """
        Given a process id return the code ids that performs that process and the valid dates

        :return: Code id and dates that perform a process
        :rtype: truple(int, datetime.date, datetime.date)

        """
        DBlogging.dblogger.debug("Entered getAllCodesFromProcess: {0}".format(proc_id))
        # will have as many values as there are codes for a process
        sq = (self.session.query(self.Code).filter_by(process_id=proc_id)
              .filter_by(newest_version=True)
              .filter_by(active_code=True))
        ans = []
        for s in sq:
            ans.append((s.code_id, s.code_start_date, s.code_stop_date))
        return ans

    def getCodeFromProcess(self, proc_id, utc_file_date):
        """
        Given a process id return the code id that makes performs that process

        :return: Code id that performs the process
        :rtype: int
        """
        DBlogging.dblogger.debug("Entered getCodeFromProcess: {0}".format(proc_id))
        # will have as many values as there are codes for a process
        sq = (self.session.query(self.Code.code_id).filter_by(process_id=proc_id)
              .filter_by(newest_version=True)
              .filter_by(active_code=True).filter(self.Code.code_start_date <= utc_file_date)
              .filter(self.Code.code_stop_date >= utc_file_date))
        if sq.count() == 0:
            return None
        elif sq.count() > 1:
            raise (DBError('More than one code active for a Given day'))
        return sq[0].code_id

    def getMissionDirectory(self):
        """
        Return the base directory for the current mission

        :return: base directory for current mission
        :rtype: str
        """
        try:
            return os.path.abspath(os.path.expanduser(
                self.session.query(self.Mission.rootdir).one()[0]))
        except sqlalchemy.orm.exc.NoResultFound:
            return None
        except sqlalchemy.orm.exc.MultipleResultsFound:
            raise (ValueError('No mission id specified and more than one mission present'))

    def getCodeDirectory(self):
        """
        Return the base directory for the current mission

        :return: base directory for current mission
        :rtype: str
        """
        return self.getDirectory('codedir', default=self.MissionDirectory)

    def getInspectorDirectory(self):
        """
        Return the base directory for the current mission

        :return: base directory for current mission
        :rtype: str
        """
        return self.getDirectory('inspectordir', default=self.CodeDirectory)

    def checkIncoming(self, glb='*'):
        """
        Check the incoming directory for the current mission and add those files to the getting list

        :return: processing list of file ids
        :rtype: list
        """
        path = self.getIncomingPath()
        DBlogging.dblogger.debug("Looking for files in {0}".format(path))
        files = glob.glob(os.path.join(path, glb))
        return sorted(files)

    def getIncomingPath(self):
        """
        Return the incoming path for the current mission
        """
        return self.getDirectory('incoming_dir')

    def getErrorPath(self):
        """
        Return the error path for the current mission
        """
        #print(os.path.join(self.getCodeDirectory(),'errors'))
        return self.getDirectory('errordir', default=os.path.join(self.CodeDirectory, 'errors'))

    def getDirectory(self, column, default=None):
        """
        Generic directory lookup function, gives directory for the specified
        column.

        The mission rootdir may be absolute or relative to current path.
        Directory requested may be in db as absolute or relative to mission
        root. Home dir references are expanded.
        """
        try:
            mission = self.session.query(self.Mission).one()
        except sqlalchemy.orm.exc.NoResultFound:
            mission = None #this will grab the default based on hasattr
        except sqlalchemy.orm.exc.MultipleResultsFound:
            raise ValueError('No mission id specified and more than one mission present')

        c = getattr(mission, column) if hasattr(mission, column) else default
        if c is None:
            return default
        #If c is absolute, join throws away the rootdir part.
        return os.path.abspath(os.path.join(os.path.expanduser(mission.rootdir),
                                            os.path.expanduser(c)))

    def getFilecodelink_byfile(self, file_id):
        """
        Given a file_id return the code_id associated with it, or None
        """
        DBlogging.dblogger.debug("Entered getFilecodelink_byfile: file_id={0}".format(file_id))
        f_id = self.getFileID(file_id)
        sq = self.session.query(self.Filecodelink.source_code).filter_by(resulting_file=f_id).first()  # can only be one
        try:
            return sq[0]
        except TypeError:
            return None

    def getFilecodelink_bycode(self, code_id):
        """
        Given a file_id return the code_id associated with it, or None
        """
        DBlogging.dblogger.debug("Entered getFilecodelink_bycode: code_id={0}".format(code_id))
        code_id = self.getCodeID(code_id)
        sq = self.session.query(self.Filecodelink.resulting_file).filter_by(source_code=code_id)
        return sq

    def getMissionID(self, mission_name):
        """
        Given a mission name return its ID
        """
        try:
            m_id = long(mission_name)
            ms = self.session.query(self.Mission).get(m_id)
            if ms is None:
                raise (DBNoData('Invalid mission id {0}'.format(m_id)))
        except (ValueError, TypeError):
            sq = self.session.query(self.Mission.mission_id).filter_by(mission_name=mission_name).all()
            if len(sq) == 0:
                raise (DBNoData('Invalid mission name {0}'.format(mission_name)))
            m_id = sq[0].mission_id
        return m_id

    def tag_release(self, rel_num):
        """
        Tag all the newest versions of files to a release number (integer)
        """
        newest_files = self.getFiles(newest_version=True)

        for f in newest_files:
            self.addRelease(f, rel_num, commit=False)
        self.commitDB()
        return len(newest_files)

    def addRelease(self, filename, release, commit=False):
        """
        Given a filename or file_id add an entry to the release table
        """
        f_id = self.getFileID(filename)  # if a number
        rel = self.Release()
        rel.file_id = f_id
        rel.release_num = release
        self.session.add(rel)
        if commit:  # so that if we are doing a lot it is faster
            self.commitDB()

    def list_release(self, rel_num, fullpath=True):
        """
        Given a release number return a list of all the filenames with the release
        """
        sq = self.session.query(self.Release.file_id).filter_by(release_num=rel_num).all()
        sq = map(itemgetter(0), sq)
        for i, v in enumerate(sq):
            if fullpath:
                sq[i] = self.getFileFullPath(v)
            else:
                sq[i] = self.getEntry('File', v).filename
        return sq

    def checkFileSHA(self, file_id):
        """
        Given a file id or name check the db checksum and the file checksum
        """
        db_sha = self.getEntry('File', file_id).shasum
        disk_sha = calcDigest(self.getFileFullPath(file_id))

        return disk_sha == db_sha

    def checkFiles(self, limit=None):
        """
        Check files in the DB, return inconsistent files and why

        :return: A list of tuple with the results. 1 is a bad checksum, 2 is not found
        """
        files = self.getAllFilenames(fullPath=False, limit=limit)
        ## check of existence and checksum
        bad_list = []
        for f in files:
            try:
                if not self.checkFileSHA(f):
                    bad_list.append((f, 1))
            except DigestError:
                bad_list.append((f, 2))
        return bad_list

    def getTraceback(self, table, in_id, in_id2=None):
        """
        Master routine for all the getXXXTraceback functions, this will make for less code

        this is some large select statements with joins in them, these are tested and do work
        """
        retval = { }
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
                  .join((self.Instrument, self.Instrumentproductlink.instrument_id == self.Instrument.instrument_id))
                  .join((self.Satellite, self.Instrument.satellite_id == self.Satellite.satellite_id))
                  .join((self.Mission, self.Satellite.mission_id == self.Mission.mission_id)).all())

            if not sq:  # did not find a matchm this is a dberror
                raise (DBError("file {0} did not have a traceback, this is a problem, fix it".format(in_id)))

            if len(sq) > 1:
                raise (DBError("Found multiple tracebacks for file {0}".format(in_id)))
            for ii, v in enumerate(vars):
                retval[v] = sq[0][ii]

        elif table.capitalize() == 'Code':
            
            in_id = self.getCodeID(in_id)
            
            # symplified version for plots (where there is no output product)
            vars = ['code', 'process']
            sq = (self.session.query(self.Code, self.Process)
                  .filter_by(code_id=in_id)
                  .join((self.Process, self.Code.process_id == self.Process.process_id)).all())

            if not sq:  # did not find a match this is a dberror
                raise (DBError("code {0} did not have a traceback, this is a problem, fix it".format(in_id)))
            
            if sq[0][1].output_timebase != 'RUN':
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
                      .join((self.Instrument, self.Instrumentproductlink.instrument_id == self.Instrument.instrument_id))
                      .join((self.Satellite, self.Instrument.satellite_id == self.Satellite.satellite_id))
                      .join((self.Mission, self.Satellite.mission_id == self.Mission.mission_id)).all())

            if not sq:  # did not find a match this is a dberror
                raise (DBError("code {0} did not have a traceback, this is a problem, fix it".format(in_id)))

            if len(sq) > 1:
                raise (DBError("Found multiple tracebacks for code {0}".format(in_id)))
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
                  .join((self.Instrument, self.Instrumentproductlink.instrument_id == self.Instrument.instrument_id))
                  .join((self.Satellite, self.Instrument.satellite_id == self.Satellite.satellite_id))
                  .join((self.Mission, self.Satellite.mission_id == self.Mission.mission_id)).all())

            if not sq:  # did not find a match this is a dberror
                raise (DBError("product {0} did not have a traceback, this is a problem, fix it".format(in_id)))

            if len(sq) > 1:
                raise (DBError("Found multiple tracebacks for product {0}".format(in_id)))
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
                  .join((self.Instrument, self.Instrumentproductlink.instrument_id == self.Instrument.instrument_id))
                  .join((self.Satellite, self.Instrument.satellite_id == self.Satellite.satellite_id))
                  .join((self.Mission, self.Satellite.mission_id == self.Mission.mission_id)).all())

            if not sq:  # did not find a match this is a dberror
                raise (DBError("process {0} did not have a traceback, this is a problem, fix it".format(in_id)))

            if len(sq) > 1:
                raise (DBError("Found multiple tracebacks for process {0}".format(in_id)))
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
                retval['input_product'].append((self.getEntry('Product', val), opt))
            retval['productprocesslink'] = []
            ppl = self.session.query(self.Productprocesslink).filter_by(process_id=retval['process'].process_id)
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
            raise (NotImplementedError('The traceback or {0} is not implemented'.format(table)))

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
        Get all the products for a Given instrument
        """
        inst_id = self.getInstrumentID(inst_id)
        sq = self.session.query(self.Instrumentproductlink.product_id).filter_by(instrument_id=inst_id).all()
        if sq:
            return map(itemgetter(0), sq)
        else:
            return None

    def getProductsByLevel(self, level):
        """
        Get all the products for a Given level
        """
        sq = self.session.query(self.Product.product_id).filter_by(level=level).all()
        if sq:
            return map(itemgetter(0), sq)
        else:
            return None

    def getAllProcesses(self, timebase='all'):
        """
        Get all processes
        """
        if timebase == 'all':
            procs = self.session.query(self.Process).all()
        else:
            procs = self.session.query(self.Process).filter_by(output_timebase=timebase.upper()).all()
        return procs

    def getProcessTimebase(self, process_id):
        """
        Return the timebase for a product
        """
        return self.getEntry('Process', process_id).output_timebase

    def getAllProducts(self, id_only=False):
        """
        Return a list of all products as instances
        """
        prods = self.session.query(self.Product).all()
        if id_only:
            prods = map(attrgetter('product_id'), prods)
        return prods

    def getEntry(self, table, args):
        """
        Master method to return a entry instance from any table in the db
        """
        # just try and get the entry
        retval = self.session.query(getattr(self, table)).get(args)
        if retval is None:  # either this was not a valid pk or not a pk that is in the db
            # see if it was a name
            if ('get' + table + 'ID') in dir(self):
                cmd = 'get' + table + 'ID'
                pk = getattr(self, cmd)(args)
                retval = self.session.query(getattr(self, table)).get(pk)
        return retval

    def getFileParents(self, file_id, id_only=False):
        """
        Given a file_id (or filename) return the files that went into making it
        """
        file_id = self.getFileID(file_id)
        f_ids = self.session.query(self.Filefilelink.source_file).filter_by(resulting_file=file_id).all()
        if not f_ids:
            return []
            
        f_ids = map(itemgetter(0), f_ids)
        if id_only:
            return f_ids

        return [self.getEntry('File', val) for val in f_ids]

    def getFileVersion(self, fileid):
        """
        Return the version instance for a file
        """
        if not isinstance(fileid, self.File):
            fileid = self.getEntry('File', fileid)
        return Version.Version(fileid.interface_version,
                               fileid.quality_version,
                               fileid.revision_version)

    def getChildTree(self, inprod):
        """
        Given an input product return a list of its output product ids
        """
        out_proc = self.getProcessFromInputProduct(inprod)
        return [self.getEntry('Process', op).output_product for op in out_proc]

    def getProductParentTree(self):
        """
        go through the db and return a tree of all products and their parents

        This will allow for a run all the non done files script
        """
        prods = self.getAllProducts()
        prods = sorted(prods, key=lambda x: x.level)
        tree = []
        # for each of the level 0 products add a base tree then iterate through them with dbu.getProcessFromInputProduct
        #  then get the output for that process
        for p in prods:
            tree.append([p.product_id, self.getChildTree(p.product_id)])
        return tree

    def updateCodeNewestVersion(self, code_id, is_newest=False):
        """
        Update a code to indicate whether it's the newest version.

        Assumption is that the newest version of a code should be the
        only active one, so sets both ``newest_version`` and
        ``active_code`` fields in the database.

        :param int code_id: ID or filename (str) of the code to change.
        :param bool is_newest: Set to newest (True) or not-newest, inactive
                               (False, default).
        """
        DBlogging.dblogger.debug\
            ("Entered updateCodeNewestVersion: code_id={0}, is_newest={1}"\
             .format(code_id, is_newest))
        code = self.getEntry('Code', code_id)
        code.newest_version = code.active_code = int(bool(is_newest))
        self.commitDB()

    def editTable(self, table, my_id, column, my_str=None, after_flag=None,
                  ins_after=None, ins_before=None, replace_str=None,
                  combine=False):
        """
        Apply string editing operations on a single row, column of a table

        For a specified row and column of a table, update the value according
        to operations specified by the combination of the kwargs.

        To replace all instances of a string with another, set
        ``replace_str`` to the string to replace and ``my_str`` to the
        new value to replace it with.

        To append a string to all instance of a string, set ``ins_after``
        to the existing string and ``my_str`` to the value to append.

        To prepend a string to all instance of a string, set ``ins_after``
        to the existing string and ``my_str`` to the value to prepend.

        When operating on the ``arguments`` column of the ``code`` table,
        and ``after_flag`` is specified, all three of these operations
        will only apply to the "word" (whitespace-separated) after the
        "word" in ``after_flag``. See examples.

        When operating on the ``arguments`` column of the ``code`` table,
        ``combine`` may be set to ``True`` to combine every word that
        follows each instance of ``after_flag`` into a comma-separated list
        after a single instance of ``after_flag``. See examples.

        One and only one of ``ins_after``, ``ins_before``, ``replace_str``
        and ``combine`` can be specified; there is no default operation. If
        ``ins_after``, ``ins_before``, or ``replace_str`` are specified,
        ``my_str`` must be.

        .. note:: Written and tested for code table. Not thoroughly
                  tested for others.
        
        :param str table: Name of the table to edit.
        :param int my_id: Specifies row to edit; most commonly the numerical
                          ID (primary key) but also supports string matching
                          on other columns as provided by :meth:`getEntry`.
        :param str column: name of column to edit
        :param str my_str: String to add or replace. (Optional; required with
                           ``ins_after``, ``ins_before``, ``replace_str``).
        :param str after_flag: Only replace string in words immediately
                               following this word. Only supported in
                               ``arguments`` column of ``code`` table
                               (optional; default: replace in all).
        :param str ins_after: Value to insert ``my_str`` after.
                              (Optional; conflicts with ``ins_before``,
                              ``replace_str``, ``combine``).
        :param str ins_before: Value to insert ``my_str`` before.
                               (Optional; conflicts with ``ins_after``,
                               ``replace_str``, ``combine``).
        :param str replace_str: Value to replace with ``my_str``.
                                (Optional; conflicts with ``ins_after``,
                                ``ins_before``, ``combine``).
        :param bool combine: If true, combine all instances of words after
                             the word in ``after_flag``. (Optional;
                             conflicts with ``ins_after``, ``ins_before``,
                             ``replace_str``).
        :raises ValueError: for any invalid combination of arguments.
        :raises RuntimeError: if multiple rows match ``my_id``.

        :examples:

        All examples assume an open :class:`DButils` instance in ``dbu`` and
        an existing code of ID 1. These examples use command line flags
        but the treatment of strings is general.

        >>> #Replace a string after a flag
        >>> code = dbu.getEntry('Code', 1)
        >>> code.arguments = '-i foobar -j foobar -k foobar'
        >>> dbu.editTable('code', 1, 'arguments', my_str='baz',
        ...               replace_str='bar', after_flag='-j')
        >>> code.arguments
        -i foobar -j foobaz -k foobar

        >>> #Combine multiple instances of a flag into one
        >>> code = dbu.getEntry('Code', 1)
        >>> code.arguments = '-i foo -i bar -j baz'
        >>> dbu.editTable('code', 1, 'arguments', after_flag='-i',
        ...               combine=True)
        >>> code.arguments
        -i foo,bar -j baz

        >>> #Append a string to every instance
        >>> code = dbu.getEntry('Code', 1)
        >>> code.relative_path = 'scripts'
        >>> dbu.editTable('code', 1, 'relative_path', ins_after='scripts',
        ...               my_str='2.0')
        >>> code.relative_path
        scripts2.0
        """
        DBlogging.dblogger.debug("Entered edit_table: my_id={0}".format(my_id))
        table = table.title()
        if not ins_after and not ins_before and not replace_str and not combine:
            raise ValueError('Nothing to be done.')
        if not combine and (sum(item is not None for item in
                                [ins_after, ins_before, replace_str]) != 1):
            raise ValueError('Only use one of ins_after, '
                             'ins_before, and replace_str.')
        if (ins_after or ins_before or replace_str) and not my_str:
            raise ValueError('Need my_str.')
        if combine and (sum(item is not None for item in \
                            [ins_after, ins_before, replace_str]) != 0):
            raise ValueError('Combine flag cannot be used with'
                             ' ins_after, ins_before, or replace_str.')
        if combine and my_str:
            raise ValueError('Do not need my_str with combine.')
        if after_flag and (column != 'arguments' or table != 'Code'):
            raise ValueError('Only use after_flag with arguments column'
                             ' in Code table.')
        if combine and not after_flag:
            raise ValueError('Must specify after_flag with combine.')

        try:
            entry = self.getEntry(table, my_id)
        except InvalidRequestError: #multiple matches for my_id, usually
            raise RuntimeError('Multiple rows match {}'.format(my_id))
        original = getattr(entry, column)
        if original is None: #nothing to do
            return

        if ins_before:
            old_str = ins_before
            new_str = my_str + ins_before
        elif ins_after:
            old_str = ins_after
            new_str = ins_after + my_str
        else:
            old_str = replace_str
            new_str = my_str

        if after_flag and original:
            parts = original.split()
            if combine:
                if parts.count(after_flag) > 1:
                    indices = [ii for ii in range(len(parts))
                               if parts[ii] == after_flag]
                    # go backwards so don't mess up order when deleting
                    for ii in range(len(indices) - 1, 0, -1):
                        parts[indices[0] + 1] = parts[indices[0] + 1] + ',' \
                                              + parts[indices[ii] + 1]
                        del parts[indices[ii] + 1]
                        del parts[indices[ii]]
            elif after_flag in parts: #combine is false
                for i, x in enumerate(parts[:-1]):
                    if x == after_flag:
                        parts[i + 1] = parts[i + 1].replace(old_str, new_str)
            setattr(entry, column, ' '.join(parts))
        else: #no after_flag provided, or the column is empty in db
            setattr(entry, column, original.replace(old_str, new_str))
            
        self.commitDB()
