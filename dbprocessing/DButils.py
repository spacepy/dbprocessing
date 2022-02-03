"""Direct interfacing to dbprocessing databases.

Provides interface specific to dbprocessing databases (not general SQL
databases) and performs most functionality for retrieving, changing, etc.
of records.
"""

from __future__ import absolute_import
from __future__ import print_function

import collections
try:
    import collections.abc
except ImportError:  # Python 2
    collections.abc = collections
import datetime
import getpass
import glob
import itertools
import os
import os.path
import posixpath
import socket  # to get the local hostname
import sys
from operator import itemgetter, attrgetter
try:
    import urllib.parse  # python 3
except ImportError:
    import urllib
    urllib.parse = urllib

try:
    str_classes = (str, unicode)
except NameError:
    str_classes = (str,)

import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.schema
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
from . import tables
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
    """Error in accessing the database"""
    pass


class DBProcessingError(Exception):
    """Higher-level error (used only for failures in logging)"""
    pass


class FilenameParse(Exception):
    """Not used"""
    pass


class DBNoData(Exception):
    """Expected data not found in the database"""
    pass


def postgresql_url(databasename):
    """Build postgresl database URL

    Environment variable ``PGUSER`` is required. Will also use ``PGHOST``,
    ``PGPORT`` (requires ``PGHOST``), ``PGPASSWORD`` to define database
    server to connect to. Anything unspecified is postgresql default.

    Parameters
    ----------
    databasename : :class:`str`
        Name of the database

    Returns
    -------
    :class:`str`
        Full postgresql URL, suitable for use in
        :func:`~sqlalchemy.create_engine`
    """
    # If no host, defaults to Unix domain on localhost.
    hostport = os.environ.get('PGHOST', '')
    if 'PGPORT' in os.environ:
        hostport = '{}:{}'.format(hostport, os.environ['PGPORT'])
    userpass = os.environ['PGUSER']
    if 'PGPASSWORD' in os.environ:
        userpass = '{}:{}'.format(userpass, urllib.parse.quote_plus(
                      os.environ['PGPASSWORD']))
    db_url = 'postgresql://{userpass}@{hostport}/{database}'.format(
        userpass=userpass, hostport=hostport, database=databasename)
    return db_url


class DButils(object):
    """Utility routines for DBProcessing class

    All of these may be user called but are meant to
    be internal routines for DBProcessing

    .. warning::
       It is strongly encouraged to make sure the database is closed before
       the program terminates, either by calling :meth:`closeDB` or deleting
       instances of this object (with an explicit :ref:`del <del>` or by
       allowing it to go out of scope.) If this object still exists at
       interpreter exit, it will attempt to close the database, but the
       functionality to do so may have already been torn down. See for
       example `Python issue 39513 <https://bugs.python.org/issue39513>`_.
    """

    def __init__(self, mission='Test', db_var=None, echo=False, engine=None):
        """
        Initialize the DButils class

        Parameters
        ----------
        mission : :class:`str`
            Name of the mission. This may be the name of a .sqlite file
            or the name of a Postgresql database; see
            :ref:`scripts_specifying_database` for Postgresql support
            (implemented by :func:`postgresql_url`).
        echo : :class:`bool`, default False
            if True, the Engine will log all statements as well as a
            repr() of their parameter lists to the logger
        engine : :class:`str`, optional
            DB engine to connect to (e.g sqlite, postgresql).
            Defaults to sqlite if mission is an existing file, else
            postgresql.

        Other Parameters
        ----------------
        db_var
            Does nothing
        """
        self.dbIsOpen = False
        if mission is None:
            raise DBError("Must input database name to create DButils instance")
        if engine is None:
            engine = 'sqlite' if os.path.isfile(os.path.expanduser(mission))\
                     else 'postgresql'
        self.mission = mission
        # Expose the format/regex routines of DBformatter
        fmtr = DBstrings.DBformatter()
        self.format = fmtr.format
        self.re = fmtr.re
        self.openDB(db_var=db_var, engine=engine, echo=echo)
        self._createTableObjects()
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
        Print out something useful when one prints the class instance

        Returns
        -------
        str
            DBProcessing class instance for mission <mission name>
        """
        return 'DBProcessing class instance for mission ' + self.mission + ', version: ' + __version__

    ####################################
    ###### DB and Tables ###############
    ####################################

    def openDB(self, engine, db_var=None, verbose=False, echo=False):
        """Setup python to talk to the database

        Parameters
        ----------
        engine : :class:`str`
            DB engine to connect to
        verbose : :class:`bool`, default False
            if True, will print out extra debugging
        echo : :class:`bool`, default False
            if True, the Engine will log all statements as well as a
            repr() of their parameter lists to the logger

        Other Parameters
        ----------------
        db_var
            Does nothing
        """
        if self.dbIsOpen == True:
            return
        if engine == 'sqlite':
            if not os.path.isfile(os.path.expanduser(self.mission)):
                raise ValueError("DB file specified doesn't exist")
            db_url = '{0}:///{1}'.format(engine, os.path.expanduser(
                self.mission))
            self.mission = os.path.abspath(os.path.expanduser(self.mission))
        elif engine == 'postgresql':
            db_url = postgresql_url(self.mission)
        else:
            raise DBError('Unknown engine {}'.format(engine))
        try:
            engineIns = sqlalchemy.create_engine(db_url, echo=echo)
            DBlogging.dblogger.info("Database Connection opened: {0}  {1}".format(str(engineIns), self.mission))

        except (DBError, ArgumentError):
            (t, v, tb) = sys.exc_info()
            raise DBError('Error creating engine: ' + str(v))
        try:
            metadata = sqlalchemy.MetaData(bind=engineIns)
            # a session is what you use to actually talk to the DB, set one up with the current engine
            Session = sessionmaker(bind=engineIns)
            session = Session()
            self.engine = engineIns
            self.metadata = metadata
            self.session = session
            self.dbIsOpen = True
            if verbose: print("DB is open: %s" % (engineIns))
            return
        except Exception as msg:
            raise DBError('Error opening database: %s' % (msg))

    def _createTableObjects(self, verbose=False):
        """
        cycle through the database and build classes for each of the tables

        Parameters
        ----------
        verbose : :class:`bool`, default False
            if True, will print out extra debugging
        """
        DBlogging.dblogger.debug("Entered _createTableObjects()")

        ## ask for the table names form the database (does not grab views)
        inspector = sqlalchemy.inspect(self.engine)
        table_names = inspector.get_table_names()

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
        """Checks the db to see if it is currently processing

        Ensures not doing 2 at the same time

        Returns
        -------
        :class:`bool` or :class:`int`
            False or the current process id

        Examples
        --------
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
            raise DBError("More than one currently_processing flag set, fix the DB")

    def resetProcessingFlag(self, comment):
        """
        Query the db and reset a processing flag

        Parameters
        ----------
        comment : :class:`str`
            the comment to enter into the processing log DB

        Returns
        -------
        :class:`bool`
            True - Success, False - Failure
        """
        sq2 = self.session.query(self.Logging).filter_by(currently_processing=True).count()
        if sq2 and comment is None:
            raise ValueError("Must enter a comment to override DB lock")
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
            raise DBError('A Currently Processing flag is still set, cannot process now')
        # save this class instance so that we can finish the logging later
        self.__p1 = self.addLogging(True,
                                    datetime.datetime.utcnow(),
                                    ## for now there is one mission only per DB
                                    # self.getMissionID(self.mission),
                                    self.session.query(self.Mission.mission_id).first()[0],
                                    getpass.getuser(),
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

        Parameters
        ----------
        currently_processing : :class:`bool`
            is the db currently processing?
        processing_start_time : :class:`~datetime.datetime`
            the time the processing started
        mission_id : :class:`int`
            the :sql:column:`~mission.mission_id` the processing is for
        user : :class:`str`
            the user doing the processing
        hostname : :class:`str`
            the hostname that initiated the processing
        pid : :class:`int`, optional
            the process id that did the processing, default null
        processing_end_time : :class:`~datetime.datetime`, optional
            the time the processing stopped, default null
        comment : :class:`str`
            comment about the processing run

        Returns
        -------
        Logging
            instance of the class for the :sql:table:`logging` table.
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

        Parameters
        ----------
        comment : :class:`str`
            a comment to insert into the DB
        """
        try:
            self.__p1
        except:
            DBlogging.dblogger.warning("Logging was not started, can't stop")
            raise DBProcessingError("Logging was not started")
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
        Check if the file existence on disk matches database record

        Parameters
        ----------
        file_id : :class:`int`
            :sql:column:`~file.file_id` of the file to check
        fix : :class:`bool`, default False
            set to have the DB fixed to match the file system
            this is **NOT** sure to be safe

        Returns
        -------
        :class:`bool`
            True if consistent, False otherwise
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

    def ProcessqueueFlush(self):
        """remove everything from the process queue

        This is as optimized as it can be
        """
        length = self.ProcessqueueLen()
        self.session.query(self.Processqueue).delete()
        self.commitDB()
        DBlogging.dblogger.info("Processqueue was cleared")
        return length

    def ProcessqueueRemove(self, item, commit = True):
        """
        remove a file from the queue by name or number

        Parameters
        ----------
        item : :class:`int` or :class:`str`
            :sql:column:`~file.filename` or :sql:column:`~file.file_id`
            of file to remove from the process queue.
        commit : :class:`bool`, default True
            Commit changes to the database when done.
        """
        # if the input is a file name need to handle that
        if isinstance(item, str_classes) \
           or not isinstance(item, collections.abc.Iterable):
            item = [item]
        for ii, v in enumerate(item):
            item[ii] = self.getFileID(v)
        sq = self.session.query(self.Processqueue).filter(self.Processqueue.file_id.in_(item))
        for v in sq:
            self.session.delete(v)
        if sq and commit:
            self.commitDB()

    def ProcessqueueGetAll(self, version_bump=False):
        """
        Return the entire contents of the process queue

        Parameters
        ----------
        version_bump : :class:`bool`, default False
            Include the version bump information

        Returns
        -------
        :class:`list`
            All :sql:column:`~file.file_id` in the process queue, optionally
            the :sql:column:`~processqueue.version_bump` information as well.
        """
        pqdata = self.session.query(self.Processqueue).all()

        if version_bump:
            ans = list(zip(map(attrgetter('file_id'), pqdata), map(attrgetter('version_bump'), pqdata)))
        else:
            ans = list(map(attrgetter('file_id'), pqdata))

        DBlogging.dblogger.debug("Entire Processqueue was read: {0} elements returned".format(len(ans)))
        return ans

    def ProcessqueuePush(self, fileid, version_bump=None, MAX_ADD=150):
        """
        Push a file onto the process queue (onto the right)

        Parameters
        ----------
        fileid : :class:`int` or :class:`str`
            the :sql:column:`~file.file_id` or :sql:column:`~file.filename`
            to put on the process queue.

        Returns
        -------
        file_id : :class:`int`
            :sql:column:`~file.file_id` of the file placed on queue,
            as grabbed from the db.
        """
        if not hasattr(fileid, '__iter__'):
            fileid = [fileid]
        else:
            # do this in chunks as too many entries breaks things
            if len(fileid) > MAX_ADD:
                outval = []
                for v in Utils.chunker(fileid, MAX_ADD):
                    outval.extend(self.ProcessqueuePush(v, version_bump=version_bump))
                return outval

        # first filter() takes care of putting in values that are not in the DB.  It is silent
        # second filter() takes care of not reading files that are already in the queue
        subq = self.session.query(self.Processqueue.file_id).subquery()

        fileid = (self.session.query(self.File.file_id)
                  .filter(self.File.file_id.in_(fileid))
                  .filter(~self.File.file_id.in_(subq.select()))).all()

        fileid = list(map(itemgetter(0), fileid))  # nested tuples to list

        pq = set(self.ProcessqueueGetAll())
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

    def ProcessqueueRawadd(self, fileid, version_bump=None, commit=True):
        """
        raw add file ids to the process queue

        .. warning::
           This might break things if an id is added that does not exist;
           it's meant to be fast and used after getting the ids.
           IS safe against adding ids that are already in the queue.

        Parameters
        ----------
        fileid : :class:`int` or :class:`~collections.abc.Iterable`
            the :sql:column:`~file.file_id` or sequence of file ids to add

        Returns
        -------
        num : :class:`int`
            the number of entries added to the processqueue
        """
        current_q = set(self.ProcessqueueGetAll())

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

    def ProcessqueueLen(self):
        """
        Return the number of files in the process queue

        Returns
        -------
        :class:`int`
            Count of files in the queue
        """

        return self.session.query(self.Processqueue).count()

    def ProcessqueuePop(self, index=0):
        """
        pop a file off the process queue (from the left)

        Other Parameters
        ----------------
        index : :class:`int`
            the index in the queue to pop

        Returns
        -------
        file_id : :class:`int`
            the :sql:column:`~processqueue.file_id` of the :sql:table:`file`
            popped from the queue
        """
        val = self.ProcessqueueGet(index=index, instance=True)
        self.session.delete(val)
        self.commitDB()
        return (val.file_id, val.version_bump)

    def ProcessqueueGet(self, index=0, instance=False):
        """
        Get the file at the head of the queue (from the left)

        Returns
        -------
        file_id : :class:`int`
            the :sql:column:`~processqueue.file_id` of the :sql:table:`file`
            popped from the queue
        """
        if index < 0:  # enable the python from the end indexing
            index = self.ProcessqueueLen() + index

        sq = self.session.query(self.Processqueue).offset(index).first()
        if instance:
            ans = sq
        else:
            ans = (sq.file_id, sq.version_bump)
        return ans

    def ProcessqueueClean(self, dryrun=False):
        """Keep only latest version of each file in the process queue.

        This is determined by :sql:column:`~file.product_id` and
        :sql:column:`~file.utc_file_date`. Also sorts queue by level, date

        Parameters
        ----------
        dryrun : :class:`bool`, default False
            Do not actually make changes to the queue.
        """

        # BAL 30 March 2017 Trying a different method here that might be cleaner

        # # TODO this might break with weekly input files
        # DBlogging.dblogger.debug("Entering ProcessqueueClean(), there are {0} entries".format(self.ProcessqueueLen()))
        # pqdata = self.ProcessqueueGetAll(version_bump=True)
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
        #     self.ProcessqueueFlush()
        #     #        self.ProcessqueuePush(ans)
        #     if not any(version_bumps2):
        #         self.ProcessqueuePush(file_entries2)
        #     else:
        #         itertools.starmap(self.ProcessqueuePush, mixed_entries)
        #     #                for v in mixed_entries:
        #     #                    itertools.startmap(self.ProcessqueuePush, v)
        # else:
        #     print(
        #         '<dryrun> Queue cleaned leaving {0} of {1} entries'.format(len(file_entries2), self.ProcessqueueLen()))
        #
        # DBlogging.dblogger.debug(
        #     "Done in ProcessqueueClean(), there are {0} entries left".format(self.ProcessqueueLen()))

        # # BAL 30 March 2017 new version
        # # get all the files from the process queue
        DBlogging.dblogger.debug("Entering ProcessqueueClean(), there are {0} entries".format(self.ProcessqueueLen()))
        pqdata = self.ProcessqueueGetAll(version_bump=True)
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
            self.ProcessqueueFlush()
            if not version_bump:
                self.ProcessqueueRawadd(list(zip(*entries))[0])
            else:
                for f in pqdata:
                    self.Processqueueadd(f)
        else:
            print(
                '<dryrun> Queue cleaned leaving {0} of {1} entries'.format(len(file_entries2), self.ProcessqueueLen()))
        DBlogging.dblogger.debug(
            "Done in ProcessqueueClean(), there are {0} entries left".format(self.ProcessqueueLen()))


    def fileIsNewest(self, filename, debug=False):
        """
        quesry the database, is this filename or file_id newest version?

        Parameters
        ----------
        filename : :class:`int` or :class:`str`
            filename or file_id

        Returns
        -------
        :class:`bool`
            True is file is lastest_version, False is not
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
            raise DBError("More than one latest for a product date")
        latest_id = latest[0].file_id
        if debug: print('latest_id', latest_id)
        return file_id == latest_id

    def _purgeFileFromDB(self, filename=None, recursive=False, verbose=False, trust_id=False, commit=True):
        """
        removes a file from the DB

        Parameters
        ----------
        filename : :class:`str` or :class:`~collections.abc.Iterable`
            name of the file to remove (or a list of names)
        recursive : :class:`bool`, default False
            remove all files that depend on the given file

        Other Parameters
        ----------------
        verbose : :class:`bool`, default False
            if True, will print out extra debugging
        trust_id : :class:`bool`, default False
            if True, assumes ``filename`` is a valid file id
        commit : :class:`bool`, default True
            Commit changes to the database when done.

        Examples
        --------
        >>>  pnl._purgeFileFromDB('Test-one_R0_evinst-L1_20100401_v0.1.1.cdf')
        """
        # if not an iterable make it a iterable
        if isinstance(filename, str_classes) \
           or not isinstance(filename, collections.abc.Iterable):
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
                self.ProcessqueueRemove(f)
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

        Returns
        -------
        :class:`dict`
            dictionaries of satellite, mission objects
        """
        ans = []
        sats = self.session.query(self.Satellite).all()
        ans = [self.getTraceback('Satellite', x.satellite_id) for x in sats]
        return ans

    def getAllInstruments(self):
        """
        Return dictionaries of instrument traceback dictionaries

        Returns
        -------
        :class:`dict`
            dictionaries of instrument traceback dictionaries
        """
        ans = []
        insts = self.session.query(self.Instrument).all()
        ans = [self.getTraceback('Instrument', x.instrument_id) for x in insts]
        return ans

    def getAllCodes(self, active=True):
        """
        Return a list of all codes

        Parameters
        ----------
        active : :class:`bool`, default False
            Only return codes which are marked :sql:column:`~code.active_code`
            and :sql:column:`~code.newest_version`.

        Returns
        -------
        :class:`list`
            All codes
        """
        ans = []
        if active:
            codes = self.session.query(self.Code).filter(and_(self.Code.newest_version, self.Code.active_code)).all()
        else:
            codes = self.session.query(self.Code).all()
        ans = [self.getTraceback('Code', x.code_id) for x in codes]
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

        All parameters are optional; if not specified, default is "all".

        Parameters
        ----------
        fullPath : :class:`bool`, default True
            Return full path (if False, just filename)
        startDate : :class:`~datetime.datetime`, optional
            First date to include, based on
            :sql:column:`~file.utc_file_date`
        endDate : :class:`~datetime.datetime`, optional
            Last date to include (inclusive)
        level : :class:`float`, optional
            Only include files of this level.
        product : :class:`int`, optional
            :sql:column:`~product.product_id` of files to include
        code : :class:`int`, optional
            Only return files created by code with ID of
            :sql:column:`~code.code_id`
        instrument : :class:`int`, optional
            Only return files with instrument
            :sql:column:`~instrument.instrument_id`
        exists : :class:`bool`, default False
            Only return files that exist on disk, based on
            :sql:column:`~file.exists_on_disk`.
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date)
        limit : :class:`int`
            Limit number of results, default all

        Returns
        -------
        :class:`list` of :class:`str`
            Filename of all files matching requirements.
        """

        files = self.getFiles(startDate, endDate, level, product, code, instrument, exists, newest_version, limit)

        if fullPath:
            # Get file_id instead, saves time since getFileFullPath gets the ID anyway
            names = [d.file_id for d in files]
            # This is probobly slow, but hopfully not slow enough to be an issue
            return list(map(self.getFileFullPath, names))
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

        Parameters
        ----------
        mission_name : :class:`str`
            the name of the mission
        rootdir : :class:`str`
            the root directory of the mission
        incoming_dir : :class:`str`
            directory for incoming files
        codedir : :class:`str`, optional
            directory containing codes; default, see :meth:`getCodeDirectory`
        inspectordir : :class:`str`, optional
            directory containing product inspectors; default, see
            :meth:`getInspectorDirectory`)
        errordir : :class:`str`, optional
            directory to contain error files; default, see :meth:`getErrorPath`
        """
        mission_name = str(mission_name)
        rootdir = str(rootdir)
        try:
            m1 = self.Mission()
        except AttributeError:
            raise DBError("Class Mission not found was it created?")

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

        Parameters
        ----------
        satellite_name : :class:`str`
            the name of the satellite
        mission_id : :class:`int`
            :sql:column:`mission.mission_id` of mission to add to

        Returns
        -------
        :class:`int`
            :sql:column:`satellite.satellite_id` of newly-added satellite.
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

        Parameters
        ----------
        process_name : :class:`str`
            the name of the process (:sql:column:`~process.process_name`).
        output_product : :class:`int`
            the output product id (:sql:column:`~process.output_product`).
        output_timebase : :class:`str`
            Timebase to use for output files, options ``RUN``, ``ORBIT``,
            ``DAILY``, ``WEEKLY``, ``MONTHLY``, ``YEARLY``, ``FILE``
            (:sql:column:`~process.output_timebase`).
        extra_params : :class:`str`, optional
            extra parameters to pass to the code
            (:sql:column:`~process.extra_params`).

        Other Parameters
        ----------------
        trigger
            Unused.
        """
        if output_timebase not in ['RUN', 'ORBIT', 'DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY', 'FILE']:
            raise ValueError("output_timebase invalid choice")

        p1 = self.Process()
        p1.output_product = Utils.toNone(output_product)
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

        Adds record to :sql:table:`product`.

        Parameters
        ----------
        product_name : :class:`str`
            the name of the product
        instrument_id : :class:`int`
            the instrument the product is from
        relative_path : :class:`str`
            relative path for the product
        format : :class:`str`
            the format of the product filenames
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

        Updates the database, replacing the generic ``{}`` references
        with the actual values for the product.

        Parameters
        ----------
        product_id : :class:`int` or :class:`str`
            :sql:column:`~product.product_id` or
            :sql:column:`~product.product_name` of product to update
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

        Updates the database, replacing the generic ``{}`` references
        with the actual values for the inspector.

        Parameters
        ----------
        insp_id : :class:`int`
            :sql:column:`~inspector.inspector_id` of inspector to update.
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

        Updates the database, replacing the generic ``{}`` references
        with the actual values for the process.

        Parameters
        ----------
        proc_id : :class:`int` or :class:`str`
            :sql:column:`~process.process_id` or
            :sql:column:`~process.process_name` of process to update
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

        Connects input product to output via :sql:table:`productprocesslink`.

        Parameters
        ----------
        input_product_id : :class:`int`
            :sql:column:`~product.product_id` of the input product.
        process_id : :class:`int`
            :sql:column:`process.process_id` of the process for which
            ``input_product_id`` is an input.
        optional : :class:`bool`
            if the input product is optional (vs. required)
        yesterday : :class:`int`, default 0
            How many extra days back do you need
        tomorrow : :class:`int`, default 0
            How many extra days forward do you need
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
        Add a file code link to the database

        Connects file to code that made it via :sql:table:`filecodelink`.

        Parameters
        ----------
        resulting_file_id : :class:`int`
            :sql:column:`~file.file_id` of the created file
        source_code : :class:`int`
            :sql:column:`~code.code_id` of the code that created the file
        """
        fcl1 = self.Filecodelink()
        fcl1.resulting_file = resulting_file_id
        fcl1.source_code = source_code
        self.session.add(fcl1)
        self.commitDB()
        return fcl1.resulting_file, fcl1.source_code

    def delInspector(self, i):
        """
        Removes an inspector from the db

        Parameters
        ----------
        i : :class:`int`
            :sql:column:`inspector.inspector_id` of inspector to delete
        """
        insp = self.getEntry('Inspector', i)
        self.session.delete(insp)
        self.commitDB()

    def delFilefilelink(self, f, commit = True):
        """
        Remove entries from Filefilelink

        Remove record from :sql:table:`filefilelink` if the file is in
        either :sql:column:`~filefilelink.source_file` or
        :sql:column:`~filefilelink.resulting_file`.

        Parameters
        ----------
        f : :class:`int` or :class:`str`
            :sql:column:`~file.file_id` or :sql:column:`~file.filename`
            of file to remove from link.
        commit : :class:`bool`, default True
            Commit changes to the database when done.
        """
        f = self.getFileID(f)  # change a name to a number
        n1 = self.session.query(self.Filefilelink).filter_by(source_file=f).delete()
        n2 = self.session.query(self.Filefilelink).filter_by(resulting_file=f).delete()
        if n1 + n2 == 0:
            raise DBNoData("No entry for ID={0} found".format(f))
        elif commit:
            self.commitDB()

    def delFilecodelink(self, f, commit = True):
        """
        Remove entries from Filecodelink for a Given file

        Remove record from :sql:table:`filecodelink` if the file was
        created by a code.

        Parameters
        ----------
        f : :class:`int` or :class:`str`
            :sql:column:`~file.file_id` or :sql:column:`~file.filename`
            of file to unassociate with code.
        commit : :class:`bool`, default True
            Commit changes to the database when done.
        """
        f = self.getFileID(f)  # change a name to a number
        n2 = self.session.query(self.Filecodelink).filter_by(resulting_file=f).delete()
        if n2 == 0:
            raise DBNoData("No entry for ID={0} found".format(f))
        elif commit:
            self.commitDB()

    def delProduct(self, pp):
        """
        Removes a product from the db

        Parameters
        ----------
        pp : :class:`int` or :class:`str`
            :sql:column:`~product.product_id` or
            :sql:column:`~product.product_name` of product to remove.
        """
        prod = self.getEntry('Product', pp)
        self.session.delete(prod)
        self.commitDB()

    def delProductProcessLink(self, ll):
        """
        Removes a product from the db

        Parameters
        ----------
        ll : :class:`list`
            Two elements, :sql:column:`~productprocesslink.process_id`
            and :sql:column:`~productprocesslink.input_product_id` of
            record to remove from :sql:table:`productprocesslink`.

        Notes
        -----
        Untested!
        """
        link = self.getEntry('Productprocesslink', ll)
        self.session.delete(link)
        self.commitDB()

    def purgeProcess(self, proc, commit = True):
        """
        Remove process and productprocesslink

        Removes a :sql:table:`process` record from the database and all
        :sql:table:`productprocesslink` records for that process.

        Parameters
        ----------
        proc : :class:`int`
            :sql:column:`~process.process_id` of process to delete.
        commit : :class:`bool`, default True
            Commit changes to the database when done.
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
        Add a file file link to the database

        Links a file to one of its input files via :sql:table:`filefilelink`.

        Parameters
        ----------
        resulting_file_id : :class:`int`
            :sql:column:`~file.file_id` of the output file.
        source_file : :class:`int`
            :sql:column:`~file.file_id` of the input file.
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
        Add a instrument product link to the database

        Links a product to its instrument via
        :sql:table:`instrumentproductlink`.

        Parameters
        ----------
        instrument_id : :class:`int`
            :sql:column:`~instrument.instrument_id` of the instrument.
        product_id : :class:`int`
            :sql:column:`~product.product_id` of the product.
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

        Creates record in :sql:table:`instrument`.

        Parameters
        ----------
        instrument_name : :class:`str`
            The name of the instrument
            (:sql:column:`~instrument.instrument_name`).
        satellite_id : :class:`int`
            :sql:column:`~satellite.satellite_id` of the satellite
            associated with the instrument.
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

        Creates a record in :sql:table:`code` table.

        Parameters
        ----------
        filename : :class:`str`
            the filename of the code.
        relative_path : :class:`str`
            the relative path (relative to mission code directory).
        code_start_date : :class:`~datetime.datetime`
            start of validity of the code.
        code_stop_date : :class:`~datetime.datetime`
            end of validity of the code.
        code_description : :class:`str`
            description of the code (50 char).
        process_id : :class:`int`
            :sql:column:`~process.process_id` of the process this code
            implements.
        version : :class:`.Version` or :class:`str`
            Version of the code.
        active_code : :class:`bool`
            if the code is active.
        code_date_written : :class:`~datetime.datetime`
            date the code was written.
        output_interface_version : :class:`int`
           Interface version of files produced by the code.
        newest_version : :class:`bool`
           Is the code the newest version.
        arguments : :class:`str`, optional
           Additional command line arguments to pass to the code, default
           none (no extra arguments).
        cpu : :class:`int`, default 1
           Relative CPU usage of code (usually in terms of threads).
        ram : :class:`float`, default 1
           Relative memory usage of code.

        Returns
        -------
        code_id : :class:`int`
            :sql:column:`~code.code_id` of newly created record.
        """
        if isinstance(version, str_classes):
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
        Add an inspector to the DB.

        Creates a record in :sql:table:`inspector` table.

        Parameters
        ----------
        filename : :class:`str`
            the filename of the inspector
        relative_path : :class:`str`
            the relative path (relative to mission inspector directory).
        description : :class:`str`
            description of the inspector (50 char).
        version : :class:`.Version` or :class:`str`
            Version of the code.
        active_code : :class:`bool`
            if the inspector is active.
        date_written : :class:`~datetime.datetime`
            date the inspector was written.
        output_interface_version : :class:`int`
           Written to database, but not used.
        newest_version : :class:`bool`
           Is the inspector the newest version.
        product : :class:`int`
           :sql:column:`~product.product_id` of the product this inspector
           identifies.
        arguments : :class:`str`, optional
           Additional keywords to pass to the :meth:`~.inspector.inspect`
           method, default none (no extra arguments).

        Returns
        -------
        inspector_id : :class:`int`
            :sql:column:`~inspector.inspector_id` of newly created record.
        """
        if isinstance(version, str_classes):
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

        Parameters
        ----------
        inStr : :class:`str`
           String on which to do substitutions
        product_id : :class:`int`
           :sql:column:`~product.product_id` of product to use in
           performing substitutions.

        Returns
        -------
        :class:`str`
            ``inStr`` with substitutions performed.

        See Also
        --------
        :ref:`concepts_substitutions`
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

        Parameters
        ----------
        inStr : :class:`str`
           String on which to do substitutions
        inspector_id : :class:`int`
           :sql:column:`~inspector.inspector_id` of inspector to use in
           performing substitutions.

        Returns
        -------
        :class:`str`
            ``inStr`` with substitutions performed.

        See Also
        --------
        :ref:`concepts_substitutions`
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

        Parameters
        ----------
        inStr : :class:`str`
           String on which to do substitutions
        process_id : :class:`int`
           :sql:column:`~process.process_id` of process to use in
           performing substitutions.

        Returns
        -------
        :class:`str`
            ``inStr`` with substitutions performed.

        See Also
        --------
        :ref:`concepts_substitutions`
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

        Parameters
        ----------
        inStr : :class:`str`
           String on which to do substitutions
        file_id : :class:`int`
           :sql:column:`~file.file_id` of file to use in
           performing substitutions.

        Returns
        -------
        :class:`str`
            ``inStr`` with substitutions performed.

        See Also
        --------
        :ref:`concepts_substitutions`
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
            raise DBError(IE)

    def closeDB(self):
        """
        Close the database connection

        Examples
        --------
        >>>  pnl.closeDB()
        """
        if self.dbIsOpen == False:
            return
        try:
            self.session.close()
            self.engine.dispose()
            self.dbIsOpen = False
            DBlogging.dblogger.info("Database connection closed")
        except DBError:
            DBlogging.dblogger.error("Database connection could not be closed")
            raise DBError('could not close DB')

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
        Add a datafile to the database.

        Adds record to :sql:table:`file`.

        Parameters
        ----------
        filename : :class:`str`
            Filename to add.
        data_level : :class:`float`
            The data level of the file.
        version : :class:`.Version`
            The version of the file to create.
        file_create_date : :class:`~datetime.datetime`
            Date and time the file was created.
        exists_on_disk : :class:`bool`
            Does the file exist on disk.
        product_id : :class:`int`
            :sql:column:`~product.product_id` of the product the file
            belongs to.
        utc_file_date : :class:`~datetime.date`
            The UTC date of the file.
        utc_start_time : :class:`~datetime.datetime`
            UTC of first timestamp in file.
        utc_end_time : :class:`~datetime.datetime`
            UTC of last timestamp in file.
        check_date : :class:`~datetime.datetime`
            The date the file was quality checked.
        verbose_provenance : :class:`str`
            Verbose provenance of the file.
        quality_comment : :class:`str`
            Comment on quality from quality check.
        caveats : :class:`str`
            Caveats on use of file.
        met_start_time : :class:`int`
            MET of first timestamp in file.
        met_stop_time : :class:`int`
            MET of last timestamp in file.

        Returns
        -------
        :class:`int`
            :sql:column:`~file.file_id` of the newly inserted file record.

        Notes
        -----
        All arguments are technically optional, but the insertion to the
        database may fail if an argument is not provided for a column
        which requires a non-NULL value. See :sql:table:`file`.
        """
        utc_start_time = Utils.toDatetime(utc_start_time)
        utc_stop_time = Utils.toDatetime(utc_stop_time)
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
        if hasattr(self, 'Unixtime'):
            # Populate file_id, but still allow rollback of file insert
            self.session.flush()
            unx0 = datetime.datetime(1970, 1, 1)
            r = self.Unixtime()
            r.file_id = d1.file_id
            # Round times down so they don't slide into next second
            # (and potentially next day)
            # If changed, also change getFiles, addUnixTimeTable,
            # updateUnixTime.py
            r.unix_start = None if utc_start_time is None \
                           else int((utc_start_time - unx0)\
                                    .total_seconds())
            r.unix_stop = None if utc_stop_time is None\
                          else int((utc_stop_time - unx0)\
                                   .total_seconds())
            self.session.add(r)
        self.commitDB()
        return d1.file_id

    def codeIsActive(self, ec_id, date):
        """Determine if a code is active and newest version.

        Parameters
        ----------
        ec_id : :class:`int` or :class:`str`
            :sql:column:`~code.code_id` or
            :sql:column:`~code.code_description` of the code to check.
        date : :class:`~datetime.date`
            Check if code is valid for files on this date (corresponds
            to :sql:column:`~file.utc_file_date`).

        Returns
        -------
        :class:`bool`
            If code is active, newest version, and ``date`` falls within
            the code's valid date range.
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
        Return the full path to a file given the name or id

        TODO, this is really slow, this query made it a lot faster but I bet it can get better

        Parameters
        ----------
        filename : :class:`str` or :class:`int`
            :sql:column:`~file.filename` or :sql:column:`~file.file_id`
            of file to look up.

        Returns
        -------
        :class:`str`
            Full path to the file.
        """
        if isinstance(filename, str_classes):
            filename = self.getFileID(filename)
        sq = self.session.query(self.File.filename, self.Product.relative_path).filter(
            self.File.file_id == filename).join((self.Product, self.File.product_id == self.Product.product_id)).one()
        path = os.path.join(self.MissionDirectory,
                            *(sq[1].split(posixpath.sep) + [sq[0]]))
        if '{' in path:
            file_entry = self.getEntry('File', filename)
            path = Utils.dirSubs(path, file_entry.filename, file_entry.utc_file_date, file_entry.utc_start_time,
                                 self.getFileVersion(file_entry.file_id))
        return path

    def getProcessFromInputProduct(self, product):
        """
        Given a product id return all the processes that use that as an input

        Use :meth:`getProductID` if have a name (or not sure).

        Parameters
        ----------
        product : :class:`int`
            :sql:column:`~product.product_id` of product.

        Returns
        -------
        :class:`list` of :class:`int`
            :sql:column:`~process.process_id` of all processes which use
            ``product`` as an input.
        """
        DBlogging.dblogger.debug("Entered getProcessFromInputProduct: {0}".format(product))
        sq = self.session.query(self.Productprocesslink.process_id).filter_by(input_product_id=product).all()
        return list(map(itemgetter(0), sq))

    def getProcessFromOutputProduct(self, outProd):
        """
        Gets process from the db that have the output product

        Parameters
        ----------
        outProd : :class:`int`
            :sql:column:`~product.product_id` of product.

        Returns
        -------
        :class:`int`
            :sql:column:`~process.process_id` of process which produces
            ``product`` as an output.

        Notes
        -----
        Assumes there is only one product that makes a process; this is
        common but not necessarily enforced.
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

        Returns
        -------
        :class:`list`
            Full :sql:table:`process` record for all ``RUN`` timebase processes.
        """
        return self.session.query(self.Process).filter_by(output_timebase='RUN').all()

    def getProcessID(self, proc_name):
        """
        Given a process name return its id

        Parameters
        ----------
        proc_name : :class:`str` or :class:`int`
            :sql:column:`~process.process_name` or
            :sql:column:`~process.process_id`.

        Returns
        -------
        :class:`int`
            :sql:column:`~process.process_id`.
        """
        try:
            proc_id = int(proc_name)
            proc_name = self.session.query(self.Process).get(proc_id)
            if proc_name is None:
                raise NoResultFound('No row was found for id={0}'.format(proc_id))
        except ValueError:  # it is not a number
            proc_id = self.session.query(self.Process.process_id).filter_by(process_name=proc_name).one()[0]
        return proc_id

    def getSatelliteMission(self, sat_name):
        """
        Given a satellite or satellite id return the mission

        Parameters
        ----------
        sat_name : :class:`int` or :class:`str`
            :sql:column:`~satellite.satellite_id` or
            :sql:column:`~satellite.satellite_name`.

        Returns
        -------
        various
            Complete record from :sql:table:`mission` table.
        """
        return self.getTraceback('Satellite', sat_name)['mission']

    def getInstrumentID(self, name, satellite_id=None):
        """
        Return the instrument_id for a given instrument.

        Parameters
        ----------
        name : :class:`str` or :class:`int`
            :sql:column:`~instrument.instrument_name` or
            :sql:column:`~instrument.instrument_id`.
        satellite_id : :class:`int` or :class:`str`
            Only return results for satellite with this
            :sql:column:`~satellite.satellite_id` or
            :sql:column:`~satellite.satellite_name`.

        Returns
        -------
        :class:`int`
            :sql:column:`~instrument.instrument_id`.
        """
        try:
            i_id = int(name)
            sq = self.session.query(self.Instrument).get(i_id)
            if sq is None:
                raise DBNoData("No instrument_id {0} found in the DB".format(i_id))
            return sq.instrument_id
        except ValueError:
            sq = self.session.query(self.Instrument).filter_by(instrument_name=name).all()
            if len(sq) == 0:
                raise DBNoData("No instrument_name {0} found in the DB".format(name))
            if len(sq) > 1:
                if satellite_id is None:
                    raise ValueError('Non unique instrument name and no satellite specified')
                sat_id = self.getSatelliteID(satellite_id)
                for v in sq:
                    if v.satellite_id == sat_id:
                        return v.instrument_id
                # I do not believe this can be reached, BAL 2-12-2016
                raise ValueError("No matching instrument, satellite found. {0}:{1}".format(name, satellite_id))
            return sq[0].instrument_id

    def getMissions(self):
        """Return a list of all the missions

        Returns
        -------
        :class:`list` of :class:`str`
            Names of all missions in the database.

        Notes
        -----
        Ordinarily there is only one mission per database.
        """
        sq = self.session.query(self.Mission.mission_name)
        return list(map(itemgetter(0), sq.all()))

    def renameFile(self, filename, newname):
        """
        Rename a file in the db

        Parameters
        ----------
        filename : :class:`str` or :class:`int`
            :sql:column:`~file.filename` or :sql:column:`~file.file_id`
            of file to rename.
        newname : :class:`str`
            New name to write to database.

        Notes
        -----
        Does not rename the file on disk. Operates on filename only
        (not entire path).
        """
        f = self.getEntry('File', filename)
        f.filename = newname
        self.session.add(f)
        self.commitDB()

    def getFileID(self, filename):
        """
        Return the fileID for the input filename

        Parameters
        ----------
        filename : :class:`str` or :class:`int`
            :sql:column:`~file.filename` or :sql:column:`~file.file_id`
            of file to look up.

        Returns
        -------
        :class:`int`
            :sql:column:`~file.file_id` of input file.
        """
        if isinstance(filename, self.File):
            return filename.file_id
        try:
            f_id = int(filename)
            sq = self.session.query(self.File).get(f_id)
            if sq is None:
                raise DBNoData("No file_id {0} found in the DB".format(filename))
            return sq.file_id
        except TypeError:  # came in as list or tuple
            return list(map(self.getFileID, filename))
        except ValueError:
            sq = self.session.query(self.File).filter_by(filename=filename).first()
            if sq is not None:
                return sq.file_id
            else:  # no file_id found
                raise DBNoData("No filename %s found in the DB" % (filename))

    def getCodeID(self, codename):
        """
        Return the codeID for a code's filename.

        Parameters
        ----------
        codename : :class:`str` or :class:`int`
            :sql:column:`~code.filename` or :sql:column:`~code.code_id`
            of code to look up.

        Returns
        -------
        :class:`int`
            :sql:column:`~code.code_id` of given code.
        """
        try:
            c_id = int(codename)
            code = self.session.query(self.Code).get(c_id)
            if code is None:
                raise DBNoData("No code id {0} found in the DB".format(c_id))
        except TypeError:  # came in as list or tuple
            return list(map(self.getCodeID, codename))
        except ValueError:
            sq = self.session.query(self.Code.code_id).filter_by(filename=codename).all()
            if len(sq) == 0:
                raise DBNoData("No code name {0} found in the DB".format(codename))
            c_id = list(map(itemgetter(0), sq))
        return c_id

    def getFileDates(self, file_id):
        """
        Given a file_id or name return the dates it spans

        Parameters
        ----------
        file_id :  :class:`int` or :class:`str`
            :sql:column:`~file.file_id` or :sql:column:`~file.filename`
            of file to look up.

        Returns
        -------
        :class:`list` of :class:`~datetime.datetime`
            First and last UTC timestamp of file.
        """
        sq = self.getEntry('File', file_id)
        start_time = sq.utc_start_time.date()
        stop_time = sq.utc_stop_time.date()
        return [start_time, stop_time]

    def file_id_Clean(self, invals):
        """
        Given a list of file IDs return only newest versions of matching files.

        Matching is defined as same :sql:column:`~file.product_id` and
        same :sql:column:`~file.utc_file_date`.

        Parameters
        ----------
        invals : :class:`list` of :class:`int` or of :class:`str`
            All :sql:column:`~file.file_id` or :sql:column:`~file.filename`
            to check.

        Returns
        -------
        :class:`list` of :class:`int`
            Those :sql:column:`~file.file_id` from ``invals`` which are
            the newest version of that file.
        """
        tmp = []
        for i in invals:
            if isinstance(i, str_classes):
                tmp.append(self.getEntry('File', i))
            else:
                tmp.append(i)
        invals = tmp
        newest = set(v for fe in invals
                       for v in self.getFilesByProductDate(fe.product_id, [fe.utc_file_date] * 2, newest_version=True))
        return list(newest.intersection(invals))

    def getInputProductID(self, process_id, range=False):
        """
        Return the input products for a particular process.

        Parameters
        ----------
        process_id : :class:`int`
            :sql:column:`~process.process_id` of process to look up.
        range : :class:`bool`, default False
            Also return number of days in past/future to use as inputs.

        Returns
        -------
        :class:`list`
           Result of query: each element has
           :sql:column:`~productprocesslink.input_product_id` and
           :sql:column:`~productprocesslink.optional`; if ``range``,
           then also :sql:column:`~productprocesslink.yesterday` and
           :sql:column:`~productprocesslink.tomorrow`.
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
        """
        Query database for file records, with filters.

        All parameters are optional; if not specified, default is "all".

        Parameters
        ----------
        startDate : :class:`~datetime.datetime`, optional
            First date to include, based on
            :sql:column:`~file.utc_file_date`
        endDate : :class:`~datetime.datetime`, optional
            Last date to include (inclusive)
        level : :class:`float`, optional
            Only include files of this level.
        product : :class:`int`, optional
            :sql:column:`~product.product_id` of files to include
        code : :class:`int`, optional
            Only return files created by code with ID of
            :sql:column:`~code.code_id`
        instrument : :class:`int`, optional
            Only return files with instrument
            :sql:column:`~instrument.instrument_id`
        exists : :class:`bool`, default False
            Only return files that exist on disk, based on
            :sql:column:`~file.exists_on_disk`.
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date)
        limit : :class:`int`
            Limit number of results, default all
        startTime : :class:`~datetime.datetime`, optional
            Include files containing timestamps at or after this time,
            :sql:column:`~file.utc_start_time`
        endTime : :class:`~datetime.datetime`, optional
            Include files containing timestamps at or before this time,
            :sql:column:`~file.utc_stop_time`

        Returns
        -------
        :class:`list`
            File records of all files matching requirements.
        """
        # if a datetime.datetime comes in this does not work, make them datetime.date
        startDate = Utils.datetimeToDate(startDate)
        endDate = Utils.datetimeToDate(endDate)
        unixtime = hasattr(self, 'Unixtime')
        # Truncate start/end seconds to match the truncation in the db.
        # Might result in false matches (e.g requested stop time is 1.2
        # and non-truncated start time of file is 1.6) but better than
        # missing a file that does overlap (e.g requested start time is 1.2
        # and non-truncated file start is 1.6, truncates to 1.0)
        # If changed, also change addFile, addUnixTimeTable, updateUnixTime.py
        if startTime is not None:
            startTime = Utils.toDatetime(startTime)
            if unixtime:
                startTime = int((startTime - datetime.datetime(1970, 1, 1))\
                                .total_seconds())
        if endTime is not None:
            endTime = Utils.toDatetime(endTime, end=True)
            if unixtime:
                endTime = int((endTime - datetime.datetime(1970, 1, 1))\
                              .total_seconds())

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

        if unixtime and (startTime is not None or endTime is not None):
            files = files.join(self.Unixtime)
        if startTime is not None:
            files = files.filter((self.Unixtime.unix_stop if unixtime
                                  else self.File.utc_stop_time) >= startTime)
        if endTime is not None:
            files = files.filter((self.Unixtime.unix_start if unixtime
                                  else self.File.utc_start_time) <= endTime)

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
        Return the files by product id with utc_file_date in range specified

        Parameters
        ----------
        product_id : :class:`int`
            :sql:column:`~product.product_id` of files to include.
        daterange : :class:`list` of :class:`~datetime.datetime`
            First and last date to include, based on
            :sql:column:`~file.utc_file_date`.
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date).

        Returns
        -------
        :class:`list`
            File records of all files matching requirements.
        """
        return self.getFiles(startDate=min(daterange),
                             endDate=max(daterange),
                             product=product_id,
                             newest_version=newest_version)

    def getFilesByProductTime(self, product_id, daterange, newest_version=False):
        """
        Return the files in the db by product id with any data in date range

        A file with a UTC time range overlapping at all with ``daterange``
        is considered a match, so a returned file may also include some
        times outside of the range.

        Parameters
        ----------
        product_id : :class:`int`
            :sql:column:`~product.product_id` of files to include.
        daterange : :class:`list` of :class:`~datetime.datetime`
            Range of times to include, based on
            :sql:column:`~file.utc_start_time` and
            :sql:column:`~file.utc_stop_time`.
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date).

        Returns
        -------
        :class:`list`
            File records of all files matching requirements.
        """
        return self.getFiles(startTime=min(daterange),
                             endTime=max(daterange),
                             product=product_id,
                             newest_version=newest_version)

    def getFilesByDate(self, daterange, newest_version=False):
        """
        Return files in the db with utc_file_date in the range specified

        Parameters
        ----------
        daterange : :class:`list` of :class:`~datetime.datetime`
            First and last date to include, based on
            :sql:column:`~file.utc_file_date`.
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date).

        Returns
        -------
        :class:`list`
            File records of all files matching requirements.
        """
        return self.getFiles(startDate=min(daterange),
                             endDate=max(daterange),
                             newest_version=newest_version)

    def getFilesByProduct(self, prod_id, newest_version=False):
        """
        Given a product_id or name return all the files associated with it

        Parameters
        ----------
        prod_id : :class:`int` or :class:`str`
            :sql:column:`~product.product_id` or
            :sql:column:`~product.product_name` of files to include.
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date).

        Returns
        -------
        :class:`list`
            File records of all files matching requirements.
        """

        return self.getFiles(product=self.getProductID(prod_id), newest_version=newest_version)

    def getFilesByInstrument(self, inst_id, level=None, newest_version=False, id_only=False):
        """
        Given an instrument_id return all the file instances associated with it

        Parameters
        ----------
        inst_id : :class:`int` or :class:`str`
            Only return files with this
            :sql:column:`~instrument.instrument_id` or
            :sql:column:`~instrument.instrument_name`
        level : :class:`float`, optional
            Only include files of this level, default all.
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date)
        id_only : :class:`bool`, default False
            Only return file IDs, not complete file record.

        Returns
        -------
        :class:`list`
            File records of all files matching requirements.
        """
        inst_id = self.getInstrumentID(inst_id)  # name or number
        files = self.getFiles(instrument=inst_id, level=level, newest_version=newest_version)

        if id_only:
            files = list(map(attrgetter('file_id'), files))  # this is faster than a list comprehension
        return files

    def getFilesByCode(self, code_id, newest_version=False, id_only=False):
        """
        Given a code_id (or name) return the files that were created using it

        Parameters
        ----------
        code_id : :class:`int` or :class:`str`
            Only return files created by code with this
            :sql:column:`~code.code_id` or
            :sql:column:`~code.code_description`
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date)
        id_only : :class:`bool`, default False
            Only return file IDs, not complete file record.

        Returns
        -------
        :class:`list`
            File records of all files matching requirements.
        """
        files = self.getFiles(code=code_id, newest_version=newest_version)

        if id_only:
            files = list(map(attrgetter('file_id'), files))  # this is faster than a list comprehension
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
        All parameters are optional; if not specified, default is "all".

        Parameters
        ----------
        startDate : :class:`~datetime.datetime`, optional
            First date to include, based on
            :sql:column:`~file.utc_file_date`
        endDate : :class:`~datetime.datetime`, optional
            Last date to include (inclusive)
        level : :class:`float`, optional
            Only include files of this level.
        product : :class:`int`, optional
            :sql:column:`~product.product_id` of files to include
        code : :class:`int`, optional
            Only return files created by code with ID of
            :sql:column:`~code.code_id`
        instrument : :class:`int`, optional
            Only return files with instrument
            :sql:column:`~instrument.instrument_id`
        exists : :class:`bool`, default False
            Only return files that exist on disk, based on
            :sql:column:`~file.exists_on_disk`.
        newest_version : :class:`bool`, default False
            Only return files that are the newest version
            (of their product and date)
        limit : :class:`int`
            Limit number of results, default all

        Returns
        -------
        :class:`list` of :class:`int`:
            File ID of all files matching requirements.

        Other Parameters
        ----------------
        fullPath : :class:`bool`, default True
            unused
        """
        files = self.getFiles(startDate=startDate, endDate=endDate, level=level, product=product, code=code,
                              instrument=instrument, exists=exists, newest_version=newest_version, limit=limit)

        return list(map(attrgetter('file_id'), files))  # this is faster than a list comprehension

    def getActiveInspectors(self):
        """
        Query the db and returns all active inspectors

        Returns
        -------
        :class:`list` of :class:`tuple`
            For each active inspector, returns full filename (from
            :sql:column:`~inspector.relative_path` and
            :sql:column:`~inspector.filename`),
            :sql:column:`~inspector.description`,
            :sql:column:`~inspector.arguments`, and
            :sql:column:`~inspector.product`.
        """
        activeInspector = collections.namedtuple(
            'activeInspector', 'path description arguments product_id')
        sq = self.session.query(self.Inspector).filter(self.Inspector.active_code == True).all()
        return [activeInspector(
            os.path.join(
                self.InspectorDirectory,
                *(ans.relative_path.split(posixpath.sep) + [ans.filename])),
            ans.description, ans.arguments, ans.product) for ans in sq]

    def getChildrenProcesses(self, file_id):
        """
        Given a file, return all the processes that use this as input

        Parameters
        ----------
        file_id : :class:`int` or :class:`str`
            :sql:column:`~file.file_id` or :sql:column:`~file.filename`

        Returns
        -------
        :class:`list` of :class:`int`
            :sql:column:`~productprocesslink.process_id` for all processes
            which can use the given file as input.
        """
        DBlogging.dblogger.debug("Entered getChildrenProcesses():  file_id: {0}".format(file_id))
        product_id = self.getEntry('File', file_id).product_id

        # get all the process ids that have this product as an input
        return self.getProcessFromInputProduct(product_id)

    def getProductID(self, product_name):
        """
        Return the product ID for an input product name

        Parameters
        ----------
        product_name : :class:`str`
            the name of the product to get the id of. Also supports a
            sequence of names, a single product ID (to confirm existence),
            or a sequence of product IDs.

        Returns
        -------
        product_id : :class:`int`
            the product ID for the input product name
        """
        is_sequence, is_name = False, False # Assume input is a product ID
        try:
            product_name = int(product_name)
        except TypeError:  # came in as list or tuple
            is_sequence = True
        except ValueError:
            is_name = True
        if is_sequence: # Call for every input
            return list(map(self.getProductID, product_name))
        if is_name: # Product name, just get ID
            sq = self.session.query(self.Product).filter_by(product_name=product_name)
            # if two products have the same name always return the lower id one
            res = sorted([x.product_id for x in sq])
            if res:
                return res[0]
            # no file_id found
            raise DBNoData("No product_name %s found in the DB" % (product_name))
        # Numerical product ID, make sure it exists
        sq = self.session.query(self.Product).get(product_name)
        if sq is not None:
            return sq.product_id
        else:
            raise DBNoData("No product_id {0} found in the DB".format(product_name))

    def getSatelliteID(self,
                       sat_name):
        """
        Returns the satellite ID for an input satellite name

        Parameters
        ----------
        sat_name : :class:`str`
            the :sql:column:`~satellite.satellite_name` to get the id of.
            Also supports a sequence of names, a single satellite ID
            (to confirm existence), or a sequence of satellite IDs.

        Returns
        -------
        satellite_id : :class:`int`
            the :sql:column:`~satellite.satellite_id` for the input
            satellite name
        """
        try:
            sat_id = int(sat_name)
            sq = self.session.query(self.Satellite).get(sat_id)
            if sq is None:
                raise NoResultFound("No satellite id={0} found".format(sat_id))
            return sq.satellite_id
        except TypeError:  # came in as list or tuple
            return list(map(self.getSatelliteID, sat_name))
        except ValueError:  # it was a name
            sq = self.session.query(self.Satellite).filter_by(satellite_name=sat_name).one()
            return sq.satellite_id  # there can be only one of each name

    def getCodePath(self, code_id):
        """
        Given a code_id list return the full name (path and all) of the code

        Parameters
        ----------
        code_id : :class:`int` or :class:`str`
            :sql:column:`~code.code_id` or :sql:column:`~code.code_description`
            of code to look up.

        Returns
        -------
        :class:`str`
            Full path to code.
        """
        code = self.getEntry('Code', code_id)
        if not code.active_code:  # not an active code
            return None
        return os.path.normpath(posixpath.join(
            self.CodeDirectory, code.relative_path, code.filename))

    def getCodeVersion(self, code_id):
        """
        Given a code_id get the code version
        Given a code_id list return the full name (path and all) of the code

        Parameters
        ----------
        code_id : :class:`int` or :class:`str`
            :sql:column:`~code.code_id` or :sql:column:`~code.code_description`
            of code to look up.

        Returns
        -------
        :class:`.Version`
            Version of the code.
        """
        code = self.getEntry('Code', code_id)
        return Version.Version(code.interface_version, code.quality_version, code.revision_version)

    def getAllCodesFromProcess(self, proc_id):
        """
        Given a process id return the code ids that performs that process

        Also returns the valid dates for each code

        Parameters
        ----------
        proc_id : :class:`int`
            :sql:column:`~process.process_id` of process to look up.

        Returns
        -------
        :class:`list` of :class:`tuple`
            For every active, newest version code that implements the process,
            :sql:column:`~code.code_id`, :sql:column:`~code.code_start_date`,
            and :sql:column:`~code.code_stop_date`.
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
        Given a process id return the code id that performs that process
        on a particular date.

        Parameters
        ----------
        proc_id : :class:`int`
            :sql:column:`~process.process_id` of process to look up.
        utc_file_date : :class:`~datetime.datetime`
            Date on which the code must be valid.

        Returns
        -------
        :class:`int`
            :sql:column:`~code.code_id` for the active, newest version code
            that implements the process, and is valid on the given date.
            Returns :data:`None` if there is no match.

        Raises
        ------
        DBError
            If there is more than one matching code.
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
            raise DBError('More than one code active for a Given day')
        return sq[0].code_id

    def getMissionDirectory(self):
        """
        Return the root directory for the current mission

        Returns
        -------
        :class:`str`
            Root directory for current mission (i.e.
            :sql:column:`~mission.rootdir`).
        """
        try:
            return os.path.abspath(os.path.expanduser(
                self.session.query(self.Mission.rootdir).one()[0]))
        except sqlalchemy.orm.exc.NoResultFound:
            return None
        except sqlalchemy.orm.exc.MultipleResultsFound:
            raise ValueError('No mission id specified and more than one mission present')

    def getCodeDirectory(self):
        """
        Return the code directory for the current mission

        Returns
        -------
        :class:`str`
            Code directory for current mission (i.e.
            :sql:column:`~mission.codedir`, if defined).

        See Also
        --------
        :meth:`getDirectory`
        """
        return self.getDirectory('codedir', default=self.MissionDirectory)

    def getInspectorDirectory(self):
        """
        Return the inspector directory for the current mission

        Returns
        -------
        :class:`str`
            Inspector directory for current mission (i.e.
            :sql:column:`~mission.inspectordir`, if defined).

        See Also
        --------
        :meth:`getDirectory`
        """
        return self.getDirectory('inspectordir', default=self.CodeDirectory)

    def checkIncoming(self, glb='*'):
        """Check the incoming directory for the current mission

        Parameters
        ----------
        glb : :class:`str`, optional
            Glob pattern that files must match.

        Returns
        -------
        :class:`list` of :class:`str`
            All files in the incoming directory
        """
        path = self.getIncomingPath()
        DBlogging.dblogger.debug("Looking for files in {0}".format(path))
        files = glob.glob(os.path.join(path, glb))
        return sorted(files)

    def getIncomingPath(self):
        """
        Return the incoming directory for the current mission

        Returns
        -------
        :class:`str`
            Incoming directory for current mission (i.e.
            :sql:column:`~mission.incoming_dir`).

        See Also
        --------
        :meth:`getDirectory`
        """
        return self.getDirectory('incoming_dir')

    def getErrorPath(self):
        """
        Return the error directory for the current mission

        Returns
        -------
        :class:`str`
            Error directory for current mission (i.e.
            :sql:column:`~mission.errordir`, if defined).

        See Also
        --------
        :meth:`getDirectory`
        """
        #print(os.path.join(self.getCodeDirectory(),'errors'))
        return self.getDirectory('errordir', default=os.path.join(self.CodeDirectory, 'errors'))

    def getDirectory(self, column, default=None):
        """
        Look up directory for the specified column.

        The mission rootdir may be absolute or relative to current path.
        Directory requested may be in db as absolute or relative to mission
        root. Home dir references are expanded.

        Parameters
        ----------
        column : :class:`str`
            Name of column in :sql:table:`mission` to look up.

        default : :class:`str`, optional
            Default to return if directory not found in mission table,
            default :data:`None`.
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
        Given a file_id return the code_id that created it, or None

        Parameters
        ----------
        file_id : :class:`int` or :class:`str`
            :sql:column:`~file.file_id` or :sql:column:`~file.filename` of
            the file to look up.

        Returns
        -------
        :class:`int`
            :sql:column:`~code.code_id` of the code that created the file.
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
        Given a code_id return the file_id of all files it created

        Parameters
        ----------
        code_id : :class:`int` or :class:`str`
            :sql:column:`~code.code_id` or :sql:column:`~code.code_description`
            of the code to look up.

        Returns
        -------
        :class:`~sqlalchemy.orm.Query`
            :sql:column:`~file.file_id` of all files created by the code.
        """
        DBlogging.dblogger.debug("Entered getFilecodelink_bycode: code_id={0}".format(code_id))
        code_id = self.getCodeID(code_id)
        sq = self.session.query(self.Filecodelink.resulting_file).filter_by(source_code=code_id)
        return sq

    def getMissionID(self, mission_name):
        """
        Given a mission name return its ID

        Parameters
        ----------
        mission_name : :class:`str`
            Name of mission, i.e. :sql:column:`~mission.mission_name`.

        Returns
        -------
        :class:`int`
            :sql:column:`~mission.mission_id` for the corresponding mission.

        See Also
        --------
        :ref:`concepts_missions`
        """
        try:
            m_id = int(mission_name)
            ms = self.session.query(self.Mission).get(m_id)
            if ms is None:
                raise DBNoData('Invalid mission id {0}'.format(m_id))
        except (ValueError, TypeError):
            sq = self.session.query(self.Mission.mission_id).filter_by(mission_name=mission_name).all()
            if len(sq) == 0:
                raise DBNoData('Invalid mission name {0}'.format(mission_name))
            m_id = sq[0].mission_id
        return m_id

    def tag_release(self, rel_num):
        """
        Tag all the newest versions of files to a release number (integer)

        Parameters
        ----------
        rel_num : :class:`int`
            Tag all "newest version" files as part of this release.

        See Also
        --------
        :ref:`concepts_releases`
        """
        newest_files = self.getFiles(newest_version=True)

        for f in newest_files:
            self.addRelease(f, rel_num, commit=False)
        self.commitDB()
        return len(newest_files)

    def addRelease(self, filename, release, commit=False):
        """
        Given a filename or file_id add an entry to the release table

        Parameters
        ----------
        filename : :class:`int` or :class:`str`
            :sql:column:`~file.filename` or :sql:column:`~file.file_id`
            of file to add to a release.
        release : :class:`int`
            Release number to add file to.
        commit : :class:`bool`, default False
            Commit changes to the database when done.

        See Also
        --------
        :ref:`concepts_releases`
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
        Given a release number return all the filenames with the release

        Parameters
        ----------
        rel_num : :class:`int`
            Release number to list
        fullpath : :class:`bool`, default True
            Include full path to files (not just filenames)

        Returns
        -------
        :class:`list` of :class:`str`
            All filenames in the release.
        """
        sq = self.session.query(self.Release.file_id).filter_by(release_num=rel_num).all()
        sq = list(map(itemgetter(0), sq))
        for i, v in enumerate(sq):
            if fullpath:
                sq[i] = self.getFileFullPath(v)
            else:
                sq[i] = self.getEntry('File', v).filename
        return sq

    def checkFileSHA(self, file_id):
        """
        Given a file id or name check the db checksum and the file checksum

        Parameters
        ----------
        file_id : :class:`int` or :class:`str`
            :sql:column:`~file.filename` or :sql:column:`~file.file_id`
            of file to check

        Returns
        -------
        :class:`bool`
            If the calculated checksum of file on disk matches the
            checksum in the database.
        """
        db_sha = self.getEntry('File', file_id).shasum
        disk_sha = calcDigest(self.getFileFullPath(file_id))

        return disk_sha == db_sha

    def checkFiles(self, limit=None):
        """
        Check files in the DB, return inconsistent files and why

        Parameters
        ----------
        limit : :class:`int`
            Maximum number of files to check, default all

        Returns
        -------
        :class:`list` of :class:`tuple`
            All files with problems. Each element is
            (:sql:column:`~file.filename`, result), where "result" is 1
            for a bad checksum and 2 if file not found on disk.
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
        Master routine for all the getXXXTraceback functions

        The "traceback" is the set of records across tables that are
        relevant to one particular record

        this is some large select statements with joins in them, these are tested and do work

        Parameters
        ----------
        table : :class:`str`
            Name of the :doc:`table </developer/tables>` to look up.
        in_id : :class:`int`
            ID, usually primary key on the table, for the record
            to look up.

        Returns
        -------
        :class:`dict`
            Keyed by table name, values are records from that table
            (instances of table types created by
            :class:`~sqlalchemy.sql.schema.Table`).

        Other Parameters
        ----------------
        in_id2
            Not used.

        Examples
        --------
        >>> tb = dbu.getTraceback('File', 500)
        >>> tb.['product'].product_name
        u'rbspb_int_ect-mageisM35-ns-L05'
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
                raise DBError("file {0} did not have a traceback, this is a problem, fix it".format(in_id))

            if len(sq) > 1:
                raise DBError("Found multiple tracebacks for file {0}".format(in_id))
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
                raise DBError("code {0} did not have a traceback, this is a problem, fix it".format(in_id))
            
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
                raise DBError("code {0} did not have a traceback, this is a problem, fix it".format(in_id))

            if len(sq) > 1:
                raise DBError("Found multiple tracebacks for code {0}".format(in_id))
            for ii, v in enumerate(vars):
                retval[v] = sq[0][ii]

        elif table.capitalize() == 'Inspector':
            retval['inspector'] = self.getEntry(table.capitalize(), in_id)
            tmp = self.getTraceback('Product', retval['inspector'].product)
            retval = dict(itertools.chain(retval.items(), tmp.items()))

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
                raise DBError("product {0} did not have a traceback, this is a problem, fix it".format(in_id))

            if len(sq) > 1:
                raise DBError("Found multiple tracebacks for product {0}".format(in_id))
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
                raise DBError("process {0} did not have a traceback, this is a problem, fix it".format(in_id))

            if len(sq) > 1:
                raise DBError("Found multiple tracebacks for process {0}".format(in_id))
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
            retval = dict(itertools.chain(retval.items(), tmp.items()))

        elif table.capitalize() == 'Satellite':
            retval['satellite'] = self.getEntry('Satellite', in_id)
            tmp = self.getTraceback('Mission', retval['satellite'].mission_id)
            retval = dict(itertools.chain(retval.items(), tmp.items()))

        elif table.capitalize() == 'Mission':
            retval['mission'] = self.getEntry('Mission', in_id)

        else:
            raise NotImplementedError('The traceback or {0} is not implemented'.format(table))

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

        Parameters
        ----------
        inst_id : :class:`int` or :class:`str`
            :sql:column:`~instrument.instrument_id` or
            :sql:column:`~instrument.instrument_name` for instrument

        Returns
        -------
        :class:`list` of :class:`int`
            :sql:column:`~product.product_id` for every product
            associated with this instrument.
        """
        inst_id = self.getInstrumentID(inst_id)
        sq = self.session.query(self.Instrumentproductlink.product_id).filter_by(instrument_id=inst_id).all()
        if sq:
            return list(map(itemgetter(0), sq))
        else:
            return None

    def getProductsByLevel(self, level):
        """
        Get all the products for a Given level

        Parameters
        ----------
        level : :class:`float`
            Data level to look up

        Returns
        -------
        :class:`list` of :class:`int`
            :sql:column:`~product.product_id` for every product
            with :sql:column:`~product.level` equal to ``level``.
        """
        sq = self.session.query(self.Product.product_id).filter_by(level=level).all()
        if sq:
            return list(map(itemgetter(0), sq))
        else:
            return None

    def getAllProcesses(self, timebase='all'):
        """
        Get all processes

        Parameters
        ----------
        timebase : :class:`str`, optional
            Limit to products with this
            :sql:column:`~process.output_timebase` (default: all).

        Returns
        -------
        :class:`~sqlalchemy.orm.Query`
            :sql:table:`process` table records
        """
        if timebase == 'all':
            procs = self.session.query(self.Process).all()
        else:
            procs = self.session.query(self.Process).filter_by(output_timebase=timebase.upper()).all()
        return procs

    def getProcessTimebase(self, process_id):
        """
        Return the timebase for a process

        Parameters
        ----------
        process_id : :class:`int` or :class:`str`
            :sql:column:`~process.process_id` or
            :sql:column:`~process.process_name` of the desired process.

        Returns
        -------
        :class:`str`
            :sql:column:`~process.output_timebase` for the process.
        """
        return self.getEntry('Process', process_id).output_timebase

    def getAllProducts(self, id_only=False):
        """
        Return a list of all products as instances

        Parameters
        ----------
        id_only : :class:`bool`, default False
            Return only the :sql:column:`~product.product_id`,
            instead of the entire record.

        Returns
        -------
        :class:`~sqlalchemy.orm.Query` or :class:`list` of :class:`int`
            Complete :sql:table:`product` records for all products,
            or just :sql:column:`~product.product_id` (if ``id_only``).
        """
        prods = self.session.query(self.Product).all()
        if id_only:
            prods = list(map(attrgetter('product_id'), prods))
        return prods

    def getEntry(self, table, args):
        """Return entry instance from any table in DB

        Parameters
        ----------
        table : :class:`str`
            Name of the table
        args : :class:`int` or :class:`str`
            Argument to identify entry. This is first tried as a
            primary key (integer or sequence of integers); if that
            fails, then assumed to be a name and used for a lookup
            via get[table]ID.

        Returns
        -------
        various types
            Matching column from the table. If there is no primary
            key match and the table does not support name lookup,
            returns ``None``.

        Raises
        ------
        DBNoData
            if argument is not found as primary key and name lookup fails
            (but not if name lookup is not available).
        """
        retval = None
        if isinstance(args, (int, collections.abc.Iterable)) \
           and not isinstance(args, str_classes):  # PK: int, non-str sequence
            retval = self.session.query(getattr(self, table)).get(args)
        if retval is None:  # Not valid PK type, or PK not found
            # see if it was a name
            if ('get' + table + 'ID') in dir(self):
                cmd = 'get' + table + 'ID'
                pk = getattr(self, cmd)(args)
                retval = self.session.query(getattr(self, table)).get(pk)
# This code will make it consistently raise DBNoData if nothing is found,
# but codebase needs to be scrubbed for callers that expect None instead.
#            else:
#                raise DBNoData('No entry found for table {}, key {}.'
#                               .format(table, args))
        return retval

    def getFileParents(self, file_id, id_only=False):
        """
        Given a file_id (or filename) return the files that went into making it

        Parameters
        ----------
        file_id : :class:`int` or :class:`str`
            :sql:column:`~file.file_id` or :sql:column:`~file.filename`
            of the file of interest.
        id_only : :class:`bool`, default False
            Return only the :sql:column:`~file.file_id`,
            instead of the entire record.

        Returns
        -------
        :class:`~sqlalchemy.orm.Query` or :class:`list` of :class:`int`
            Complete :sql:table:`file` records for all input files,
            or just :sql:column:`~file.file_id` (if ``id_only``).
        """
        file_id = self.getFileID(file_id)
        f_ids = self.session.query(self.Filefilelink.source_file).filter_by(resulting_file=file_id).all()
        if not f_ids:
            return []
            
        f_ids = list(map(itemgetter(0), f_ids))
        if id_only:
            return f_ids

        return [self.getEntry('File', val) for val in f_ids]

    def getFileVersion(self, fileid):
        """
        Return the version instance for a file

        Parameters
        ----------
        fileid : :class:`int` or :class:`str`
            :sql:column:`~file.file_id` or :sql:column:`~file.filename`
            of the file of interest.

        Returns
        -------
        :class:`~.Version.Version`
            Version of the file.
        """
        if not isinstance(fileid, self.File):
            fileid = self.getEntry('File', fileid)
        return Version.Version(fileid.interface_version,
                               fileid.quality_version,
                               fileid.revision_version)

    def getChildTree(self, inprod):
        """
        Given an input product return a list of its output product ids

        Parameters
        ----------
        inprod : :class:`int`
            :sql:column:`~product.product_id` of the input product.

        Returns
        -------
        :class:`list` of :class:`int`
            :sql:column:`~product.product_id` of all products that can
            be made from ``inprod``.
        """
        out_proc = self.getProcessFromInputProduct(inprod)
        out_prod = [self.getEntry('Process', op).output_product
                    for op in out_proc]
        # Skip if no output product ('', null, but allow prodid of 0)
        return [op for op in out_prod if op or op == 0]

    def getProductParentTree(self):
        """
        go through the db and return a tree of all products and their parents

        This will allow for a run all the non done files script

        Returns
        -------
        :class:`list`
            Each entry has two elements: a :sql:column:`~product.product_id`
            and another list of :sql:column:`~product.product_id` for all
            product that can be made from it.

        See Also
        --------
        :meth:`getChildTree`
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

        Parameters
        ----------
        code_id : :class:`int` or :class:`str`
            :sql:column:`~code.code_id` or
            :sql:column:`~code.code_description` for code to update.

        is_newest : :class:`bool`, default False
            Set :sql:column:`~code.newest_version` and
            :sql:column:`~code.active_code` (True), or not newest, and
            inactive (False).
        """
        DBlogging.dblogger.debug\
            ("Entered updateCodeNewestVersion: code_id={0}, is_newest={1}"\
             .format(code_id, is_newest))
        code = self.getEntry('Code', code_id)
        code.newest_version = code.active_code = bool(is_newest)
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

        Parameters
        ----------
        table : :class:`str`
            Name of the table to edit.
        my_id : :class:`int`
            Record to edit; most commonly the numerical ID (primary key)
            but also supports string matching on other columns as provided
            by :meth:`getEntry`.
        column : :class:`str`
            name of column to edit
        my_str : :class:`str`, optional
            String to add or replace. Required with
            ``ins_after``, ``ins_before``, ``replace_str``.
        after_flag : :class:`str`, optional
            Only replace string in words immediately following this word.
            Only supported in ``arguments`` column of ``code`` table.
            Default: replace in all.
        ins_after : :class:`str`, optional
            Value to insert ``my_str`` after. Conflicts with ``ins_before``,
            ``replace_str``, ``combine``.
        ins_before : :class:`str`, optional
            Value to insert ``my_str`` before. Conflicts with ``ins_after``,
            ``replace_str``, ``combine``.
        replace_str : :class:`str`, optional
            Value to replace with ``my_str``. Conflicts with ``ins_after``,
            ``ins_before``, ``combine``.
        combine : :class:`bool`, default False
            If true, combine all instances of words after the word in
            ``after_flag``. Conflicts with ``ins_after``, ``ins_before``,
            ``replace_str``.

        Raises
        ------
        ValueError
            for any invalid combination of arguments.
        RuntimeError
            if multiple rows match ``my_id``.

        Examples
        --------
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

    def addUnixTimeTable(self):
        """Add a table containing a file's Unix start/stop time.

        Used for migrating databases; doing file searches based on the
        Unix time is faster than the UTC timestamp. This will also
        populate the time columns from a file's UTC start/stop time.

        Raises
        ------
        RuntimeError
            If the Unix time table already exists
        """
        if hasattr(self, 'Unixtime'):
            raise RuntimeError('Unixtime table already seems to exist.')
        unixtime = sqlalchemy.Table(
            'unixtime', self.metadata, *tables.definition('unixtime'))
        self.metadata.create_all(tables=[unixtime])
        # Make object for the new table definition (skips existing tables)
        self._createTableObjects()
        unx0 = datetime.datetime(1970, 1, 1)
        for f in self.getFiles(): # Populate the times
            r = self.Unixtime()
            r.file_id = f.file_id
            # If changed, also change addFile, getFiles, updateUnixTime.py
            r.unix_start = int((f.utc_start_time - unx0)\
                               .total_seconds())
            r.unix_stop = int((f.utc_stop_time - unx0)\
                              .total_seconds())
            self.session.add(r)
        self.commitDB()


def create_tables(filename='dbprocessing_default.db', dialect='sqlite'):
    """
    Step through and create the DB structure, relationships and constraints

    """
    if dialect == 'sqlite':
        url = 'sqlite:///' + filename
    elif dialect == 'postgresql':
        url = postgresql_url(filename)
    else:
        raise ValueError('Unknown dialect {}'.format(dialect))

    metadata = sqlalchemy.schema.MetaData()
    for name in tables.names:
        data_table = sqlalchemy.schema.Table(
            name, metadata, *tables.definition(name))
    engine = sqlalchemy.engine.create_engine(url, echo=False)
    metadata.bind = engine
    metadata.create_all(checkfirst=True)
    engine.dispose()
