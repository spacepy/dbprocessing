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
import DBlogging
import socket # to get the local hostname
import sys

import DBStrings
import Version


## This goes in the processing comment field in the DB, do update it
__version__ = '2.0.3'


class DBError(Exception):
    pass
class DBProcessingError(Exception):
    pass
class FilenameParse(Exception):
    pass



class DBUtils2(object):
    """
    @summary: DBUtils - utility routines for the DBProcessing class, all of these may be user called but are meant to
    be internal routines for DBProcessing
    """

    def __init__(self, mission='Test'):
        """
        @summary: Initialize the DBUtils class, default mission is 'Test'
        """
        if mission == None:
            raise(DBError("Must input mission name to create DBUtils2 instance"))
        self.mission = mission
        self.dbIsOpen = False
        #Expose the format/regex routines of DBFormatter
        fmtr = DBStrings.DBFormatter()
        self.format = fmtr.format
        self.re = fmtr.re

    def __del__(self):
        """
        try and clean up a little bit
        """
        self._closeDB()

    def __repr__(self):
        """
        @summary: Print out something usefule when one prints the class instance

        @return: DBProcessing class instance for mission <mission name>
        """
        return 'DBProcessing class instance for mission ' + self.mission + ', version: ' + __version__


    @classmethod
    def _test_SQLAlchemy_version(self, version= sqlalchemy.__version__):
        """This tests the version to be sure that it is compatable"""
        expected = '0.7'
        if version[0:len(expected)] != expected:
            raise DBError(
                "SQLAlchemy version %s was not expected, expected %s.x" %
                (version, expected))
        return True


    @classmethod
    def _build_fname(self,
                     rootdir = '',
                     relative_path = '',
                     mission_name = '',
                     satellite_name = '',
                     product_name = '',
                     date = '',
                     release = '',
                     quality = '',
                     revision = '',
                     extension = '.cdf'):
        """This builds a filename from the peices contained in the filename

        @keyword rootdir: root directory of the filename to create (default '')
        @keyword relative_path: relative path for filename (default '')
        @keyword mission_name: mission name (default '')
        @keyword satellite_name: satellite name  (default '')
        @keyword product_name: data product name (default '')
        @keyword date: file date (default '')
        @keyword release: release version number (default '')
        @keyword quality: quality version number (default '')
        @keyword revision: revision version number  (default '')

        @return: A full filename that can be used by OS calls

        >>> nl._ProcessNext__build_fname('/root/file/', 'relative/', 'Test', 'test1', 'Prod1', '20100614', 1, 1, 1)
            Out[9]: '/root/file/relative/Test-test1_Prod1_20100614_v1.1.1.cdf'

        """
        dir = rootdir + relative_path
        fname = mission_name + '-' + satellite_name + '_' + product_name
        ver = 'v' + str(release) + '.' + str(quality) + '.' + str(revision)
        fname = fname + '_' + date + '_' + ver + extension
        return dir + fname



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

                # this holds the metadata, tables names, attributes, etc
            elif self.mission == 'Polar':
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

## create a dictionary of all the table names that will be used as calss names.
## this uses the db table name as the tabel name and a cap 1st letter as the class
## when interacting using python use the class
        table_dict = {}
        for val in table_names:
            table_dict[val[0].upper() + val[1:]] = val

##  dynamincally create all the classes (c1)
##  dynamicallly create all the tables in the db (c2)
##  dynaminically create all the mapping between class and table (c3)
## this just saves a lot of typing and is equilivant to:
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


######################################
### Gather files from DB #############
######################################

    def _gatherFiles(self, level, verbose=False):
        """
        gather all the files for a given mission into a list of sqlalchemy objects

        @keyword verbose: (optional) print more out to the command line

        @return: list of Data_files objects

        >>>  pnl._gatherFiles()
        123
        """
        try: self.Data_file
        except AttributeError: self._createTableObjects()
        try: self.Files_by_mission
        except AttributeError: self._createViews()

        files = self.session.query(self.Data_files).filter(self.Data_sources.ds_id == self.Data_files.ds_id).filter(self.Satellites.s_id == self.Data_sources.s_id).filter(self.Missions.m_id == self.Satellites.m_id).filter_by(data_level=level).all()
        return {'files':files, 'process':[True]*len(files)}




###########################################
### Decide which files NOT to process #####
###########################################

    def _procCodeDates(self, verbose = False):
        """
        go through bf dict and if files have processing code in the right date range

        @keyword verbose: (optional) - print out lots of info

        @return: Counter - number of files added to the list from this check

        >>>  pnl._procCodeDates()
        """
        # is there a processing code with the right dates?
        # this is broken as there is more than one processing file that spans the data L0-L1 conversion
        try: self.del_names
        except AttributeError: self.initDelNames()
        counter = 0
        for fname in self.bf:
            all_false = np.array([])
            for sq in self.session.query(self.Processing_paths).filter_by(p_id = self.bf[fname]['out_p_id']):
                if self.bf[fname]['utc_file_date'] < sq.code_start_date or self.bf[fname]['utc_file_date'] > sq.code_stop_date:
                    all_false = np.append(all_false, True)
            if ~all_false.any() and len(all_false) != 0:
                print("\t<procCodeDates> %s didnt have valid, %s, %s, %s" % (fname, sq.code_start_date, sq.code_stop_date, all_false))
                self.del_names.append(fname)
                counter += 1
        return counter


    def _newerFileVersion(self, id, bool=False, verbose=False):
        """
        given a data_file ID decide if there is a newer version
        (maybe _delNewerVersion() can extend this but not yet)

        @param id: the code or file id to check
        @keyword bool: (optional) if set answers the question is there a newer version of the file?

        @return: id of the newest version

        >>>  pnl._newerFileVersion(101)
        101
        """
        try: self.Data_files
        except AttributeError: self._createTableObjects()

        mul = []
        vall = []
        for sq_bf in self.session.query(self.Data_files).filter_by(base_filename = self._getBaseFilename(id)):
            mul.append([sq_bf.filename,
                        sq_bf.f_id])
            vall.append(self.__get_V_num(sq_bf.interface_version,
                                         sq_bf.quality_version,
                                         sq_bf.revision_version))
            if verbose:
                print("\t\t%s %d %d %d %d" % (sq_bf.filename,
                                              sq_bf.interface_version,
                                              sq_bf.quality_version,
                                              sq_bf.revision_version,
                                              self.__get_V_num(sq_bf.interface_version,
                                                               sq_bf.quality_version,
                                                               sq_bf.revision_version)) )

        if len(vall) == 0:
            return None

        self.mul = mul
        self.vall = vall

        ## make sure that there is just one of each v_num in vall
        cnts = [vall.count(val) for val in vall]
        if cnts.count(1) != len(cnts):
            raise(DBProcessingError('More than one file with thwe same computed V_num'))

        ## test to see if the newest is the id passed in
        ind = np.argsort(vall)
        if mul[ind[-1]][1] != id:
            if bool: return True
            else: return mul[ind[-1]][1]
        else:
            if bool: return False
            else: return mul[ind[-1]][1]



#####################################
####  Do processing and input to DB
#####################################

    def _currentlyProcessing(self):
        """
        Checks the db to see if it is currently processing, dont want to do 2 at the same time

        @return: false or the pid
        @rtype: (bool, long)

        >>>  pnl._currentlyProcessing()
        """
        DBlogging.dblogger.info("Checking currently_processing")

        sq = self.session.query(self.Logging).filter_by(currently_processing = True)
        if sq.count() == 1:
            DBlogging.dblogger.warning("currently_processing is set.  PID: %d" % (sq[0].pid))
            return sq[0].pid
        elif sq.count() == 0:
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
            print("Must enter a comment to override DB lock")
            return False
        sq = self.session.query(self.Logging).filter_by(currently_processing = True)
        for val in sq:
            val.currently_processing = False
            val.processing_end = datetime.datetime.now()
            val.comment = 'Overridden:' + comment + ':' + __version__
            DBlogging.dblogger.error( "Logging lock overridden: %s" % ('Overridden:' + comment + ':' + __version__) )
            self.session.add(val)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return True



    def _startLogging(self):
        """
        Add an entry to the logging table in the DB, logging

        >>>  pnl._startLogging()
        """
        # this is the logging of the processing, no real use for it yet but maybe we will inthe future
        # helps to know is the process ran and if it succeeded
        if self._currentlyProcessing():
            raise(DBError('A Currently Processing flag is still set, cannot process now'))
        # save this class instance so that we can finish the logging later
        self.__p1 = self._addLogging(True,
                              datetime.datetime.now(),
                              self._getMissionID(),
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
        add an entry to th logging table

        @param currently_processing: is the db currently processing?
        @type currently_processing: bool
        @param processing_start_time: the time the proessing started
        @type processing_start_time: datetime.datetime
        @param mission_id: the mission idthe processing if for
        @type mission_id: int
        @param user: the user doing the processing
        @type user: str
        @param hostname: the hostname that initiated the processing
        @type hostname: str

        @keyword pid: the process id that id the processing
        @type pid: int
        @keyword processing_end_time: the time the processing stopped
        @type processing_end_time: datetime.datetime
        @keyword comment: commen about the processing run
        @type comment: str

        @return: instance of the Logging class
        @rtype: Logging

        """
        try:
            l1 = self.Logging()
        except AttributeError:
            raise(DBError("Class Logging not found was it created?"))

        l1.currently_processing = currently_processing
        l1.processing_start_time = processing_start_time
        l1.mission_id = mission_id
        l1.user = user
        l1.hostname = hostname
        l1.pid = pid
        l1.processing_end_time = processing_end_time
        l1.comment = comment
        self.session.add(l1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return l1    # so we can use the same session to stop the logging

    def _stopLogging(self, comment):
        """
        Finish the entry to the processing table in the DB, logging

        @param comment: (optional) a comment to insert intot he DB
        @type param: str

        >>>  pnl._stopLogging()
        """
        try: self.__p1
        except:
            DBlogging.dblogger.warning( "Logging was not started, can't stop")
            raise(DBProcessingError("Logging was not started"))
        # clean up the logging, we are done processing and we can realease the lock (currently_processing) and
        # put in the complete time
        if comment == None:
            print("Must enter a comment for the log")
            return False

        self.__p1.processing_end = datetime.datetime.now()
        self.__p1.currently_processing = False
        self.__p1.comment = comment+':' + __version__
        self.session.add(self.__p1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        DBlogging.dblogger.info( "Logging stopped: %s comment '%s' " % (self.__p1.processing_end, self.__p1.comment) )

    def _addLoggingFile(self,
                        logging_id,
                        file_id,
                        code_id,
                        comment=None):
        """
        add a Logging_files entry to  DB

        @param logging_id: the id of the logging session
        @type logging_id: int
        @param file_id: file id of the file being logged
        @type file_id: int
        @param code_id: the id of ete code being used
        @type code_id: int
        @keyword comment: comment on the logged file
        @type comment: str
        """
        pf1 = self.Logging_files()
        pf1.logging_id = logging_id
        pf1.file_id = file_id
        pf1.code_id = code_id
        pf1.comment = comment
        self.session.add(pf1)
        DBlogging.dblogger.info( "File Logging added for file:%d code:%d with comment:%s"  % (pf1.file_id, pf1.code_id, pf1.comment) )
        # TODO  think on if session should be left open or if a list shoud be passed in
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return pf1.logging_file_id

    def _checkDiskForFile(self, fix=False):
        """
        Check the filesystem tosee if the file exits or not as it says in the db

        @keyword fix: (optional) set to have the DB fixed to match the filesystem
           this is **NOT** sure to be safe
        @return: count - return the count of out of sync files

        """
        counter = 0  # count how many were wrong
        for fname in self.bf:
            for sq in self.session.query(self.Data_files).filter_by(f_id = self.bf[fname]['f_id']):
                if sq.exists_on_disk != os.path.exists(self.bf[fname]['absolute_name']):
                    counter += 1
                    if sq.exists_on_disk == True:
                        print("%s DB shows exists, must have been deleted" % (self.bf[fname]['absolute_name']))
                        if fix == True:
                            sq.exists_on_disk = False
                            self.session.add(sq)
                    else:
                        print("%s file in filesystem, DB didnt have it so, manually added?" % (self.bf[fname]['absolute_name']))
                        if fix == True:
                            sq.exists_on_disk = True
                            self.session.add(sq)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return counter

    def processqueueFlush(self):
        """
        remove everyhting from he process queue
        """
        while self.processqueueLen() > 0:
            self.processqueuePop()
        DBlogging.dblogger.info( "Processqueue was cleared")

    def processqueueGetAll(self):
        """
        return the entire contents of the process queue
        """
        pqdata = [self.processqueueGet(ii) for ii in range(self.processqueueLen())]
        if len(pqdata) != self.processqueueLen():
            DBlogging.dblogger.error( "Entire Processqueue was read incorrectly")
            raise(DBError("Something went wrong with processqueue readall"))
        DBlogging.dblogger.debug( "Entire Processqueue was read")
        return pqdata

    def processqueuePush(self, fileid):
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
        try:
            fileid = int(fileid)
        except ValueError: # must have been a filename
            fileid = self._getFileID(fileid)
        pq1 = self.Processqueue()
        pq1.file_id = fileid
        self.session.add(pq1)
        DBlogging.dblogger.info( "File added to process queue {0}:{1}".format(fileid, self._getFilename(fileid) ) )
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        pqid = self.session.query(self.Processqueue.file_id).all()
        return pqid[-1]

    def processqueueLen(self):
        """
        return the number of files in the process queue
        """
        return self.session.query(self.Processqueue).count()

    def processqueuePop(self, index=0):
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
        num = self.processqueueLen()
        if num == 0:
            return None
        elif index >= num:
            return None
        else:
            for ii, fid in enumerate(self.session.query(self.Processqueue)):
                if ii == index:
#                    if self.mission not in self._getMissionName(self.getFileMission(fid.file_id)): # file does not below to this mission
#                        fid_ret = self.processqueueGet(ii+1)
                    self.session.delete(fid)
                    fid_ret = fid.file_id
                    break # there can be only one
            try:
                self.session.commit()
            except IntegrityError as IE:
                self.session.rollback()
                raise(DBError(IE))
            return fid_ret

    def processqueueGet(self, index=0):
        """
        get the file at the head of the queue (from the left)

        Returns
        =======
        file_id : int
            the file_id of the file popped from the queue
        """
        num = self.processqueueLen()
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
#                    if self.mission not in self._getMissionName(self.getFileMission(fid_ret)): # file does not below to this mission
#                        fid_ret = self.processqueueGet(ii+1)
                    break # there can be only one
            DBlogging.dblogger.info( "processqueueGet() returned: {0}".format(fid_ret) )
            return fid_ret

    def _purgeFileFromDB(self, filename=None):
        """
        removes a file from the DB

        @keyword filename: name of the file to remove (or a list of names)
        @return: True - Success, False - Failure

        >>>  pnl._purgeFileFromDB('Test-one_R0_evinst-L1_20100401_v0.1.1.cdf')

        """
        raise(NotImplemented('This went way and needs to be reimplemented'))

    def getAllFilenames(self):
        """
        return all the filenames in the database
        """
        ans = []
        sq = self.session.query(self.File.filename).all()
        for v in sq:
            ans.append( (v[0], self._getFileFullPath(v[0])) )
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
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
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
        s1.mission_id = self._getMissionID()
        s1.satellite_name = satellite_name
        self.session.add(s1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return self._getSatelliteID(satellite_name)

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
        @keyword extra_params: extra paramerts to pass to the code
        @type extra_params: str
        @keyword super_process_id: th process id of the superprocess for this process
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
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
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
        @param relative_path:relative path for th product
        @type relative_path: str
        @param super_product_id: th product id of the super product for this product
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
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return p1.product_id

    def addproductprocesslink(self,
                    input_product_id,
                    process_id,
                    optional):
        """ add a product process link to the database

        @param input_product_id: id of the produc to link
        @type input_product_id: int
        @param process_id: id of the process to link
        @type process_id: int

        """
        ppl1 = self.Productprocesslink()
        ppl1.input_product_id = input_product_id
        ppl1.process_id = process_id
        ppl1.optional = optional
        self.session.add(ppl1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return ppl1.input_product_id, ppl1.process_id

    def addFilecodelink(self,
                     resulting_file_id,
                     source_code):
        """ add a file code  link to the database

        @param resulting_file_id: id of the produc to link
        @type resulting_file_id: int
        @param source_code: id of the process to link
        @type source_code: int

        """

        try:
            fcl1 = self.Filecodelink()
        except AttributeError:
            raise(DBError("Class Filecodelink not found was it created?"))
        fcl1.resulting_file = resulting_file_id
        fcl1.source_code = source_code
        self.session.add(fcl1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return fcl1.resulting_file, fcl1.source_code

    def addFilefilelink(self,
                     source_file,
                     resulting_file_id):
        """ add a file file  link to the database

        @param source_file: id of the produc to link
        @type souce_file: int
        @param resulting_file_id: id of the process to link
        @type resulting_file_id: int

        """

        try:
            ffl1 = self.Filefilelink()
        except AttributeError:
            raise(DBError("Class Filefilelink not found was it created?"))
        ffl1.source_file = source_file
        ffl1.resulting_file = resulting_file_id
        self.session.add(ffl1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
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

        try:
            ipl1 = self.Instrumentproductlink()
        except AttributeError:
            raise(DBError("Class Instrumentproductlink not found was it created?"))
        ipl1.instrument_id = instrument_id
        ipl1.product_id = product_id
        self.session.add(ipl1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
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
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
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
        @param code_start_date: start of valaitdy of the code (datetime)
        @type code_start_date: datetime
        @param code_stop_date: end of validity of the code (datetime)
        @type code_stop_date: datetime
        @param code_description: description of th code (50 char)
        @type code_description: str
        @param process_id: the id of the process this code is part of
        @type process_id: int
        @param version: the version of the code
        @type version: Version.Version
        @param active_code: boolean True means the code is active
        @type active_code: boolean
        @param date_written: the dat the cod was written
        @type date_written: date
        @param output_interface_version: the interface version of the output (effects the data file names)
        @type output_interface_version: int
        @param newest_version: is this code the newestversion in the DB?
        @type newest_version: bool

        @return: the code_id of the newly inserted code
        @rtype: long

        """
        try:
            c1 = self.Code()
        except:
            raise(DBError("Class Code not found was it created?"))
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
        if arguments is not None:
            c1.arguments = arguments

        self.session.add(c1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
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
        @param description: description of th code (50 char)
        @type description: str
        @param product: the id of the product this inspector finds
        @type product: int
        @param version: the version of the code
        @type version: Version.Version
        @param active_code: boolean True means the code is active
        @type active_code: boolean
        @param date_written: the dat the cod was written
        @type date_written: date
        @param output_interface_version: the interface version of the output (effects the data file names)
        @type output_interface_version: int
        @param newest_version: is this code the newestversion in the DB?
        @type newest_version: bool

        @return: the inspector_id of the newly inserted code
        @rtype: long

        """
        try:
            c1 = self.Inspector()
        except:
            raise(DBError("Class Inspector not found was it created?"))
        c1.filename = filename
        c1.relative_path = relative_path
        c1.description = description
        c1.product = product
        c1.interface_version = version.interface
        c1.quality_version = version.quality
        c1.revision_version = version.revision
        c1.active_code = active_code
        c1.date_written = date_written
        c1.output_interface_version = output_interface_version
        c1.newest_version = newest_version
        c1.arguments = arguments

        self.session.add(c1)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return c1.inspector_id

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


    def _addFile(self,
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
                release_number = None,
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
        @param file_create_date: dat the fie was created
        @type file_create_date: datetime.datetime
        @param exists_on_disk: dpes the file exist on disk?
        @type exists_on_disk: bool
        @param product_id: the product id of he product he file belongs to
        @type product_id: int

        @keyword utc_file_date: The UTC date of the file
        @type utc_file_date: datetime.date
        @keyword utc_start_time: utc start time of the file
        @type utc_start_time: datetime.datetime
        @keyword utc_end_time: itc end time of the file
        @type utc_end_time: datetime.datetime
        @keyword check_date: the date the file was quality checked
        @type check_date: datetime.datetime
        @keyword verbose_provenance: Verbose provenance of the file
        @type verbose_provenance: str
        @keyword quality_comment: comment on quality from quality check
        @type quality_comment: str
        @keyword caveats: caveates associated with the file
        @type caveates: str
        @keyword met_start_time: met start time of the file
        @type met_start_time: long
        @keyword met_stop_time: metstop time of the file
        @type met_stop_time: long

        @ return: file_id of the newly inserted file
        @rtype: long
        """
        try:
            d1 = self.File()
        except AttributeError:
            raise(DBError("Class File not found was it created?"))

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
        d1.release_number = release_number
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
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))
        return d1.file_id

    def _codeIsActive(self, ec_id, date):
        """
        Given a ec_id and a date is that code active for that date

        @param ec_id: executable code id to see if is active
        @param date: date object to use when checking

        @return: True - the code is active for that date, False otherwise

        """
        try: self.Executable_codes
        except AttributeError: self._createTableObjects()

        # can only be one here (sq)
        for sq in self.session.query(self.Executable_codes).filter_by(ec_id = ec_id):
            if sq.active_code == False:
                return False
            if sq.code_start_date > date:
                return False
            if sq.code_stop_date < date:
                return False
        return True

    def _newerCodeVersion(self, ec_id, date=None, bool=False, verbose=False):
        """
        given a executable_code ID decide if there is a newer version
        # TODO think on if this needs to check avtive dates etc

        @param ec_id: the executable code id to check
        @keyword date: (optional) the date to check for the newer version
        @keyword bool: if set return boolean True=there s a newer version
        @return
             - id of the newest version if bool is False
             - True there is a newer version, False otherwise if bool is set

        """
        try: self.Executable_codes
        except AttributeError: self._createTableObjects()

        # if bool then we just wat to know if this is the newest version
        mul = []
        vall = []
        for sq_bf in self.session.query(self.Executable_codes).filter_by(p_id = self._getPID(ec_id)):
            mul.append([sq_bf.filename,
                        sq_bf.ec_id])
            vall.append(self.__get_V_num(sq_bf.interface_version,
                                         sq_bf.quality_version,
                                         sq_bf.revision_version))

            if verbose:
                print("\t\t%s %d %d %d %d" % (sq_bf.filename,
                                              sq_bf.interface_version,
                                              sq_bf.quality_version,
                                              sq_bf.revision_version,
                                              self.__get_V_num(sq_bf.interface_version,
                                                               sq_bf.quality_version,
                                                               sq_bf.revision_version)) )


        if len(mul) == 0:
            if bool: return False
            return None

        self.mul = mul
        self.vall = vall

        ## make sure that there is just one of each v_num in vall
        cnts = [vall.count(val) for val in vall]
        if cnts.count(1) != len(cnts):
            raise(DBProcessingError('More than one code with the same v_num'))

        ## test to see if the newest is the id passed in
        ind = np.argsort(vall)
        if mul[ind[-1]][1] != id:
            if bool: return True
            else: return mul[ind[-1]][1]
        else:
            if bool: return False
            else: return mul[ind[-1]][1]

    def _copyDataFile(self,
                      f_id,
                      interface_version,
                      quality_version,
                      revision_version):
        """
        Given an input file change its versions and insert it to DB

        @param f_id: the file_id to copy
        @param interface_version: new interface version
        @param quality_version: new quality version
        @param revision_version: new revision version
        @return: True - Success, False - Failure

        >>> dbp = DBProcessing()
        >>> dbp._copyDataFile(18,999 , 1 ,1)
        """
        if self.__dbIsOpen == False:
            self._openDB()
        try: self.Base_filename
        except AttributeError: self._createTableObjects()
        try: self.Build_filenames
        except AttributeError: self._createViews()
        # should only do this once
        sq = self.session.query(self.Data_files).filter_by(f_id = f_id)
        DF = self.Data_files()
        DF.utc_file_date = sq[0].utc_file_date
        DF.utc_start_time = sq[0].utc_start_time
        DF.utc_end_time = sq[0].utc_end_time
        DF.data_level = sq[0].data_level
        DF.consistency_check = sq[0].consistency_check
        DF.interface_version = interface_version
        DF.verbose_provenance = None
        DF.quality_check = 0
        DF.quality_comment = None
        DF.caveats = None
        DF.release_number = None
        DF.ds_id = sq[0].ds_id
        DF.quality_version = quality_version
        DF.revision_version = revision_version
        DF.file_create_date = datetime.now()
        DF.dp_id = sq[0].dp_id
        DF.met_start_time = sq[0].met_start_time
        DF.met_stop_time = sq[0].met_stop_time
        DF.exists_on_disk = True
        DF.base_filename = self._getBaseFilename(f_id)
        DF.filename = DF.base_filename + '_v' + \
                      repr(interface_version) + '.' + \
                      repr(quality_version) + '.' + \
                      repr(revision_version) + '.cdf'
               ## if not np.array([DF.interface_version != sq.interface_version,
               ##       DF.quality_version != sq.quality_version,
               ##       DF.revision_version != sq.revision_version]).any()
        self.session.add(DF)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))


    def _copyExecutableCode(self,
                            ec_id,
                            new_filename,
                            interface_version,
                            quality_version,
                            revision_version,
                            active_code = True):
        """
        Given an input executable code change its version ands insert it to DB

        @param ec_id: the file_id to copy
        @param new_filename: the new name of the file
        @param interface_version: new interface version
        @param quality_version: new quality version
        @param revision_version: new revision version
        @param active_code: (optional) is the code active, default True

        @return: True = Success / False = Failure

        """
        if self.__dbIsOpen == False:
            self._openDB()
        try: self.Base_filename
        except AttributeError: self._createTableObjects()
        try: self.Build_filenames
        except AttributeError: self._createViews()
        # should only do this once
        sq = self.session.query(self.Executable_codes).filter_by(ec_id = ec_id)
        EC = self.Executable_codes()
        EC.relative_path = sq[0].relative_path
        EC.code_start_date = sq[0].code_start_date
        EC.code_stop_date = sq[0].code_stop_date
        EC.code_id = sq[0].code_id
        EC.p_id = sq[0].p_id
        EC.ds_id = sq[0].ds_id
        EC.interface_version = interface_version
        EC.quality_version = quality_version
        EC.revision_version = revision_version
        EC.active_code = active_code
        basefn = sq[0].filename.split('_v')[0]
        EC.filename = new_filename

        self.session.add(EC)
        try:
            self.session.commit()
        except IntegrityError as IE:
            self.session.rollback()
            raise(DBError(IE))

    def _getFileFullPath(self, filename):
        """
        return the full path to a file given the name or id
        (name or id is based on type)

        """
        if isinstance(filename, (int, long)):
            filename = self._getFilename(filename)
        # need to know file product and mission to get whole path
        try:
            product_id = self.session.query(self.File.product_id).filter_by(filename = filename)[0][0]
            rel_path = self.session.query(self.Product.relative_path).filter_by(product_id = product_id)[0][0]
            root_dir = self.session.query(self.Product, self.Mission.rootdir).filter(self.Product.product_id == product_id).join((self.Instrument, self.Product.instrument_id == self.Instrument.instrument_id)).join(self.Satellite).join(self.Mission)[0][1]
        except IndexError:
            return None
        return os.path.join(root_dir, rel_path, filename)

    def getProcessFromInputProduct(self, product):
        """
        given an product name or id return all the processes that use that as an input
        """
        try:
            product = int(product)
        except ValueError: # it was a string
            p_id = self._getProductID(product)
        else:
            p_id = product
        sq = self.session.query(self.Productprocesslink).filter_by(input_product_id = p_id).all()
        ans = []
        for v in sq:
            ans.append(v.process_id)
        return ans

    def getProcessTimebase(self, process):
        """
        given a product id or product name return the timebase
        """
        try:
            product = int(process)
        except ValueError: # it was a string
            p_id = self.getProcessID(process)
        else:
            p_id = product
        sq = self.session.query(self.Process.output_timebase).filter_by(process_id = p_id).all()
        return sq[0][0]

    def getProcessID(self, proc_name):
        """
        given a process name return its id
        """
        sq = self.session.query(self.Process.process_id).filter_by(process_name = proc_name).all()
        return sq[0]

    def getFileProduct(self, filename):
        """
        given a filename or file_id return the product id it belongs to
        """
        try:
            f_id = int(filename)  # if a number
        except ValueError:
            f_id = self._getFileID(filename) # is a name
        try:
            product_id = self.session.query(self.File.product_id).filter_by(file_id = f_id)[0][0]
            return product_id
        except IndexError:
            return None

    def getFileVersion(self, filename):
        """
        given a filename or fileid return a Version instance
        """
        try:
            f_id = int(filename)  # if a number
        except ValueError:
            f_id = self._getFileID(filename) # is a name
        sq = self.session.query(self.File).filter_by(file_id = f_id)[0]
        return Version.Version(sq.interface_version, sq.quality_version, sq.revision_version)

    def getFileMission(self, filename):
        """
        given an a file name or a file ID return the mission(s) that file is
        associated with
        """
        product_id = self.getFileProduct(filename)
        # get all the instruments
        inst_id = self.getInstrumentFromProduct(product_id)
        # get all the satellites
        sat_id = self.getInstrumentSatellite(inst_id)
        # get the missions
        mission_id = self.getSatelliteMission(sat_id)
        return mission_id

    def getSatelliteMission(self, sat_name):
        """
        given a satellite or satellit id return the mission
        """
        if not isinstance(sat_name, (tuple, list)):
            sat_name = [sat_name]
        i_id = []
        for val in sat_name:
            try:
                i_id.append(int(val))  # if a number
            except ValueError:
                i_id.append(self._getSatelliteID(val)) # is a name

        m_id = []
        for v in i_id:
            sq = self.session.query(self.Satellite.mission_id).filter_by(satellite_id = v).all()
            tmp = [v[0] for v in sq]
            m_id.extend(tmp)
        return m_id

    def getInstrumentSatellite(self, instrument_name):
        """
        given an instrument name or ID return the satellite it is on
        """
        if not isinstance(instrument_name, (tuple, list)):
            instrument_name = [instrument_name]
        i_id = []
        for val in instrument_name:
            try:
                i_id.append(int(val))  # if a number
            except ValueError:
                i_id.append(self._getInstruemntID(val)) # is a name

        sat_id = []
        for v in i_id:
            sq = self.session.query(self.Instrument.satellite_id).filter_by(instrument_id = v).all()
            tmp = [v[0] for v in sq]
            sat_id.extend(tmp)
        return sat_id

    def getInstrumentFromProduct(self, product_id):
        """
        given a product ID get the instrument(s) id associated with it
        """
        sq = self.session.query(self.Instrumentproductlink.instrument_id).filter_by(product_id = product_id).all()
        inst_id = [v[0] for v in sq]
        return inst_id

    def getProductLevel(self, productID):
        """
        given a product ID return the level
        """
        sq = self.session.query(self.Product).filter_by(product_id = productID)
        return sq[0].level

    def _getMissionID(self):
        """
        Return the current mission ID

        @return: mission_id - the current mission ID

        >>> dbp = DBProcessing()
        >>> dbp._getMissionID()
        19
        """
        sq = self.session.query(self.Mission.mission_id).filter_by(mission_name = self.mission)
        return sq[0][0]

    def _getMissionName(self, id=None):
        """
        Return the current mission ID

        @return: mission_id - the current mission ID

        >>> dbp = DBProcessing()
        >>> dbp._getMissionID()
        19
        """
        if id is None:
            sq = self.session.query(self.Mission).filter_by(mission_name = self.mission)
            return sq[0].mission_name
        else:
            if not isinstance(id, (tuple, list)):
                id = [id]
            i_out = []
            for i in id:
                sq = self.session.query(self.Mission.mission_name).filter_by(mission_id = i)
                tmp = [v[0] for v in sq]
                i_out.extend(tmp)
            return i_out

    def _getInstruemntID(self, name):
        """
        Return the instrument_id for a givem instrument

        @return: instrument_id - the instrument ID

        """
        sq = self.session.query(self.Instrument).filter_by(instrument_name = name)
        return sq[0].instrument_id

    def _getMissions(self):
        sq = self.session.query(self.Mission.mission_name)
        return [val[0] for val in sq.all()]

    def _getFileID(self, filename):
        """
        Return the fileID for the input filename

        @param filename: filename to return the fileid of
        @type filename: str

        @return: file_id: file_id of the input file
        @rtype: long
        """
        sq = self.session.query(self.File).filter_by(filename = filename)
        try:
            return sq[0].file_id
        except IndexError: # no file_id found
            raise(DBError("No filename %s found in the DB" % (filename)))

    def _getCodeID(self, codename):
        """
        Return the codeID for the input code

        @param codename: filename to return the fileid of
        @type filename: str

        @return: code_id: code_id of the input file
        @rtype: long
        """
        sq = self.session.query(self.Code).filter_by(filename = codename)
        try:
            return sq[0].code_id
        except IndexError: # no file_id found
            raise(DBError("No filename %s found in the DB" % (filename)))

    def _getFilename(self, file_id):
        """
        Return the filename for the input file_id

        @param file_id: file_id to return the name from
        @type filename: long

        @return: filename: filename associated with the file_id
        @rtype: str
        """
        sq = self.session.query(self.File).filter_by(file_id = file_id)
        return sq[0].filename

    def getFileProcess_keywords(self, file_id):
        """
        given a file_id return the process keywords string
        """
        sq = self.session.query(self.File).filter_by(file_id = file_id)
        return sq[0].process_keywords        

    def getFileUTCfileDate(self, file_id):
        """
        Return the utc_file_date for the input file_id

        @param file_id: file_id to return the date  from
        @type filename: long

        @return: utc_file_date: date of the file  associated with the file_id
        @rtype: datetime
        """
        sq = self.session.query(self.File).filter_by(file_id = file_id)
        return sq[0].utc_file_date

    def getFiles_product_utc_file_date(self, product_id, date):
        """
        given a product id and a utc_file_date return all the files that match [(file_id, Version, product_id, product_id, utc_file_date), ]
        """
        if isinstance(date, datetime.datetime):
            date = date.date()
        sq = self.session.query(self.File).filter_by(product_id = product_id).filter_by(utc_file_date = date)
        sq = [(v.file_id, Version.Version(v.interface_version, v.quality_version, v.revision_version), self.getFileProduct(v.file_id), self.getFileUTCfileDate(v.file_id) ) for v in sq]
        return sq

    def getFiles_product_utc_file_daterange(self, product_id, daterange):
        """
        given a product id and a utc_file_date return all the files that have data in the range [(file_id, Version, product_id, utc_file_date), ]
        """
        sq11 = self.session.query(self.File).filter_by(product_id = product_id).filter(self.File.utc_stop_time >= daterange[0]).filter(self.File.utc_start_time <= daterange[1]).all()
        vers = [(v.file_id, Version.Version(v.interface_version, v.quality_version, v.revision_version), self.getFileProduct(v.file_id), self.getFileUTCfileDate(v.file_id) ) for v in sq11]
        # need to drop all the same files with lower versions
        ans = self.file_id_Clean(vers)
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
#                print '@@@@',  k1, k2, data2[k1], data2[k2]
                tmp = data2[k1]
                tmp = data2[k2]
            except KeyError:
#                print '\n'
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

        @param process_id: process_)id to return the inout_product_id for
        @type process_id: long

        @return: list of input_product_ids
        @rtype: list
        """
        sq = self.session.query(self.Productprocesslink).filter_by(process_id = process_id)
        return [(val.input_product_id, val.optional) for val in sq.all()]  # the zero is because all() returns a list of one element tuples

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

#        sq1 = self.session.query(self.File).filter_by(product_id = product_id).filter_by(exists_on_disk = True).filter_by(utc_start_time <= daterange[0]).filter_by(newest_version = True).all()


    def _getProductFormats(self, productID=None):
        """
        Return the product formats for all the formats

        @return: list of all the product format strings and ids from the database
        """
        if productID == None:
            sq = self.session.query(self.Product.format, self.Product.product_id)
            return sq.order_by(asc(self.Product.product_id)).all()
        else:
            sq = self.session.query(self.Product.format).filter_by(product_id = productID)
            return sq[0][0]

    def _getProductNames(self, productID=None):
        """
        Return the mission, Satellite, Instrument,  product, product_id   names as a tuple

        @return: list of tuples of the mission, Satellite, Instrument,  product, product id  names
        """
        if productID == None:
            sq = self.session.query(self.Mission.mission_name,
                                                    self.Satellite.satellite_name,
                                                    self.Instrument.instrument_name,
                                                    self.Product.product_name,
                                                    self.Product.product_id).join(self.Satellite).join(self.Instrument).join(self.Instrumentproductlink).join(self.Product)
            return sq.order_by(asc(self.Product.product_id)).all()
        else:
            sq = self.session.query(self.Mission.mission_name,
                                        self.Satellite.satellite_name,
                                        self.Instrument.instrument_name,
                                        self.Product.product_name,
                                        self.Product.product_id).join(self.Satellite).join(self.Instrument).join(self.Instrumentproductlink).join(self.Product).filter(self.Product.product_id == productID)
            return sq[0]


    def getActiveInspectors(self):
        """
        query the db and return a list of all the active inspector filenames [(filename, arguments, product), ...]
        """
        sq = self.session.query(self.Inspector).filter(self.Inspector.active_code == True).all()
        basedir = self._getMissionDirectory()
        retval = [(os.path.join(basedir, ans.relative_path, ans.filename), ans.arguments, ans.product) for ans in sq]
        return retval

    def getChildrenProducts(self, file_id):
        """
        given a file ID return all the processes that use this as input
        """
        DBlogging.dblogger.debug( "Entered findChildrenProducts():  file_id: {0}".format(file_id) )
        product_id = self.getFileProduct(file_id)

        # get all the process ids that have this product as an input
        proc_ids = self.getProcessFromInputProduct(product_id)
        return proc_ids

    def _getProductID(self,
                     product_name):
        """
        Return the product ID for an input product name

        @param product_name: the name of the product to et the id of
        @type product_name: str

        @return: product_id -the product  ID for the input product name
        """
        sq = self.session.query(self.Product.product_id).filter_by(product_name = product_name)
        if sq.count() == 0:
            raise(DBError('Product %s was not found' % (product_name)))
        if sq.count() == 1:
            return sq.first()[0]
        elif sq.count() > 1:
            return sq

    def _getSatelliteID(self,
                        sat_name):
        """
        @param sat_name: the satellie name to look up the id
        @type sat_name: str

        @return: satellite_id - the requested satellite  ID
        """
        sq = self.session.query(self.Satellite.satellite_id).filter_by(mission_id = self._getMissionID()).filter_by(satellite_name = sat_name)
        if sq.count() == 0:
            raise(DBError('Satellite %s was not found' % (sat_name)))
        return sq.first()[0]  # there can be only one of each name

    def getCodePath(self, code_id):
        """
        Given a code_id list return the full name (path and all) of the code
        """
        DBlogging.dblogger.debug("Entered getCodePath:")
        sq1 =  self.session.query(self.Code.relative_path).filter_by(code_id = code_id)  # should only have one value
        sq2 =  self._getMissionDirectory()
        sq3 =  self.session.query(self.Code.filename).filter_by(code_id = code_id)  # should only have one value
        return os.path.join(sq2, sq1[0][0], sq3[0][0])  # the [0][0] is ok (ish) since there can only be one

    def getOutputProductFromProcess(self, process):
        """
        given an process id return the output product
        """
        sq2 = self.session.query(self.Process.output_product).filter_by(process_id = process)
        # there can only be one
        return sq2[0][0]

    def getProcessFromOutputProduct(self, outProd):
        """
        Gets process from the db that have the output product
        """
        # TODO maybe this should move to DBUtils2
        DBlogging.dblogger.debug("Entered getProcessFromOutputProduct:")
        sq1 =  self.session.query(self.Process.process_id).filter_by(output_product = outProd).all()  # should only have one value
        return sq1[0][0]

    def getCodeFromProcess(self, proc_id):
        """
        given a process id return the code that makes perfoms that process
        """
        DBlogging.dblogger.debug("Entered getCodeFromProcess:")
        sq1 =  self.session.query(self.Code.code_id).filter_by(process_id = proc_id)  # should only have one value
        return sq1[0][0]

    def getCodeArgs(self, code_id):
        """
        Given a code_id list return the arguments to the code
        """
        DBlogging.dblogger.debug("Entered getCodeArgs:")
        sq1 =  self.session.query(self.Code.arguments).filter_by(code_id = code_id)  # should only have one value
        return sq1[0][0]  # the [0][0] is ok (ish) since there can only be one

    def _getMissionDirectory(self):
        """
        return the base direcorty for the current mission

        @return: base directory for thcurrent mission
        @rtype: str
        """
        sq = self.session.query(self.Mission.rootdir).filter_by(mission_name  = self.mission)
        if sq.count() == 0:
            raise(DBError('Mission %s was not found' % (self.mission)))
        return sq.first()[0]  # there can be only one of each name

    def _checkIncoming(self):
        """
        check the incoming directory for the current mision and add those files to the geting list

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
        try:
            self.Mission
        except AttributeError:
            self._createTableObjects()

        basedir = self._getMissionDirectory()
        path = os.path.join(basedir, 'incoming/')
        return path

    def getErrorPath(self):
        """
        return the erro path for the current mission
        """
        try:
            self.Mission
        except AttributeError:
            self._createTableObjects()

        basedir = self._getMissionDirectory()
        path = os.path.join(basedir, 'errors/')
        return path

    def _parseFileName(self,
                       filename):
        """
        Parse a filename from incoming (or anywhere) and return a file object populated with the infomation

        @param filename: list of filenames to parse
        @type filename: list
        @return: file object populated from the filename
        @rtype: DBUtils2.File
        """
        output = []
        for val in filename:
            # this is an if elif block testing by mission
            if not self.__isTestFile(val):
                raise(FilenameParse("filename %s not found to belong to mission %s" %   ( val, self.mission)))
            f1 = self.File()
            f1.filename = val
            file_date
            f1.utc_file_date = None





