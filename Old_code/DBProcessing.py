from DBUtils import DBUtils
from datetime import datetime

__package__  = None


class DBProcessing(DBUtils):
    """
    Class that contains all the processing wrappers

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov
    
    @version: V1: 17-Jul-2010 (BAL)
    """
    def __init__(self, mission='Test'):
        """
        Initialize the CheckExistsOnDisk class,
        default mission is 'Test'

        @keyword mission: the mission to process

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 17-Jul-2010 (BAL)
        """
        DBUtils.__init__(self, mission=mission)

    def clearProcessingFlag(self, comment=None, verbose=False):
        """
        Clears a stuck processing flag, must enter a comment
        
        @keyword comment: Must input a comment string explaining the override
        @return: True = Success / False = Failure

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 17-Jul-2010 (BAL)
        """ 
        self._openDB(verbose=verbose)
        self._createTableObjects()
        self._createViews()
        if not self._currentlyProcessing():
            print("DB is not currently processing")
            return True
        else:
            if not self._resetProcessingFlag(comment):
                self._closeDB(verbose=verbose)
                raise(Exception("Must enter a comment, lock not released"))
        self._closeDB()
        print("Processing Lock Removed")

    def checkExistsOnDisk(self, fix=False, verbose=False):
        """
        Checks to see if the ExistsOnDisk column in the DB is up to date with the
        filesystem (optionally can bring the DB into the same state)
        
        @keyword fix: (optional) DB will remove or add files to match filesystem
        @keyword verbose: (optional) more will be printed to the screen
        @return: True = Success / False = Failure

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 17-Jun-2010 (BAL)
        @version: V2: 25-Aug-2010 (BAL) - added logging
       
        """ 
        self._openDB(verbose=verbose)
        self._createTableObjects()
        self._createViews()
        if not self._startLogging(verbose = verbose):
            raise(Exception("_startLogging"))
        cnt = self._gatherFiles(verbose=verbose)
        print("\t%d files gathered" % (cnt))
        cnt = self._checkDiskForFile(fix=fix)
        if fix:
            print("\t\tFixed %d files that were out of sync with DB" % (cnt))
        else:
            print("\t\tFound %d files that were out of sync with DB" % (cnt))
        if not self._stopLogging('checkExistsOnDisk:Nominal', verbose = verbose):
            raise(Exception("_stopLogging"))
        self._closeDB(verbose=verbose)

    def processNextLevel(self, verbose = False):
        """
        Go through the selected misison and do all the Lx+1 processing that can be done
        
        @keyword verbose: (optional) more will be printed to the screen
        @return: True = Success / False = Failure
                 
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 17-Jun-2010 (BAL)

        >>>  pnl.processNextLevel()
        """ 
        self._openDB(verbose = verbose)
        self._createTableObjects(verbose = verbose)
        self._createViews(verbose = verbose)
        if not self._startLogging(verbose = verbose):
            raise(Exception("_startLogging"))
        cnt = self._gatherFiles(verbose = verbose)
        print("\t%d files gathered" % (cnt))
        
        if not self._backupBF(verbose = verbose):
            raise(Exception("_backupBF"))
        if not self._initDelNames():
            raise(Exception("_initDelNames"))
        cnt = self._Lxp1exists(verbose = verbose)
        print("\t\tRemoving %d files since their Lx+1 exisits" % (cnt))
        cnt = self._procCodeDates(verbose = verbose)
        print("\t\tRemoving %d files that dont have a processing code in the right dates" % (cnt))
        cnt = self._activeProcCode(verbose = verbose)
        print("\t\tRemoving %d files that dont have an active processing code" % (cnt))
        cnt = self._existsOnDisk(verbose = verbose)
        print("\t\tRemoving %d files that dont exist on disk" % (cnt))
        cnt = self._delNewerVersion(verbose = verbose)
        print("\t\tRemoving %d files that have a new version" % (cnt))
        if not self._delNames(verbose = True):
            raise(Exception("_delNames"))
        if not self._buildOutName(verbose = verbose):
            raise(Exception("_buildOutName"))
        if not self._doNextLevelProcessing(verbose = verbose):
            raise(Exception("_doNextLevelProcessing"))
        if not self._stopLogging('ProcessNextLevel:Nominal', verbose = verbose):
            raise(Exception("_stopLogging"))
        self._closeDB(verbose = verbose)

    def purgeFileFromDB(self, filename, comment, verbose=False):
        """
        purge a file form the DB. This is **NOT** guaranteed to be safe. 
        
        @param filename: name of the file to remove (or a list of names)
        @param comment: string comment on why the files are nbeing purged

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 18-Jun-2010 (BAL)
             
        >>>  pnl._purgeFileFromDB('Test-one_R0_evinst-L1_20100401_v0.1.1.cdf', 'Debugging purge')

        """
        self._openDB(verbose=verbose)
        self._createTableObjects()
        self._createViews()
        if not self._startLogging():
            raise(Exception("_startLogging"))
        if not self._purgeFileFromDB(filename):
            self._closeDB(verbose=verbose)
            raise(Exception("_purgeFileFromDB"))
        self._closeDB(verbose=verbose)
        if not self._stopLogging('purgeFileFromDB:' + comment):
            raise(Exception("_stopLogging"))
        return True


    def reprocessFiles(self, verbose=False):
        """
        step through the database and see which files need to be reprocessed (this is what
        reprocesses files that have new versions of dependencies)

        
        @keyword verbose: (optional) more will be printed to the screen
        @return: True = Success / False = Failure

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 18-Jun-2010 (BAL)
        """
        self._openDB(verbose=verbose)
        self._createTableObjects(verbose=verbose)
        self._createViews(verbose=verbose)
        if not self._startLogging(verbose=verbose):
            raise(Exception("_startLogging"))
        cnt = self._gatherFiles(verbose=verbose)
        print("\t%d files gathered" % (cnt))
        if not self._backupBF(verbose=verbose):
            raise(Exception("_backupBF"))
        if not self._initDelNames(verbose=verbose):
            raise(Exception("_initDelNames"))
        cnt = self._noDependencies(verbose=verbose)
        print('\t\t%d files have no dependencies and cant be checked (probably L0 files)' % (cnt))
        cnt = self._existsOnDisk(verbose=verbose)
        print("\t\tRemoving %d files that dont exist on disk" % (cnt))
        ## get rid of the files we know we arent using and reset self.del_names
        if not self._delNames(verbose = True):
            raise(Exception("_delNames"))
        if not self._initDelNames(verbose=verbose):
            raise(Exception("_initDelNames"))
        if not self._collectDependencies(verbose=verbose):
            raise(Exception("_collectDependencies"))
        cnt = self._dependenciesNewest(verbose=verbose)
        print("\t\tRemoving %d files that have newest dependencies" % (cnt))
        ## go in and check all the dependencies for a file to see if they have newer versions
        if not self._delNames(verbose=True):
            raise(Exception("_delNames"))
        
        if not self._buildOutNameReprocess(verbose=verbose, incRevision=True):
            raise(Exception("_buildOutName"))
        if not self._doNextLevelProcessing(verbose = verbose):
            raise(Exception("_doNextLevelProcessing"))


        if not self._stopLogging('reprocessFiles:Nominal', verbose=verbose):
            raise(Exception("_stopLogging"))
        self._closeDB(verbose=verbose)
        return True


    def addFileToDB(self, \
                    filename = None, \
                    utc_file_date = None, \
                    utc_start_time = None, \
                    utc_end_time = None, \
                    data_level = 0, \
                    consistency_check = None, \
                    interface_version = 0, \
                    verbose_provenance = None, \
                    quality_check = 0, \
                    quality_comment = None, \
                    caveats = None, \
                    release_number = None, \
                    ds_id = None, \
                    quality_version = 0, \
                    revision_version = 0, \
                    file_create_date = datetime.now(), \
                    dp_id = None, \
                    met_start_time = None, \
                    met_stop_time = None, \
                    exists_on_disk = True, \
                    base_filename = None, \
                    comment = None, \
                    verbose = False):
        """
        add a file to the DB. 
        
        These are the inputs to add a new file to the DB, not all are required to be
        different that the defualts

        @param filename: None
        @param utc_file_date: None
        @param utc_start_time: None
        @param utc_end_time: None
        @param data_level: 0
        @param consistency_check: None
        @param interface_version: 0
        @param verbose_provenance: None
        @param quality_check: 0
        @param quality_comment: None
        @param caveats: None
        @param release_number: None
        @param ds_id: None
        @param quality_version: 0
        @param revision_version: 0
        @param file_create_date: datetime.now()
        @param dp_id: None
        @param met_start_time: None
        @param met_stop_time: None
        @param exists_on_disk: True
        @param base_filename: None
        @param comment: None
        @param verbose: False
        @return: True = Success / False = Failure

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 1-Jul-2010 (BAL)
        
        >>> TODO add an example

        """
        if comment == None:
            raise(Exception("Must enter a comment (like filename)"))
        self._openDB(verbose = verbose)
        self._createTableObjects(verbose = verbose)
        if not self._startLogging(verbose = verbose):
            raise(Exception("_startLogging"))
        if not self._addDataFile(filename,  
                                 utc_file_date,
                                 utc_start_time,
                                 utc_end_time,
                                 data_level,
                                 consistency_check,
                                 interface_version,
                                 verbose_provenance,
                                 quality_check,
                                 quality_comment,
                                 caveats,
                                 release_number,
                                 ds_id,
                                 quality_version,
                                 revision_version,
                                 file_create_date,
                                 dp_id,
                                 met_start_time,
                                 met_stop_time,
                                 exists_on_disk,
                                 base_filename):
            self._closeDB(verbose = verbose)
            raise(Exception("_addDataFile"))
        if not self._stopLogging('addFileToDB:' + comment, verbose = verbose):
            raise(Exception("_stopLogging"))
        self._closeDB(verbose = verbose)

        return True


    def allDependenciesExist(self,
                             verbose = False):
        """
        step through all the files for the mission and make sure that the dependency tables
        are up to date, meaniung they all exist in the DB

        @keyword verbose: (optional) print to screen the results as they are gathered
        @return: a list of data file ids that have broken dependencies

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 6-Jul-2010 (BAL)
        
        >>> dbp = DBProcessing()
        >>> dbp.allDependenciesExist()
        About to process 273 files from Test to the next data level
        Mission Test has 5 data products to step through
          len(bf)  273 files elegible to process
        Test-one_H0_evinst-L2_20100208_v0.1.1.cdf depens ok?:	True
        Test-one_R0_evinst-L1_20100123_v0.1.1.cdf depens ok?:	True
        Test-one_R0_evinst-L0_20100208_v0.0.0.cdf depens ok?:	True
        Test-one_R0_evinst-L0_20100104_v0.0.0.cdf depens ok?:	True
        Test-one_R0_evinst-L0_20100316_v0.0.0.cdf depens ok?:	True
        Test-one_R0_evinst-L1_20100306_v0.1.1.cdf depens ok?:	True
        """
        self._openDB(verbose=verbose)
        self._createTableObjects()
        self._createViews()
        if not self._gatherFiles():
            self._closeDB(verbose=verbose)
            raise(Exception("_gatherFiles"))
        broken = []
        for fname in self.bf:
            if not self._fileDependenciesExist(self.bf[fname]['f_id']):
                broken.append(self.bf[fname]['f_id'])
            if verbose:
                print('\t' + fname + ' depens ok?:\t'),
                print(self._fileDependenciesExist(self.bf[fname]['f_id']))
        
        self._closeDB(verbose=verbose)
        return broken





