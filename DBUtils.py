import sqlalchemy
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Boolean, Date, Float, Table
from sqlalchemy import desc, and_, select
import os.path
from datetime import datetime
import subprocess
import numpy as np
import copy
from sqlalchemy.exceptions import IntegrityError



## This goes in the processing comment field in the DB, do update it
__version__ = '1.1.2'
__package__  = None


class DBError(Exception):
    pass
class ProcessingError(Exception):
    pass




class DBUtils(object):
    """
    @summary: DBUtils - utility routines for the DBProcessing class, all of these may be user called but are meant to
    be internal routines for DBProcessing
    """
    
    def __init__(self, mission='Test'):
        """
        @summary: Initialize the DBUtils class, default mission is 'Test'
        """
        self.mission = mission
        bf = {}
        if not self._initDelNames():
            raise(Exception("_initDelNames"))
        self.bf = bf
        self.__dbIsOpen = False

    def __repr__(self):
        """
        @summary: Print out something usefule when one prints the class instance
        
        @return: DBProcessing class instance for mission <mission name>
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 7-Jul-2010 (BAL)
        """  
        return 'DBProcessing class instance for mission ' + self.mission + ', version: ' + __version__ 


   
## quick subroutine to build filenames up of the the required elements, called when processing Lx to Lx+1
## and example is  Test-one_R0_evinst-L0_20100118_v0.0.0.cdf
    def __build_fname(self,
                      rootdir = '', 
                      relative_path = '', 
                      mission_name = '',
                      satellite_name = '',
                      product_name = '', 
                      date = '', 
                      release = '', 
                      quality = '', 
                      revision = ''):
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

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 14-Jun-2010 (BAL)
     
        >>> nl._ProcessNext__build_fname('/root/file/', 'relative/', 'Test', 'test1', 'Prod1', '20100614', 1, 1, 1)
            Out[9]: '/root/file/relative/Test-test1_Prod1_20100614_v1.1.1.cdf'

        """  
        dir = rootdir + relative_path
        fname = mission_name + '-' + satellite_name + '_' + product_name
        ver = 'v' + str(release) + '.' + str(quality) + '.' + str(revision)
        ext = '.cdf'
        fname = fname + '_' + date + '_' + ver + ext 
        return dir + fname

    def __get_V_num(self, interface = None, 
                    quality = None,
                    revision = None):
        """
        formula to use to determine is a file is of a newer version, weightings can be changed here

        @keyword interface:
        @keyword quality:
        @keyword revision:
        @return: The version 'score' of the input versions
             
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 14-Jun-2010 (BAL)

        >>>  pnl._ProcessNextLevel__get_V_num(1,1, 1)
        Out[10]: 1101
        """
        if (interface == None) or (quality == None) or (revision == None):
            raise(ProcessingError('Error in input'))
        tmp = interface *1000  + quality * 100 + revision
        return tmp


####################################
###### DB and Tables ###############
####################################

    def _openDB(self, verbose=False):
        """
        setup python to talk to the database, this is where it is, name and password.

        @keyword verbose: (optional) - print information out to the command line
                     
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 14-Jun-2010 (BAL)
        @version: V2: 7-Jul-2010 (BAL) added verbose and printing
        @version: V3: 25-Aug-2010 (BAL) added DBerror instead of False return

        @todo: change the user form owner to ops as DB permnissons are fixed

        >>>  pnl._openDB()
        """ 
        if self.__dbIsOpen == True:
            return 
        try:
            if self.mission == 'Test':
                engine = sqlalchemy.create_engine('postgresql+psycopg2://rbsp_owner:rbsp_owner@edgar:5432/rbsp', echo=False)
                # this holds the metadata, tables names, attributes, etc
            metadata = sqlalchemy.MetaData(bind=engine)
            # a session is what you use to actually talk to the DB, set one up with the current engine
            Session = sessionmaker(bind=engine)
            session = Session()
            self.engine = engine
            self.metadata = metadata
            self.session = session
            self.__dbIsOpen = True
            if verbose: print("DB is open: %s" % (engine))
            return 
        except:
            raise(DBError('Error opening database'))
        
        
    def _createTableObjects(self, verbose = False):
        """
        cycle through the database and build classes for each of the tables 

        @keyword verbose: (optional) - print information out to the command line
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 14-Jun-2010 (BAL)
        @version: V2: 25-Aug-2010 (BAL) - chnaged to throuw exception not return False

        >>>  pnl._createTableObjects()

        """ 

        # make siure the db is open
        if self.__dbIsOpen == False:
            self._openDB()
        
        
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
        try: self.Missions   #  if they are already defined dont do it again
        except AttributeError:
            try:
                for val in table_dict:
                    c1 = compile("""class %s(object):\n\tpass""" % (val), '', 'exec')
                    c2 = compile("%s = Table('%s', self.metadata, autoload=True)" % (str(table_dict[val]), table_dict[val]) , '', 'exec')
                    c3 = compile("mapper(%s, %s)" % (val, str(table_dict[val])), '', 'exec')
                    c4 = compile("self.%s = %s" % (val, val), '', 'exec' )
                    exec(c1)
                    exec(c2)
                    exec(c3)
                    exec(c4)
                    if verbose: print("Class %s created" % (val))
            except:
                raise(DBError('Error is setting up table->class mapping'))

                
###################################
### Views #########################
###################################


    def _createViews(self, verbose = False):
        """
        cycle through the database and build classes for each of the Views 
        views cannot be dynamically allocated since you have to manually define the promary keys
        also views cannot be grabbed form the engine the same way that tables are.  They are typed so
        I wont change this to some standard.
        --- When creating a new verw make really usre it has something unique that can be used as
        --- a primary key. some below use f_id others use pk whisch is a call to a sequence

        @keyword verbose: (optional) - print information out to the command line

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 14-Jun-2010 (BAL)
        @version: V2: 25-Aug-2010 (BAL) - chnaged to throuw exception not return False

        >>>  pnl._createViews()
        """
        try: self.Build_filenames  # if they already are defined dont do it again
        except AttributeError:
            try:
                class Build_filenames(object):
                    pass
                build_filenames = Table('build_filenames', self.metadata,
                                        Column('pk', BigInteger, primary_key=True),
                                        autoload=True)
                mapper(Build_filenames, build_filenames)
                self.Build_filenames = Build_filenames
                if verbose: print("Class %s created" % ('Build_filenames'))

                
                class Build_output_filenames(object):
                    pass
                build_output_filenames = Table('build_output_filenames', self.metadata,
                                               Column('pk', BigInteger, primary_key=True),
                                               autoload=True)
                mapper(Build_output_filenames, build_output_filenames)
                self.Build_output_filenames = Build_output_filenames
                if verbose: print("Class %s created" % ('Build_output_filenames'))

                class Files_by_mission(object):
                    pass
                files_by_mission = Table('files_by_mission', self.metadata,
                                         Column('f_id', BigInteger,  primary_key=True),
                                         autoload = True)
                mapper(Files_by_mission,files_by_mission)
                self.Files_by_mission = Files_by_mission
                if verbose: print("Class %s created" % ('Files_by_mission'))
                                

                class Data_product_by_mission(object):
                    pass
                data_product_by_mission = Table('data_product_by_mission', self.metadata,
                                                Column('dp_id', BigInteger, primary_key=True),
                                                autoload=True)
                mapper(Data_product_by_mission,data_product_by_mission)
                self.Data_product_by_mission = Data_product_by_mission
                if verbose: print("Class %s created" % ('Data_product_by_mission'))

                
                class Processing_paths(object):
                    pass
                processing_paths = Table('processing_paths', self.metadata,
                                         Column('pk', BigInteger, primary_key=True),
                                         autoload=True)
                mapper(Processing_paths, processing_paths)
                self.Processing_paths = Processing_paths
                if verbose: print("Class %s created" % ('Processing_paths'))

                class Build_reprocess_filenames(object):
                    pass
                build_reprocess_filenames = Table('build_reprocess_filenames', self.metadata,
                                               Column('pk', BigInteger, primary_key=True),
                                               autoload=True)
                mapper(Build_reprocess_filenames, build_reprocess_filenames)
                self.Build_reprocess_filenames = Build_reprocess_filenames
                if verbose: print("Class %s created" % ('Build_reprocess_filenames'))

            except:
                raise(DBError('error creating view<->class mapping'))

######################################
### Gather files from DB #############
######################################

    def _gatherFiles(self, verbose=False):
        """
        gather all the files for a given mission into L{bf}

        @keyword verbose: (optional) print more out to the command line

        @return: Number of files gathered
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 14-Jun-2010 (BAL)

        >>>  pnl._gatherFiles()
        123
        """
        try: self.Data_file
        except AttributeError: self._createTableObjects()
        try: self.Files_by_mission
        except AttributeError: self._createViews()
         
        files = self.session.query(self.Files_by_mission.f_id, self.Files_by_mission.filename).filter_by(mission_name=self.mission).all()
        files = zip(*files)
        if verbose:
            print("About to process %d files from %s to the next data level" % (len(files[0]), self.mission))

        # get the number of products for that mission
        products = self.session.query(self.Data_product_by_mission.dp_id,
                                      self.Data_product_by_mission.product_name).filter_by(mission_name=self.mission).all()
        products = zip(*products)
        if verbose:
            print("\tMission %s has %d data products to step through" % (self.mission, len(products[0])))

        # create a list of everyting keys by filename that is an input to a process
        # this dictionary is the key to everyhting, just keep it up to date and then you can add/delete form it
        # to decide what needs to be processed
        for val in self.session.query(self.Build_filenames).filter_by(type=0).filter(self.Build_filenames.f_id != None):
            self.bf[val.filename] = {'mission_name':val.mission_name,
                                'missions_rootdir':val.missions_rootdir,
                                'p_id':val.p_id,
                                'dp_id':val.dp_id,
                                'type':val.type,
                                'relative_path':val.relative_path,
                                'product_name':val.product_name,
                                'satellite_name':val.satellite_name,
                                'f_id':val.f_id,
                                'utc_file_date':val.utc_file_date,
                                'absolute_name':val.absolute_name,
                                'interface_version':val.interface_version,
                                'quality_version':val.quality_version,
                                'revision_version':val.revision_version,
                                'ec_interface_version':val.ec_interface_version,
                                'base_filename':val.base_filename,
                                'exists_on_disk':val.exists_on_disk,
                                'data_level':val.data_level}

## add to the dict the data product that is the output of the process in the bf dict
        for fname in self.bf:
            for sq in self.session.query(self.File_processes).filter_by(type=1).filter_by(p_id = self.bf[fname]['p_id']):
                self.bf[fname]['out_dp_id'] = sq.dp_id
                self.bf[fname]['out_p_id']  = sq.p_id

## now get the output data_product information form the given out_dp_id
        for fname in self.bf:
            for sq in self.session.query(self.Data_products).filter_by(dp_id = self.bf[fname]['out_dp_id']):
                self.bf[fname]['out_product_name'] = sq.product_name
                self.bf[fname]['out_ds_id'] = sq.ds_id
                self.bf[fname]['out_relative_path'] = sq.relative_path

        if verbose:
            print("\t\t%s  %d files elegible to process" % ("len(bf)", len(self.bf)))
        return len(self.bf)


###########################################
### Decide which files NOT to process #####
###########################################

    def _backupBF(self, verbose=False):
        """
        create a backup _bf of the file dictionary, useful for debugging

        @keyword verbose: (optional) - print information out to the command line
        @return: True - Success, False - Failure
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 16-Jun-2010 (BAL)

        >>>  pnl._backupBF()
        """
        try:
            self._bf = copy.deepcopy(self.bf)
            if verbose: print("self.bf backed up as self._bf")
            return True
        except:
            return False

    def _initDelNames(self, verbose=False):
        """
        Initialize the del_names part so that we can remove naems from bf that dont need processing
        
        @keyword verbose: (optional) - print information out to the command line
        @return: True - Success, False - Failure
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 16-Jun-2010 (BAL)

        >>>  pnl._initDelNames()
        True
        
        """
        try:
            del_names = []
            self.del_names = del_names
            multis = np.array([])
            self.multis = multis
            if verbose: print("self.del_names initialized")
            return True
        except:
            return False

        
    def _Lxp1exists(self, verbose=False):
        """
        go through bf dict and see which files have a Lx+1 file already, add them to self.del_names 
        
        @keyword verbose: (optional) - print information out to the command line

        @return: Counter - number of files added to the list from this check
                       
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 16-Jun-2010 (BAL)

        >>>  pnl._Lxp1exists()
    
        """
        # step through and see which files have next level files and codes that are in date,
        #        if not in date or the L+1 exits delte it form the dict
        # the next level file exists
        # there is no code to do the process, inactive or dates
        try: self.del_names
        except AttributeError: self.initDelNames()
        counter = 0
        for fname in self.bf:
            # does a Lx+1 exist?
            if self.session.query(self.Build_filenames).filter_by(utc_file_date = self.bf[fname]['utc_file_date']).filter_by(product_name =  self.bf[fname]['out_product_name']).count() != 0:
                self.del_names.append(fname)
                if verbose: print("\t%s has a Lx_1" % (fname))
                counter +=  1
        return counter

    def _procCodeDates(self, verbose = False):
        """
        go through bf dict and if files have processing code in the right date range
        
        @keyword verbose: (optional) - print out lots of info

        @return: Counter - number of files added to the list from this check
                 
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 16-Jun-2010 (BAL)

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
                if self.bf[fname]['utc_file_date'] < sq.code_start_date or self.bf[fname]['utc_file_date'] > sq.code_end_date:
                    all_false = np.append(all_false, True)
            if ~all_false.any() and len(all_false) != 0:
                print("\t<procCodeDates> %s didnt have valid, %s, %s, %s" % (fname, sq.code_start_date, sq.code_end_date, all_false))
                self.del_names.append(fname)
                counter += 1
        return counter

    def _activeProcCode(self, verbose=False):
        """
        go through bf dict and if files have active processing codes
        
        @keyword verbose: (optional) - print information out to the command line

        @return: Counter - number of files added to the list from this check
                        
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 16-Jun-2010 (BAL)
        
        >>>  pnl._procCodeDates()
        """
        # is there an active processing code?
        try: self.del_names
        except AttributeError: self.initDelNames()
        counter = 0
        for fname in self.bf:
            all_false = np.array([sq.active_code for sq in self.session.query(self.Executable_codes).filter_by(p_id = self.bf[fname]['out_p_id'])])
            if ~all_false.any():
                self.del_names.append(fname)
                if verbose: print("\t%s does not have an active proc code" % (fname))
                counter += 1
        return counter

    def _existsOnDisk(self, verbose=False):
        """
        go through bf dict and if files exist on disk (just a db query)
        
        @keyword verbose: (optional) - print information out to the command line

        @return: Counter - number of files added to the list from this check
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 16-Jun-2010 (BAL)
        
        >>>  pnl._existsOnDisk()
        """
        try: self.del_names
        except AttributeError: self.initDelNames()
        # does the file exist on disk?
        counter = 0
        for fname in self.bf:
            if self.bf[fname]['exists_on_disk'] == False:
                    self.del_names.append(fname)
                    if verbose: print("\t%s does not exist on disk" % (fname))
                    counter += 1
        return counter

    def _delNewerVersion(self, verbose = False):
        """
        go through bf dict and if files have new versions, no need to process old versions
        
        @keyword verbose: (optional) - print information out to the command line

        @return: Counter - number of files added to the list from this check
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 16-Jun-2010 (BAL)

        >>>  pnl._procCodeDates()
        """
        try: self.del_names
        except AttributeError: self.initDelNames()
        counter = 0
        for fname in self.bf:
            if self.bf[fname]['f_id'] != self._newerFileVersion(self.bf[fname]['f_id']):
                self.del_names.append(fname)
                if verbose: print("\t%s has a newer version, no need to proc" % (fname))
                counter += 1
        return counter


    def _newerFileVersion(self, id, bool=False, verbose=False):
        """
        given a data_file ID decide if there is a newer version
        (maybe _delNewerVersion() can extend this but not yet)
        
        @param id: the code or file id to check
        @keyword bool: (optional) if set answers the question is there a newer version of the file?
             
        @return: id of the newest version
             
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 18-Jun-2010 (BAL)
        @version: V2: 1-Jul-2010 (BAL) added bool keyword

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
            raise(ProcessingError('More than one file with thwe same computed V_num'))

        ## test to see if the newest is the id passed in
        ind = np.argsort(vall)
        if mul[ind[-1]][1] != id:
            if bool: return True
            else: return mul[ind[-1]][1]
        else:
            if bool: return False
            else: return mul[ind[-1]][1]
       
       
                
    def _getBaseFilename(self, id):
        """
        given a file id return the file basename

        @param id: file id to return the vase name of
        @return: base filename for input file id

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
        
        @version: V1: 8-Jul-2010 (BAL)
        """
        try: self.Data_files
        except AttributeError: self._createTableObjects()
        sq_id = self.session.query(self.Data_files).filter_by(f_id = id)
        if sq_id.count() == 0:
            return None
        else:
            return sq_id[0].base_filename



    def _delNames(self, verbose=False):
        """
        actually remove from the dictionary those files on the delList
        
        @keyword verbose: (optional) print more to screen
        @return: True - Success, False - Failure
    
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 14-Jun-2010 (BAL)
       
        >>>  pnl._delNames()
        True
        """
        try: self.del_names
        except AttributeError: self.initDelNames()
        ## need uniqueness for the list to remove
        self.del_names = np.unique(self.del_names).tolist()   # leave this as a list or causes issues later
        try:
            if verbose:
                print("\t\tRemoving duplicates, this leaves %d files to remove" % (len(self.del_names)))
        except TypeError:
            print("\t\tThere are 0 files to remove form the list, processing all")

        for i, val in enumerate(self.del_names):
            try:
                del self.bf[val]
            except KeyError:
                pass
        try:
            if verbose:
                print("\t\tLeaving %d files that will be processed" % (len(self.bf)))
        except:
            pass
        return True # probably should check somehting here
                  
                
#########################################
## now that we have all the files that actiually need to be processed, grab more info

    def _buildOutName(self, incQuality=False, incRevision=False, verbose=False):
        """
        TODO this needs updateing to make the versions set correctly for reporcessing
                    
        build up the output name of the file that will be output of processing
        to add a file to the database one needs to collect and do the following
          - create a new data file from running a process (above stored in commands)
          - insert that file into the data_files table
          - INFO NEEDED:
          - filename  -- from the command  (out_fname)
          - utc_file_date  -- from the command  (utc_file_date)
          - utc_start_date  -- from inpout files  (get from a query)
          - utc_end_date  -- from inout files  (get from a query)
          - data_level  -- from processes table    (get from a query)
          - consistency_check  -- (optional blank on additon)
          - ec_interface_version  -- copy from Executable_code interface version (inferface_version)
          - verbose_provenance  -- (optional, to add later)
          - quality_check  -- (optional, blank for new files)
          - quality_comment  -- (optional, blank for new files)
          - caveats  -- (optional, black for new files)
          - release_number  -- (optional, blank for new files)
          - ds_id  -- what data source it came from   (out_ds_id)
          - quality_version -- starts at 1 always
          - revision_version  -- starts at 1 always
          - file_create_date  -- the date and time file was created   (DB defaults to now)
          - dp_id  -- the data product    (out_dp_id)
          - met_start_time  -- (optional if we know it)
          - met_stop_time  -- (optional if we knowq it)
          - exists_on_disk  --  true for all new files
          - base_filename   --  built from the output filename
          
        @keyword verbose: (optional) print information out to the command line
        @return: True - Success, False - Failure
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 14-Jun-2010 (BAL)

        >>>  pnl._buildOutName()
        """
        # gather and put the processing info for each fiole in the dictionary
        for fname in self.bf:
            for sq in self.session.query(self.Processing_paths).filter_by(p_id=self.bf[fname]['out_p_id']).filter_by(active_code=True):
                self.bf[fname]['exc_absolute_name'] = sq.absolute_name
                self.bf[fname]['ec_id'] = sq.ec_id

            # gather and put the output file name into the dictionary
            for sq in self.session.query(self.Build_output_filenames).filter_by(p_id=self.bf[fname]['out_p_id']):
                if incQuality:
                    self.bf[fname]['outquality_version'] = self.bf[fname]['quality_version'] + 1
                else:
                    self.bf[fname]['outquality_version'] = self.bf[fname]['quality_version']
                if incRevision:
                    self.bf[fname]['outrevision_version'] = self.bf[fname]['revision_version'] + 1
                else:
                    self.bf[fname]['outrevision_version'] = self.bf[fname]['revision_version']
                interface_ver = self.session.query(self.Executable_codes).filter_by(ec_id=self.bf[fname]['ec_id']).all()[0]
                out_fname = self.__build_fname(sq.path,
                                          '',
                                          sq.mission_name,
                                          sq.satellite_name,
                                          sq.product_name,
                                          datetime.strftime(self.bf[fname]['utc_file_date'], '%Y%m%d'),
                                          interface_ver.interface_version,
                                          self.bf[fname]['outquality_version'],
                                          self.bf[fname]['outrevision_version'])                                         
                self.bf[fname]['out_absolute_name'] = out_fname
                self.bf[fname]['out_fname'] = os.path.basename(self.bf[fname]['out_absolute_name'])
                self.bf[fname]['level'] = sq.level
                break # hack to just do this once
                
            for sq in self.session.query(self.Data_files).filter_by(f_id = self.bf[fname]['f_id']):
                self.bf[fname]['utc_start_time'] = sq.utc_start_time
                self.bf[fname]['utc_end_time'] = sq.utc_end_time
                self.bf[fname]['data_level'] = sq.data_level
                self.bf[fname]['met_start_time'] = sq.met_start_time
                self.bf[fname]['met_stop_time'] = sq.met_stop_time
                f2 = self.bf[fname]['out_fname'].split('.')[0]
                f3 = f2.split('_v')[0]
                self.bf[fname]['base_filename'] = f3
                ## self.bf[fname]['quality_version'] += 1
                ## self.bf[fname]['revision_version'] += 1
                
                if verbose: print("\t build the outname %s" % (self.bf[fname]['out_fname']))
        return True  # should actually check something



    def _buildOutNameReprocess(self, incQuality=False, incRevision=False, verbose=False):
        for fname in self.bf:
            for sq in self.session.query(self.Code_dependencies).filter_by(cd_id=self.bf[fname]['code_dependencies'][0]):
                self.bf[fname]['ec_id'] = self._newerCodeVersion(sq.dependent_ec_id)
            for sq in self.session.query(self.Processing_paths).filter_by(ec_id=self.bf[fname]['ec_id']).filter_by(active_code=True):
                self.bf[fname]['exc_absolute_name'] = sq.absolute_name
             

            # gather and put the output file name into the dictionary
            for sq in self.session.query(self.Build_reprocess_filenames).filter_by(p_id=self.bf[fname]['p_id']):
                if incQuality:
                    self.bf[fname]['quality_version'] = self.bf[fname]['quality_version'] + 1
                if incRevision:
                    self.bf[fname]['revision_version'] = self.bf[fname]['revision_version'] + 1
                interface_ver = self.session.query(self.Executable_codes).filter_by(ec_id=self.bf[fname]['ec_id']).all()[0]
                out_fname = self.__build_fname(sq.path,
                                          '',
                                          sq.mission_name,
                                          sq.satellite_name,
                                          sq.product_name,
                                          datetime.strftime(self.bf[fname]['utc_file_date'], '%Y%m%d'),
                                          interface_ver.interface_version,
                                          self.bf[fname]['quality_version'],
                                          self.bf[fname]['revision_version'])                                     
                self.bf[fname]['out_absolute_name'] = out_fname
                self.bf[fname]['out_fname'] = os.path.basename(self.bf[fname]['out_absolute_name'])
                self.bf[fname]['level'] = sq.level
                break   #kack to just do this once
            if verbose: print("\t build the outname %s" % (self.bf[fname]['out_fname']))
                
            for sq in self.session.query(self.Data_files).filter_by(f_id = self.bf[fname]['f_id']):
                self.bf[fname]['utc_start_time'] = sq.utc_start_time
                self.bf[fname]['utc_end_time'] = sq.utc_end_time
                self.bf[fname]['data_level'] = sq.data_level
                self.bf[fname]['met_start_time'] = sq.met_start_time
                self.bf[fname]['met_stop_time'] = sq.met_stop_time
                f2 = self.bf[fname]['out_fname'].split('.')[0]
                f3 = f2.split('_v')[0]
                self.bf[fname]['base_filename'] = f3
            ## if incQuality:
            ##     self.bf[fname]['quality_version'] = self.bf[fname]['quality_version'] + 1
            ## if incRevision:
            ##     self.bf[fname]['revision_version'] = self.bf[fname]['revision_version'] + 1 
                

                
                ## if verbose: print("\t build the outname %s" % (self.bf[fname]['out_fname']))
        return True  # should actually check something
        

#####################################
####  Do processing and input to DB
#####################################

    def _currentlyProcessing(self, verbose=False):
        """
        Checks the db to see if it is currently processing, dont want to do 2 at the same time
        
        @keyword verbose: (optional) print out verbose informaiton
        @return: True - Success, False - Failure
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 17-Jun-2010 (BAL)

        >>>  pnl._currentlyProcessing()
        """ 
        sq = self.session.query(self.Logging).filter_by(currently_processing = True).count()
        if sq != 0:
            return True
        else:
            return False

    def _resetProcessingFlag(self, comment=None):
        """
        Query the db and reset a processing flag
        
        @keyword comment: the comment to enter into the processing log DB
        @return: True - Success, False - Failure
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 17-Jun-2010 (BAL)

        >>>  pnl._resetProcessingFlag()
        """
        if comment == None:
            print("Must enter a comment to override DB lock")
            return False
        sq = self.session.query(self.Logging).filter_by(currently_processing = True)
        for val in sq:
            val.currently_processing = False
            val.processing_end = datetime.now()
            val.comment = 'Overridden:' + comment + ':' + __version__
            self.session.add(val)
        self.session.commit()
        return True
        
    def _startLogging(self, verbose = False):
        """
        Add an entry to the processing table in the DB, logging
        
        @keyword verbose: (optional) print information out to the command line
        @return: True - Success, False - Failure
                        
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 17-Jun-2010 (BAL)
        
        >>>  pnl._startLogging()
        """
        # this is the logging of the processing, no real use for it yet but maybe we will inthe future
        # helps to know is the process ran and if it succeeded
        import socket # to get the local hostname
        if self._currentlyProcessing():
            raise(Exception('A Currently Processing flag is still set, cannot process now'))
        p1 = self.Logging()
        p1.currently_processing = True
        p1.pid = os.getpid()
        p1.processing_start = datetime.now()
        p1.mission_id = self._getMissionID()
        p1.user = os.getlogin()
        p1.hostname = socket.gethostname()
        self.session.add(p1)
        self.session.commit()
        self.__p1 = p1 # save this class instance so that we can finish the logging later
        if verbose: print("Logging started: %s, PID: %s, M_id: %s, user: %s, hostmane: %s" %
                          (p1.processing_start, p1.pid, p1.mission_id, p1.user, p1.hostname))
        return True   # should do some checking here

    
    def _stopLogging(self, comment=None, verbose=False):
        """
        Finish the entry to the processing table in the DB, logging
        
        @keyword comment: (optional) a comment to insert intot he DB
        @keyword verbose: (optional) print information out to the command line
        @return: True - Success, False - Failure
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 17-Jun-2010 (BAL)

        >>>  pnl._stopLogging()
        """
        try: self.__p1
        except:
            print("Logging was not started")
            return False
        # clean up the logging, we are done processing and we can realease the lock (currently_processing) and
        # put in the complete time
        if comment == None:
            print("Must enter a comment for the log")
            return False

        self.__p1.processing_end = datetime.now()
        self.__p1.currently_processing = False
        self.__p1.comment = comment+':' + __version__
        self.session.add(self.__p1)
        self.session.commit()
        if verbose: print("Logging stopped: %s comment '%s' entered" % (self.__p1.processing_end, self.__p1.comment))
        return True 

    def _doNextLevelProcessing(self, verbose = False):
        """
        Actually do the processing, makes system calls, populates the processing log and adds  files to the DB
      
        @keyword verbose: Print out a lot of information
        @return: True - Success, False - Failure
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 16-Jun-2010 (BAL)
        @version: V2: 7-Jul-2010 (BAL) split this up creating _insertFileDependencies, _insertCodeDependencies

        >>>  pnl._doNextLevelProcessing()
        """
        ## now run the process to create the file

        # session.add adds things to a "queue" pof things to add to the database
        # the "queue" seems quite large (I havent found the end yet)
        # the session.commit processes the "queue" and adds them to the DB (to disk)
        try:
            self.__p1
        except:
            print("Must start logging with _startLogging() before you may process")
            return False

        for i, fname in enumerate(self.bf):
            print("Processing %s,         file %d of %d" % ( fname, i+1, len(self.bf)))
            print("\t%s %s %s" % (self.bf[fname]['exc_absolute_name'],
                                  self.bf[fname]['absolute_name'],
                                  self.bf[fname]['out_absolute_name']))
            subprocess.call([self.bf[fname]['exc_absolute_name'],
                             self.bf[fname]['absolute_name'],
                             self.bf[fname]['out_absolute_name']])
            pf1 = self.Logging_files()
            pf1.processing_id = self.__p1.processing_id
            pf1.f_id = self.bf[fname]['f_id']
            pf1.ec_id = self.bf[fname]['ec_id']
            self.session.add(pf1)
            
        self.session.commit()
    
# insert the new created file into the DB
        for fname in self.bf:
            self._addDataFile(os.path.basename(self.bf[fname]['out_absolute_name']),
                              self.bf[fname]['utc_file_date'],
                              self.bf[fname]['utc_start_time'],
                              self.bf[fname]['utc_end_time'],
                              self.bf[fname]['level'] + 1, # might need to think on this +1 part
                              interface_version = self.bf[fname]['ec_interface_version'],
                              ds_id = self.bf[fname]['out_ds_id'],
                              quality_version = self.bf[fname]['quality_version'] ,
                              revision_version = self.bf[fname]['revision_version'] ,
                              dp_id = self.bf[fname]['out_dp_id'],
                              met_start_time = self.bf[fname]['met_start_time'],
                              met_stop_time = self.bf[fname]['met_start_time'],
                              exists_on_disk = True,
                              base_filename = self.bf[fname]['base_filename'])
            if not self._insertFileDependencies(fname):
                raise(Exception("_insertFileDependencies, %s" % (fname)))
            if not self._insertCodeDependencies(fname):
                raise(Exception("_insertCodeDependencies, %s" % (fname)))
        return True # worked if we got here

    def _insertFileDependencies(self, fname):
        """
        given a fname from the dict enter information into the File_dependencies table

        @param fname: a filename from the dict bf
        @return: True - Success, False - Failure
   
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 7-Jul-2010 (BAL)
        """
        #insert info into the file_dependencies table
        # INFO NEEDED:
        # dependent_f_id  -- f_id's of the input files that went into it
        # f_id  -- the file id of the newly inseted file
        try:
            self.bf[fname]['out_absolute_name']
        except:
            print("Must create outname before you can commit (_buildOutName)")
            return False
        for sq in self.session.query(self.Data_files).filter_by(filename = os.path.basename(self.bf[fname]['out_absolute_name'])):
            self.bf[fname]['out_f_id'] = sq.f_id
            d1 = self.File_dependencies()
            d1.dependent_f_id = self.bf[fname]['f_id']
            d1.f_id = sq.f_id
            self.session.add(d1)
            
        self.session.commit()
        return True

    def _insertCodeDependencies(self, fname):
        """
        given a fname from the dict enter information into the Code_dependencies table

        @param fname: a filename from the dict bf
        @return: True - Success, False - Failure

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 7-Jul-2010 (BAL)
        """
        try:
            self.bf[fname]['out_absolute_name']
        except:
            print("Must create outname before you can commit (_buildOutName)")
            return False
        #insert info into the code_dependencies table
        # INFO NEEDED:
        # f_id  -- the file id of the newly created file
        # dependent_ec_id  -- ec_id of the code that created the file
        for sq in self.session.query(self.Data_files).filter_by(filename = os.path.basename(self.bf[fname]['out_absolute_name'])):
            d1 = self.Code_dependencies()
            # write in the code that created the file not the one it uses to go L+1 later
            d1.dependent_ec_id = self.bf[fname]['ec_id']
            d1.f_id =  sq.f_id
            self.session.add(d1)
        
        self.session.commit()
        return True   




    def _checkDiskForFile(self, fix=False):
        """
        Check the filesystem tosee if the file exits or not as it says in the db
        
        @keyword fix: (optional) set to have the DB fixed to match the filesystem
           this is **NOT** sure to be safe
        @return: count - return the count of out of sync files
             
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 17-Jun-2010 (BAL)
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
        self.session.commit()
        return counter


    def _purgeFileFromDB(self, filename=None):
        """
        removes a file from the DB
        
        @keyword filename: name of the file to remove (or a list of names)
        @return: True - Success, False - Failure

        >>>  pnl._purgeFileFromDB('Test-one_R0_evinst-L1_20100401_v0.1.1.cdf')
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 18-Jun-2010 (BAL)
        """
        # filenames are unique so no need to loop (DB assures uniqueness)
        if not isinstance(filename, (list, np.ndarray)):
            filename = [filename]
        for fname in filename:
            for sq_df in self.session.query(self.Data_files).filter_by(filename = fname):
                self.__sq_df = sq_df
                # not unique so loop
                for sq_cd in self.session.query(self.Code_dependencies).filter_by(f_id = sq_df.f_id):
                    self.session.delete(sq_cd)
                self.session.commit()
                for sq_fd in self.session.query(self.File_dependencies).filter_by(f_id = sq_df.f_id):
                    self.session.delete(sq_fd)
                self.session.commit()
            self.session.delete(sq_df)
            print("File %s purged" % (sq_df.filename))
        self.session.commit()
        return True  # need some error checking here



    def _purgeLevelFiles(self, level, sure = False):
        """
        removes all files of a certain level from db
        
        @param level: the level to purge from the DB
        @keyword sure: additional idiot guard (default=False)
        @return: True - Success, False - Failure
                        
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    

        @version: V1: 18-Jun-2010 (BAL)
        
        >>>  pnl._purgeFileFromDB('Test-one_R0_evinst-L1_20100401_v0.1.1.cdf')
        """
        if not sure:
            return False
        if len(self.bf) == 0:
            print("bf is empty, run _gatherFiles()")
            return False
        bftmp = copy.deepcopy(self.bf)
        for fname in self.bf:
            if bftmp[fname]['data_level'] != level:
                del bftmp[fname]
        filenames = []
        for fname in bftmp:
            filenames.append(fname)
        if not self._purgeFileFromDB(filenames):
            return False
        return True


    def _collectDependencies(self, f_id=None, verbose=False):
        """
        Collect the dependencies for a file and add it to the BF dict or if f_id is set return
        that file's dependencies.  NOTE this collects the cd_id and fd_id form those tables not the
        actual file, have to query the tables.
        
        @keyword f_id: (optional) changes routine to return lists of file and code dependencies

        @return: 
             - without f_id True means success, False means failure
             - with f_id returns a list of file dependicies and a list of code dependencies
                         
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 18-Jun-2010 (BAL)
        
        >>>  pnl._collectDependencies()
        """
        try: self.File_dependencies
        except AttributeError: self._createTableObjects()
        if f_id == None:
            for fname in self.bf:
                self.bf[fname]['file_dependencies'] = []
                self.bf[fname]['code_dependencies'] = []
                
                # query and find dependencies, add them as a list to the BF dict
                for sq_fd in self.session.query(self.File_dependencies).filter_by(f_id = self.bf[fname]['f_id']):
                    self.bf[fname]['file_dependencies'].append(sq_fd.fd_id)
                for sq_cd in self.session.query(self.Code_dependencies).filter_by(f_id = self.bf[fname]['f_id']):
                    self.bf[fname]['code_dependencies'].append(sq_cd.cd_id)
            return True   # add error checking
        else:  # just to make it obvious what is going on
            fd = []
            cd = []
            for sq_fd in self.session.query(self.File_dependencies).filter_by(f_id = f_id):
                fd.append(sq_fd.fd_id)
            for sq_cd in self.session.query(self.Code_dependencies).filter_by(f_id = f_id):
                cd.append(sq_cd.cd_id)
            return (fd, cd)
            

    def _addExecutableCode(self,
                           ec_id, 
                           filename,
                           relative_path,
                           code_start_date,
                           code_end_date,
                           code_id,
                           p_id,
                           ds_id,
                           interface_version,
                           quality_version,
                           revision_version,
                           code_active):
        """
        Add an executable code to the DB
        
        @param ec_id: the DB number for the code, should be auto but didnt work TODO
        @param filename: the filename of the code
        @param relative_path: the relative path (relative to mission base dir)
        @param code_start_date: start of valaitdy of the code (datetime)
        @param code_end_date: end of validity of the code (datetime)
        @param code_id: optional default None
        @param p_id: the process id that the code belongs to
        @param ds_id: the data source the code belongs to
        @param interface_version: the interface version of the code
        @param quality_version: the quality version of the code
        @param revision_version: the revision version of the code
        @param code_active: boolean True means the cade is active
        @return: True - Success, False - Failure
           
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 18-Jun-2010 (BAL)

        >>>  pnl._addExecutableCode(TODO)
        """
        if not isinstance(code_start_date, (datetime)):
            print("code_start_date must be a datetime object")
            return False
        if not isinstance(code_end_date, (datetime)):
            print("code_end_date must be a datetime object")
            return False
        c1 = self.Executable_codes()
        c1.ec_id = ec_id  # for seome reason this the not auto incrementing TODO
        c1.filename = filename
        c1.relative_path = relative_path
        c1.code_start_date = code_start_date
        c1.code_end_date = code_end_date
        c1.code_id = code_id
        c1.p_id = p_id
        c1.ds_id = ds_id
        c1.interface_version = interface_version
        c1.quality_version = quality_version
        c1.revision_version = revision_version
        c1.code_active = code_active   # on first test this was not made true...
        self.session.add(c1)
        self.session.commit()
        return True

    def _noDependencies(self, verbose=False):
        """
        Add files with no deopendencies to the del_names list 
        
        @keyword verbose: Print out a lot of information
        @return: count - the number of files added to the del_names list
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 18-Jun-2010 (BAL)
        
        >>>  pnl._noDependencies()
        """
        try: self.del_names
        except AttributeError: self.initDelNames()
        # step through and remove files with no dependecies
        count = 0
        for fname in self.bf:
            # do the file_dependencies 
            n_fd = self.session.query(self.File_dependencies).filter_by(f_id = self.bf[fname]['f_id']).count()
            n_cd = self.session.query(self.Code_dependencies).filter_by(f_id = self.bf[fname]['f_id']).count()
            if (n_fd == 0) & (n_cd == 0):
                self.del_names.append(fname)
                if verbose: print("\t%s removed as it has no dependencies" % (fname))
                count += 1
        return count

    def _closeDB(self, verbose=False):
        """
        Close the database connection
        
        @keyword verbose: (optional) print information out to the command line
                           
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 14-Jun-2010 (BAL)
        @version: V2: 25-Aug-2010 (BAL) - changed to raise exception not returtn False

        >>>  pnl._closeDB()
        """
        if self.__dbIsOpen == False:
            return True
        try:
            self.session.close()
            self.__dbIsOpen = False
            if verbose: print("DB is now closed")
        except:
            raise(DBError('could not close DB'))


    def _addDataFile(self,
                     filename = None,
                     utc_file_date = None,
                     utc_start_time = None,
                     utc_end_time = None,
                     data_level = 0,
                     consistency_check = None,
                     interface_version = 0,
                     verbose_provenance = None,
                     quality_check = 0,
                     quality_comment = None,
                     caveats = None,
                     release_number = None,
                     ds_id = None,
                     quality_version = 0,
                     revision_version = 0,
                     file_create_date = datetime.now(),
                     dp_id = None,
                     met_start_time = None,
                     met_stop_time = None,
                     exists_on_disk = True,
                     base_filename = None):
        """
        add a datafile to the database

        @keyword filename: None
        @keyword utc_file_date: None
        @keyword utc_start_time: None
        @keyword utc_end_time: None
        @keyword data_level: 0
        @keyword consistency_check: None
        @keyword interface_version: 0
        @keyword verbose_provenance: None
        @keyword quality_check: 0
        @keyword quality_comment: None
        @keyword caveats: None
        @keyword release_number: None
        @keyword ds_id: None
        @keyword quality_version: 0
        @keyword revision_version: 0
        @keyword file_create_date: datetime.now()
        @keyword dp_id: None
        @keyword met_start_time: None
        @keyword met_stop_time: None
        @keyword exists_on_disk: True
        @keyword base_filename: None
        @return: True - Success, False - Failure
 
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 18-Jun-2010 (BAL)

        """
        if filename == None:
            filename = input("Enter the filename: ")
        if utc_file_date == None:
            utc_file_date = input("Enter the UTC file date: ")
        if utc_start_time == None:
            utc_start_time = input("Enter the UTC start date: ")
        if utc_end_time == None:
            utc_end_time = input("Enter the UTC end date: ")
        if ds_id == None:
            ds_id = input("Enter the ds_id: ")
        if dp_id == None:
            dp_id = input("Enter the dp_id: ")
        if base_filename == None:
            base_filename = input("Enter the base filename: ")

        if self.__dbIsOpen == False:
            self._openDB()
            
        try:
            d1 = self.Data_files()
        except AttributeError:
            self._createTableObjects()
            d1 = self.Data_files()
        d1.filename = filename
        d1.utc_file_date = utc_file_date
        d1.utc_start_time = utc_start_time
        d1.utc_end_time = utc_end_time
        d1.data_level = data_level
        d1.consistency_check = consistency_check
        d1.interface_version = interface_version
        d1.verbose_provenance = verbose_provenance
        d1.quality_check = quality_check
        d1.quality_comment = quality_comment
        d1.caveats = caveats
        d1.release_number = release_number
        d1.ds_id = ds_id
        d1.quality_version = quality_version
        d1.revision_version = revision_version
        d1.file_create_date = file_create_date
        d1.dp_id = dp_id
        d1.met_start_time = met_start_time
        d1.met_stop_time = met_stop_time
        d1.exists_on_disk = exists_on_disk
        d1.base_filename = base_filename
        self.session.add(d1)
        self.session.commit()
        return True # should add some error checking here
        

    def _getPID(self, ec_id):
        """
        given a executable_code (ec) ID return the process id that created it

        @param ec_id: the code id to check
             
        @return: id of the newest version
             
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 1-Jul-2010 (BAL)
        """
        try: self.Executable_codes
        except AttributeError: self._createTableObjects()
        for sq in self.session.query(self.Executable_codes).filter_by(ec_id = ec_id):
            return sq.p_id


    def _codeIsActive(self, ec_id, date):
        """
        Given a ec_id and a date is that code active for that date

        @param ec_id: executable code id to see if is active
        @param date: date object to use when checking
             
        @return: True - the code is active for that date, False otherwise

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 1-Jul-2010 (BAL)
        """
        try: self.Executable_codes
        except AttributeError: self._createTableObjects()

        # can only be one here (sq)
        for sq in self.session.query(self.Executable_codes).filter_by(ec_id = ec_id):
            if sq.active_code == False:
                return False
            if sq.code_start_date > date:
                return False
            if sq.code_end_date < date:
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
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 1-Jul-2010 (BAL)
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
            raise(ProcessingError('More than one code with the same v_num'))

        ## test to see if the newest is the id passed in
        ind = np.argsort(vall)
        if mul[ind[-1]][1] != id:
            if bool: return True
            else: return mul[ind[-1]][1]
        else:
            if bool: return False
            else: return mul[ind[-1]][1]







    def _dependenciesNewest(self, verbose=False):
        """
        Step through the self.bf files and add to the del_list those whose dependicies are newest
        
        @keyword verbose: (optional) - print information out to the command line
        @return: number of files assed to the del_names list
        @rtype: int
            
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 1-Jul-2010 (BAL)
        """
        try: self.del_names
        except AttributeError: self.initDelNames()
        try: self.bf
        except:
            raise(ProcessingError('bf is not defined, must gather first'))
        count = 0
        for fname in self.bf:
            fd_ans = np.array([])
            for fd in self.bf[fname]['file_dependencies']:
                f_id = self._fileDependenciesToFID(fd)
                fd_ans = np.append(fd_ans, not self._newerFileVersion(f_id, bool=True))
                if verbose:
                    print("\t%s has file dependencies %d, newer available? %i" % (fname, fd,self._newerFileVersion(f_id, bool=True)) )
            if not fd_ans.all():
                if verbose:
                    print("%d %s has new file depen" % (self.bf[fname]['f_id'], fname))
                    print("\t%s is the new file id (%d is the old)" % ( self._newerFileVersion(f_id), f_id))
                continue   # no need to check code, a file failed newest, needs reprocessing
            cd_ans = np.array([])
            for cd in self.bf[fname]['code_dependencies']:
                cd_id = self._codeDependenciesToFID(cd)
                cd_ans = np.append(cd_ans, not self._newerCodeVersion(cd_id, bool=True))
                #self.bf[fname]['ec_id'] = self._newerCodeVersion(cd_id)
                # 1/0
            if not cd_ans.all():
                if verbose:
                    print("%s has new code depen" % (fname))
                    print("\t%s is the new code id (%d is the old)" % (self._newerCodeVersion(cd_id), cd_id))
                continue   # file failed newest, needs reprocessing 
            # to get here the file is newest
            self.del_names.append(fname)
            if verbose:
                print('\t\tadded %s to the del_names list' % (fname))
            count += 1
        return count  # the number added to the del_names list


    def _fileDependenciesExist(self, f_id):
        """
        Given an input file check to see that its file dependencies exist
        
        @param f_id: data file id to check if its dependencies still exist
             
        @return: True - all the dependencies exist, False otherwise

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 1-Jul-2010 (BAL)
        """
        try: self.Data_files
        except AttributeError: self._createTableObjects()

        fd, cd = self._collectDependencies(f_id)
        for val in fd:
            sq_fd = self.session.query(self.File_dependencies).filter_by(fd_id = val)
            sq_cnt = self.session.query(self.Data_files.f_id).filter_by(f_id = sq_fd[0].f_id).count()
            if sq_cnt != len(fd):
                print("f_id: %d has a file depen %d that is missing..." % (f_id, val))
                return False
        for val in cd:
            sq_ec = self.session.query(self.Code_dependencies).filter_by(cd_id = val)
            sq_cnt = self.session.query(self.Executable_codes).filter_by(ec_id = sq_ec[0].dependent_ec_id).count()
            if sq_cnt != len(cd):
                print("f_id: %d has an execuatable code depen %d that is missing..." % (f_id, val))
                return False
        return True # to make it here they all exist

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

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 6-Jul-2010 (BAL)

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
        except IntegrityError:
            print("cannot have two fo the same file in DB, must change at least one version number")
            return False
        return True

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

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 6-Jul-2010 (BAL)
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
        EC.code_end_date = sq[0].code_end_date
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
        except IntegrityError:
            print("cannot have two fo the same file in DB, must change at least one version number or the name")
            return False
        return True


    def _fileDependenciesToFID(self, fd_id):
        """
        Given a file dependency id (fd_id) return the dependendt_f_id 
        
        @param fd_id: file dependencies id to change to data file id
             
        @return: file id of the dependent file (or False if it doesnt exist)

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 6-Jul-2010 (BAL)
        """
        try: self.Data_files
        except AttributeError: self._createTableObjects()
        try: self.bf
        except NameError:
            print("self.bf does not exist, must gather first")
            return False

        sq_fd = self.session.query(self.File_dependencies).filter_by(fd_id = fd_id)
        return sq_fd[0].dependent_f_id

    def _codeDependenciesToFID(self, cd_id):
        """
        Given a code dependency id (cd_id) return the dependendt_f_id 
        
        @param cd_id: code dependencies id to change to data file id
             
        @return: file id of the dependent file (or False if it doesnt exist)

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 27-Aug-2010 (BAL)
        """
        try: self.Data_files
        except AttributeError: self._createTableObjects()
        try: self.bf
        except NameError:
            print("self.bf does not exist, must gather first")
            return False

        sq_fd = self.session.query(self.Code_dependencies).filter_by(cd_id = cd_id)
        return sq_fd[0].dependent_ec_id


    def _getMissionID(self):
        """
        Return the current mission ID 
        
        @return: mission_id - the current mission ID
             
        
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov
    
        @version: V1: 6-Jul-2010 (BAL)

        >>> dbp = DBProcessing()
        >>> dbp._getMissionID()
        19
        """
        try: self.session
        except AttributeError: self._openDB()
        try: self.Missions
        except AttributeError: self._createTableObjects()
        
        sq = self.session.query(self.Missions).filter_by(mission_name = self.mission)
        return sq[0].m_id

            
            
