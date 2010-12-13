import sqlalchemy
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Boolean, Date, Float, Table
from sqlalchemy import desc, and_
import os.path
from datetime import datetime
import subprocess
import sqlalchemy
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Boolean, Date, Float, Table
from sqlalchemy import desc, and_
import os.path
from datetime import datetime
import subprocess
import numpy as np


## quick subroutine to build filenames up of the the required elements, called when processing Lx to Lx+1
## and example is  Test-one_R0_evinst-L0_20100118_v0.0.0.cdf
def __build_fname(rootdir = '', 
                  relative_path = '', 
                  mission_name = '',
                  satellite_name = '',
                  product_name = '', 
                  date = '', 
                  release = '', 
                  quality = '', 
                  revision = ''):
    dir = rootdir + relative_path
    fname = mission_name + '-' + satellite_name + '_' + product_name
    ver = 'v' + str(release) + '.' + str(quality) + '.' + str(revision)
    ext = '.cdf'
    fname = fname + '_' + date + '_' + ver + ext 
    return dir + fname

def __get_V_num(interface = '', 
                quality = '',
                revision = ''):
    tmp = interface *1000  + quality * 100 + revision
    return tmp


####################################
###### DB and Tables ###############
####################################

## setup python to talk to the database, this is where it is, name and password.
## TODO change the user form owner to ops as DB permnissons are fixed
engine = sqlalchemy.create_engine('postgresql+psycopg2://rbsp_owner:rbsp_owner@edgar:5432/rbsp', echo=False)
## this holds the metadata, tables names, attributes, etc
metadata = sqlalchemy.MetaData(bind=engine)
## a session is what you use to actually talk to the DB, set one up with the current engine
Session = sessionmaker(bind=engine)
session = Session()


## ask for the table names form the database (does not grab views)
table_names = engine.table_names()

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
for val in table_dict:
    c1 = compile("""class %s(object):\n\tpass""" % (val), '', 'exec')
    c2 = compile("%s = Table('%s', metadata, autoload=True)" % (str(table_dict[val]), table_dict[val]) , '', 'exec')
    c3 = compile("mapper(%s, %s)" % (val, str(table_dict[val])), '', 'exec')
    exec(c1)
    exec(c2)
    exec(c3)

###################################
### Views #########################
###################################

##  views cannot be dynamically allocated since you have to manually define the promary keys
##  also views cannot be grabbed form the engine the same way that tables are.  They are typed so
##  I wont change this to some standard.
##   --- When creating a new verw make really usre it has something unique that can be used as
##   --- a primary key. some below use f_id others use pk whisch is a call to a sequence

class Build_filenames(object):
    pass
build_filenames = Table('build_filenames', metadata,
                        Column('pk', BigInteger, primary_key=True),
                        autoload=True)
mapper(Build_filenames, build_filenames)

class Build_output_filenames(object):
    pass
build_output_filenames = Table('build_output_filenames', metadata,
                        Column('pk', BigInteger, primary_key=True),
                        autoload=True)
mapper(Build_output_filenames, build_output_filenames)

class Files_by_mission(object):
    pass
files_by_mission = Table('files_by_mission', metadata,
                         Column('f_id', BigInteger,  primary_key=True),
                         autoload = True)
mapper(Files_by_mission,files_by_mission)

class Data_product_by_mission(object):
    pass
data_product_by_mission = Table('data_product_by_mission', metadata,
                                Column('dp_id', BigInteger, primary_key=True),
                                autoload=True)
mapper(Data_product_by_mission,data_product_by_mission)

class Processing_paths(object):
    pass
processing_paths = Table('processing_paths', metadata,
                         Column('pk', BigInteger, primary_key=True),
                         autoload=True)
mapper(Processing_paths, processing_paths)


#######################################
## Populate the dictionary with all the info we can get
#######################################

## get the files for a given mission
mission = 'Test'
files = session.query(Files_by_mission.f_id, Files_by_mission.filename).filter_by(mission_name=mission).all()
files = zip(*files)
print("About to process %d files from %s for consistency" % (len(files[0]), mission))

## get the number of products for that mission
products = session.query(Data_product_by_mission.dp_id, Data_product_by_mission.product_name).filter_by(mission_name=mission).all()
products = zip(*products)
print("\tMission %s has %d data products to step through" % (mission, len(products[0])))

## create a list of everyting keys by filename that is an input to a process
bf = {}
for val in session.query(Build_filenames).filter_by(type=0).filter(Build_filenames.f_id != None).order_by(desc(Build_filenames.utc_file_date)):
    bf[val.filename] = {'mission_name':val.mission_name,
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
                        'ec_interface_version':val.ec_interface_version,
                        'interface_version':val.interface_version,
                        'quality_version':val.quality_version,
                        'revision_version':val.revision_version,
                        'file_create_date':val.file_create_date,
                        'version_num':__get_V_num(val.interface_version,val.quality_version,val.revision_version)}


#################################
#### which files need to be deleted from dict
#################################

## if the file doesnt exist on disk, then it needs to go
del_names = []
for fname in bf:
    if session.query(Data_files).filter_by(f_id = bf[fname]['f_id']).filter_by(exists_on_disk = False).count() != 0:
        del_names.append(fname)
del_names = np.unique(del_names)
for val in del_names:
    del bf[val]
print('\t\t%d files dont exist on disk and cant be checked (hopefully old versions)' % (len(del_names)))


## step through and remove files with no dependecies
del_names = []
for fname in bf:
    ## do the file_dependencies 
    n_fd = session.query(File_dependencies).filter_by(f_id = bf[fname]['f_id']).count()
    n_cd = session.query(Code_dependencies).filter_by(f_id = bf[fname]['f_id']).count()
    if (n_fd == 0) & (n_cd == 0):
        del_names.append(fname)
del_names = np.unique(del_names)
for val in del_names:
    del bf[val]
print('\t\t%d files have no dependencies and cant be checked (probably L0 files)' % (len(del_names)))

## add the dependencies to bf is a list for each file and code dependencies
for fname in bf:
    bf[fname]['dependent_f_id'] = []
    for sq in session.query(File_dependencies).filter_by(f_id = bf[fname]['f_id']):
        bf[fname]['dependent_f_id'].append(sq.dependent_f_id)
    bf[fname]['dependent_ec_id'] = []
    for sq in session.query(Code_dependencies).filter_by(f_id = bf[fname]['f_id']):
        bf[fname]['dependent_ec_id'].append(sq.dependent_ec_id)
        
## step through this list of dependent_f_id and see if they have changed
## for fname in bf:
##     for dep_f_id in bf[fname]['dependent_f_id']:
##         for sq in session.query(Data_files).filter_by(f_id = dep_f_id):
##             if len(sq) != 0:
##                 bf[fname]['file_dependencies'] = [sq[0].dependent_f_id]
##             else:
##                 bf[fname]['file_dependencies'] = [None]
##     ## do the code dependencies
##     sq = session.query(Code_dependencies).filter_by(f_id = bf[fname]['f_id']).all()
##     if len(sq) != 0:
##         bf[fname]['code_dependencies'] = [sq[0].dependent_ec_id]
##     else:
##         bf[fname]['code_dependencies'] = [None]
##     if (len(bf[fname]['code_dependencies']) == 0) & (len(bf[fname]['file_dependencies'])):
##         del_names.append(fname)
        
del_names = np.unique(del_names)
for val in del_names:
    del bf[val]
print('\t\tThere are %d files that dont need checking (probably L0 files)' % (len(del_names)))

## search for multi versions of files and remove old versions from the dict
for fname in bf:
    sqcount = session.query(Data_files).filter_by(base_filename = bf[fname]['base_filename']).count()
    print fname, sqcount


        
raise(Exception("STOP"))



print("\t\t%s  %d files that will be processed" % ("len(bf)", len(bf)))


### check for orphans, if the file has not prossing code  it is an orphan, or isnt used as inpout anywhere
     # there is no code to do the process, inactive or dates

#########################################

## now step through each files dependencies and see if they are still up to date

## is the code still active that created the code?
for fname in bf:
    for sq in session.query(Executable_codes).filter_by(ec_id = bf[fname]['code_dependencies']):
        if bf[fname].keys().count('code_active') == 0:
            bf[fname]['code_active'] = [sq.active_code]
        else:
            bf[fname]['code_active'].append(sq.active_code)
## is there a new code that repalced it?
for fname in bf:
    if not np.array(bf[fname]['code_active']).all():
        ## a processing code is not active, see if there is a replacement
        ## the p_id will be the same for the new code
        for val in bf[fname]['p_id']:
            for sq in session.query(Processing_paths).filter_by(p_id = val).filter_by(active_code=True):
                bf[fname]['use_processing_code'] = sq.ec_id 
                
            


## HMM WHAT IS IT THAT TELLS UP IF A L-1 FILE HAS CHANGED, WE KNOW WHICH ARE DEPENDENTS BUT NOT WHICH
##   ARE CHANGED...
##     lets look at the file_create_dates, if the dependent is newer we have to reprocess
for fname in bf:
    for sq in session.query(Data_files).filter_by(f_id = bf[fname]['file_dependencies']):
        if bf[fname].keys().count('file_dependencies_changed') == 0:
            if bf[fname]['file_create_date'] < sq.file_create_date:
                bf[fname]['file_dependencies_changed'] = [True]
            else:
                bf[fname]['file_dependencies_changed'] = [False]
        else:
            if bf[fname]['file_create_date'] > sq.file_create_date:
                bf[fname]['file_dependencies_changed'].append(True)
            else:
                bf[fname]['file_dependencies_changed'].append(False)


## now step through the dict looking for the no changed answers and delete the ones with no need for
##   reporcessing.  file_dependencies_changed=False  AND  code_active=True
for name in bf:
    if all(bf[fname]['code_active']) == True and  all(bf[fname]['file_dependencies_changed']) == False:
        del_names.append(fname)
## actually get rid of the files from the dict
        ## we need the try since some were already removed
for val in del_names:
    try:
        del bf[val]
    except:
        pass

    
#raise(Exception('stop reached'))






## gather and put the processing info for each file in the dictionary
for fname in bf:
    for sq in session.query(Processing_paths).filter_by(p_id=bf[fname]['out_p_id']):
        #    bf[fname]['exc_filename'] = sq.filename
        #    bf[fname]['exc_relative_path'] = sq.relative_path
        #    bf[fname]['exc_rootdir'] = sq.rootdir
        bf[fname]['exc_absolute_name'] = sq.absolute_name
        bf[fname]['ec_id'] = sq.ec_id


## gather and put the output file name into the dictionary
for fname in bf:
    for sq in session.query(Build_output_filenames).filter_by(p_id=bf[fname]['out_p_id']):
        out_fname = __build_fname(sq.path,
                                  '',
                                  sq.mission_name,
                                  sq.satellite_name,
                                  sq.product_name,
                                  datetime.strftime(bf[fname]['utc_file_date'], '%Y%m%d'),
                                  sq.interface_version,
                                  1,
                                  1)                                         
        bf[fname]['out_absolute_name'] = out_fname
        bf[fname]['out_fname'] = os.path.basename(bf[fname]['out_absolute_name'])
        bf[fname]['level'] = sq.level

        
## to add a file to the database one needs to collect and do the following

# create a new data file from running a process (above stored in commands)
# insert that file into the data_files table
      # INFO NEEDED:
                        # filename  -- from the command  (out_fname)
                        # utc_file_date  -- from the command  (utc_file_date)
                        # utc_start_date  -- from inpout files  (get from a query)
                        # utc_end_date  -- from inout files  (get from a query)
                        # data_level  -- from processes table    (get from a query)
                        # consistency_check  -- (optional blank on additon)
                        # interface_version  -- copy from Executable_code interface version (inferface_version)
                        # verbose_provenance  -- (optional, to add later)
                        # quality_check  -- (optional, blank for new files)
                        # quality_comment  -- (optional, blank for new files)
                        # caveats  -- (optional, black for new files)
                        # release_number  -- (optional, blank for new files)
                        # ds_id  -- what data source it came from   (out_ds_id)
                        # quality_version -- starts at 1 always
                        # revision_version  -- starts at 1 always
                        # file_create_date  -- the date and time file was created   (DB defaults to now)
                        # dp_id  -- the data product    (out_dp_id)
                        # met_start_time  -- (optional if we know it)
                        # met_stop_time  -- (optional if we know it)

for fname in bf:
    for sq in session.query(Data_files).filter_by(f_id = bf[fname]['f_id']):
        bf[fname]['utc_start_time'] = sq.utc_start_time
        bf[fname]['utc_end_time'] = sq.utc_end_time
        bf[fname]['data_level'] = sq.data_level
        bf[fname]['met_start_time'] = sq.met_start_time
        bf[fname]['met_stop_time'] = sq.met_stop_time
        
## now run the process to create the file
sq = session.query(Processing).filter_by(currently_processing = True).count()
if sq != 0:
    raise(Exception('A Currently Processing flag is still set, cannot process now'))

p1 = Processing()
p1.currently_processing = True
p1.pid = os.getpid()
p1.processing_start = datetime.now()
session.add(p1)
session.commit()

for i, fname in enumerate(bf):
    print("Processing %s,         file %d of %d" % ( fname, i+1, len(bf)))
    subprocess.call([bf[fname]['exc_absolute_name'],
                     bf[fname]['absolute_name'],
                     bf[fname]['out_absolute_name']])
    pf1 = Processing_files()
    pf1.processing_id = p1.processing_id
    pf1.f_id = bf[fname]['f_id']
    pf1.ec_id = bf[fname]['ec_id']
    session.add(pf1)

session.commit()
    
## insert the new created file into the DB
for fname in bf:   
    d1 = Data_files()
    d1.filename = os.path.basename(bf[fname]['out_absolute_name'])
    d1.utc_file_date = bf[fname]['utc_file_date']
    d1.utc_start_time = bf[fname]['utc_start_time']
    d1.utc_end_time = bf[fname]['utc_end_time']
    d1.data_level = bf[fname]['level'] + 1   # might need to think on this +1 part
    d1.interface_version = bf[fname]['interface_version']
    d1.ds_id = bf[fname]['out_ds_id']
    d1.quality_version = 1
    d1.revision_version = 1
    d1.dp_id = bf[fname]['out_dp_id']
    d1.met_start_time = bf[fname]['met_start_time']
    d1.met_stop_time = bf[fname]['met_start_time']
    session.add(d1)

session.commit()

#insert info into the file_dependencies table
      # INFO NEEDED:
                        # dependent_f_id  -- f_id's of the input files that went into it
                        # f_id  -- the file id of the newly inseted file
for fname in bf:
    for sq in session.query(Data_files).filter_by(filename = os.path.basename(bf[fname]['out_absolute_name'])):
        bf[fname]['out_f_id'] = sq.f_id
        d1 = File_dependencies()
        d1.dependent_f_id = bf[fname]['f_id']
        d1.f_id = sq.f_id
        session.add(d1)
        
session.commit()

#insert info into the code_dependencies table
      # INFO NEEDED:
                        # f_id  -- the file id of the newly created file
                        # dependent_ec_id  -- ec_id of the code that created the file
for fname in bf:
    for sq in session.query(Executable_codes).filter_by(p_id = bf[fname]['p_id']):
        bf[fname]['dependent_ec_id'] = sq.ec_id
        d1 = Code_dependencies()
        d1.dependent_ec_id = sq.ec_id 
        d1.f_id =  bf[fname]['f_id']
        session.add(d1)
        
session.commit()
    

p1.processing_end = datetime.now()
p1.currently_processing = False
session.add(p1)
session.commit()























