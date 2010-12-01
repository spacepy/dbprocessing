import sqlalchemy
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Boolean, Date, Float, Table
from sqlalchemy import desc, and_, select
import os.path
from datetime import datetime
import subprocess
import numpy as np
import copy

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
engine = sqlalchemy.create_engine('postgresql+psycopg2://rbsp_owner:rbsp_owner@edgar:5432/rbsp', echo=True)
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



######################################
### Gather files form DB #############
######################################

## get the files for a given mission
mission = 'Test'   
files = session.query(Files_by_mission.f_id, Files_by_mission.filename).filter_by(mission_name=mission).all()
files = zip(*files)
print("About to process %d files from %s to the next data level" % (len(files[0]), mission))

## get the number of products for that mission
products = session.query(Data_product_by_mission.dp_id, Data_product_by_mission.product_name).filter_by(mission_name=mission).all()
products = zip(*products)
print("\tMission %s has %d data products to step through" % (mission, len(products[0])))

## create a list of everyting keys by filename that is an input to a process
## this dictionary is the key to everyhting, just keep it up to date and then you can add/delete form it
## to decide what needs to be processed
bf = {}
for val in session.query(Build_filenames).filter_by(type=0).filter(Build_filenames.f_id != None):
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
                        'interface_version':val.interface_version,
                        'quality_version':val.quality_version,
                        'revision_version':val.revision_version,
                        'ec_interface_version':val.ec_interface_version}


## add to the dict the data product that is the output of the process in the bf dict
for fname in bf:
    for sq in session.query(File_processes).filter_by(type=1).filter_by(p_id = bf[fname]['p_id']):
        bf[fname]['out_dp_id'] = sq.dp_id
        bf[fname]['out_p_id']  = sq.p_id

## now get the output data_product information form the given out_dp_id
for fname in bf:
    for sq in session.query(Data_products).filter_by(dp_id = bf[fname]['out_dp_id']):
        bf[fname]['out_product_name'] = sq.product_name
        bf[fname]['out_ds_id'] = sq.ds_id
        bf[fname]['out_relative_path'] = sq.relative_path

print("\t\t%s  %d files elegible to process" % ("len(bf)", len(bf)))

## this is a common place to stop, bf is full and has not been checked for files not to process
#raise(Exception('stop reached'))



######################################
### Gather info and process the next level
######################################
             
## now run the process to create the file
        ## this is the logging of the processing, no real use for it yet but maybe we will inthe future
        ## helps to know is the process ran and if it succeeded
sq = session.query(Processing).filter_by(currently_processing = True).count()
if sq != 0:
    raise(Exception('A Currently Processing flag is still set, cannot process now'))

p1 = Processing()
p1.currently_processing = True
p1.pid = os.getpid()
p1.processing_start = datetime.now()
session.add(p1)
session.commit()


print("############################\n################")
## insert the new created file into the DB
for fname in bf:   
    d1 = Data_files()
    f2 = fname.split('.')[0]
    f3 = f2.split('_v0')[0]
    for val in session.query(Data_files).filter_by(f_id=bf[fname]['f_id']):
        val.base_filename = f3
    session.commit()


## clean up the logging, we are done processing and we can realease the lock (currently_processing) and
## put in the complete time

p1.processing_end = datetime.now()
p1.currently_processing = False
session.add(p1)
session.commit()


session.close()





















