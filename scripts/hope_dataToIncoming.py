#!/usr/bin/env python2.6

import itertools
import os
import re
import shutil

import dbprocessing.DBUtils as DBUtils
import dbprocessing.DBlogging as DBlogging

# Code users rsync to build an incremental list of files not already
# processed (i.e. not in /n/space_data/cda/rbsp) and saves that list to a file
# then the file is opened and each file is copied using shutils.copy
# also checks the error directory for the filename and does not copy again

dbu = DBUtils.DBUtils(os.path.expanduser('~ectsoc/RBSP_HOPE.sqlite'))

mission_path = dbu.getMissionDirectory()
g_inc_path = dbu.getIncomingPath()
sc = ['a', 'b']
data_path = [os.path.join('/', 'usr', 'local', 'ectsoc', 'data', 'level_0', val, 'hope') for val in sc]
mag_types = ['quicklook', 'l2']
tmp = [os.path.join('/', 'n', 'space_data', 'cda', 'rbsp', 'rbsp{0}'.format(s), 'emfisis' , t) for s, t in itertools.product(sc, mag_types)]
data_path += tmp

error_path = dbu.getErrorPath()
dbu._closeDB()

cull_re = [r'robots.*', r'^\.']


def build_db_set():
    """
    get the files from the db to compare against
    """
    files = dbu.getAllFilenames(fullPath=False)
    return set(files)

def build_data_set(data_paths):
    """
    go through a list of paths and grab all the filenames out of there
    """
    files = set()
    for path in data_paths:
        for f in os.listdir(path):
            files.add(os.path.realpath(os.path.join(path, f)))
    for cull_r in cull_re:
        files_to_cull = [f for f in files if re.match(cull_r, os.path.basename(f))]
        files = files.difference(files_to_cull)
    # also cull directories
    files_to_cull = [f for f in files if not os.path.isfile(f)]
    files = files.difference(files_to_cull)
    # cull files from error directory also
    err_files = os.listdir(error_path)
    cull_set = set()
    for f in files:
        os.path.basename(f) in err_files:
            cull_set.add(f)
    files = files.difference(cull_set)    
    return files

def files_to_move(data_files, db_files):
    """
    given the data_set files, and the files form the db cull those in thge db
    """
    cull_set = set()
    for f in data_files:
        if os.path.basename(f) in db_files:
            cull_set.add(f)
    data_files = data_files.difference(cull_set)
    return data_files

db_files = build_db_set()
data_files = build_data_set(data_path)
files = files_to_move(data_files, db_files)

for f in files:
    try:
        shutil.copy(f, g_inc_path)
        DBlogging.dblogger.info("{0}: moved {1} to {2}".format(__file__, f, g_inc_path))
    except:
        DBlogging.dblogger.error("{0}: failed moving {1} to {2}".format(__file__, f, g_inc_path))





