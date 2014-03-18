#!/usr/bin/env python2.6

import itertools
import os
import re
import shutil

from spacepy import toolbox as tb

import dbprocessing.DBUtils as DBUtils
import dbprocessing.DBlogging as DBlogging

# Code users rsync to build an incremental list of files not already
# processed (i.e. not in /n/space_data/cda/rbsp) and saves that list to a file
# then the file is opened and each file is copied using shutils.copy
# also checks the error directory for the filename and does not copy again

dbu = DBUtils.DBUtils(os.path.expanduser('~ectsoc/RBSP_MAGEIS.sqlite'))

DBlogging.dblogger.setLevel(DBlogging.LEVELS['info'])

mission_path = dbu.getMissionDirectory()
g_inc_path = dbu.getIncomingPath()
sc = ['a', 'b']
data_path = [os.path.join('/', 'usr', 'local', 'ectsoc', 'data', 'level_0', val, 'mageis') for val in sc]
mag_types = ['quicklook', 'l2']
tmp = [os.path.join('/', 'n', 'space_data', 'cda', 'rbsp', 'rbsp{0}'.format(s), 'emfisis' , t) for s, t in itertools.product(sc, mag_types)]
data_path += tmp
magephem_types = ['pre']
tmp = [os.path.join('/', 'n', 'projects', 'rbsp', 'rbsp{0}'.format(s), 'MagEphem' , t) for s, t in itertools.product(sc, magephem_types)]
data_path += tmp

tmp = ['/n/space_data/cda/rbsp/rbspa/magephem_def', '/n/space_data/cda/rbsp/rbspb/magephem_def']

data_path += tmp


error_path = dbu.getErrorPath()
dbu._closeDB()

cull_re = [r'robots.*', r'^\.', r'.*WFR*', r'.*HFR*']


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
    for ii, path in enumerate(data_paths):
        print("    checking {0}".format(path))
        for f in os.listdir(path):
            if 'MagEphem' in f:
                if 'OP77' in f and 'txt' in f:
                    files.add(os.path.realpath(os.path.join(path, f)))
            else:
                files.add(os.path.realpath(os.path.join(path, f)))
            tb.progressbar(ii+1, 1, len(data_paths), text="Collecting disk files")

    for cull_r in cull_re:
        files_to_cull = [f for f in files if re.match(cull_r, os.path.basename(f))]
        files = files.difference(files_to_cull)
    print("Culled files")
    # also cull directories
    files_to_cull = [f for f in files if not os.path.isfile(f)]
    files = files.difference(files_to_cull)
    print("Culled directories")
    # cull files from error directory also
    err_files = os.listdir(error_path)
    cull_set = set()
    for f in files:
        if os.path.basename(f) in err_files:
            cull_set.add(f)
    files = files.difference(cull_set)
    # no need to resync what is in incoming either
    inc_files = os.listdir(g_inc_path)
    cull_set = set()
    for f in files:
        if os.path.basename(f) in inc_files:
            cull_set.add(f)
    files = files.difference(cull_set)    

    return files

def files_to_move(data_files, db_files):
    """
    given the data_set files, and the files form the db cull those in the db
    """
    cull_set = set()
    for f in data_files:
        if os.path.basename(f) in db_files:
            cull_set.add(f)
    data_files = data_files.difference(cull_set)
    return data_files

db_files = build_db_set()
print("Collected db files")
data_files = build_data_set(data_path)
print("Collected disk files")
files = files_to_move(data_files, db_files)
print("Computed files to move")

n_files = len(files)
for ii, f in enumerate(files):
    try:
        if "emfisis" in f or "MagEphem" in f: # make a link not a copy
            os.symlink(f, os.path.join(g_inc_path, os.path.basename(f)))
            DBlogging.dblogger.debug("{0}: Linked {1} to {2}".format(__file__, f, g_inc_path))
        else:
            shutil.copy(f, g_inc_path)
            DBlogging.dblogger.debug("{0}: copied {1} to {2}".format(__file__, f, g_inc_path))
    except:
        DBlogging.dblogger.error("{0}: failed copying {1} to {2}".format(__file__, f, g_inc_path))
    tb.progressbar(ii, 1, n_files, text="Populating incoming ")


