#!/usr/bin/env python2.6

import itertools
import os
import re
import shutil
import glob
from operator import itemgetter, attrgetter

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

tmp = [os.path.join('/n/space_data/cda/rbsp/MagEphem/predicted/a/'), os.path.join('/n/space_data/cda/rbsp/MagEphem/predicted/b/')]

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
    for path in data_paths:
        for f in os.listdir(path):
            if 'MagEphem' in f:
                if 'OP77' in f and 'txt' in f:
                    files.add(os.path.realpath(os.path.join(path, f)))
            else:
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
data_files = build_data_set(data_path)
files = files_to_move(data_files, db_files)

for f in files:
    try:
        if "emfisis" in f: # make a link not a copy
            os.symlink(f, os.path.join(g_inc_path, os.path.basename(f)))
            DBlogging.dblogger.info("{0}: Linked {1} to {2}".format(__file__, f, g_inc_path))
        else:
            shutil.copy(f, g_inc_path)
            DBlogging.dblogger.info("{0}: copied {1} to {2}".format(__file__, f, g_inc_path))
    except:
        DBlogging.dblogger.error("{0}: failed copying {1} to {2}".format(__file__, f, g_inc_path))





#########################
# TMP hack for magephem def seperately
#########################
op77_files = dbu.session.query(dbu.File.filename).filter(dbu.File.filename.contains('OP77')).all()
op77_files = set(map(itemgetter(0), op77_files))
op77_dirs = ['/n/space_data/cda/rbsp/rbspa/MagEphem/def/2012',
             '/n/space_data/cda/rbsp/rbspa/MagEphem/def/2013',
             '/n/space_data/cda/rbsp/rbspa/MagEphem/def/2014',
             '/n/space_data/cda/rbsp/rbspa/MagEphem/def/2015',
             '/n/space_data/cda/rbsp/rbspa/MagEphem/def/2016',
             '/n/space_data/cda/rbsp/rbspa/MagEphem/def/2017',
             '/n/space_data/cda/rbsp/rbspa/MagEphem/def/2018',
             '/n/space_data/cda/rbsp/rbspa/MagEphem/def/2019',
             '/n/space_data/cda/rbsp/rbspb/MagEphem/def/2012',
             '/n/space_data/cda/rbsp/rbspb/MagEphem/def/2013',
             '/n/space_data/cda/rbsp/rbspb/MagEphem/def/2014',
             '/n/space_data/cda/rbsp/rbspb/MagEphem/def/2015',
             '/n/space_data/cda/rbsp/rbspb/MagEphem/def/2016',
             '/n/space_data/cda/rbsp/rbspb/MagEphem/def/2017',
             '/n/space_data/cda/rbsp/rbspb/MagEphem/def/2018',
             '/n/space_data/cda/rbsp/rbspb/MagEphem/def/2019',
             ]


op77_files2 = []
for f in op77_files:
    if 'rbspa' in f:
        if '2012' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2012', f))
        elif '2013' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2013', f))
        elif '2014' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2014', f))
        elif '2015' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2015', f))
        elif '2016' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2016', f))
        elif '2017' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2017', f))
        elif '2018' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2018', f))
        elif '2019' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2019', f))
    elif 'rbspb' in f:
        if '2012' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2012', f))
        elif '2013' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2013', f))
        elif '2014' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2014', f))
        elif '2015' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2015', f))
        elif '2016' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2016', f))
        elif '2017' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2017', f))
        elif '2018' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2018', f))
        elif '2019' in f:
            op77_files2.append(os.path.join('/n/space_data/cda/rbsp/rbspa/MagEphem/def/2019', f))
        

op77_disk = []
for v in op77_dirs:
    op77_disk.extend(glob.glob(os.path.join(v, '*OP*')))

#op77_disk = [os.path.basename(v) for v in op77_disk]

op77_disk = set(op77_disk)
op77_use = op77_disk.difference(op77_files2)


for f in op77_use:
    try:
        os.symlink(f, os.path.join(g_inc_path, os.path.basename(f)))
    except OSError:
        pass
    print("Symlink {0} to {1}".format(f, os.path.join(g_inc_path, os.path.basename(f))))
