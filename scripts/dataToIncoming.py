#!/usr/bin/env python2.6

import itertools
import os
import tempfile
import shutil
import subprocess

import dbprocessing.DBUtils as DBUtils
import dbprocessing.DBlogging as DBlogging

# Code users rsync to build an incremental list of files not already
# processed (i.e. not in /n/space_data/cda/rbsp) and saves that list to a file
# then the file is opened and each file is copied using shutils.copy

dbu = DBUtils.DBUtils('rbsp')

mission_path = dbu.getMissionDirectory()
inc_path = dbu.getIncomingPath()
data_path = os.path.expanduser(os.path.join('~ectsoc', 'data', 'level_0'))

def sync_data(sc, inst):
    global mission_path
    global inc_path
    global data_path
    data_path_inst = os.path.join(data_path, sc.lower(), inst.lower())
    miss_path_inst = os.path.join(mission_path, 'rbsp'+sc.lower(),
                                  inst, 'level0')

    curdir = os.path.abspath(os.curdir)
    tmp_path = tempfile.mkdtemp(suffix='_dbprocessing')
    os.chdir(tmp_path)
    
    subprocess.check_call(' '.join(['/usr/bin/rsync ', '--dry-run ', '-auIv ',
                                    os.path.join(data_path_inst, '*'),
                                    miss_path_inst, ' > files.txt']),
                          shell=True )
    
    with open('files.txt', 'r') as fp:
        dat = fp.readlines()
    DBlogging.dblogger.info('Copying {0} files to incoming for processing'.format(len(dat)-4)) # the 4 is for header and footer
    for line in dat:
        if '.ptp.gz' in line: # file we want to move
            fname = os.path.join(data_path_inst, line.strip())
            shutil.copy(fname, inc_path)
            DBlogging.dblogger.debug('Copying {0} to incoming for processing'.format(fname))
            
    os.chdir(curdir)
    shutil.rmtree(tmp_path)


sats = ['a', 'b']
#insts = ['rept', 'mageis', 'hope']
# TODO, change this when the other instements are ready to sync also
insts = ['rept'] #, 'mageis', 'hope']

for s, i in itertools.product(sats, insts):
    sync_data(s, i)
    #print s, i


