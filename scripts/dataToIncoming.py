#!/usr/bin/env python2.6

import os
import tempfile
import shutil
import subprocess

import dbprocessing.DBUtils as DBUtils
import dbprocessing.DBlogging as DBlogging

dbu = DBUtils.DBUtils('rbsp')


mission_path = dbu.getMissionDirectory()
inc_path = dbu.getIncomingPath()
data_path = os.path.expanduser(os.path.join('~ectsoc', 'data', 'level_0'))

data_path_repta = os.path.join(data_path, 'a', 'rept')
miss_path_repta = os.path.join(mission_path, 'rbspa', 'rept', 'level0')

curdir = os.path.abspath(os.curdir)
tmp_path = tempfile.mkdtemp(suffix='_dbprocessing')
os.chdir(tmp_path)

subprocess.check_call(' '.join(['/usr/bin/rsync ', '--dry-run ', '-auIv ',
                                os.path.join(data_path_repta, '*'), miss_path_repta, ' > files.txt']), shell=True )

with open('files.txt', 'r') as fp:
    dat = fp.readlines()
DBlogging.dblogger.info('Copying {0} files to incoming for processing'.format(len(dat)-5))
for line in dat:
    if '.ptp.gz' in line: # file we want to move
        fname = os.path.join(data_path_repta, line.strip())
        shutil.copy(fname, inc_path)
        DBlogging.dblogger.debug('Copying {0} to incoming for processing'.format(fname))

os.chdir(curdir)
shutil.rmtree(tmp_path)




