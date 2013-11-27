#!/usr/bin/env python2.6

import itertools
import glob
import os
import tempfile
import shutil
import subprocess

import dbprocessing.DBUtils as DBUtils
import dbprocessing.DBlogging as DBlogging

# Code users rsync to build an incremental list of files not already
# processed (i.e. not in /n/space_data/cda/rbsp) and saves that list to a file
# then the file is opened and each file is copied using shutils.copy
# also checks the error directory for the filename and does not copy again

dbu = DBUtils.DBUtils('rbsp')

mission_path = dbu.getMissionDirectory()
g_inc_path = dbu.getIncomingPath()
data_path = os.path.expanduser(os.path.join('/', 'usr', 'local', 'ectsoc', 'data', 'level_0'))
error_path = dbu.getErrorPath()
dbu._closeDB()


def sync_data(sc, inst):
    global mission_path
    global data_path
    global error_path

    try:
        inc_path = g_inc_path.replace('incoming', inst + '_incoming')
    
        data_path_inst = os.path.join(data_path, sc.lower(), inst.lower())
        miss_path_inst = os.path.join(mission_path, 'rbsp'+sc.lower(),
                                      inst, 'level0')

        curdir = os.path.abspath(os.curdir)
        tmp_path = tempfile.mkdtemp(suffix='_dbprocessingIncomming')
        os.chdir(tmp_path)
        
        subprocess.check_call(' '.join(['/usr/bin/rsync ', '--dry-run ', '-auIv ',
                                        os.path.join(data_path_inst, '*'),
                                        miss_path_inst, ' > files.txt']),
                              shell=True )
        
        with open('files.txt', 'r') as fp:
            dat = fp.readlines()
        dat = set([v.strip() for v in dat if '.ptp.gz' in v])
        # get the files in incoming
        dat_error = set([os.path.basename(v) for v in glob.glob(os.path.join(error_path, '*'))])
        dat = dat.difference(dat_error)

        if dat: # no need for a message if we ar movng nothing
            DBlogging.dblogger.info('Copying {0} files to incoming for processing'.format(len(dat)-4)) # the 4 is for header and footer

        for line in dat:
            fname = os.path.join(data_path_inst, line.strip())
            shutil.copy(fname, inc_path)
            #print 'copy', fname, inc_path
            DBlogging.dblogger.debug('Copying {0} to incoming for processing'.format(fname))

    finally:
        os.chdir(curdir)
        shutil.rmtree(tmp_path)


sats = ['a', 'b']
#insts = ['rept', 'mageis', 'hope']
# TODO, change this when the other instements are ready to sync also
insts = ['hope'] #['rept', 'hope'] #, 'mageis', ]

for s, i in itertools.product(sats, insts):
    sync_data(s, i)


############################
## do the magephem
############################

cmdline = ' '.join(['/usr/bin/rsync ', '-auIv ',
                                '/u/ectsoc/data/moc_data/?/ephemerides/*',
                                '/n/space_data/cda/rbsp/MagEphem/incoming'])
subprocess.check_call(cmdline, 
                      shell=True )

cmdline = ' '.join(['/usr/bin/rsync ', '-auIv ',
                                '/u/ectsoc/data/moc_data/?/ephemeris_predict/*',
                                '/n/space_data/cda/rbsp/MagEphem/incoming'])
subprocess.check_call(cmdline, 
                      shell=True )



