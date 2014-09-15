#!/usr/bin/env python2.6

import datetime
import itertools
import glob
import os
import re
import shutil
import subprocess
import tempfile

import naif

import dbprocessing.DBUtils as DBUtils
import dbprocessing.DBlogging as DBlogging

"""
make a meta kernel each day that is dated 2 weeks out
1) figure out what files we have
2) figure out what 2 weeks from today is
3) make a list of all dates that would be today through 2 weeks
4) if we do not have that file make it
  - if yes, done, if no runnewMetaKernel.py and drop in incoming
"""

def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


dbu = DBUtils.DBUtils(os.path.expanduser('~ectsoc/RBSP_MAGEPHEM_def.sqlite'))

mission_path = dbu.getMissionDirectory()
g_inc_path = dbu.getIncomingPath()

files = dbu.getFilesByProduct('rbspa-def_kernel')
#files = sorted(files, key=lambda x: x.utc_file_date)[-1]
dbu._closeDB()

dbfiles = set(v.filename for v in files)

# make a metakernel and get the end date from it
tmpfile = tempfile.NamedTemporaryFile(delete=False, prefix='magephem_def')
tmpfile.close()
cmd = [os.path.expanduser('~/.local/bin/newMetaKernel.py'), '-d', tmpfile.name]
print(' '.join(cmd))
subprocess.check_call(cmd, shell=False)
naif.spice.load_kern(tmpfile.name)
kern_dates = []
for i in range(naif.spice.n_kernels()):
    try:
        kern_dates.append(naif.spice.kernel_coverage(i).values()[0][0][1])
    except TypeError:
        pass
enddate = min(kern_dates).date()
os.remove(tmpfile.name)

allfiles = []
for result in perdelta(datetime.date(2012, 8, 30), enddate, datetime.timedelta(days=1)):
    allfiles.append('rbspa_Setup_{0}.ker'.format(result.strftime('%Y%m%d')))

allfiles = set(allfiles)

files_to_make = allfiles.difference(dbfiles)

for f in files_to_make:
    cmd = [os.path.expanduser('~/.local/bin/newMetaKernel.py'), '-d',
           os.path.join(g_inc_path, f)]
    print(' '.join(cmd))
    subprocess.check_call(cmd, shell=False)



####################################################################################################


dbu = DBUtils.DBUtils(os.path.expanduser('~ectsoc/RBSP_MAGEPHEM_def.sqlite'))

mission_path = dbu.getMissionDirectory()
g_inc_path = dbu.getIncomingPath()

files = dbu.getFilesByProduct('rbspb-def_kernel')
#files = sorted(files, key=lambda x: x.utc_file_date)[-1]
dbu._closeDB()

dbfiles = set(v.filename for v in files)

# make a metakernel and get the end date from it
tmpfile = tempfile.NamedTemporaryFile(delete=False, prefix='magephem_def')
tmpfile.close()
cmd = [os.path.expanduser('~/.local/bin/newMetaKernel.py'), '-d', tmpfile.name]
print(' '.join(cmd))
subprocess.check_call(cmd, shell=False)
naif.spice.load_kern(tmpfile.name)
kern_dates = []
for i in range(naif.spice.n_kernels()):
    try:
        kern_dates.append(naif.spice.kernel_coverage(i).values()[0][0][1])
    except TypeError:
        pass
enddate = min(kern_dates).date()
os.remove(tmpfile.name)

allfiles = []
for result in perdelta(datetime.date(2012, 8, 30), enddate, datetime.timedelta(days=1)):
    allfiles.append('rbspb_Setup_{0}.ker'.format(result.strftime('%Y%m%d')))

allfiles = set(allfiles)

files_to_make = allfiles.difference(dbfiles)

for f in files_to_make:
    cmd = [os.path.expanduser('~/.local/bin/newMetaKernel.py'), '-d',
           os.path.join(g_inc_path, f)]
    print(' '.join(cmd))
    subprocess.check_call(cmd, shell=False)
    

##################################################################
## # Also need to do the same thing making QinDenton inputs
## ##################################################################
## dbu = DBUtils.DBUtils(os.path.expanduser('~ectsoc/MagEphem_def_processing.sqlite'))

## mission_path = dbu.getMissionDirectory()
## g_inc_path = dbu.getIncomingPath()

## files = dbu.getFilesByProduct('QinDenton_Virbo')
## Virbo_dbfiles = set(v.filename for v in files)

## files = dbu.getFilesByProduct('QinDenton_NRT')
## NRT_dbfiles = set(v.filename for v in files)

## dbu._closeDB()


## Virbo_files = glob.glob('/n/space_data/MagModelInputs/QinDenton/201[23456789]/QinDenton_*_1min.txt')
## Virbo_files = set(['Virbo_' + os.path.basename(v) for v in Virbo_files])

## NRT_files = glob.glob('/n/space_data/MagModelInputs/nrtQinDenton/201[23456789]/QinDenton_*_1min.txt')
## NRT_files = set(['NRT_' + os.path.basename(v) for v in NRT_files])

## files_to_make_virbo = Virbo_files.difference(Virbo_dbfiles)
## files_to_make_nrt   = NRT_files.difference(NRT_dbfiles)

## for f in files_to_make_virbo:
##     cmd = ['touch',  os.path.join(g_inc_path, "{0}".format(f))]
##     print(' '.join(cmd))
##     subprocess.check_call(cmd, shell=False)

## for f in files_to_make_nrt:
##     cmd = ['touch',  os.path.join(g_inc_path, "{0}".format(f))]
##     print(' '.join(cmd))
##     subprocess.check_call(cmd, shell=False)



