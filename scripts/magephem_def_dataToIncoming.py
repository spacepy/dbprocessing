#!/usr/bin/python

import datetime
import itertools
import os
import re
import shutil
import subprocess

import dbprocessing.DButils as DBUtils
import dbprocessing.DBlogging as DBlogging

"""
make a meta kernel each day that is dated 2 weeks out
1) figure out what files we have
2) figure out what 2 weeks from today is
3) make a list of all dates that would be today through 2 weeks
4) if we do not have that file make it
  - if yes, done, if no runnewMetaKernel.py and drop in incoming
"""

dbu = DBUtils.DButils(os.path.expanduser('/home/ectsoc/PROCESSING_DB/magephem_def.sqlite'))

mission_path = dbu.getMissionDirectory()
g_inc_path = dbu.getIncomingPath()

filesa = dbu.getFilesByProduct('rbspa_def_kernel')

#files = sorted(files, key=lambda x: x.utc_file_date)[-1]
dbu.closeDB()

files = set(v.filename for v in filesa)

dt = datetime.datetime(2012, 8, 31)
dates = []
while dt < datetime.datetime.today():
    dates.append(dt)
    dt += datetime.timedelta(days=1)

files_2weeks = []
for dt in dates:
    files_2weeks.append('rbspa_def_kernel_{0}.ker'.format(dt.strftime('%Y%m%d')))
files_2weeks = set(files_2weeks)

files_to_make = files_2weeks.difference(files)

for f in files_to_make:
    cmd = [os.path.expanduser('~/.local/bin/newMetaKernel.py'), '-d',
           os.path.join(g_inc_path, f)]
    print(' '.join(cmd))
    subprocess.check_call(cmd, shell=False)





