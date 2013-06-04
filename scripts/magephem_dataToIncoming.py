#!/usr/bin/env python2.6

import datetime
import itertools
import os
import re
import shutil
import subprocess

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

dbu = DBUtils.DBUtils(os.path.expanduser('~ectsoc/MagEphem_processing.sqlite'))

mission_path = dbu.getMissionDirectory()
g_inc_path = dbu.getIncomingPath()

files = dbu.getFilesByProduct('rbsp_pre_kernel')
#files = sorted(files, key=lambda x: x.utc_file_date)[-1]
dbu._closeDB()

files = set(v.filename for v in files)

dates = [datetime.datetime.utcnow().date() + datetime.timedelta(days=v) for v in range(21)]

files_2weeks = []
for dt in dates:
    files_2weeks.append('Setup_{0}.ker'.format(dt.strftime('%Y%m%d')))
files_2weeks = set(files_2weeks)

files_to_make = files_2weeks.difference(files)

for f in files_to_make:
    cmd = [os.path.expanduser('~/.local/bin/newMetaKernel.py'), '-p',
           os.path.join(g_inc_path, f)]
    print(' '.join(cmd))
    subprocess.check_call(cmd, shell=False)





