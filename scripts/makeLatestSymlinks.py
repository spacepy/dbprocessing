#!/usr/bin/env python

"""
in a given directory make symlinks to all the newest versions of files into another directory
"""

from __future__ import print_function

import argparse
import collections
try:
    import collections.abc
except ImportError:  # Python 2
    collections.abc = collections
import datetime
import glob
from pprint import pprint
import os
import sys
import re
import traceback
import warnings

from dateutil import parser as dup

from dbprocessing import inspector
import dbprocessing.Utils
import dbprocessing.DButils


################################################################
# 1) In the current directory get all the file ids from the directory
# 2) If those files are not current version remove them from set
# 3) Check that the files are in the wanted dates
# 4) Create sumlinks to the file in a specified dir (latest by default)
################################################################


def argsort(seq):
    #http://stackoverflow.com/questions/3382352/equivalent-of-numpy-argsort-in-basic-python/3382369#3382369
    #by ubuntu
    return sorted(range(len(seq)), key=seq.__getitem__)

def get_all_files(indir, outdir, glb='*'):
    """
    in indir get all the files that follow the glob glb
    - indir is a full path
    - glb is a file glob
    """
    files = sorted(glob.glob(os.path.join(indir, glb)))
    files_out = sorted(glob.glob(os.path.join(outdir, glb)))
    return files, files_out

def getBaseVersion(f):
    """
    given an input filename return a tuple of base an version
    """
    base = re.split(r'v\d\d?\.\d\d?\.\d\d?\.', f)[0]
    version = inspector.extract_Version(f)
    return base, version

def cull_to_newest(files, options=None):
    """
    given a list of files cull to only the newest ones

    match everything in front of v\d\d?\.\d\d?\.\d\d?\.
    """
    ans = []
    # make a set of all the file bases
    tmp = [getBaseVersion(f) for f in files]
    tmp = list(zip(*tmp))
    bases = tmp[0]
    versions = tmp[1]
    uniq_bases = list(set(bases))
    while uniq_bases:
        val = uniq_bases.pop(0)
        tmp = [f for f in files if val in f ]
        if len(tmp) > 1:
            inds = [i for i in range(len(files)) if bases[i] == val]
            vers = [versions[i] for i in inds]
            maxver = max(vers)
            maxfile = [files[i] for i in inds if versions[i] == maxver][0]
            ans.append(maxfile)
        else:
            ans.append(tmp[0])
    return ans

def cull_to_dates(files, startdate, enddate, nodate=False, options=None):
    """
    loop over the files and drop the ones that are outside of the range we want to include
    - call this after cull_to_newest()  # maybe doesn't matter
    """
    ans = []
    if nodate:
        return files
    for f in files:
        date = inspector.extract_YYYYMMDD(f)
        if not date:
            date = inspector.extract_YYYYMM(f)
        else:
            date = date.date()
        if not date:
            if options.verbose: print('skipping {0} no date found'.format(f))
            continue
        if date >= startdate and date <= enddate:
            ans.append(f)
        elif options.verbose:
            print("File {0} culled by date".format(f))
    return ans

def toBool(value):
    if value in ['True', 'true', True, 1, 'Yes', 'yes']:
        return True
    else:
        return False

def make_symlinks(files, files_out, outdir, linkdirs, mode, options):
    """
    for all the files make symlinks into outdir
    """
    if isinstance(files, dbprocessing.DButils.str_classes) \
           or not isinstance(files, collections.abc.Iterable):
        files = [files]
    if isinstance(files_out, dbprocessing.DButils.str_classes) \
           or not isinstance(files_out, collections.abc.Iterable):
        files_out = [files_out]
    # if files_out then cull the files to get rid of the ones
    for f in files:
        if not os.path.isdir(outdir):
            if options.verbose:
                print('making outdir: {0} mode:{1}'.format(outdir, int(mode,8)))
            os.makedirs(outdir, int(mode, 8))
        outf = os.path.join(outdir, os.path.basename(f))
        #if options.verbose: print("  f:{0}:{1} outf:{2}:{3}".format(f, os.path.isfile(f),  outf,os.path.isfile(outf) ))

        try:
            if os.path.isfile(f) and not os.path.isfile(outf):
                if options.verbose: print("linking1 {0}->{1}".format(f, outf))
                os.symlink(f, outf)
            elif toBool(linkdirs):
                if options.verbose: print("linking2 {0}->{1}".format(f, outf))
                os.symlink(f, outf)
        except:
            warnings.warn("File {0} not linked:\n\t{1}".format(f, traceback.format_exc()))

def delete_unneeded(files, files_out, options):
    """
    delete the link that are not needed
    """
    files_tmp = [os.path.basename(f) for f in files]
    for f in files_out:
        if os.path.basename(f) not in files_tmp:
            try:
                if os.path.islink(f):
                    os.remove(f)
                else:
                    warnings.warn("Trying to remove a non link: {0}".format(os.path.abspath(f)))
            except OSError:
                pass
            if options.verbose: print("removing unneeded link {0}".format(f))


def readconfig(config_filepath):
    expected_items = ['sourcedir', 'destdir', 'deltadays', 'startdate',
                      'enddate', 'filter', 'linkdirs', 'outmode', 'nodate']
    # Read each parameter in turn
    ans = dbprocessing.Utils.readconfig(config_filepath)
    # make sure that for each section the reqiured items are present
    for k in ans:
        for ei in expected_items:
            if ei not in ans[k]:
                raise ValueError('Section [{0}] does not have required key "{1}"'.format(k, ei))
    # check that we can parse the dates
    for k in ans:
        try:
            tmp = dup.parse(ans[k]['startdate'])
        except:
            raise ValueError('Date "{0}" in [{1}][{2}] is not valid'.format(ans[k]['startdate'], k, 'startdate',))
        try:
            tmp = dup.parse(ans[k]['enddate'])
        except:
            raise ValueError('Date "{0}" in [{1}][{2}] is not valid'.format(ans[k]['enddate'], k, 'enddate'))
        try:
            tmp = int(ans[k]['deltadays'])
        except:
            raise ValueError('Invalid "{0}" in [{1}][{2}]'.format(ans[k]['deltadays'], k, 'deltadays'))
        try:
            tmp = int(ans[k]['outmode'])
        except:
            raise ValueError('Invalid "{0}" in [{1}][{2}]'.format(ans[k]['outmode'], k, 'outmode'))
    for k in ans:
        ans[k]['sourcedir'] = os.path.abspath(os.path.expanduser(os.path.expandvars(ans[k]['sourcedir'])))
        ans[k]['destdir']   = os.path.abspath(os.path.expanduser(os.path.expandvars(ans[k]['destdir'])))
                
    return ans


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose",
                        action='store_true',
                        help="Print out verbose information", default=False)
    parser.add_argument("-l", "--list",
                        action='store_true',
                        help="Instead of syncing list the sections of the conf file", default=False)
    parser.add_argument("-f", "--filter",
                  help="Comma seperated list of strings that must be in the sync conf name (e.g. -f hope,rbspa)", default=None)
    parser.add_argument('config', type=str, help='Configuration file')

    options = parser.parse_args()

    conffile = os.path.abspath(os.path.expanduser((os.path.expandvars(options.config))))
    if not os.path.isfile(conffile):
        parser.error("Config file not readable ({0})".format(conffile))
        
    config = readconfig(conffile)

    config2 = {}
    if options.filter is not None:
        filters = options.filter.split(',')
        if options.verbose:
            print("Filters: {0}".format(filters))
        for c in config:
            num = 0 
            for f in filters:
                if options.verbose: print("Filter {0}".format(filters))
                if f.strip() in c:
                    num += 1
            if num == len(filters):
                config2[c] = config[c]
        config = config2
        
    if options.list:
        out = []
        for c in config:
            pprint(c)
        sys.exit(0)
    pprint(config)

    for sec in config:
        print('Processing [{0}]'.format(sec))
        filter = config[sec]['filter']
        for filt in filter.split(','):
            files = []
            files_out = []
            print(filt.strip())
            files_t, files_out_t = get_all_files(config[sec]['sourcedir'], config[sec]['destdir'], filt.strip())
            #if options.verbose: print files_t
            #if options.verbose: print files_out_t
                        

            files.extend(files_t)
            files_out.extend(files_out_t)
            delete_unneeded(files, files_out, options)
            if files:
                files = cull_to_newest(files, options=options)
                startdate = dup.parse(config[sec]['startdate']).date()
                enddate   = dup.parse(config[sec]['enddate']).date()
                delta     = datetime.date.today() - datetime.timedelta(days = int(config[sec]['deltadays']))
                if delta < enddate:
                    enddate = delta
                if not toBool(config[sec]['nodate']):
                    files = cull_to_dates(files, startdate, enddate, options=options)
            else:
                print('   No files found for [{0}]'.format(sec))
                delete_unneeded(files, files_out, options)
            delete_unneeded(files, files_out, options)
            #if options.verbose: print files
            #if options.verbose: print files_out
            make_symlinks(files, files_out, config[sec]['destdir'], config[sec]['linkdirs'], config[sec]['outmode'], options)

# Example configuration file, copy and remove leading "##" to use
##[isois]
### Directory containing the data files
##sourcedir = ~/dbp_py3/data/ISOIS/level1/
### Directory to make the symlinks in
##destdir = ~/tmp/
### First date to link
##startdate = 2010-01-01
### Last date to link
##enddate = 2021-01-01
### Number of days before present not to link (e.g. to keep internal-only)
##deltadays = 60
### glob for files to match
##filter = psp_isois_l1-sc-hk_*.cdf
### Link directories as well as files
##linkdirs = True
### Mode to use when making output directory
##outmode = 775
### Do not limit based on date (i.e., ignore date options; they're still required)
##nodate = False
