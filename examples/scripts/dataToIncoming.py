#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

from __future__ import division

import ConfigParser
import copy
import datetime
import glob
import fnmatch
import itertools
import shutil
import subprocess
from optparse import OptionParser
import os
from operator import itemgetter, attrgetter
import sys

import dateutil.parser as dup
import numpy as np
import spacepy.toolbox as tb

from dbprocessing import Utils
from dbprocessing import DButils



def readconfig(config_filepath):
    # Create a ConfigParser object, to read the config file
    cfg=ConfigParser.SafeConfigParser()
    cfg.read(config_filepath)
    sections = cfg.sections()
    # Read each parameter in turn
    ans = {}
    for section in sections:
        ans[section] = dict(cfg.items(section))
    return ans

def _fileTest(filename):
    """
    open up the file as txt and do a check that there are no repeated section headers
    """
    def rep_list(inval):
        seen = set()
        seen_twice = set( x for x in inval if x in seen or seen.add(x) )
        return list(seen_twice)

    with open(filename, 'r') as fp:
        data = fp.readlines()
    data = [v.strip() for v in data if v[0] == '[']
    seen_twice = rep_list(data)
    if seen_twice:
        raise(ValueError('Specified section(s): "{0}" is repeated!'.format(seen_twice) ))

def _processSubs(conf):
    """
    go through the conf object and deal with any substitutions

    this works by looking for {}
    """
    for key in conf:
        for v in conf[key]:
            while True:
                if isinstance(conf[key][v], (str,unicode)):
                    if '{' in conf[key][v] and '}' in conf[key][v]:
                        sub = conf[key][v].split('{')[1].split('}')[0]
                        if sub == 'Y':
                            sub_v = '????'
                        else:
                            raise(NotImplementedError("Unsupported substitution {0} found".format(sub)))
                        conf[key][v] = conf[key][v].replace('{' + sub + '}', sub_v)
                    else:
                        break
                else:
                    break
    return conf

def _processBool(conf):
    for k in conf:
        if 'link' in conf[k]:
            conf[k]['link'] = Utils.toBool(conf[k]['link'])
        if 'error' in conf[k]:
            conf[k]['error'] = Utils.toBool(conf[k]['error'])
    return conf


def _processNone(conf):
    for k in conf:
        if 'ignore' in conf[k]:
            conf[k]['ignore'] = Utils.toNone(conf[k]['ignore'])
    return conf
    

def getFilesFromDisk(conf):
    """
    given a partial config file return the files on disk
    ** this returns full path

    this is done as conf['sync1'] from teh upper level
    """
    ans = glob.glob(os.path.join(conf['source'], conf['glob']))
    return ans


def getFilesFromDB(conf, dbu):
    """
    given the same conf get the files form the database
    ** rememebnr that filenames have to be unique

    this returns only basename
    """
    files = dbu.session.query(dbu.File.filename).filter(dbu.File.filename.like(conf['like'])).all()
    files = map(itemgetter(0), files)
    return files

def getFilesFromIncoming(conf, incoming):
    """
    get current files form incoming and don't try and add them again
    """
    files = glob.glob(os.path.join(incoming, conf['glob']))
    return [os.path.basename(v) for v in files]

def getFilesFromError(conf, error):
    """
    get current files form error and don't try and add them again
    """
    files = glob.glob(os.path.join(error, conf['glob']))
    return [os.path.basename(v) for v in files]
 
def basenameToFullname(diff, diskfiles):
    """
    diff is the files that we need to find in diskfiles
    """
    ans = []
    for d in diff:
        a2 = [v for v in diskfiles if d in v]
        ans.extend(a2)
    return ans

def makeLinks(files, incoming, dryrun=False):
    """
    given an incoming desitroy (destination) and files make symlinks between them
    """
    good = 0
    bad = 0
    for f in files:
        newf = os.path.join(incoming, os.path.basename(f))
        try:
            if not dryrun:
                try:
                    os.symlink(f, newf)
                except IOError as e:
                    print( "I/O error({0}): {1} : {2}".format(e.errno, e.strerror, f))
                    continue
            print("Symlink: {0}->{1}".format(f, newf))
            good += 1
        except OSError:
            bad += 1
    return good, bad
    
def copyFiles(files, incoming, dryrun=False):
    """
    given an incoming desitroy (destination) and files copy files between them
    """
    good = 0
    bad = 0
    for f in files:
        newf = os.path.join(incoming, os.path.basename(f))
        try:
            if not dryrun:
                try:
                    shutil.copy(f, incoming)
                except IOError as e:
                    print("I/O error({0}): {1} : {2}".format(e.errno, e.strerror, f))
                    continue                
            print("Copy: {0}->{1}".format(f, newf)) 
            good += 1
        except shutil.Error:
            bad += 1
    return good, bad

def filterConf(conf, filter_opt):
    """
    filter keys from conf based on input
    """
    conf_out = copy.copy(conf)
    for k in conf:
        if not k.startswith('sync'):
            continue       
        filter_count = 0
        len_filter = 0
        if filter_opt is not None:
            len_filter = len(filter_opt.split(','))
            for f in filter_opt.split(','):
                if f in k:
                    filter_count += 1
        if filter_count != len_filter:
            del conf_out[k]
    return conf_out

def printConf(conf, incoming):
    """
    print out the conf file
    """
    print("Mission: {0}".format(conf['settings']['mission']))
    print("  Incomging directory: {0}".format(incoming))
    for k in conf:
        if not k.startswith('sync'):
            continue
        print(k)
        if conf[k]['link']:
            print("    Linking from {0} to incoming".format(os.path.join(conf[k]['source'], conf[k]['glob'])))
        else:
            print("    Copying from {0} to incoming".format(os.path.join(conf[k]['source'], conf[k]['glob'])))


def processIgnore(conf, diskfiles):
    count = 0
    if conf['ignore'] is None:
        return diskfiles, 0
    for v in conf['ignore'].split(','):
        ignore = fnmatch.filter(diskfiles, v)
        for i in ignore:
            diskfiles.remove(i)
            count += 1
    return diskfiles, count    


if __name__ == "__main__":
    usage = "usage: %prog [options] configfile"
    parser = OptionParser(usage=usage)
    parser.add_option("-d", "--dryrun", dest="dryrun", action="store_true",
                      help="only do a dryrun of incoming", default=False)
    parser.add_option("-f", "--filter",
                      dest="filter", 
                      help="Comma seperated list of strings that must be in the sync conf name (e.g. -f mag_l2)", default=None)
    parser.add_option("-l", "--list",
                      dest="list", action='store_true',
                      help="Instead of processing list the sections of the conf file", default=False)
    parser.add_option("-c", "--count",
                      dest="count", action='store_true',
                      help="Instead of copying or linking just print the counts", default=False)

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    conffile = os.path.expanduser(os.path.expandvars(os.path.abspath(args[0])))
    if not os.path.isfile(conffile):
        parser.error("could not read config file: {0}".format(conffile))
        
    conf = readconfig(conffile)
    conf = _processSubs(conf)
    conf = _processBool(conf)
    conf = _processNone(conf)
    print('Read and parsed config file: {0}'.format(conffile))

    dbu = DButils.DButils(conf['settings']['mission'])

    inc_dir = dbu.getIncomingPath()
    err_dir = dbu.getErrorPath()

    conf = filterConf(conf, options.filter)

    if options.list:
        printConf(conf, inc_dir)
        sys.exit(0)

    for k in conf:
        if not k.startswith('sync'):
            continue # this is not a sync
        print("Section: {0}".format(k))
        diskfiles = getFilesFromDisk(conf[k])
        print(" Found {0} files on disk".format(len(diskfiles)))
        diskfiles, ignorenum = processIgnore(conf[k], diskfiles)
        print(" Ignored {0} files on disk, {1} left".format(ignorenum, len(diskfiles)))

        dbfiles = getFilesFromDB(conf[k], dbu)
        print(" Found {0} files in db".format(len(dbfiles)))
        len_tmp = len(dbfiles)
        dbfiles.extend(getFilesFromIncoming(conf[k], inc_dir))
        print("  Added {0} files from incoming".format(len(dbfiles)-len_tmp))
        if conf[k]['error']:
            dbfiles.extend(getFilesFromError(conf[k], err_dir))
            print("  Added {0} files from error".format(len(dbfiles)-len_tmp))


        # the files that need to be linked or copied are the set difference
        df2 = set([os.path.basename(v) for v in diskfiles])
        diff = df2.difference(set(dbfiles))
        # the files in diff need to be found in the full path
        tocopy = basenameToFullname(diff, diskfiles)
        if not options.count:
            if conf[k]['link']:
                g, b = makeLinks(tocopy, inc_dir, options.dryrun)
            else:
                g, b = copyFiles(tocopy, inc_dir, options.dryrun)
            print("  {0} Files successfully placed in {1}.  {2} Failures".format(g, inc_dir, b))

    




        
#############################
# sample config file
#############################
