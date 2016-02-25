#!/usr/bin/env python

from __future__ import division
from __future__ import print_function

"""

This script sets up a database, processes, and products in the current directory structure

Useful for testing end to end functionality


"""

import glob
import os
import re
import shutil
import subprocess

import dbprocessing.DButils as DBU

DIRBASE = 'Test_DB_'
BASEDIR = os.path.abspath(os.path.dirname(__file__))
FULLPATH = os.path.join(BASEDIR, DIRBASE)
INSPECTORDIR = os.path.join(BASEDIR, 'Scripts', 'inspectors')
CONFIGFILE = os.path.join(BASEDIR, 'Scripts', 'config.txt')
STARTDATE = '20130101'
ENDDATE = '20130101'


def get_number_of_dbs():
    """
    figure out the number of tmp databases we have created
    """
    dirs = get_all_chains()
    return len(dirs)


def get_all_chains():
    """
    return all the temporary chains
    """
    dirs = glob.glob(os.path.join(BASEDIR, '{0}????'.format(DIRBASE)))
    return dirs


def make_new_chain_dir():
    """
    make a new chain
    """
    num = get_number_of_dbs()
    dirname = '{0}{1:04}'.format(DIRBASE, num)
    try:
        os.mkdir(os.path.join(BASEDIR, dirname))
    except Exception, e:
        raise (OSError('Failed making directory: {0}, {1}'.format(dirname, e.message)))
    else:
        print('Made directory: {0}'.format(dirname))
    return dirname


def get_inspectors():
    """
    return the inspectors
    """
    return glob.glob(os.path.join(INSPECTORDIR, '*.py'))


def read_file(fname):
    """
    read in a whole text file
    """
    with open(fname, 'r') as fp:
        txt = fp.readlines()
    return txt


def sub_config_file(cf, dirname):
    """
    given a config file perform substitutions [[]]
    """
    didsub = False
    dat = read_file(cf)
    for i in range(len(dat)):
        rem = re.search(r'.*(\[\[.*\]\]).*', dat[i])
        if rem:
            sub = rem.group(1)
            if 'MISSION_DIR' in sub:
                didsub = True
                dat[i] = dat[i].replace('[[MISSION_DIR]]', dirname)
            elif 'BASE_DIR' in sub:
                didsub = True
                dat[i] = dat[i].replace('[[BASE_DIR]]', BASEDIR)
    if didsub:
        with open(cf, 'w') as fp:
            fp.writelines(dat)
    return didsub


def L0_to_incoming(incdir):
    """
    put L0 files into incoming
    """
    cmd = [os.path.join(BASEDIR, 'Scripts', 'mk_all_l0.py'),
           STARTDATE, ENDDATE, '1.0.0',
           incdir]
    subprocess.check_call(cmd)


def make_new_chain():
    """
    create a whole new chain
    """
    ################################################
    # first we need a new dir
    dirname = make_new_chain_dir()
    ################################################
    # then we need to make some directories in the chain
    for d in ['incoming', 'error', 'L0']:
        dname = os.path.join(dirname, d)
        os.mkdir(dname)
        print("    Made directory: {0}".format(dname))
    ################################################
    # create an empty database in the chain dir
    dbname = os.path.join(dirname, 'Testing_db.sqlite')
    subprocess.check_call([os.path.expanduser(os.path.join('~', 'dbUtils', 'CreateDB.py')),
                           dbname])
    print("        Created database: {0}".format(dbname))
    ################################################
    # copy and populate the config file
    shutil.copy(CONFIGFILE, dirname)
    cfgfile = os.path.join(dirname, os.path.basename(CONFIGFILE))
    os.chmod(cfgfile, 0o700)
    print("        Copied {0}".format(os.path.basename(CONFIGFILE)))
    ################################################
    # substitute the config file
    while True:
        tmp = sub_config_file(cfgfile, dirname)
        if tmp:
            print("            Performed substitutions")
        else:
            break
    ################################################
    # apply the config file to the database
    print('******** Using config file ********')
    subprocess.check_call([os.path.expanduser(os.path.join('~', 'dbUtils', 'addFromConfig.py')),
                           '-m', dbname, cfgfile])
    print('******** End config file ********')
    ################################################
    # Open the database so we can use it
    dbu = DBU.DButils(dbname)
    print('   Opened database: {0}'.format(dbname))
    missionid = dbu.getMissionID(dirname)
    incdir = dbu.getIncomingPath(missionid)

    ################################################
    # create the codes directory and copy inspector in
    os.mkdir(os.path.join(dirname, 'codes'))
    print("    Made directory: {0}".format('codes'))
    os.mkdir(os.path.join(dirname, 'codes/inspectors'))
    print("    Made directory: {0}".format('codes/inspectors'))
    ################################################
    # copy the inspectors into here
    insp = get_inspectors()
    for i in insp:
        shutil.copy(i, os.path.join(dirname, 'codes', 'inspectors'))
        print("        Copied {0}".format(os.path.basename(i)))
    ################################################
    # put some L0 files into incoming
    L0_to_incoming(incdir)
    print("    Putting files into incoming:")
    for f in glob.glob(os.path.join(incdir, '*')):
        print("        {0}".format(os.path.basename(f)))
    ################################################
    # Ingest the L0 files
    print('******** Ingesting L0 ********')
    cmd = [os.path.expanduser(os.path.join('~', 'dbUtils', 'ProcessQueue.py')),
           '-m', dbname, '-i', '--log-level=debug']
    print("    {0}".format(' '.join(cmd)))
    subprocess.check_call(cmd)
    print('******** Done Ingesting L0 ********')


def remove_all_chains():
    """
    remove all craeted temp chains
    """
    dirs = get_all_chains()
    for d in dirs:
        shutil.rmtree(d, )
        print("Removed directory: {0}".format(d))
