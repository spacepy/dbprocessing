#!/usr/bin/env python2.6

import itertools
import glob
import os
from optparse import OptionParser
import re
import sys
import traceback
import warnings

from spacepy import pycdf

from rbsp import Version


from dbprocessing import inspector


import dbprocessing.DBlogging as DBlogging
import dbprocessing.dbprocessing as dbprocessing
from dbprocessing import DBUtils

"""
go into the database and update the shasum entry for a file that is changed after ingestion
"""


def getSHA(options, dbu, filename):
    """
    go into the db and get the VP
    """
    try:
        sha = dbu.getEntry('File', dbu.getFileID(os.path.basename(filename))).shasum
    except DBUtils.DBNoData:
        sha = 'NOFILE'
    return sha

def updateSHA(options, dbu, filename):
    """
    update the shasum in the db
    """
    file = dbu.getEntry('File', dbu.getFileID(os.path.basename(filename)))
    file.shasum = DBUtils.calcDigest(filename)
    dbu.session.commit()

    

if __name__ == '__main__':
    usage = "usage: %prog infile"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("incorrect number of arguments")

    infile = os.path.expanduser(os.path.expandvars(args[0]))
    if not os.path.isfile(infile):
        parser.error("Input file {0} did not exist".format(infile))

    if not os.path.isfile(options.mission):
        parser.error("Mission database {0} did not exist".format(options.mission))

    dbu = DBUtils.DBUtils(options.mission)

    dbsha = getSHA(options, dbu, infile)
    if dbsha == 'NOFILE': # file not in db we are done
        sys.exit(-1)

    updateSHA(options, dbu, filename)

    dbu.closeDB()
        
