#!/usr/bin/env python

import os
from optparse import OptionParser

from dbprocessing import DButils

"""
go into the database and update the shasum entry for a file that is changed after ingestion
"""

def updateSHA(dbu, filename):
    """
    update the shasum in the db
    """
    file = dbu.getEntry('File', dbu.getFileID(os.path.basename(filename)))
    file.shasum = DButils.calcDigest(filename)
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

    dbu = DButils.DButils(options.mission)
    updateSHA(dbu, infile)

    dbu.closeDB()
        
