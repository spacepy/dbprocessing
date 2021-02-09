#!/usr/bin/env python

import argparse
import os

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
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database", default=None)
    parser.add_argument('infile', action='store',
                        help='File to update.')
    options = parser.parse_args()

    infile = os.path.expanduser(os.path.expandvars(options.infile))
    if not os.path.isfile(infile):
        parser.error("Input file {0} did not exist".format(infile))

    if not os.path.isfile(options.mission):
        parser.error("Mission database {0} did not exist".format(options.mission))

    dbu = DButils.DButils(options.mission)
    updateSHA(dbu, infile)

    dbu.closeDB()
        
