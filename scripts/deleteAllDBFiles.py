#!/usr/bin/env python

from __future__ import print_function

import argparse

from dbprocessing import DButils

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", dest="mission",
                        help="selected mission database", required=True)
    options = parser.parse_args()

    a = DButils.DButils(options.mission)
    f = a.getAllFilenames(fullPath=False)
    for ff in f:
        a._purgeFileFromDB(ff)
    print('deleted {0} files'.format(len(f)))
    a.closeDB()
