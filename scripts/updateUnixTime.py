#!/usr/bin/env python

"""Update Unix file timestamps in a database from the UTC start/stop time"""

import argparse
import datetime
import sys

import dbprocessing.DButils


def parse_args(argv=None):
    """Parse arguments for this script

    Parameters
    ==========
    argv : list
        Argument list, default from sys.argv

    Returns
    =======
    options : argparse.Values
        Arguments from command line, from flags and non-flag arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database")
    options = parser.parse_args(argv)
    return vars(options)


def main(mission):
    """Rewrite Unix timestamps for a database.

    Opens an existing database and rewrites all Unix timestamps based on the
    UTC file timestamps.

    Parameters
    ==========
    mission : str
        Path to the mission file
    """
    dbu = dbprocessing.DButils.DButils(mission)
    unx0 = datetime.datetime(1970, 1, 1)
    for f in dbu.getFiles(): # Populate the times
        r = dbu.getEntry('Unixtime', f.file_id)
        # If changed, also change addFile, addUnixTimeTable, getFiles
        # (all in DButils)
        r.unix_start = int((f.utc_start_time - unx0)\
                           .total_seconds())
        r.unix_stop = int((f.utc_stop_time - unx0)\
                          .total_seconds())
    dbu.commitDB()


if __name__ == "__main__":
    main(**parse_args())

