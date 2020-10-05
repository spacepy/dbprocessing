#!/usr/bin/env python

"""Migrate a dbprocessing database to latest structure"""

import argparse
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
    parser.add_argument("-y", "--yes", action="store_true", dest="always",
                        default=False,
                          help="Always update without prompting (Default:"
                          " ask before proceeding.)")
    options = parser.parse_args(argv)
    return vars(options)


def check_unix_time(dbu):
    """Check if database needs a table for Unix time

    Parameters
    ==========
    dbu : dbprocessing.DButils.DButils
        Open DButils instances for the mission to update

    Returns
    =======
    bool
        True if update needed (there is no Unix time table);
        False otherwise (Unix time table exists)
    """
    return not hasattr(dbu, 'Unixtime')


def do_unix_time(dbu):
    """Add Unix time table to database

    Parameters
    ==========
    dbu : dbprocessing.DButils.DButils
        Open DButils instances for the mission to update
    """
    dbu.addUnixTimeTable()


checkme = [
    ("Unix time table", check_unix_time, do_unix_time)
]
"""List of all possible updates. tuple of name, function to check if needed,
   function to perform the update. Check functions take the open DBUtils and
   return True if the update is needed, else False."""


def main(mission, always=False):
    """Update a database

    Opens an existing database and updates the structure as needed.

    Parameters
    ==========
    mission : str
        Path to the mission file

    always : bool
        Always update without prompt (default False, prompt before changes)
    """
    dbu = dbprocessing.DButils.DButils(mission)
    needed = []
    """List of (name, function to perform update) for every update needed"""
    for name, checkfunc, dofunc in checkme:
        dothis = checkfunc(dbu)
        print('{}: {}'.format(name, 'PENDING' if dothis else 'up to date'))
        if dothis:
            needed.append((name, dofunc))
    if not needed:
        print('\nNo updates.')
        return
    print('\nWill apply updates: ' + ', '.join([name for name, _ in needed]))
    if always:
        print('Proceeding without prompt.')
    else:
        try:
            ans = raw_input('Proceed (y/n)? ')
        except NameError: #Py3k, no raw_input
            ans = input('Proceed (y/n)? ')
        if ans.lower()[0] != 'y':
            print('Canceling.')
            return
        print('Proceeding.')
    print('\nUpdating.')
    for name, do_func in needed:
        sys.stdout.write('{}: '.format(name))
        sys.stdout.flush()
        do_func(dbu)
        print('done.')
    print('Complete.')
            


if __name__ == "__main__":
    main(**parse_args())

