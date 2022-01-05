#!/usr/bin/env python
"""Script to create the database structure for dbprocessing"""

import argparse
import os

import dbprocessing.DButils


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dialect", dest="dialect", default='sqlite',
                        help="sqlalchemy dialect (sqlite or postgresql)")
    parser.add_argument('dbname', action='store', type=str,
                        help='Name of database (or sqlite file) to create.')

    options = parser.parse_args()

    filename = options.dbname
    if options.dialect == 'sqlite':
        filename = os.path.abspath(filename)
        if os.path.isfile(filename):
            parser.error("file: {0} exists will not overwrite".format(filename))

    db = dbprocessing.DButils.create_tables(
        filename=filename, dialect=options.dialect)
