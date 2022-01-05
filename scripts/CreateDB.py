#!/usr/bin/env python
"""
Module to create the database structure for dbprocessing

@author: Brian Larsen
@organization: LANL
@contact: balarsen@lanl.gov

@version: V1: 24-Mar-2011 (BAL)
"""
from __future__ import division  # may not be needed but start with it

import argparse
import os

from sqlalchemy import schema, types
from sqlalchemy.engine import create_engine

import dbprocessing.DButils
import dbprocessing.tables


def create_tables(filename='dbprocessing_default.db', dialect='sqlite'):
    """
    Step through and create the DB structure, relationships and constraints

    """
    # TODO move this out so that the user chooses the db type
    if dialect == 'sqlite':
        url = 'sqlite:///' + filename
    elif dialect == 'postgresql':
        url = dbprocessing.DButils.postgresql_url(filename)
    else:
        raise ValueError('Unknown dialect {}'.format(dialect))

    metadata = schema.MetaData()
    for name in dbprocessing.tables.names:
        data_table = schema.Table(
            name, metadata, *dbprocessing.tables.definition(name))
    engine = create_engine(url, echo=False)
    metadata.bind = engine
    metadata.create_all(checkfirst=True)
    engine.dispose()


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

    db = create_tables(filename=filename, dialect=options.dialect)
