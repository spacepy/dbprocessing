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

from dbprocessing import DButils
import dbprocessing.tables


class dbprocessing_db(object):
    """
    Main workhorse class for the CreateDB module
    """

    def __init__(self, filename='dbprocessing_default.db', overwrite=False,
                 create=True, dialect='sqlite'):
        self.filename = filename
        self.overwrite = overwrite
        self.dialect = dialect
        self.dbIsOpen = False
        if create:
            if os.path.isfile(filename) != True:
                self.createDB()

    def createDB(self):
        """
        Step through and create the DB structure, relationships and constraints

        """
        if self.overwrite:
            raise (NotImplementedError('overwrite is not yet implemented'))

        # TODO move this out so that the user chooses the db type
        if self.dialect == 'sqlite':
            url = 'sqlite:///' + self.filename
        elif self.dialect == 'postgresql':
            url = dbprocessing.DButils.postgresql_url(self.filename)
        else:
            raise ValueError('Unknown dialect {}'.format(self.dialect))

        metadata = schema.MetaData()

        for name in dbprocessing.tables.names:
            data_table = schema.Table(
                name, metadata, *dbprocessing.tables.definition(name))

        engine = create_engine(url, echo=False)
        metadata.bind = engine

        metadata.create_all(checkfirst=True)
        self.engine = engine
        self.metadata = metadata


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

    db = dbprocessing_db(filename=filename, dialect=options.dialect)
