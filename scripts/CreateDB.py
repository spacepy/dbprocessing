#!/usr/bin/env python
"""
Module to create the database structure for dbprocessing

@author: Brian Larsen
@organization: LANL
@contact: balarsen@lanl.gov

@version: V1: 24-Mar-2011 (BAL)
"""
from __future__ import division  # may not be needed but start with it

import os
from optparse import OptionParser

from sqlalchemy import schema, types
from sqlalchemy.engine import create_engine

from dbprocessing import DButils
import dbprocessing.tables


class dbprocessing_db(object):
    """
    Main workhorse class for the CreateDB module
    """

    def __init__(self, filename='dbprocessing_default.db', overwrite=False, create=True):
        self.filename = filename
        self.overwrite = overwrite
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

        metadata = schema.MetaData()

        for name in dbprocessing.tables.names:
            data_table = schema.Table(
                name, metadata, *dbprocessing.tables.definition(name))

        # TODO move this out so that the user chooses the db type
        engine = create_engine('sqlite:///' + self.filename, echo=False)
        metadata.bind = engine

        metadata.create_all(checkfirst=True)
        self.engine = engine
        self.metadata = metadata

    def addMission(self, filename):
        """utility to add a mission"""
        self.dbu = DButils.DButils(filename)
        self.mission = self.dbu.addMission('rbsp', os.path.join('/', 'n', 'space_data', 'cda', 'rbsp'))

    def addSatellite(self):
        """add satellite utility"""
        self.satellite = self.dbu.addSatellite('rbspa')  # 1
        self.satellite = self.dbu.addSatellite('rbspb')  # 2

    def addInstrument(self):
        """addInstrument utility"""
        self.instrument = self.dbu.addInstrument('hope', 1)
        self.instrument = self.dbu.addInstrument('hope', 2)
        self.instrument = self.dbu.addInstrument('rept', 1)
        self.instrument = self.dbu.addInstrument('rept', 2)
        self.instrument = self.dbu.addInstrument('mageis', 1)
        self.instrument = self.dbu.addInstrument('mageis', 2)


if __name__ == "__main__":
    usage = "usage: %prog [options] filename"
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")
    filename = os.path.abspath(args[0])

    if os.path.isfile(filename):
        parser.error("file: {0} exists will not overwrite".format(filename))

    db = dbprocessing_db(filename=filename)
