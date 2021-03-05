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

import sqlalchemy
from sqlalchemy import schema, types
from sqlalchemy.engine import create_engine
from sqlalchemy.sql import func

from dbprocessing import DButils


class dbprocessing_db(object):
    """
    Main workhorse class for the CreateDB module
    """

    def __init__(self, create=True):
        self.user = os.environ["PGUSER"]
        self.password = ''
        self.db_name = os.environ["PGDATABASE"]
        self.dbIsOpen = False
        if create:
            self.createDB()

    def init_db(self, user, password, db, host='localhost', port=5432):
        url = "postgresql://{0}:{1}@{2}:{3}/{4}"
        url = url.format(user, password, host, port, db)
        self.engine = create_engine(url, echo=False, encoding='utf-8')
        self.metadata = sqlalchemy.MetaData(bind=self.engine)
        self.metadata.reflect()

    def createDB(self):
        """
        Step through and create the DB structure, relationships and constraints
        **Note that order matters here, have to define a Table before you can link to it**

        TODO this can/should all be redone using the new syntax and relations
        see: http://docs.sqlalchemy.org/en/rel_0_7/orm/relationships.html# for
        some examples.

        NOTE: if one stops using sqlite then change file_id, logging_id and file_logging_id
              to BigIntegers (sqlite doesn't know BigInteger)
        """

        self.init_db(self.user, self.password, self.db_name)
        metadata = self.metadata

        data_table = schema.Table('mission', metadata,
                                  schema.Column('mission_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False),
                                  schema.Column('mission_name', types.String(20), nullable=False, unique=True),
                                  schema.Column('rootdir', types.String(150), nullable=False, ),
                                  schema.Column('incoming_dir', types.String(150), nullable=False, ),
                                  schema.Column('codedir', types.String(150), nullable=True, ),
                                  schema.Column('inspectordir', types.String(150), nullable=True, ),
                                  schema.Column('errordir', types.String(150), nullable=True, ),
                                  extend_existing=True)

        data_table = schema.Table('satellite', metadata,
                                  schema.Column('satellite_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False),
                                  schema.Column('satellite_name', types.String(20), nullable=False),  # hmm long enough?
                                  schema.Column('mission_id', types.Integer,
                                                schema.ForeignKey('mission.mission_id'), nullable=False, ),
                                  schema.UniqueConstraint('satellite_name', 'mission_id', name='unique_pairs_satellite'),
                                  extend_existing=True)

        data_table = schema.Table('instrument', metadata,
                                  schema.Column('instrument_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False),
                                  schema.Column('instrument_name', types.String(20), nullable=False),
                                  # hmm long enough?
                                  schema.Column('satellite_id', types.Integer,
                                                schema.ForeignKey('satellite.satellite_id'), nullable=False, ),
                                  schema.UniqueConstraint('instrument_name', 'satellite_id',
                                                          name='unique_pairs_instrument'),
                                  extend_existing=True)

        data_table = schema.Table('product', metadata,
                                  schema.Column('product_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False, index=True),
                                  schema.Column('product_name', types.String(100), nullable=False, index=True),
                                  # hmm long enough?
                                  schema.Column('instrument_id', types.Integer,
                                                schema.ForeignKey('instrument.instrument_id'), nullable=False, ),
                                  schema.Column('relative_path', types.String(100), nullable=False),  # hmm long enough?
                                  schema.Column('level', types.Float, nullable=False),
                                  schema.Column('format', types.Text, nullable=False),  # hmm long enough?
                                  schema.Column('product_description', types.Text, nullable=True),  # hmm long enough?
                                  schema.UniqueConstraint('product_name', 'instrument_id', 'relative_path',
                                                          name='unique_triplet_product'),
                                  extend_existing=True)

        data_table = schema.Table('instrumentproductlink', metadata,
                                  schema.Column('instrument_id', types.Integer,
                                                schema.ForeignKey('instrument.instrument_id'), nullable=False),
                                  schema.Column('product_id', types.Integer,
                                                schema.ForeignKey('product.product_id'), nullable=False),
                                  schema.PrimaryKeyConstraint('instrument_id', 'product_id'),
                                  extend_existing=True)

        data_table = schema.Table('process', metadata,
                                  schema.Column('process_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False, index=True),
                                  schema.Column('process_name', types.String(50), nullable=False),  # hmm long enough?
                                  schema.Column('output_product', types.Integer,
                                                schema.ForeignKey('product.product_id'), nullable=True, index=True),
                                  schema.Column('output_timebase', types.String(10), nullable=True, index=True),
                                  schema.Column('extra_params', types.Text, nullable=True),
                                  schema.UniqueConstraint('process_name', 'output_product'),
                                  extend_existing=True)

        data_table = schema.Table('productprocesslink', metadata,
                                  schema.Column('process_id', types.Integer,
                                                schema.ForeignKey('process.process_id'), nullable=False),
                                  schema.Column('input_product_id', types.Integer,
                                                schema.ForeignKey('product.product_id'), nullable=False),
                                  schema.Column('optional', types.Boolean, nullable=False),
#                                  schema.Column('yesterday', types.Integer, nullable=False),
#                                  schema.Column('tomorrow', types.Integer, nullable=False),
                                  schema.PrimaryKeyConstraint('process_id', 'input_product_id'),
                                  extend_existing=True)

        data_table = schema.Table('file', metadata,
                                  # this was a bigint, sqlalchemy doesn't seem to like this... think here
                                  schema.Column('file_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False, index=True),
                                  schema.Column('filename', types.String(250), nullable=False, unique=True, index=True),
                                  schema.Column('utc_file_date', types.Date, nullable=True, index=True),
                                  schema.Column('utc_start_time', types.DateTime, nullable=True, index=True),
                                  schema.Column('utc_stop_time', types.DateTime, nullable=True, index=True),
                                  schema.Column('data_level', types.Float, nullable=False, index=True),
                                  schema.Column('interface_version', types.SmallInteger, nullable=False),
                                  schema.Column('quality_version', types.SmallInteger, nullable=False),
                                  schema.Column('revision_version', types.SmallInteger, nullable=False),
                                  schema.Column('verbose_provenance', types.Text, nullable=True),
                                  schema.Column('check_date', types.DateTime, nullable=True),
                                  schema.Column('quality_comment', types.Text, nullable=True),
                                  schema.Column('caveats', types.Text, nullable=True),
                                  schema.Column('file_create_date', types.DateTime, nullable=False),
                                  schema.Column('met_start_time', types.Float, nullable=True),
                                  schema.Column('met_stop_time', types.Float, nullable=True),
                                  schema.Column('exists_on_disk', types.Boolean, nullable=False),
                                  schema.Column('quality_checked', types.Boolean, nullable=True, default=False),
                                  schema.Column('product_id', types.Integer,
                                                schema.ForeignKey('product.product_id'), nullable=False),
                                  schema.Column('shasum', types.String(40), nullable=True),
                                  schema.Column('process_keywords', types.Text, nullable=True),
                                  schema.CheckConstraint('utc_stop_time is not NULL OR met_stop_time is not NULL'),
                                  schema.CheckConstraint('utc_start_time is not NULL OR met_start_time is not NULL'),
                                  schema.CheckConstraint('met_start_time <= met_stop_time'),  # in case of one entry
                                  schema.CheckConstraint('utc_start_time <= utc_stop_time'),  # in case of one entry
                                  schema.CheckConstraint('interface_version >= 1'),
                                  schema.UniqueConstraint('utc_file_date',
                                                          'product_id',
                                                          'interface_version',
                                                          'quality_comment',
                                                          'revision_version', name='Unique file tuple'), 
                                  extend_existing=True)
        schema.Index('ix_file_big', data_table.columns['filename'],
                     data_table.columns['utc_file_date'],
                     data_table.columns['utc_start_time'],
                     data_table.columns['utc_stop_time'], unique=True
        )

        data_table = schema.Table('unixtime', metadata,
                                  schema.Column('file_id', types.Integer,
                                                schema.ForeignKey('file.file_id'), primary_key=True, index=True),
                                  schema.Column('unix_start', types.Integer, index=True),
                                  schema.Column('unix_stop', types.Integer, index=True),
                                  schema.CheckConstraint('unix_start <= unix_stop'),
        )

        data_table = schema.Table('filefilelink', metadata,
                                  schema.Column('source_file', types.Integer,
                                                schema.ForeignKey('file.file_id'), nullable=False, index=True),
                                  schema.Column('resulting_file', types.Integer,
                                                schema.ForeignKey('file.file_id'), nullable=False, index=True),
                                  schema.PrimaryKeyConstraint('source_file', 'resulting_file'),
                                  schema.CheckConstraint('source_file <> resulting_file'),
                                  # TODO this is supposed to be more general than !=
                                  extend_existing=True)

        data_table = schema.Table('code', metadata,
                                  schema.Column('code_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False, index=True),
                                  schema.Column('filename', types.String(250), nullable=False, unique=False),
                                  schema.Column('relative_path', types.String(100), nullable=False),
                                  schema.Column('code_start_date', types.Date, nullable=False),
                                  schema.Column('code_stop_date', types.Date, nullable=False),
                                  schema.Column('code_description', types.Text, nullable=False),
                                  schema.Column('process_id', types.Integer,
                                                schema.ForeignKey('process.process_id'), nullable=False, index=True),
                                  schema.Column('interface_version', types.SmallInteger, nullable=False),
                                  schema.Column('quality_version', types.SmallInteger, nullable=False),
                                  schema.Column('revision_version', types.SmallInteger, nullable=False),
                                  schema.Column('output_interface_version', types.SmallInteger, nullable=False),
                                  schema.Column('active_code', types.Boolean, nullable=False, default=False),
                                  schema.Column('date_written', types.Date, nullable=False),
                                  schema.Column('shasum', types.String(40), nullable=True),
                                  schema.Column('newest_version', types.Boolean, nullable=False),
                                  schema.Column('arguments', types.Text, nullable=True),
                                  schema.Column('ram', types.Float, nullable=True),  # amanount of ram used in Gigs
                                  schema.Column('cpu', types.SmallInteger, nullable=True),  # number of cpus used
                                  schema.CheckConstraint('code_start_date <= code_stop_date'),
                                  schema.CheckConstraint('interface_version >= 1'),
                                  schema.CheckConstraint('output_interface_version >= 1'),
                                  extend_existing=True
                                  )

        data_table = schema.Table('processqueue', metadata,
                                  schema.Column('file_id', types.Integer,
                                                schema.ForeignKey('file.file_id'),
                                                primary_key=True, nullable=False, unique=True, index=True),
                                  schema.Column('version_bump', types.SmallInteger, nullable=True),
                                  schema.Column('instrument_id', types.Integer,
                                                schema.ForeignKey('instrument.instrument_id'), nullable=False),
                                  schema.CheckConstraint('version_bump is NULL or version_bump < 3'),
                                  extend_existing=True
                                  )

        data_table = schema.Table('filecodelink', metadata,
                                  schema.Column('resulting_file', types.Integer,
                                                schema.ForeignKey('file.file_id'), nullable=False),
                                  schema.Column('source_code', types.Integer,
                                                schema.ForeignKey('code.code_id'), nullable=False),
                                  schema.PrimaryKeyConstraint('resulting_file', 'source_code'),
                                  extend_existing=True
                                  )

        data_table = schema.Table('release', metadata,
                                  schema.Column('file_id', types.Integer,
                                                schema.ForeignKey('file.file_id'), nullable=False, ),
                                  schema.Column('release_num', types.String(20), nullable=False),
                                  schema.PrimaryKeyConstraint('file_id', 'release_num'),
                                  extend_existing=True
                                  )

        data_table = schema.Table('processpidlink', metadata,
                                  schema.Column('ppl_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False),
                                  schema.Column('pid', types.Integer, nullable=True),
                                  schema.Column('hostname', types.String(100), nullable=True),
                                  schema.Column('process_id', types.Integer,
                                                schema.ForeignKey('process.process_id'), nullable=True),
                                  schema.Column('currentlyprocessing', types.Boolean, nullable=True, default='f'),
                                  schema.Column('start_time', types.DateTime, nullable=True, default=func.now()),
                                  schema.Column('end_time', types.DateTime, nullable=True, default=func.now())
                                  )

        data_table = schema.Table('logging', metadata,
                                  schema.Column('logging_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False),
                                  schema.Column('currently_processing', types.Boolean, nullable=False, default=False),
                                  schema.Column('pid', types.Integer, nullable=True),
                                  schema.Column('processing_start_time', types.DateTime, nullable=False),
                                  # might have to be a TIMESTAMP
                                  schema.Column('processing_end_time', types.DateTime, nullable=True),
                                  schema.Column('comment', types.Text, nullable=True),
                                  schema.Column('mission_id', types.Integer,
                                                schema.ForeignKey('mission.mission_id'), nullable=False),
                                  schema.Column('user', types.String(30), nullable=False),
                                  schema.Column('hostname', types.String(100), nullable=False),
                                  # schema.PrimaryKeyConstraint('logging_id'),
                                  schema.CheckConstraint('processing_start_time < processing_end_time'),
                                  extend_existing=True
                                  )

        data_table = schema.Table('logging_file', metadata,
                                  schema.Column('logging_file_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False),
                                  schema.Column('logging_id', types.Integer,
                                                schema.ForeignKey('logging.logging_id'), nullable=False),
                                  schema.Column('file_id', types.Integer,
                                                schema.ForeignKey('file.file_id'), nullable=False),
                                  schema.Column('code_id', types.Integer,
                                                schema.ForeignKey('code.code_id'), nullable=False),
                                  schema.Column('comments', types.Text, nullable=True),
                                  # schema.PrimaryKeyConstraint('logging_file_id'),
                                  extend_existing=True
                                  )

        data_table = schema.Table('inspector', metadata,
                                  schema.Column('inspector_id', types.Integer, autoincrement=True, primary_key=True,
                                                nullable=False, index=True),
                                  schema.Column('filename', types.String(250), nullable=False, unique=False),
                                  schema.Column('relative_path', types.String(250), nullable=False),
                                  schema.Column('description', types.Text, nullable=False),
                                  schema.Column('interface_version', types.SmallInteger, nullable=False),
                                  schema.Column('quality_version', types.SmallInteger, nullable=False),
                                  schema.Column('revision_version', types.SmallInteger, nullable=False),
                                  schema.Column('output_interface_version', types.SmallInteger, nullable=False),
                                  schema.Column('active_code', types.Boolean, nullable=False, default=False,
                                                index=True),
                                  schema.Column('date_written', types.Date, nullable=False),
                                  schema.Column('shasum', types.String(40), nullable=True),
                                  schema.Column('newest_version', types.Boolean, nullable=False, index=True),
                                  schema.Column('arguments', types.Text, nullable=True),
                                  schema.Column('product', types.Integer,
                                                schema.ForeignKey('product.product_id'), nullable=False),
                                  schema.CheckConstraint('interface_version >= 1'),
                                  schema.CheckConstraint('output_interface_version >= 1'),
                                  extend_existing=True
                                  )

        # TODO move this out so that the user chooses the db type
        # engine = create_engine('postgres:///' + self.filename, echo=False)
        # metadata.bind = engine

        metadata.create_all(checkfirst=True)
        # self.engine = engine
        # self.metadata = metadata

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
    # usage = "usage: %prog [options] filename"
    # parser = OptionParser(usage=usage)

    # (options, args) = parser.parse_args()
    # if len(args) != 1:
    #     parser.error("incorrect number of arguments")
    # filename = os.path.abspath(args[0])

    # if os.path.isfile(filename):
    #     parser.error("file: {0} exists will not overwrite".format(filename))

    db = dbprocessing_db()
