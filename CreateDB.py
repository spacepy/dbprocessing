#!/usr/bin/env python
"""
Module to create the database structure for dbprocessing

@author: Brian Larsen
@organization: LANL
@contact: balarsen@lanl.gov

@version: V1: 24-Mar-2011 (BAL)
"""
from __future__ import division # may not be needed but start with it

import os

from sqlalchemy import schema, types, Table, orm
from sqlalchemy.engine import create_engine


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
                self._createTableObjects()

    def _createTableObjects(self, verbose = False):
        """
        cycle through the database and build classes for each of the tables

        @keyword verbose: (optional) - print information out to the command line

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 14-Jun-2010 (BAL)
        @version: V2: 25-Aug-2010 (BAL) - chnaged to throuw exception not return False

        >>>  pnl._createTableObjects()
        """
        table_names = self.engine.table_names()

        ## create a dictionary of all the table names that will be used as calss names.
        ## this uses the db table name as the tabel name and a cap 1st letter as the class
        ## when interacting using python use the class
        table_dict = {}
        for val in table_names:
            table_dict[val[0].upper() + val[1:]] = val

        ##  dynamincally create all the classes (c1)
        ##  dynamicallly create all the tables in the db (c2)
        ##  dynaminically create all the mapping between class and table (c3)
        ## this just saves a lot of typing and is equilivant to:
        ##     class Missions(object):
        ##         pass
        ##     missions = Table('missions', metadata, autoload=True)
        ##     mapper(Missions, missions)
        for val in table_dict:
            if verbose: print val
            if not hasattr(self, val):  # then make it
                c1 = compile("""class %s(object):\n\tpass""" % (val), '', 'exec')
                c2 = compile("%s = Table('%s', self.metadata, autoload=True)" % (str(table_dict[val]), table_dict[val]) , '', 'exec')
                c3 = compile("orm.mapper(%s, %s)" % (val, str(table_dict[val])), '', 'exec')
                c4 = compile("self.%s = %s" % (val, val), '', 'exec' )
                exec(c1)
                exec(c2)
                exec(c3)
                exec(c4)
                if verbose: print("Class %s created" % (val))

    def createDB(self):
        """
        Step through and create the DB structre, relationships and constriants
        **Note that order matters here, have to defive a Table before you can link to it**
        """
        if self.overwrite:
            raise(NotImplementedError('overwrite is not yet implemented'))

        metadata = schema.MetaData()

        data_table = schema.Table('mission', metadata,
            schema.Column('mission_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('mission_name', types.String(20), nullable=False, unique=True),  # hmm long enough?
            schema.Column('rootdir', types.String(20), nullable=False,),
        )

        data_table = schema.Table('satellite', metadata,
            schema.Column('satellite_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('satellite_name', types.String(20), nullable=False),  # hmm long enough?
            schema.Column('mission_id', types.Integer,
                          schema.ForeignKey('mission.mission_id'), nullable=False,),
            schema.UniqueConstraint('satellite_name', 'mission_id', name='unique_pairs_satellite')
        )

        data_table = schema.Table('instrument', metadata,
            schema.Column('instrument_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('instrument_name', types.String(20), nullable=False),  # hmm long enough?
            schema.Column('satellite_id', types.Integer,
                          schema.ForeignKey('satellite.satellite_id'), nullable=False,),
            schema.UniqueConstraint('instrument_name', 'satellite_id', name='unique_pairs_instrument')
        )

        data_table = schema.Table('product', metadata,
            schema.Column('product_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('product_name', types.String(30), nullable=False),  # hmm long enough?
            schema.Column('instrument_id', types.Integer,
                          schema.ForeignKey('instrument.instrument_id'), nullable=False,),
            schema.Column('relative_path', types.String(50), nullable=False),  # hmm long enough?
            schema.Column('super_product_id', types.Integer, nullable=True),
            schema.Column('format', types.String(20), nullable=False),  # hmm long enough?
            schema.UniqueConstraint('product_name', 'instrument_id', 'relative_path', name='unique_triplet_product')
        )

        data_table = schema.Table('instrumentproductlink', metadata,
            schema.Column('instrument_id', types.Integer,
                          schema.ForeignKey('instrument.instrument_id'), nullable=False),
            schema.Column('product_id', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False),
            schema.PrimaryKeyConstraint('instrument_id', 'product_id' )
        )

        data_table = schema.Table('process', metadata,
            schema.Column('process_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('process_name', types.String(20), nullable=False),  # hmm long enough?
            schema.Column('output_product', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False, unique=True),
            schema.Column('super_process_id', types.Integer, nullable=True),
            schema.UniqueConstraint('process_name', 'output_product')
        )

        data_table = schema.Table('productprocesslink', metadata,
            schema.Column('process_id', types.Integer,
                          schema.ForeignKey('process.process_id'), nullable=False),
            schema.Column('input_product_id', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False),
            schema.PrimaryKeyConstraint('process_id', 'input_product_id' )
        )

        data_table = schema.Table('file', metadata,
                # this was a bigint, sqlalchemy doesnt seem to like this... think here
            schema.Column('file_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('filename', types.String(50), nullable=False, unique=True),  # hmm long enough?
            schema.Column('utc_file_date', types.Date, nullable=True),
            schema.Column('utc_start_time', types.DateTime, nullable=True),  # might have to be a TIMESTAMP
            schema.Column('utc_stop_time', types.DateTime, nullable=True),
            schema.Column('data_level', types.Float, nullable=False),
            schema.Column('interface_version', types.SmallInteger, nullable=False),
            schema.Column('quality_version', types.SmallInteger, nullable=False),
            schema.Column('revision_version', types.SmallInteger, nullable=False),
            schema.Column('verbose_provenance', types.String(500), nullable=True),
            schema.Column('check_date', types.DateTime, nullable=True),
            schema.Column('quality_comment', types.String(100), nullable=True),
            schema.Column('caveats', types.String(20), nullable=True),
            schema.Column('release_number', types.String(2), nullable=True),
            schema.Column('file_create_date', types.DateTime, nullable=False),
            schema.Column('met_start_time', types.BigInteger, nullable=True),
            schema.Column('met_stop_time', types.BigInteger, nullable=True),
            schema.Column('exists_on_disk', types.Boolean, nullable=False),
            schema.Column('quality_checked', types.Boolean, nullable=True, default=False),
            schema.Column('product_id', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False),
            schema.Column('md5sum', types.String(40), nullable=True),
            schema.Column('newest_version', types.Boolean, nullable=False),
            schema.CheckConstraint('utc_stop_time is not NULL OR met_stop_time is not NULL'),
            schema.CheckConstraint('utc_start_time is not NULL OR met_start_time is not NULL'),
            schema.CheckConstraint('met_start_time < met_stop_time'),
            schema.CheckConstraint('utc_start_time < utc_stop_time'),
            schema.CheckConstraint('interface_version >= 1'),
        )

        data_table = schema.Table('filefilelink', metadata,
            schema.Column('source_file', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False),
            schema.Column('resulting_file', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False),
            schema.PrimaryKeyConstraint('source_file', 'resulting_file' )
        )

        data_table = schema.Table('code', metadata,
            schema.Column('code_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('filename', types.String(50), nullable=False, unique=True),  # hmm long enough?
            schema.Column('relative_path', types.String(50), nullable=False),
            schema.Column('code_start_date', types.Date, nullable=False),  # might have to be a TIMESTAMP
            schema.Column('code_stop_date', types.Date, nullable=False),
            schema.Column('code_description', types.String(50), nullable=False),
            schema.Column('process_id', types.Integer,
                          schema.ForeignKey('process.process_id'), nullable=False),
            schema.Column('interface_version', types.SmallInteger, nullable=False),
            schema.Column('quality_version', types.SmallInteger, nullable=False),
            schema.Column('revision_version', types.SmallInteger, nullable=False),
            schema.Column('output_interface_version', types.SmallInteger, nullable=False),
            schema.Column('active_code', types.Boolean, nullable=False, default=False),
            schema.Column('date_written', types.Date, nullable=False),
            schema.Column('md5sum', types.String(40), nullable=True),
            schema.Column('newest_version', types.Boolean, nullable=False),
            schema.Column('arguments', types.Text, nullable=False),
            schema.CheckConstraint('code_start_date < code_stop_date'),
            schema.CheckConstraint('interface_version >= 1'),
            schema.CheckConstraint('output_interface_version >= 1'),
        )

        data_table = schema.Table('filecodelink', metadata,
            schema.Column('resulting_file', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False),
            schema.Column('source_code', types.Integer,
                          schema.ForeignKey('code.code_id'), nullable=False),
            schema.PrimaryKeyConstraint('resulting_file', 'source_code' )
        )

        data_table = schema.Table('logging', metadata,
            schema.Column('logging_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('currently_processing', types.Boolean, nullable=False, default=False),
            schema.Column('pid', types.Integer, nullable=True),
            schema.Column('processing_start_time', types.DateTime, nullable=False),  # might have to be a TIMESTAMP
            schema.Column('processing_end_time', types.DateTime, nullable=True),
            schema.Column('comment', types.Text, nullable=True),
            schema.Column('mission_id', types.Integer,
                          schema.ForeignKey('mission.mission_id'), nullable=False),
            schema.Column('user', types.String(20), nullable=False),
            schema.Column('hostname', types.String(50), nullable=False),
            #schema.PrimaryKeyConstraint('logging_id'),
            schema.CheckConstraint('processing_start_time < processing_end_time'),
        )

        data_table = schema.Table('logging_file', metadata,
            schema.Column('logging_file_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('logging_id', types.Integer,
                          schema.ForeignKey('logging.logging_id'), nullable=False),
            schema.Column('file_id', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False),
            schema.Column('code_id', types.Integer,
                          schema.ForeignKey('code.code_id'), nullable=False),
            schema.Column('comments', types.Text, nullable=True),
            #schema.PrimaryKeyConstraint('logging_file_id'),
        )

        data_table = schema.Table('inspector', metadata,
            schema.Column('inspector_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
            schema.Column('filename', types.String(50), nullable=False, unique=True),  # hmm long enough?
            schema.Column('relative_path', types.String(50), nullable=False),
            schema.Column('description', types.String(50), nullable=False),
            schema.Column('interface_version', types.SmallInteger, nullable=False),
            schema.Column('quality_version', types.SmallInteger, nullable=False),
            schema.Column('revision_version', types.SmallInteger, nullable=False),
            schema.Column('output_interface_version', types.SmallInteger, nullable=False),
            schema.Column('active_code', types.Boolean, nullable=False, default=False),
            schema.Column('date_written', types.Date, nullable=False),
            schema.Column('md5sum', types.String(40), nullable=True),
            schema.Column('newest_version', types.Boolean, nullable=False),
            schema.Column('arguments', types.Text, nullable=False),
            schema.CheckConstraint('interface_version >= 1'),
            schema.CheckConstraint('output_interface_version >= 1'),
            schema.Column('product', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False),
        )


        #TODO move this out so that the user chooses the db type
        engine = create_engine('sqlite:///' + self.filename, echo=False)
        metadata.bind = engine

        metadata.create_all(checkfirst=True)
        self.engine = engine
        self.metadata = metadata

if __name__ == "__main__":
    # as a demo create the db in sqlite
    try:
        os.remove('dbprocessing_main.db')
    except OSError:
        pass
    db = dbprocessing_db(filename = 'dbprocessing_main.db')
