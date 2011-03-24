#!/usr/bin/env python

"""
Test suite for the Lstar_db file

@author: Brian Larsen
@organization: LANL
@contact: balarsen@lanl.gov

@version: V1: 20-Dec-2010 (BAL)
"""

import unittest
import shutil
import os
import datetime
import socket

from spacepy.datamodel import dmarray
import numpy
import sqlalchemy
import sqlalchemy
from sqlalchemy import schema, types, exceptions, orm, Table
from sqlalchemy.engine import create_engine
from sqlalchemy.exceptions import SQLAlchemyError, IntegrityError

import CreateDB

class CreateDBTests(unittest.TestCase):
    """
    Tests related to CreateDB
    @author: Brian Larsen
    @organization: LANL
    @contact: balarsen@lanl.gov

    @version: V1: 24-Mar-2011 (BAL)
    """

    def setUp(self):
        super(CreateDBTests, self).setUp()
        try:
            os.remove('dbprocessing_test.db')
        except OSError:
            pass
        self.db = CreateDB.dbprocessing_db(filename = 'dbprocessing_test.db', create=True)
        Session = orm.sessionmaker(bind=self.db.engine)
        session = Session()
        self.session = session

    def tearDown(self):
        super(CreateDBTests, self).tearDown()
        #try:
        #    os.remove('dbprocessing_test.db')
        #except OSError:
        #    pass

    def addMission(self):
        """add a mission, convienience routine"""
        mission = self.db.Mission()
        mission.rootdir = 'rootdir'
        mission.mission_name = 'Test me'
        self.session.add(mission)
        self.session.commit()

    def addSatellite(self):
        """add a satellite, convienience routine"""
        if self.session.query(self.db.Mission).count() == 0:
            self.addMission()
        if self.session.query(self.db.Satellite).count() == 0:
            sat = self.db.Satellite()
            sat.mission_id = 1
            sat.satellite_name = 'sat name'
            self.session.add(sat)
            self.session.commit()

    def addInstrument(self):
        if self.session.query(self.db.Satellite).count() == 0:
            self.addSatellite()
        if self.session.query(self.db.Instrument).count() == 0:
            inst = self.db.Instrument()
            inst.satellite_id = 1
            inst.instrument_name = 'inst name'
            self.session.add(inst)
            self.session.commit()

    def addProduct(self):
        if self.session.query(self.db.Instrument).count() == 0:
            self.addInstrument()
        if self.session.query(self.db.Product).count() == 0:
            prod = self.db.Product()
            prod.product_name = 'prod name'
            prod.instrument_id = 1
            prod.relative_path = 'relpath'
            prod.format = 'format'
            self.session.add(prod)
            self.session.commit()

    def test_mission_rootdir(self):
        """Mission table rootdir is not nullable (regression)"""
        mission = self.db.Mission()
        mission.mission_name = 'Test me'
        self.session.add(mission)
        # rootdir is not nullable
        self.assertRaises(IntegrityError, self.session.commit)

    def test_mission_name(self):
        """Mission table mission_name is not nullable (regression)"""
        mission = self.db.Mission()
        mission.rootdir = 'rootdir'
        self.session.add(mission)
        # mission name is not nullable
        self.assertRaises(IntegrityError, self.session.commit)

    def test_mission_nameunique(self):
        """Mission table mission_name is unque (regression)"""
        self.addMission()
        # there should be one entry
        self.assertEqual(self.session.query(self.db.Mission).count(), 1)
        mission = self.db.Mission()
        mission.mission_name = 'Test me'
        mission.rootdir = 'rootdir_diff'
        self.session.add(mission)
        # mission name is unique
        self.assertRaises(IntegrityError, self.session.commit)

    def test_satellite_name(self):
        """Satelite table satellite_name not nullable (regression)"""
        # have to have a mission to have a sat
        self.addMission()
        sat = self.db.Satellite()
        sat.mission_id = 1
        self.session.add(sat)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_satellite_mission_id(self):
        """Satelite table mission_id is not nullable (regression)"""
        sat = self.db.Satellite()
        sat.satellite_name = 'sat name'
        self.session.add(sat)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_satellite_unique(self):
        """Satelite table pairs are unique (regression)"""
        self.addSatellite()
        self.assertEqual(self.session.query(self.db.Satellite).count(), 1)
        # try and readd the same sat
        sat = self.db.Satellite()
        sat.mission_id = 1
        sat.satellite_name = 'sat name'
        self.session.add(sat)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_instrument_satellite_id(self):
        """Instrument table satellite_id cannot be null (regression)"""
        inst = self.db.Instrument()
        inst.instrument_name = 'inst name'
        self.session.add(inst)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_instrument_instrument_name(self):
        """Instrument table instrument_name cannot be null (regression)"""
        # create a satellite (and a mission)
        self.addSatellite()
        # add an instrument (partial)
        inst = self.db.Instrument()
        inst.satellite_id = 1
        self.session.add(inst)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_instrument_unique(self):
        """Instrument table has unique pairs (regression)"""
        # create an instrument
        self.addInstrument()
        # now try and add the same one again
        inst = self.db.Instrument()
        inst.satellite_id = 1
        inst.instrument_name = 'inst name'
        self.session.add(inst)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_product_product_name(self):
        """Product table product_name is not nullable"""
        # create an instrument
        self.addInstrument()
        prod = self.db.Product()
        prod.instrument_id = 1
        prod.relative_path = 'relpath'
        prod.format = 'format'
        self.session.add(prod)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_product_instrument_id(self):
        """Product table product_name is not nullable"""
        prod = self.db.Product()
        prod.product_name = 'prod name'
        prod.relative_path = 'relpath'
        prod.format = 'format'
        self.session.add(prod)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_product_relative_path(self):
        """Product table relative_path is not nullable"""
        # create an instrument
        self.addInstrument()
        prod = self.db.Product()
        prod.product_name = 'prod name'
        prod.instrument_id = 1
        prod.format = 'format'
        self.session.add(prod)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_product_format(self):
        """Product table format is not nullable"""
        # create an instrument
        self.addInstrument()
        prod = self.db.Product()
        prod.product_name = 'prod name'
        prod.instrument_id = 1
        prod.relative_path = 'relpath'
        self.session.add(prod)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_product_unique(self):
        """Product table format has unique triplets"""
        # create a product
        self.addProduct()
        # try and add the same one
        prod = self.db.Product()
        prod.product_name = 'prod name'
        prod.instrument_id = 1
        prod.relative_path = 'relpath'
        prod.format = 'format'
        self.session.add(prod)
        self.assertRaises(IntegrityError, self.session.commit)
        self.session.rollback()
        prod.relative_path = 'relpath2'
        self.session.add(prod)
        self.session.commit()
        self.assertEqual(self.session.query(self.db.Product).count(), 2)

    def test_instrumentproductlink_instrument_id(self):
        """instrumentproductlink table instrument_id is not nullable"""
        # add a product
        self.addProduct()
        ipl = self.db.Instrumentproductlink()
        ipl.product_id = 1
        self.session.add(ipl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_instrumentproductlink_product_id(self):
        """instrumentproductlink table product_id is not nullable"""
        # add an instrument
        self.addInstrument()
        ipl = self.db.Instrumentproductlink()
        ipl.instrument_id = 1
        self.session.add(ipl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_instrumentproductlink_unique(self):
        """instrumentproductlink table has unique pairs"""
        # add a product
        self.addProduct()
        # add an instrument
        self.addInstrument()
        ipl = self.db.Instrumentproductlink()
        ipl.instrument_id = 1
        ipl.product_id = 1
        self.session.add(ipl)
        self.session.commit()
        self.assertEqual(self.session.query(self.db.Instrumentproductlink).count(), 1)
        ipl = self.db.Instrumentproductlink()
        ipl.instrument_id = 1
        ipl.product_id = 1
        self.session.add(ipl)
        self.assertRaises(IntegrityError, self.session.commit)






        #data_table = schema.Table('process', metadata,
        #    schema.Column('process_id', types.Integer, autoincrement=True, primary_key=True, nullable=False),
        #    schema.Column('process_name', types.String(20), nullable=False),  # hmm long enough?
        #    schema.Column('output_product', types.Integer,
        #                  schema.ForeignKey('product.product_id'), nullable=False, unique=True),
        #    schema.Column('super_process_id', types.Integer, nullable=True),
        #)





if __name__ == '__main__':
    unittest.main()
