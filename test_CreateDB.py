#!/usr/bin/env python

"""
Test suite for the Lstar_db file

@author: Brian Larsen
@organization: LANL
@contact: balarsen@lanl.gov

@version: V1: 20-Dec-2010 (BAL)
"""

try:
    import unittest_pretty as ut
except ImportError:
    import unittest as ut
import unittest

import os
import datetime

from sqlalchemy import orm
from sqlalchemy.exceptions import IntegrityError

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
        del self.db
        try:
            os.remove('dbprocessing_test.db')
        except OSError:
            pass

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

    def addProcess(self):
        if self.session.query(self.db.Product).count() == 0:
            prod = self.db.Product()
        if self.session.query(self.db.Process).count() == 0:
            proc = self.db.Process()
            proc.process_name = 'proc name'
            proc.output_product = 1
            self.session.add(proc)
            self.session.commit()

    def addFile(self):
        if self.session.query(self.db.Product).count() == 0:
            prod = self.db.Product()
        if self.session.query(self.db.File).count() == 0:
            file = self.db.File()
            file.filename = 'filename'
            file.data_level = 1
            file.interface_version = 1
            file.quality_version = 1
            file.revision_version = 1
            file.file_create_date = datetime.datetime.now()
            file.exists_on_disk = False
            file.quality_checked = False
            file.product_id = 1
            file.newest_version = True
            file.utc_start_time = datetime.datetime(2000, 1, 1)
            file.utc_stop_time = datetime.datetime(2000, 1, 2)
            self.session.add(file)
            self.session.commit()

    def addCode(self):
        # add a process
        if self.session.query(self.db.Process).count() == 0:
            prod = self.db.Process()
        if self.session.query(self.db.Code).count() == 0:
            code = self.db.Code()
            code.filename = 'code filename'
            code.relative_path = 'rel path'
            code.code_start_date = datetime.datetime(2000, 1, 1)
            code.code_stop_date = datetime.datetime(2000, 1, 10)
            code.code_description = 'code_description'
            code.process_id = 1
            code.interface_version = 1
            code.quality_version = 0
            code.revision_version = 0
            code.output_interface_version = 1
            code.active_code = True
            code.date_written = datetime.datetime(2000, 1, 1)
            code.newest_version = True
            code.arguments = 'args'
            self.session.add(code)
            self.session.commit()

    def addLogging(self):
        if self.session.query(self.db.Mission).count() == 0:
            self.addMission()
        if self.session.query(self.db.Logging).count() == 0:
            log = self.db.Logging()
            log.currently_processing = False
            log.processing_start_time = datetime.datetime(2000, 1, 1, 1, 1, 1)
            log.mission_id = 1
            log.user = 'username'
            log.hostname = 'hostname'
            self.session.add(log)
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

    def test_process_process_name(self):
        """Process table process_name not null"""
        # add a product
        self.addProduct()
        proc = self.db.Process()
        proc.output_product = 1
        self.session.add(proc)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_process_output_product(self):
        """Process table output_product not null"""
        # add a product
        self.addProduct()
        proc = self.db.Process()
        proc.process_name = 'proc name'
        self.session.add(proc)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_process_unique(self):
        """Process table has unique pairs"""
        # add a product
        self.addProduct()
        # add a Process
        self.addProcess()
        proc = self.db.Process()
        proc.process_name = 'proc name'
        proc.output_product = 1
        self.session.add(proc)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_productprocesslink_process_id(self):
        """productprocesslink table process_id is not nullable"""
        # add process
        self.addProcess()
        ppl = self.db.Productprocesslink()
        ppl.input_product_id = 1
        self.session.add(ppl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_productprocesslink_input_product_id(self):
        """productprocesslink table process_id is not nullable"""
        # add process
        self.addProcess()
        ppl = self.db.Productprocesslink()
        ppl.process_id = 1
        self.session.add(ppl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_productprocesslink_unique(self):
        """productprocesslink table has unique pairs"""
        # add process
        self.addProcess()
        ppl = self.db.Productprocesslink()
        ppl.process_id = 1
        ppl.input_product_id = 1
        self.session.add(ppl)
        self.session.commit()
        self.assertEqual(self.session.query(self.db.Productprocesslink).count(), 1)
        ppl = self.db.Productprocesslink()
        ppl.process_id = 1
        ppl.input_product_id = 1
        self.session.add(ppl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_filename(self):
        """file table filename cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        # file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_data_level(self):
        """file table data_level cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        # file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_interface_version(self):
        """file table interface_version cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        # file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_interface_version_range(self):
        """file table interface_version cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 0
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_quality_version(self):
        """file table quality_version cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        # file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_revision_version(self):
        """file table revision_version cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        # file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_file_create_date(self):
        """file table file_create_date cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        # file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_exists_on_disk(self):
        """file table exists_on_disk cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        # file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_product_id(self):
        """file table product_id cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        # file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_newest_version(self):
        """file table newest_version cannot be null"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        # file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_utc_met_start(self):
        """file table either utc or met spcified start"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        # file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_utc_met_stop(self):
        """file table either utc or met spcified start"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        # file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_utc_start_stop(self):
        """file table utc_start_time < utc_stop_time"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 2)
        file.utc_stop_time = datetime.datetime(2000, 1, 1)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_met_start_stop(self):
        """file table met_start_time < met_stop_time"""
        # add a product
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        # file.utc_start_time = datetime.datetime(2000, 1, 2)
        # file.utc_stop_time = datetime.datetime(2000, 1, 1)
        file.met_start_time = 100
        file.met_stop_time  =  99
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_file_filename_unique(self):
        """file table filename is unique"""
        self.addFile()
        self.addProduct()
        file = self.db.File()
        file.filename = 'filename'
        file.data_level = 1
        file.interface_version = 1
        file.quality_version = 1
        file.revision_version = 1
        file.file_create_date = datetime.datetime.now()
        file.exists_on_disk = False
        file.quality_checked = False
        file.product_id = 1
        file.newest_version = True
        file.utc_start_time = datetime.datetime(2000, 1, 1)
        file.utc_stop_time = datetime.datetime(2000, 1, 2)
        self.session.add(file)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_filefilelink_source_file(self):
        """filefilelink table source_file is not nullable"""
        # add a file
        self.addFile()
        ffl = self.db.Filefilelink()
        ffl.resulting_file = 1
        # ffl.source_file = 1
        self.session.add(ffl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_filefilelink_resulting_file(self):
        """filefilelink table resulting_file is not nullable"""
        self.addFile()
        ffl = self.db.Filefilelink()
        # ffl.resulting_file = 1
        ffl.source_file = 1
        self.session.add(ffl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_filefilelink_unique(self):
        """filefilelink table unique pairs"""
        self.addFile()
        ffl = self.db.Filefilelink()
        ffl.resulting_file = 1
        ffl.source_file = 1
        self.session.add(ffl)
        self.session.commit()
        self.assertEqual(self.session.query(self.db.Filefilelink).count(), 1)
        ffl = self.db.Filefilelink()
        ffl.resulting_file = 1
        ffl.source_file = 1
        self.session.add(ffl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_filename(self):
        """code table filename is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        # code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_relative_path(self):
        """code table relative_path is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        # code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_code_start_date(self):
        """code table code_start_date is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        # code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_code_stop_date(self):
        """code table code_stop_date is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        # code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_code_description(self):
        """code table code_description is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        # code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_process_id(self):
        """code table process_id is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        # code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_interface_version(self):
        """code table interface_version is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        # code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_interface_version_range(self):
        """code table interface_version is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 0
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_quality_version(self):
        """code table quality_version is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        # code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_revision_version(self):
        """code table revision_version is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        # code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_output_interface_version(self):
        """code table output_interface_version is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        # code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_active_code(self):
        """code table active_code is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        # code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.session.commit()
        self.assertEqual(self.session.query(self.db.Code).count(), 1)
        self.assertEqual(self.session.query(self.db.Code.active_code).all()[0][0], False)

    def test_code_date_written(self):
        """code table date_written is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        # code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_newest_version(self):
        """code table newest_version is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        # code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_arguments(self):
        """code table arguments is not nullable"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 1)
        code.code_stop_date = datetime.datetime(2000, 1, 10)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        # code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_code_code_stopstart_order(self):
        """code table code_stop_date code_start_date in order"""
        # add a process
        self.addProcess()
        code = self.db.Code()
        code.filename = 'code filename'
        code.relative_path = 'rel path'
        code.code_start_date = datetime.datetime(2000, 1, 10)
        code.code_stop_date = datetime.datetime(2000, 1, 1)
        code.code_description = 'code_description'
        code.process_id = 1
        code.interface_version = 1
        code.quality_version = 0
        code.revision_version = 0
        code.output_interface_version = 1
        code.active_code = True
        code.date_written = datetime.datetime(2000, 1, 1)
        code.newest_version = True
        code.arguments = 'args'
        self.session.add(code)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_filecodelink_resulting_file(self):
        """filecodelink table resulting_file not null"""
        # add Code
        self.addCode()
        # add file
        self.addFile()
        fcl = self.db.Filecodelink()
        # fcl.resulting_file = 1
        fcl.source_code = 1
        self.session.add(fcl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_filecodelink_source_code(self):
        """filecodelink table source_code not null"""
        # add Code
        self.addCode()
        # add file
        self.addFile()
        fcl = self.db.Filecodelink()
        fcl.resulting_file = 1
        # fcl.source_code = 1
        self.session.add(fcl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_filecodelink_unique(self):
        """filecodelink table unique pairs"""
        # add Code
        self.addCode()
        # add file
        self.addFile()
        fcl = self.db.Filecodelink()
        fcl.resulting_file = 1
        fcl.source_code = 1
        self.session.add(fcl)
        self.session.commit()
        self.assertEqual(self.session.query(self.db.Filecodelink).count(), 1)
        fcl = self.db.Filecodelink()
        fcl.resulting_file = 1
        fcl.source_code = 1
        self.session.add(fcl)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_logging_currently_processing(self):
        """logging table currently_processing has a default"""
        log = self.db.Logging()
        # log.currently_processing = False
        log.pid = 100
        log.processing_start_time = datetime.datetime(2000, 1, 1, 1, 1, 1)
        log.mission_id = 1
        log.user = 'username'
        log.hostname = 'hostname'
        self.session.add(log)
        self.session.commit()
        self.assertEqual(self.session.query(self.db.Logging.currently_processing).all()[0][0], False)

    def test_logging_processing_start_time(self):
        """logging table processing_start_time is not nullable"""
        log = self.db.Logging()
        log.currently_processing = False
        # log.processing_start_time = datetime.datetime(2000, 1, 1, 1, 1, 1)
        log.mission_id = 1
        log.user = 'username'
        log.hostname = 'hostname'
        self.session.add(log)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_logging_mission_id(self):
        """logging table mission_id is not nullable"""
        log = self.db.Logging()
        log.currently_processing = False
        log.processing_start_time = datetime.datetime(2000, 1, 1, 1, 1, 1)
        # log.mission_id = 1
        log.user = 'username'
        log.hostname = 'hostname'
        self.session.add(log)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_logging_user(self):
        """logging table user is not nullable"""
        log = self.db.Logging()
        log.currently_processing = False
        log.processing_start_time = datetime.datetime(2000, 1, 1, 1, 1, 1)
        log.mission_id = 1
        # log.user = 'username'
        log.hostname = 'hostname'
        self.session.add(log)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_logging_hostname(self):
        """logging table hostname is not nullable"""
        log = self.db.Logging()
        log.currently_processing = False
        log.processing_start_time = datetime.datetime(2000, 1, 1, 1, 1, 1)
        log.mission_id = 1
        log.user = 'username'
        # log.hostname = 'hostname'
        self.session.add(log)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_logging_processing_start_time_order(self):
        """logging table processing_start_time order matters """

        log = self.db.Logging()
        log.currently_processing = False
        log.processing_start_time = datetime.datetime(2000, 1, 1, 1, 1, 1)
        log.processing_end_time = datetime.datetime(1999, 1, 1, 1, 1, 1)
        log.mission_id = 1
        log.user = 'username'
        log.hostname = 'hostname'
        self.session.add(log)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_logging_file_logging_id(self):
        """logging_file table logging_id is not nullable"""
        # add a file
        self.addFile()
        # add a code
        self.addCode()
        # add logging
        self.addLogging()
        lf = self.db.Logging_file()
        # lf.logging_id = 1
        lf.file_id = 1
        lf.code_id = 1
        self.session.add(lf)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_logging_file_file_id(self):
        """logging_file table file_id is not nullable"""
        # add a file
        self.addFile()
        # add a code
        self.addCode()
        # add logging
        self.addLogging()
        lf = self.db.Logging_file()
        lf.logging_id = 1
        # lf.file_id = 1
        lf.code_id = 1
        self.session.add(lf)
        self.assertRaises(IntegrityError, self.session.commit)

    def test_logging_file_code_id(self):
        """logging_file table code_id is not nullable"""
        # add a file
        self.addFile()
        # add a code
        self.addCode()
        # add logging
        self.addLogging()
        lf = self.db.Logging_file()
        lf.logging_id = 1
        lf.file_id = 1
        # lf.code_id = 1
        self.session.add(lf)
        self.assertRaises(IntegrityError, self.session.commit)




if __name__ == '__main__':
    ut.main()
