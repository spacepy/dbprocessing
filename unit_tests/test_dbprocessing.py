#!/usr/bin/env python
from __future__ import print_function

"""Unit tests for dbprocessing.dbprocessing module"""

__author__ = 'Jonathan Niehof <Jonathan.Niehof@unh.edu>'


import datetime
import os
import os.path
import shutil
import tempfile
import unittest

#The log is opened on import, so need to quarantine the log directory
#right away
os.environ['DBPROCESSING_LOG_DIR'] = os.path.join(os.path.dirname(__file__),
                                                  'unittestlogs')
import dbprocessing.DButils
import dbprocessing.dbprocessing
import dbprocessing.Version


class BuildChildrenTests(unittest.TestCase):
    """Tests of ProcessQueue.buildChildren, checking what runMes are made"""

    def setUp(self):
        """Make a copy of db and open it so have something to work with"""
        super(BuildChildrenTests, self).setUp()
        self.td = tempfile.mkdtemp()
        shutil.copy2(
            os.path.join(os.path.dirname(__file__), 'emptyDB.sqlite'),
            self.td)
        dbu = dbprocessing.DButils.DButils(os.path.join(
            self.td, 'emptyDB.sqlite'))
        # Set up the baseline mission environment, BEFORE making processqueue
        mission_id = dbu.addMission(
            'Test mission',
            os.path.join(self.td, 'data'),
            os.path.join(self.td, 'incoming'),
            os.path.join(self.td, 'codes'),
            os.path.join(self.td, 'inspectors'),
            os.path.join(self.td, 'errors'))
        satellite_id = dbu.addSatellite('Satellite', mission_id)
        # Make two instruments (so can test interactions between them)
        self.instrument_ids = [
            dbu.addInstrument(instrument_name='Instrument {}'.format(i),
                              satellite_id=satellite_id)
            for i in range(1, 3)]
        del dbu
        self.pq = dbprocessing.dbprocessing.ProcessQueue(
            os.path.join(self.td, 'emptyDB.sqlite'))
        self.dbu = self.pq.dbu

    def tearDown(self):
        """Remove the copy of db"""
        # Unfortunately all the cleanup is in the destructor
        del self.pq
        shutil.rmtree(self.td)
        super(BuildChildrenTests, self).tearDown()

    def addProduct(self, product_name, instrument_id=None, level=0):
        """Add a product to database (incl. inspector)

        Won't actually work, just getting the record in
        """
        if instrument_id is None:
            instrument_id = self.instrument_ids[0]
        pid = self.dbu.addProduct(
            product_name=product_name,
            instrument_id=instrument_id,
            relative_path='junk',
            format=product_name.replace(' ', '_') + '_{Y}{m}{d}_v{VERSION}',
            level=level,
            product_description='Test product {}'.format(product_name)
            )
        self.dbu.addInstrumentproductlink(instrument_id, pid)
        self.dbu.addInspector(
            filename='fake.py',
            relative_path='inspectors',
            description='{} inspector'.format(product_name),
            version=dbprocessing.Version.Version(1, 0, 0),
            active_code=True,
            date_written='2010-01-01',
            output_interface_version=1,
            newest_version=True,
            product=pid)
        return pid

    def addProcess(self, process_name, output_product_id,
                   output_timebase='DAILY'):
        """Add a process + code record to the database

        Again, just the minimum to get the records in
        """
        process_id = self.dbu.addProcess(
            process_name,
            output_product=output_product_id,
            output_timebase=output_timebase)
        code_id = self.dbu.addCode(
            filename='junk.py',
            relative_path='scripts',
            code_start_date='2010-01-01',
            code_stop_date='2099-01-01',
            code_description='{} code'.format(process_name),
            process_id=process_id,
            version='1.0.0',
            active_code=1,
            date_written='2010-01-01',
            output_interface_version=1,
            newest_version=1,
            arguments=process_name.replace(' ', '_') + '_args')
        return process_id, code_id

    def addProductProcessLink(self, product_id, process_id, optional=False,
                              yesterday=0, tomorrow=0):
        """Minimum record for product-process link in db"""
        self.dbu.addproductprocesslink(product_id, process_id, optional,
                                       yesterday, tomorrow)

    def addFile(self, filename, product_id, utc_date, version='1.0.0',
                utc_start=None, utc_stop=None):
        """Add a file to the database"""
        level = self.dbu.getEntry('Product', product_id).level
        if utc_start is None:
            utc_start = utc_date.replace(
                hour=0, minute=0, second=0, microsecond=0)
        if utc_stop is None:
            utc_stop = utc_date.replace(
                hour=23, minute=59, second=59, microsecond=999999)
        fid = self.dbu.addFile(
            filename=filename,
            data_level=level,
            version=dbprocessing.Version.Version.fromString(version),
            product_id=product_id,
            utc_file_date=utc_date,
            utc_start_time=utc_start,
            utc_stop_time=utc_stop,
            file_create_date=datetime.datetime.now(),
            exists_on_disk=True,
        )
        return fid

    def checkCommandLines(self, fid, expected):
        """Check the command line built for a file ID

        fid is the (single) file ID

        expected is a list-of-lists, all the expected commands to be called.

        Note that, since last arg of expected is the output file, the runMe
        tmp dir will be added to it.
        """
        self.pq.buildChildren([fid, None])
        self.assertEqual(
            len(self.pq.runme_list), len(expected))
        actual = []
        for rm in self.pq.runme_list:
            rm.make_command_line(rundir='')
            actual.append(rm.cmdline)
        actual.sort()
        for e, a in zip(sorted(expected), actual):
            self.assertEqual(e, a)

    def testSimple(self):
        """Single daily file making another single daily file"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        fid = self.addFile('level_0_20120101_v1.0.0', l0pid, datetime.datetime(2012, 1, 1))
        expected = [[
            '{}/codes/scripts/junk.py'.format(self.td),
            'level_0-1_args',
            '{}/data/junk/level_0_20120101_v1.0.0'.format(self.td),
            'level_1_20120101_v1.0.0']]
        self.checkCommandLines(fid, expected)
        


if __name__ == '__main__':
    unittest.main()

