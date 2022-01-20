#!/usr/bin/env python
"""Unit testing for linkUningested script"""

import datetime
import io
import os.path
import shutil
import sys
import tempfile
import unittest

import dbp_testing
dbp_testing.add_scripts_to_path()

import linkUningested
import dbprocessing.dbprocessing
import dbprocessing.DButils


class LinkUningestedTests(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """linkUningested tests"""

    def test_parse_linkuningested_args(self):
        """Parse the command line arguments"""
        kwargs = linkUningested.parse_args(['-m', 'foo.sqlite'])
        self.assertEqual(
            'foo.sqlite', kwargs['mission'])
        self.assertEqual(
            None, kwargs['products'])
        kwargs = linkUningested.parse_args(['-m', 'foo.sqlite', '-p', '1',
                                            '-p', 'level1'])
        self.assertEqual(
            'foo.sqlite', kwargs['mission'])
        self.assertEqual(
            ['1', 'level1'], kwargs['products'])

    def test_list_files(self):
        """List files for product"""
        self.makeTestDB()
        try:
            self.dbu = dbprocessing.DButils.DButils(self.dbname)
            mis = self.dbu.addMission('mission', self.td, self.td)
            sat = self.dbu.addSatellite('sat', mis)
            self.dbu.addInstrument('inst', sat)
            p1 = self.addProduct('product1',
                                 format='{PRODUCT}/{Y}/prod1{Y}{m}{d}.txt')
            p2 = self.addProduct('product2', format='prod2{Y}{m}{d}.txt')
            os.makedirs(os.path.join(self.td, 'junk', 'product1', '2000'))
            open(os.path.join(
                self.td, 'junk', 'product1', '2000', 'prod120000101.txt'),
                 'w').close()
            open(os.path.join(
                self.td, 'junk', 'product1', '2000', 'prod120000102.txt'),
                 'w').close()
            open(os.path.join(
                self.td, 'junk', 'product1', '2000', 'somethingelse.txt'),
                 'w').close()
            open(os.path.join(self.td, 'junk', 'prod219990501.txt'),
                 'w').close()
            open(os.path.join(self.td, 'junk', 'another.txt'), 'w').close()
            res1 = linkUningested.list_files(self.dbu, p1)
            res2 = linkUningested.list_files(self.dbu, p2)
        finally:
            self.removeTestDB()
        self.assertEqual(
            [os.path.join('junk', 'product1', '2000', 'prod12000010{}.txt'
                          .format(i))
             for i in range(1, 3)],
            sorted(res1))
        self.assertEqual(
            [os.path.join('junk', 'prod219990501.txt')], sorted(res2))

class LinkUningestedTestsWithDB(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """
    Tests that use the magEIS database
    """
    def setUp(self):
        super(LinkUningestedTestsWithDB, self).setUp()
        self.makeTestDB()
        self.loadData(os.path.join(dbp_testing.testsdir, 'data', 'db_dumps',
                                   'RBSP_MAGEIS_dump.json'))

    def tearDown(self):
        super(LinkUningestedTestsWithDB, self).tearDown()
        self.removeTestDB()

    def test_indb(self):
        """See if files are in database"""
        self.assertTrue(linkUningested.indb(
            self.dbu, 'rbspb_pre_MagEphem_OP77Q_20130906_v1.0.0.txt', 138))
        self.assertFalse(linkUningested.indb(
            self.dbu, 'rbspb_pre_MagEphem_OP77Q_20130906_v1.0.0.txt', 43))


if __name__ == "__main__":
    unittest.main()
