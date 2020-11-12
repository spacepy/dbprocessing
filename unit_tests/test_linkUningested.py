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
        td = tempfile.mkdtemp()
        try:
            testdb = os.path.join(td, 'emptyDB.sqlite')
            shutil.copy2(os.path.join(dbp_testing.testsdir,
                                      'emptyDB.sqlite'),
                         testdb)
            self.dbu = dbprocessing.DButils.DButils(testdb)
            mis = self.dbu.addMission('mission', td, td)
            sat = self.dbu.addSatellite('sat', mis)
            self.dbu.addInstrument('inst', sat)
            p1 = self.addProduct('product1',
                                 format='{PRODUCT}/{Y}/prod1{Y}{m}{d}.txt')
            p2 = self.addProduct('product2', format='prod2{Y}{m}{d}.txt')
            os.makedirs(os.path.join(td, 'junk', 'product1', '2000'))
            open(os.path.join(
                td, 'junk', 'product1', '2000', 'prod120000101.txt'), 'w')\
                .close()
            open(os.path.join(
                td, 'junk', 'product1', '2000', 'prod120000102.txt'), 'w')\
                .close()
            open(os.path.join(
                td, 'junk', 'product1', '2000', 'somethingelse.txt'), 'w')\
                .close()
            open(os.path.join(td, 'junk', 'prod219990501.txt'), 'w').close()
            open(os.path.join(td, 'junk', 'another.txt'), 'w').close()
            res1 = linkUningested.list_files(self.dbu, p1)
            res2 = linkUningested.list_files(self.dbu, p2)
        finally:
            try:
                self.dbu.closeDB()
                del self.dbu
            except:
                pass
            shutil.rmtree(td)
        self.assertEqual(
            [os.path.join('junk', 'product1', '2000', 'prod12000010{}.txt'
                          .format(i))
             for i in range(1, 3)],
            sorted(res1))
        self.assertEqual(
            [os.path.join('junk', 'prod219990501.txt')], sorted(res2))

    def test_indb(self):
        """See if files are in database"""
        td = tempfile.mkdtemp()
        try:
            testdb = os.path.join(td, 'RBSP_MAGEIS.sqlite')
            shutil.copy2(os.path.join(dbp_testing.testsdir,
                                      'RBSP_MAGEIS.sqlite'),
                         testdb)
            dbu = dbprocessing.DButils.DButils(testdb)
            self.assertTrue(linkUningested.indb(
                dbu, 'rbspb_pre_MagEphem_OP77Q_20130906_v1.0.0.txt', 138))
            self.assertFalse(linkUningested.indb(
                dbu, 'rbspb_pre_MagEphem_OP77Q_20130906_v1.0.0.txt', 43))
        finally:
            try:
                dbu.closeDB()
                del dbu
            except:
                pass
            shutil.rmtree(td)
        


if __name__ == "__main__":
    unittest.main()
