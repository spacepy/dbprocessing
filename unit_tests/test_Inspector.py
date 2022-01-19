#!/usr/bin/env python
from __future__ import print_function

import datetime
import unittest
import tempfile
import imp
import warnings
import os

import dbp_testing

from dbprocessing import inspector
from dbprocessing import Version
from dbprocessing import DButils
from dbprocessing import Diskfile

class InspectorFunctions(unittest.TestCase):
    """Tests of the inspector functions"""
        
    def test_extract_YYYYMMDD(self):
        """extract_YYYYMMDD works"""
        self.assertEqual( datetime.datetime(2013, 2, 14, 0, 0), inspector.extract_YYYYMMDD('rbspa_pre_ect-hope-L1_20130214_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMMDD('rbspa_pre_ect-hope-L1_20130231_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMMDD('rbspa_pre_ect-hope-L1_19520201_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMMDD('rbspa_pre_ect-hope-L1_20510201_v1.0.0.cdf') )

    def test_extract_YYYYMM(self):
        """extract_YYYYMM works"""
        self.assertEqual( datetime.date(2013, 2, 1), inspector.extract_YYYYMM('rbspa_pre_ect-hope-L1_20130214_v1.0.0.cdf') )
        self.assertEqual( datetime.date(2013, 2, 1), inspector.extract_YYYYMM('rbspa_pre_ect-hope-L1_20130231_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMM('rbspa_pre_ect-hope-L1_2013_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMM('rbspa_pre_ect-hope-L1_19520201_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMM('rbspa_pre_ect-hope-L1_20510201_v1.0.0.cdf') )

    def test_valid_YYYYMMDD(self):
        """valid_YYYYMMDD works"""
        self.assertTrue(inspector.valid_YYYYMMDD('20121212'))
        self.assertTrue(inspector.valid_YYYYMMDD('20120229'))
        self.assertFalse(inspector.valid_YYYYMMDD('20120230'))

    def test_extract_Version(self):
        """extract_Version works"""
        self.assertEqual(Version.Version(1,0,0), inspector.extract_Version('rbspa_pre_ect-hope-L1_20130231_v1.0.0.cdf'))
        self.assertEqual(Version.Version(1,0,0), inspector.extract_Version('rbspa_pre_ect-hope-L1_20130202_v1.0.0.cdf'))
        self.assertEqual(Version.Version(1,10,0), inspector.extract_Version('rbspa_pre_ect-hope-L1_20130202_v1.10.0.cdf'))
        self.assertEqual((Version.Version(1,10,0), 'rbspa_pre_ect-hope-L1_20130202_'), inspector.extract_Version('rbspa_pre_ect-hope-L1_20130202_v1.10.0.cdf', basename=True))
        self.assertEqual(None, inspector.extract_Version('rbspa_pre_ect-hope-L1_20130202_v1.f.0.cdf'))
        self.assertEqual(None, inspector.extract_Version('rbspa_pre_ect-hope-L1_20130202_v1.0.cdf'))
        self.assertEqual(None, inspector.extract_Version('rbspa_pre_ect-hope-L1_20130202_v1.0.cdf', basename=True))


class InspectorClass(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """Tests of the inspector class"""

    def setUp(self):
        """Makes a copy of the DB to run tests on without altering the original."""
        super(InspectorClass, self).setUp()
        self.makeTestDB()
        self.loadData(os.path.join(dbp_testing.testsdir, 'data', 'db_dumps',
                                   'testDB_dump.json'))
        self.inspect = imp.load_source('inspect', os.path.join(
            dbp_testing.testsdir, 'inspector', 'rot13_L1.py'))

    def tearDown(self):
        super(InspectorClass, self).tearDown()
        self.removeTestDB()

    def test_inspector(self):
        """Test inspector class"""

        # File doesn't match the inspector pattern...
        self.assertEqual(None, self.inspect.Inspector(os.path.join(
            dbp_testing.testsdir, 'inspector', 'testDB_01_first.raw'),
                                                      self.dbu, 1,)())

        # File matches pattern...
        goodfile = os.path.join(
            dbp_testing.testsdir, 'inspector', 'testDB_001_first.raw')
        self.assertEqual(repr(Diskfile.Diskfile(goodfile, self.dbu)), repr(self.inspect.Inspector(goodfile, self.dbu, 1,)()))
        #self.assertEqual(None, self.inspect.Inspector(goodfile, self.dbu, 1,).extract_YYYYMMDD())
        # This inspector sets the data_level - not allowed
        inspect = imp.load_source('inspect', os.path.join(
            dbp_testing.testsdir, 'inspector', 'rot13_L1_dlevel.py'))
        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(repr(Diskfile.Diskfile(goodfile, self.dbu)), repr(self.inspect.Inspector(goodfile, self.dbu, 1,)()))
        self.assertEqual(len(w), 1)
        self.assertTrue(isinstance(w[0].message, UserWarning))
        self.assertEqual('Inspector rot13_L1_dlevel.py:  set level to 2.0, '
                         'this is ignored and set by the product definition',
                         str(w[0].message))

        # The file doesn't match the inspector pattern...
        badfile =  os.path.join(
            dbp_testing.testsdir, 'inspector', 'testDB_01_first.raw')
        inspect = imp.load_source('inspect', os.path.join(
            dbp_testing.testsdir, 'inspector', 'rot13_L1.py'))
        self.assertEqual(None, inspect.Inspector(badfile, self.dbu, 1,)())

    def test_inspector_regex(self):
        """Test regex expansion of inspector"""
        # Simple inspector class
        class testi(inspector.inspector):
            last = None
            code_name = 'foo.py'
            def inspect(self, kwargs):
                # Save everything passed in
                last = {k: getattr(self, k) for k in
                        ('dbu', 'filename', 'basename', 'dirname',
                         'product', 'filenameformat', 'filenameregex',
                         'diskfile')}
                last['kwargs'] = kwargs
                type(self).last = last
                return None
        fspec = os.path.join(self.td, 'testDB_2016-01-01.cat')
        open(fspec, 'w').close()
        testi(fspec, self.dbu, 1)
        last = testi.last
        self.assertEqual('testDB_((19|2\\d)\\d\\d(0\\d|1[0-2])[0-3]\\d).cat',
                         last['filenameregex'])
        # Force a different pattern
        p = self.dbu.getEntry('Product', 1)
        p.format = 'testDB_{APID}.cat'
        testi(fspec, self.dbu, 1)
        last = testi.last
        self.assertEqual('testDB_([\\da-fA-F]+).cat',
                         last['filenameregex'])
        # Force a pattern we don't expand
        p = self.dbu.getEntry('Product', 1)
        p.format = 'testDB_{nonsense}.cat'
        testi(fspec, self.dbu, 1)
        last = testi.last
        self.assertEqual('testDB_(.*).cat',
                         last['filenameregex'])
        # testi goes out of scope here, so will clean up db objects


class InspectorSupportClass(unittest.TestCase):
    """Test inspector support classes"""

    def testDefaultFields(self):
        """default dict that returns generic match"""
        d = inspector.DefaultFields(a=5)
        self.assertEqual(
            ('{foo}', '.*'), d['foo'])
        self.assertEqual(
            5, d['a'])
        self.assertTrue('nothing' in d)

    def testDefaultFormatter(self):
        """formatter that returns generics"""
        f = inspector.DefaultFormatter()
        self.assertEqual(('{nothing}', '.*'), f.SPECIAL_FIELDS['nothing'])
        self.assertEqual(
            'foo_{bar}_{Y:04d}_{nothing}',
            f.expand_format('foo_{bar}_{Y}_{nothing}'))
        self.assertEqual(
            'foo_bar_2010_(.*)',
            f.re('foo_{bar}_{Y}_{nothing}', bar='bar', arg='none',
                     datetime=datetime.datetime(2010, 1, 1)))
        self.assertEqual('foobar', f.re('foobar'))


if __name__ == "__main__":
    unittest.main()
