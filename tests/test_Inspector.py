#!/usr/bin/env python
from __future__ import print_function

import datetime
import unittest
from distutils.dir_util import copy_tree, remove_tree
import tempfile
import imp
import warnings

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


class InspectorClass(unittest.TestCase):
    """Tests of the inspector class"""

    def setUp(self):
        """Makes a copy of the DB to run tests on without altering the original."""
        super(InspectorClass, self).setUp()
        # These tests shouldn't change the DB but use a copy anyway
        # Would need to at least update DB path if we wanted to
        # use DB
        self.tempD = tempfile.mkdtemp()
        copy_tree('testDB/', self.tempD)

        self.dbu = DButils.DButils(self.tempD + '/testDB.sqlite')
        self.inspect = imp.load_source('inspect', 'testDB/codes/inspectors/rot13_L1.py')

    def tearDown(self):
        super(InspectorClass, self).tearDown()
        remove_tree(self.tempD)

    def test_inspector(self):
        """Test inspector class"""

        # File doesn't match the inspector pattern...
        self.assertEqual(None, self.inspect.Inspector('testDB/codes/inspectors/testDB_01_first.raw', self.dbu, 1,))

        # File matches pattern...
        goodfile = 'testDB/codes/inspectors/testDB_001_first.raw'
        self.assertEqual(repr(Diskfile.Diskfile(goodfile, self.dbu)), repr(self.inspect.Inspector(goodfile, self.dbu, 1,)))
        #self.assertEqual(None, self.inspect.Inspector(goodfile, self.dbu, 1,).extract_YYYYMMDD())
        
        # This inspector sets the data_level - not allowed
        inspect = imp.load_source('inspect', 'testDB/codes/inspectors/rot13_L1_dlevel.py')
        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(repr(Diskfile.Diskfile(goodfile, self.dbu)), repr(self.inspect.Inspector(goodfile, self.dbu, 1,)))
        self.assertEqual(len(w), 1)
        self.assertTrue(isinstance(w[0].message, UserWarning))
        self.assertEqual('Inspector rot13_L1_dlevel.py:  set level to 2.0, '
                         'this is ignored and set by the product definition',
                         str(w[0].message))

        # The file doesn't match the inspector pattern...
        badfile = 'testDB/codes/inspectors/testDB_01_first.raw'
        inspect = imp.load_source('inspect', 'testDB/codes/inspectors/rot13_L1.py')
        self.assertEqual(None, inspect.Inspector(badfile, self.dbu, 1,))



if __name__ == "__main__":
    unittest.main()
