#!/usr/bin/env python2.6

import datetime
import unittest

from dbprocessing import inspector
from dbprocessing import Version

__version__ = '2.0.3'


class InspectorFunctions(unittest.TestCase):
    """Tests of the inspector functions"""
        
    def test_extract_YYYYMMDD(self):
        """extract_YYYYMMDD works"""
        self.assertEqual( datetime.datetime(2013, 2, 14, 0, 0), inspector.extract_YYYYMMDD('rbspa_pre_ect-hope-L1_20130214_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMMDD('rbspa_pre_ect-hope-L1_20130231_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMMDD('rbspa_pre_ect-hope-L1_19520201_v1.0.0.cdf') )
        self.assertEqual( None, inspector.extract_YYYYMMDD('rbspa_pre_ect-hope-L1_20510201_v1.0.0.cdf') )
    
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
        self.assertEqual(None, inspector.extract_Version('rbspa_pre_ect-hope-L1_20130202_v1.f.0.cdf'))
        self.assertEqual(None, inspector.extract_Version('rbspa_pre_ect-hope-L1_20130202_v1.0.cdf'))

class InspectorClass(unittest.TestCase):
    """Tests of the inspector class"""




if __name__ == "__main__":
    unittest.main()
