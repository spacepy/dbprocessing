#!/usr/bin/env python2.6

import datetime
import unittest
import os
import tempfile


import Utils
import Version



class UtilsTests(unittest.TestCase):
    """Tests for DBfile class"""
    
    def setUp(self):
        super(UtilsTests, self).setUp()

    def tearDown(self):
        super(UtilsTests, self).tearDown()

    def test_dirSubs(self):
        """dirSubs substitutions should work"""
        path = '{Y}{m}{d}'
        filename = 'test_filename'
        utc_file_date = datetime.date(2012, 4, 12)
        utc_start_time = datetime.datetime(2012, 4, 12, 1, 2, 3)
        version = '1.2.3'
        version2 = Version.Version(3,2,1)
        self.assertEqual('20120412', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        path = '{DATE}'
        self.assertEqual('20120412', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        path = '{Y}{b}{d}'
        self.assertEqual('2012Apr12',Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        path = '{Y}{j}'
        self.assertEqual('2012103', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        path = '{VERSION}'
        self.assertEqual('1.2.3', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        self.assertEqual('3.2.1', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version2))


if __name__ == "__main__":
    unittest.main()
