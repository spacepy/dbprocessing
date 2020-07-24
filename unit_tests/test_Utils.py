#!/usr/bin/env python
from __future__ import print_function

import datetime
from distutils.dir_util import copy_tree, remove_tree
import os
import sys
import tempfile
import unittest

try:
    import StringIO
except:
    import io as StringIO

from dbprocessing import Utils
from dbprocessing import DButils
from dbprocessing import Version


class UtilsTests(unittest.TestCase):
    """Tests for DBfile class"""

    def setUp(self):
        super(UtilsTests, self).setUp()
        # Not changing the DB now but use a copy anyway
        # Would need to at least update DB path if we wanted to
        # do more than vanilla dirSubs
        self.tempD = tempfile.mkdtemp()
        copy_tree(os.path.dirname(__file__) + '/../functional_test/', self.tempD)

        self.dbu = DButils.DButils(self.tempD + '/testDB.sqlite')

    def tearDown(self):
        super(UtilsTests, self).tearDown()
        remove_tree(self.tempD)

    def test_dirSubs(self):
        """dirSubs substitutions should work"""
        path = '{Y}{m}{d}'
        filename = 'test_filename'
        utc_file_date = datetime.date(2012, 4, 12)
        utc_start_time = datetime.datetime(2012, 4, 12, 1, 2, 3)
        version = '1.2.3'
        version2 = Version.Version(3, 2, 1)
        self.assertEqual('20120412', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        path = '{DATE}'
        self.assertEqual('20120412', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        path = '{Y}{b}{d}'
        self.assertEqual('2012Apr12', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        path = '{y}{j}'
        self.assertEqual('12103', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        path = '{VERSION}'
        self.assertEqual('1.2.3', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        self.assertEqual('3.2.1', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version2))
        path = '{H}{M}{S}'
        self.assertEqual('010203', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version))
        # Substitutions that require referring to the DB...
        filename = 'testDB_000_000.raw'
        path = '{INSTRUMENT}'
        self.assertEqual('rot13', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version, dbu=self.dbu))
        path = '{SATELLITE}'
        self.assertEqual('testDB-a',
                         Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version, dbu=self.dbu))
        path = '{SPACECRAFT}'
        self.assertEqual('testDB-a',
                         Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version, dbu=self.dbu))
        path = '{MISSION}'
        self.assertEqual('testDB', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version, dbu=self.dbu))
        path = '{PRODUCT}'
        self.assertEqual('testDB_rot13_L0_first',
                         Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version, dbu=self.dbu))
        # Verify that unknown values are ignored
        path = '{xxx}'
        self.assertEqual('{xxx}', Utils.dirSubs(path, filename, utc_file_date, utc_start_time, version, dbu=self.dbu))

    def test_chunker(self):
        """chunker()"""
        self.assertEqual(list(Utils.chunker(range(10), 3)), [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])
        self.assertEqual(list(Utils.chunker(range(10), 4)), [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]])
        self.assertEqual(list(Utils.chunker(range(10), 4)), [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]])
        self.assertEqual(list(Utils.chunker(range(10), 10)), [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])
        self.assertEqual(list(Utils.chunker(range(10), 20)), [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])

    def test_unique(self):
        """unique"""
        self.assertEqual(Utils.unique(range(5)), range(5))
        self.assertEqual(Utils.unique([1, 1, 2, 2, 3]), [1, 2, 3])
        self.assertEqual(Utils.unique([1, 1, 3, 2, 2, 3]), [1, 3, 2])

    def test_expandDates(self):
        """expandDates"""
        d1 = datetime.datetime(2013, 1, 1)
        d2 = datetime.datetime(2013, 1, 2)
        d3 = datetime.datetime(2013, 1, 3)

        self.assertEqual(list(Utils.expandDates(d1, d1)), [d1])
        self.assertEqual(list(Utils.expandDates(d1, d2)), [d1, d2])
        self.assertEqual(list(Utils.expandDates(d1, d3)), [d1, d2, d3])

    def test_parseDate(self):
        """parseDate"""
        self.assertEqual(datetime.datetime(2013, 1, 1), Utils.parseDate('2013-01-01'))
        self.assertRaises(ValueError, Utils.parseDate, '2013-13-01')

    def test_parseVersion(self):
        """parseVersion"""
        self.assertEqual(Version.Version(1, 2, 3), Utils.parseVersion('1.2.3'))
        self.assertRaises(TypeError, Utils.parseVersion, '1.2')

    def test_flatten(self):
        """flatten"""
        self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9], list(Utils.flatten([[1, 2], [3, 4, 5], [6, 7], [8], [9]])))

    def test_toBool(self):
        """toBool"""
        invals = ['True', 'true', True, 1, 'Yes', 'yes']
        for v in invals:
            self.assertTrue(Utils.toBool(v))
        invals = ['sdg', 'false', False, 'sagdfa']
        for v in invals:
            self.assertFalse(Utils.toBool(v))

    def test_toNone(self):
        """toNone"""
        invals = ['', 'None', 'none', 'NONE']
        for v in invals:
            self.assertTrue(Utils.toNone(v) is None)
        invals = ['sdg', 'false', False, 'sagdfa']
        for v in invals:
            self.assertFalse(Utils.toNone(v) is None)

    def test_daterange_to_dates(self):
        """daterange_to_dates"""
        daterange = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 6)]
        expected = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5), datetime.datetime(2000, 1, 6)]
        self.assertEqual(expected, Utils.daterange_to_dates(daterange))
        daterange = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5, 23)]
        expected = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5)]
        self.assertEqual(expected, Utils.daterange_to_dates(daterange))

    def test_strargs_to_args1(self):
        """strargs_to_args"""
        self.assertTrue(Utils.strargs_to_args(None) is None)

    def test_strargs_to_args2(self):
        """strargs_to_args"""
        self.assertEqual(Utils.strargs_to_args('--arg1=arg'), {'--arg1': 'arg'})

    def test_strargs_to_args3(self):
        """strargs_to_args"""
        self.assertEqual(Utils.strargs_to_args(['--arg1=arg', '--arg2=arg2']), {'--arg1': 'arg', '--arg2': 'arg2'})

    def test_strargs_to_args4(self):
        """strargs_to_args"""
        self.assertEqual(Utils.strargs_to_args(['--arg2=arg2']), {'--arg2': 'arg2'})

    def test_strargs_to_args5(self):
        """strargs_to_args"""
        self.assertEqual(Utils.strargs_to_args('--arg'), {})

    def test_dateForPrinting(self):
        """dateForPrinting"""
        dt = datetime.datetime(2012, 8, 30, 8, 5)
        ans1 = '[2012-08-30T08:05:00]'
        self.assertEqual(ans1, Utils.dateForPrinting(dt))
        self.assertEqual('[' + datetime.datetime.now().replace(microsecond=0).isoformat() + ']',
                         Utils.dateForPrinting())

    def test_split_code_args(self):
        """split_code_args"""
        self.assertEqual(["code", "hello", "outfile"], Utils.split_code_args("code hello outfile"))
        self.assertEqual(["code", "-n hello", "outfile"], Utils.split_code_args("code -n hello outfile"))
        self.assertEqual(["code", "infile", "--flag hello", "outfile"],
                         Utils.split_code_args("code infile --flag hello outfile"))

    def test_processRunnin1(self):
        """processRunning"""
        self.assertTrue(Utils.processRunning(os.getpid()))
        self.assertFalse(Utils.processRunning(44565))

    def test_progressbar(self):
        """progressbar shouldhave a known output"""
        realstdout = sys.stdout
        output = StringIO.StringIO()
        sys.stdout = output
        self.assertEqual(Utils.progressbar(0, 1, 100), None)
        result = output.getvalue()
        output.close()
        self.assertEqual(result, "\rDownload Progress ...0%")
        sys.stdout = realstdout

    def test_readconfig(self):
        """test readconfig"""
        self.assertEqual({'section2': {'sect2a': 'sect2_value1'}, 'section1': {'sect1a': 'sect1_value1', 'sect1b': 'sect1_value2'}}, Utils.readconfig(os.path.dirname(__file__) + '/testconfig.txt'))

    def test_datetimeToDate(self):
        """test datetimeToDate"""
        self.assertEqual(Utils.datetimeToDate(datetime.date(2016, 12, 10)), datetime.date(2016, 12, 10))
        self.assertEqual(Utils.datetimeToDate(datetime.datetime(2016, 12, 10, 11, 5)), datetime.date(2016, 12, 10))


if __name__ == "__main__":
    unittest.main()
