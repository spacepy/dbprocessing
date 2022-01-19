#!/usr/bin/env python
from __future__ import print_function

import datetime
import os
import os.path
import sys
import tempfile
import unittest

try:
    import StringIO
except:
    import io as StringIO

import dbp_testing
from dbprocessing import Utils
from dbprocessing import DButils
from dbprocessing import Version


class UtilsTests(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """Tests for DBfile class"""

    longMessage = True
    maxDiff = None

    def setUp(self):
        super(UtilsTests, self).setUp()
        self.makeTestDB()
        self.loadData(os.path.join(dbp_testing.testsdir, 'data', 'db_dumps',
                                   'testDB_dump.json'))

    def tearDown(self):
        super(UtilsTests, self).tearDown()
        self.removeTestDB()

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
        # Turn iterator-of-iterators into list-of-lists
        def unfold_iter(i):
            return [list(j) for j in i]
        self.assertEqual(unfold_iter(Utils.chunker(range(10), 3)),
                         [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])
        self.assertEqual(unfold_iter(Utils.chunker(range(10), 4)),
                         [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]])
        self.assertEqual(unfold_iter(Utils.chunker(range(10), 4)),
                         [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]])
        self.assertEqual(unfold_iter(Utils.chunker(range(10), 10)),
                         [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])
        self.assertEqual(unfold_iter(Utils.chunker(range(10), 20)),
                         [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])

    def test_unique(self):
        """unique"""
        self.assertEqual(Utils.unique(range(5)), list(range(5)))
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
        self.assertEqual({'section2': {'sect2a': 'sect2_value1'}, 'section1': {'sect1a': 'sect1_value1', 'sect1b': 'sect1_value2'}}, Utils.readconfig(os.path.join(os.path.dirname(__file__), 'testconfig.txt')))

    def test_readconfigAll(self):
        """Check readconfig output, as used in addFromConfig"""

        conf = Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs', 'testDB.conf'))
        # Regression testing, just copy-pasted from the actual output
        ans = {
            'satellite': {'satellite_name': '{MISSION}-a'},
            'product_concat': {
                'inspector_output_interface': '1',
                'inspector_version': '1.0.0',
                'inspector_arguments': '-q',
                'format': 'testDB_{nnn}.cat',
                'level': '1.0',
                'product_description': '',
                'relative_path': 'L1',
                'inspector_newest_version': 'True',
                'inspector_relative_path': 'codes/inspectors',
                'inspector_date_written': '2016-05-31',
                'inspector_filename': 'rot13_L1.py',
                'inspector_description': 'Level 1',
                'inspector_active': 'True',
                'product_name': '{MISSION}_rot13_L1'},
            'product_rot13': {
                'inspector_output_interface': '1',
                'inspector_version': '1.0.0',
                'inspector_arguments': '-q',
                'format': 'testDB_{nnn}.rot',
                'level': '2.0',
                'product_description': '',
                'relative_path': 'L2',
                'inspector_newest_version': 'True',
                'inspector_relative_path': 'codes/inspectors',
                'inspector_date_written': '2016-05-31',
                'inspector_filename': 'rot13_L2.py',
                'inspector_description': 'Level 2',
                'inspector_active': 'True',
                'product_name': '{MISSION}_rot13_L2'},
            'mission': {
                'incoming_dir': 'L0',
                'rootdir': '/home/myles/dbprocessing/test_DB',
                'mission_name': 'testDB'},
            'instrument': {'instrument_name': 'rot13'},
            'process_rot13_L1-L2': {'code_cpu': '1',
                'code_start_date': '2010-09-01',
                'code_stop_date': '2020-01-01',
                'code_filename': 'run_rot13_L1toL2.py',
                'code_relative_path': 'scripts',
                'required_input1': ('product_concat', 0, 0),
                'code_version': '1.0.0',
                'process_name': 'rot_L1toL2',
                'code_output_interface': '1',
                'code_newest_version': 'True',
                'code_date_written': '2016-05-31',
                'code_description': 'Python L1->L2',
                'output_product': 'product_rot13',
                'code_active': 'True',
                'code_arguments': '',
                'extra_params': '',
                'output_timebase': 'FILE',
                'code_ram': '1'}
        }
        key_diff = set(ans.keys()).symmetric_difference(conf.keys())
        if key_diff:
            self.fail('Keys in only one of actual/expected: '
                      + ', '.join(key_diff))
        for k in ans:
            self.assertEqual(ans[k], conf[k], k)
        self.assertEqual(ans, conf)

    def test_datetimeToDate(self):
        """test datetimeToDate"""
        self.assertEqual(Utils.datetimeToDate(datetime.date(2016, 12, 10)), datetime.date(2016, 12, 10))
        self.assertEqual(Utils.datetimeToDate(datetime.datetime(2016, 12, 10, 11, 5)), datetime.date(2016, 12, 10))

    def test_toDatetime(self):
        """Test toDatetime"""
        self.assertEqual(
            datetime.datetime(2010, 1, 1),
            Utils.toDatetime('2010-1-1'))
        self.assertEqual(
            datetime.datetime(2010, 1, 1),
            Utils.toDatetime(datetime.datetime(2010, 1, 1)))
        self.assertEqual(
            datetime.datetime(2010, 1, 1, 5),
            Utils.toDatetime(datetime.datetime(2010, 1, 1, 5)))
        self.assertEqual(
            datetime.datetime(2010, 1, 1),
            Utils.toDatetime(datetime.date(2010, 1, 1)))

        self.assertEqual(
            datetime.datetime(2010, 1, 1, 23, 59, 59, 999999),
            Utils.toDatetime('2010-1-1', end=True))
        self.assertEqual(
            datetime.datetime(2010, 1, 1),
            Utils.toDatetime(datetime.datetime(2010, 1, 1), end=True))
        self.assertEqual(
            datetime.datetime(2010, 1, 1, 5),
            Utils.toDatetime(datetime.datetime(2010, 1, 1, 5), end=True))
        self.assertEqual(
            datetime.datetime(2010, 1, 1, 23, 59, 59, 999999),
            Utils.toDatetime(datetime.date(2010, 1, 1), end=True))


if __name__ == "__main__":
    unittest.main()
