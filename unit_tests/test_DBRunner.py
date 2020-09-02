#!/usr/bin/env python
"""Unit testing for DBRunner script"""

import datetime
import io
import os.path
import shutil
import sys
import tempfile
import unittest

import dbp_testing
dbp_testing.add_scripts_to_path()

import DBRunner
import dbprocessing.dbprocessing


class DBRunnerTests(unittest.TestCase):
    """DBRunner tests"""

    def test_parse_dbrunner_args(self):
        """Parse the command line arguments"""
        args, options = DBRunner.parse_args([
            '-m', 'foo.sqlite', '-s', '20180101', '5'])
        self.assertEqual(
            ['5'], args)
        self.assertEqual(
            'foo.sqlite', options.mission)
        self.assertEqual(
            datetime.datetime(2018, 1, 1),
            options.startDate)
        self.assertTrue(
            datetime.datetime.now() - options.endDate
            < datetime.timedelta(seconds=10))
        self.assertEqual(
            False, options.echo)
        self.assertEqual(1, options.numproc)


class DBRunnerCalcRunmeTests(unittest.TestCase):
    """DBRunner tests of calc_runme"""

    def setUp(self):
        """Make temp dir, redirect stdout"""
        self.pq = None # Define the symbol so the del later doesn't fail
        self.newstdout = self.oldstdout = None # Similar
        self.td = tempfile.mkdtemp()
        self.testdb = os.path.join(self.td, 'RBSP_MAGEIS.sqlite')
        shutil.copy2(os.path.join(dbp_testing.testsdir,
                                  'RBSP_MAGEIS.sqlite'),
                     self.testdb)
        self.pq = dbprocessing.dbprocessing.ProcessQueue(self.testdb)
        # There's a bunch of print statements to smash...
        self.oldstdout = sys.stdout
        self.newstdout = (io.BytesIO if str is bytes else io.StringIO)()
        sys.stdout = self.newstdout

    def tearDown(self):
        """Clean up temp dir and stdout"""
        if self.oldstdout is not None:
            sys.stdout = self.oldstdout
        if self.newstdout is not None:
            self.newstdout.close()
        del self.pq # Cleaned up by its destructor only
        shutil.rmtree(self.td)

    def test_runme_list(self):
        """Get a list of processes to run"""
        runme = DBRunner.calc_runme(self.pq, datetime.datetime(2013, 9, 6),
                                datetime.datetime(2013, 9, 9), 38)
        self.assertEqual(4, len(runme))
        for r in runme:
            self.assertEqual(38, r.process_id)
            self.assertEqual(
                '/n/space_data/cda/rbsp/codes/mageis/'
                'run_mageis_L2combined_v3.0.0.py', r.codepath)
        self.assertEqual(
            [datetime.date(2013, 9, i) for i in range(6, 10)],
            sorted([r.utc_file_date for r in runme]))

    def test_runme_list_no_results(self):
        """Get a list of processes to run, for day with nothing"""
        runme = DBRunner.calc_runme(self.pq, datetime.datetime(2010, 9, 6),
                                    datetime.datetime(2010, 9, 9), 38)
        self.assertEqual(0, len(runme))


if __name__ == "__main__":
    unittest.main()
