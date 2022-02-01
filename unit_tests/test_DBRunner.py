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
        options = DBRunner.parse_args([
            '-m', 'foo.sqlite', '-s', '20180101', '5'])
        self.assertEqual(
            '5', options.process_id)
        self.assertEqual(
            'foo.sqlite', options.mission)
        self.assertEqual(
            datetime.datetime(2018, 1, 1),
            options.startDate)
        self.assertTrue(
            datetime.datetime.now() - options.endDate
            < datetime.timedelta(seconds=10))
        self.assertFalse(options.echo)
        self.assertEqual(1, options.numproc)
        self.assertFalse(options.update)
        self.assertFalse(options.ingest)
        self.assertTrue(options.force is None)

    def test_parse_dbrunner_args_other(self):
        """Parse the command line with other options"""
        options = DBRunner.parse_args([
            '-m', 'foo.sqlite', '-s', '20180101', '5', '--force', '0'])
        self.assertEqual(0, options.force)
        options = DBRunner.parse_args([
            '-m', 'foo.sqlite', '-s', '20180101', '5', '-u'])
        self.assertTrue(options.update)
        options = DBRunner.parse_args([
            '-m', 'foo.sqlite', '-s', '20180101', '5', '-u', '-i'])
        self.assertTrue(options.update)
        self.assertTrue(options.ingest)
        options = DBRunner.parse_args([
            '-m', 'foo.sqlite', '-s', '20180101', '5', '--force', '1', '-i'])
        self.assertFalse(options.update)
        self.assertEqual(1, options.force)
        self.assertTrue(options.ingest)
        options = DBRunner.parse_args([
            '-m', 'foo.sqlite', '-s', '20180101', 'foo'])
        self.assertEqual(
            'foo' , options.process_id)

    def test_parse_dbrunner_args_bad(self):
        """Parse the command line arguments with errors"""
        arglist = [
            ['--force', '6'],
            ['--force', '0', '-u'],
            ['-i'],
            ]
        msgs = [
            'argument --force: invalid choice: 6 (choose from 0, 1, 2)',
            'argument -u/--update: not allowed with argument --force',
            'argument -i/--ingest: requires --force or --update',
            ]
        oldstderr = sys.stderr
        for args, msg in zip(arglist, msgs):
            sys.stderr = (io.BytesIO if str is bytes else io.StringIO)()
            try:
                with self.assertRaises(SystemExit) as cm:
                    DBRunner.parse_args(
                        ['-m', 'foo.sqlite', '-s', '20180101', '5'] + args)
                # Cut off all the usage information, just get the error.
                # (ends with blank line, so skip that).
                err = sys.stderr.getvalue().split('\n')[-2]
            finally:
                sys.stderr.close()
                sys.stderr = oldstderr
            self.assertEqual(
                '{}: error: {}'.format(os.path.basename(sys.argv[0]), msg), err)


class DBRunnerCalcRunmeTests(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """DBRunner tests of calc_runme"""

    def setUp(self):
        """Make temp dir, redirect stdout"""
        self.pq = None # Define the symbol so the del later doesn't fail
        self.newstdout = self.oldstdout = None # Similar
        self.makeTestDB()
        self.loadData(os.path.join(dbp_testing.testsdir, 'data', 'db_dumps',
                                   'RBSP_MAGEIS_dump.json'))
        self.pq = dbprocessing.dbprocessing.ProcessQueue(self.dbu)
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
        self.removeTestDB()

    def test_runme_list(self):
        """Get a list of processes to run"""
        runme = DBRunner.calc_runme(self.pq, datetime.datetime(2013, 9, 6),
                                datetime.datetime(2013, 9, 9), 38)
        self.assertEqual(4, len(runme))
        for r in runme:
            self.assertEqual(38, r.process_id)
            self.assertEqual(
                os.path.join(dbp_testing.driveroot, 'n', 'space_data',
                             'cda', 'rbsp', 'codes', 'mageis',
                             'run_mageis_L2combined_v3.0.0.py'),
                r.codepath)
        self.assertEqual(
            [datetime.date(2013, 9, i) for i in range(6, 10)],
            sorted([r.utc_file_date for r in runme]))

    def test_runme_list_no_results(self):
        """Get a list of processes to run, for day with nothing"""
        runme = DBRunner.calc_runme(self.pq, datetime.datetime(2010, 9, 6),
                                    datetime.datetime(2010, 9, 9), 38)
        self.assertEqual(0, len(runme))

    def test_runme_list_no_input(self):
        """List of processes to run for no-input processes"""
        # Create a product to serve as the output of the process
        prodid = self.addProduct(
            product_name='triggered_output',
            instrument_id=1,
            format='trigger_{Y}{m}{d}_v{VERSION}.out',
            level=2)
        procid, codeid = self.addProcess('no_input', output_product_id=prodid)
        runme = DBRunner.calc_runme(self.pq, datetime.datetime(2010, 1, 1),
                                    datetime.datetime(2010, 1, 5), procid)
        self.assertEqual(5, len(runme))
        # Quick checks; test_runMe has extensive tests with no input products.
        rm = runme[0]
        self.assertEqual(
            os.path.join(dbp_testing.driveroot, 'n', 'space_data',
                         'cda', 'rbsp', 'scripts', 'junk.py'),
            rm.codepath)
        self.assertEqual('trigger_20100101_v1.0.0.out', rm.filename)

    def test_runme_list_options(self):
        """List of processes to run, no input, various forcing"""
        prodid = self.addProduct(
            product_name='triggered_output',
            instrument_id=1,
            format='trigger_{Y}{m}{d}_v{VERSION}.out',
            level=2)
        procid, codeid = self.addProcess('no_input', output_product_id=prodid)
        with self.assertRaises(ValueError): # --force with -u, essentially
            runme = DBRunner.calc_runme(self.pq, datetime.datetime(2010, 1, 1),
                                        datetime.datetime(2010, 1, 1), procid,
                                        version_bump=1, update=True)
        # Most of this is really a test of runMe, and it's tested there,
        # but this makes sure that the chosen arguments to runMe have the
        # desired result.
        # No output files yet, so all ways of running should be the same.
        for vb, u in [(None, False), (1, False), (None, True)]:
            runme = DBRunner.calc_runme(self.pq, datetime.datetime(2010, 1, 1),
                                        datetime.datetime(2010, 1, 1), procid,
                                        version_bump=vb, update=u)
            self.assertEqual(1, len(runme))
            rm = runme[0]
            self.assertTrue(rm.ableToRun)
            self.assertEqual('trigger_20100101_v1.0.0.out', rm.filename)
        # Make an existing file
        fid = self.addFile('trigger_20100101_v1.0.0.out', prodid)
        self.dbu.addFilecodelink(fid, codeid)
        # Now only version bump or no-update should make output
        for vb, u, v in [(None, False, '1.0.0'), # default
                         (1, False, '1.1.0'), # --force 1
                         (2, False, '1.0.1'), # --force 2
                         (None, True, None)]: # -u
            runme = DBRunner.calc_runme(self.pq, datetime.datetime(2010, 1, 1),
                                        datetime.datetime(2010, 1, 1), procid,
                                        version_bump=vb, update=u)
            # -u can't make output
            if v is None:
                self.assertEqual(0, len(runme))
                continue
            self.assertEqual(1, len(runme))
            rm = runme[0]
            self.assertTrue(rm.ableToRun)
            self.assertEqual('trigger_20100101_v{}.out'.format(v),
                             rm.filename)
        # Update the code
        oldcode = self.dbu.getEntry('Code', codeid)
        newcode = self.dbu.Code()
        for k in dir(newcode):
            if not k.startswith('_'):
                setattr(newcode, k, getattr(oldcode, k))
        newcode.code_id = None
        newcode.filename = 'new_junk.py'
        newcode.quality_version = 1
        self.dbu.session.add(newcode)
        self.dbu.commitDB()
        # Old code no longer active
        self.dbu.updateCodeNewestVersion(codeid)
        # Now all should make output, but default doesn't update version
        for vb, u, v in [(None, False, '1.0.0'), # default
                         (1, False, '1.1.0'), # --force 1
                         (2, False, '1.0.1'), # --force 2
                         (None, True, '1.1.0')]: # -u
            runme = DBRunner.calc_runme(self.pq, datetime.datetime(2010, 1, 1),
                                        datetime.datetime(2010, 1, 1), procid,
                                        version_bump=vb, update=u)
            self.assertEqual(1, len(runme))
            rm = runme[0]
            if v is None:
                self.assertFalse(rm.ableToRun)
            else:
                self.assertTrue(rm.ableToRun)
                self.assertEqual('trigger_20100101_v{}.out'.format(v),
                                 rm.filename)


if __name__ == "__main__":
    unittest.main()
