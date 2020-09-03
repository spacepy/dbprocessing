#!/usr/bin/env python
from __future__ import print_function

"""Unit tests for runMe module, including runMe class and related"""

__author__ = 'Jonathan Niehof <Jonathan.Niehof@unh.edu>'

import datetime
import os
import os.path
import shutil
import tempfile
import unittest

import dbp_testing

import dbprocessing.DButils
import dbprocessing.runMe


class RunMeCmdArgTests(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """Tests command, argument, and command line building"""

    def setUp(self):
        """Make a copy of db and open it so have something to work with"""
        super(RunMeCmdArgTests, self).setUp()
        self.td = tempfile.mkdtemp()
        shutil.copy2(
            os.path.join(os.path.dirname(__file__), 'RBSP_MAGEIS.sqlite'),
            self.td)
        self.dbu = dbprocessing.DButils.DButils(os.path.join(
            self.td, 'RBSP_MAGEIS.sqlite'))

    def tearDown(self):
        """Remove the copy of db"""
        self.dbu.closeDB()
        del self.dbu
        shutil.rmtree(self.td)
        super(RunMeCmdArgTests, self).tearDown()

    def testSimpleArgs(self):
        """Check for simple command line arguments"""
        #Fake the processqueue (None)
        #Use process 38 (mageis L2 combined) and all mandatory
        #inputs for one day
        #Bump version, because output already exists
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), 38,
            [5774, 5762, 1791, 5764], None, version_bump=2)
        #Make sure this is runnable
        self.assertTrue(rm.ableToRun)
        #Components of the command line
        self.assertEqual(['TEMPLATE-L2.cdf', 'v1.3.4', 'v1.0.8'], rm.args)
        self.assertEqual([], rm.extra_params)
        self.assertEqual(
            '/n/space_data/cda/rbsp/codes/mageis/'
            'run_mageis_L2combined_v3.0.0.py',
            rm.codepath)
        self.assertEqual(
            'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf',
            rm.filename)
        #And now the command line itself
        rm.make_command_line()
        shutil.rmtree(rm.tempdir) #remove the dir made for the command
        self.assertEqual([
            '/n/space_data/cda/rbsp/codes/mageis/'
            'run_mageis_L2combined_v3.0.0.py',
            'TEMPLATE-L2.cdf', 'v1.3.4', 'v1.0.8',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisHIGH-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisLOW-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/MagEphem/predicted/b/'
            'rbspb_pre_MagEphem_OP77Q_20130921_v1.0.0.txt',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisM75-L2_20130921_v3.0.0.cdf',
            os.path.join(rm.tempdir,
                         'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf')
        ], rm.cmdline)

    def testSubRootdir(self):
        """Check for specifying ROOTDIR in arguments"""
        code = self.dbu.getEntry('Code', 38)
        code.arguments = '-m {ROOTDIR}/TEMPLATE-L2.cdf'
        self.dbu.session.add(code)
        self.dbu.commitDB()
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), 38,
            [5774, 5762, 1791, 5764], None, version_bump=2)
        self.assertTrue(rm.ableToRun)
        #Components of the command line
        self.assertEqual(['-m', '/n/space_data/cda/rbsp/TEMPLATE-L2.cdf'],
                         rm.args)
        self.assertEqual([], rm.extra_params)
        self.assertEqual(
            '/n/space_data/cda/rbsp/codes/mageis/'
            'run_mageis_L2combined_v3.0.0.py',
            rm.codepath)
        self.assertEqual(
            'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf',
            rm.filename)
        rm.make_command_line()
        shutil.rmtree(rm.tempdir) #remove the dir made for the command
        self.assertEqual([
            '/n/space_data/cda/rbsp/codes/mageis/'
            'run_mageis_L2combined_v3.0.0.py',
            '-m', '/n/space_data/cda/rbsp/TEMPLATE-L2.cdf',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisHIGH-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisLOW-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/MagEphem/predicted/b/'
            'rbspb_pre_MagEphem_OP77Q_20130921_v1.0.0.txt',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisM75-L2_20130921_v3.0.0.cdf',
            os.path.join(rm.tempdir,
                         'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf')
        ], rm.cmdline)

    def testSubRootdirProcess(self):
        """Check for specifying ROOTDIR in process 'extras'"""
        process = self.dbu.getEntry('Process', 38)
        process.extra_params = '-d|{ROOTDIR}'
        self.dbu.session.add(process)
        self.dbu.commitDB()
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), 38,
            [5774, 5762, 1791, 5764], None, version_bump=2)
        self.assertTrue(rm.ableToRun)
        #Components of the command line
        self.assertEqual(['TEMPLATE-L2.cdf', 'v1.3.4', 'v1.0.8'], rm.args)
        self.assertEqual(['-d', '/n/space_data/cda/rbsp'], rm.extra_params)
        self.assertEqual(
            '/n/space_data/cda/rbsp/codes/mageis/'
            'run_mageis_L2combined_v3.0.0.py',
            rm.codepath)
        self.assertEqual(
            'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf',
            rm.filename)
        rm.make_command_line()
        shutil.rmtree(rm.tempdir) #remove the dir made for the command
        self.assertEqual([
            '/n/space_data/cda/rbsp/codes/mageis/'
            'run_mageis_L2combined_v3.0.0.py',
            '-d', '/n/space_data/cda/rbsp',
            'TEMPLATE-L2.cdf', 'v1.3.4', 'v1.0.8',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisHIGH-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisLOW-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/MagEphem/predicted/b/'
            'rbspb_pre_MagEphem_OP77Q_20130921_v1.0.0.txt',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisM75-L2_20130921_v3.0.0.cdf',
            os.path.join(rm.tempdir,
                         'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf')
        ], rm.cmdline)

    def testSubCodedir(self):
        """Check for specifying CODEDIR in arguments"""
        code = self.dbu.getEntry('Code', 38)
        code.arguments = '-m {CODEDIR}/TEMPLATE-L2.cdf'
        self.dbu.session.add(code)
        self.dbu.commitDB()
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), 38,
            [5774, 5762, 1791, 5764], None, version_bump=2)
        self.assertTrue(rm.ableToRun)
        #Components of the command line
        self.assertEqual([
            '-m', '/n/space_data/cda/rbsp/codes/mageis/TEMPLATE-L2.cdf'],
                         rm.args)
        self.assertEqual([], rm.extra_params)
        self.assertEqual(
            '/n/space_data/cda/rbsp/codes/mageis/'
            'run_mageis_L2combined_v3.0.0.py',
            rm.codepath)
        self.assertEqual(
            'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf',
            rm.filename)
        rm.make_command_line()
        shutil.rmtree(rm.tempdir) #remove the dir made for the command
        self.assertEqual([
            '/n/space_data/cda/rbsp/codes/mageis/'
            'run_mageis_L2combined_v3.0.0.py',
            '-m', '/n/space_data/cda/rbsp/codes/mageis/TEMPLATE-L2.cdf',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisHIGH-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisLOW-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/MagEphem/predicted/b/'
            'rbspb_pre_MagEphem_OP77Q_20130921_v1.0.0.txt',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisM75-L2_20130921_v3.0.0.cdf',
            os.path.join(rm.tempdir,
                         'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf')
        ], rm.cmdline)

    def testSubCodever(self):
        """Use code version in path and arguments"""
        code = self.dbu.getEntry('Code', 38)
        code.arguments = '-v {CODEVERSION}'
        code.relative_path = 'codes/mageis_{CODEVERSION}/'
        code.filename = 'run_mageis_L2combined_{CODEVERSION}.py'
        self.dbu.session.add(code)
        self.dbu.commitDB()
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), 38,
            [5774, 5762, 1791, 5764], None, version_bump=2)
        self.assertTrue(rm.ableToRun)
        self.assertEqual(['-v', '3.0.0'], rm.args)
        self.assertEqual([], rm.extra_params)
        self.assertEqual(
            '/n/space_data/cda/rbsp/codes/mageis_3.0.0/'
            'run_mageis_L2combined_3.0.0.py',
            rm.codepath)
        self.assertEqual(
            'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf',
            rm.filename)
        rm.make_command_line()
        shutil.rmtree(rm.tempdir) #remove the dir made for the command
        self.assertEqual([
            '/n/space_data/cda/rbsp/codes/mageis_3.0.0/'
            'run_mageis_L2combined_3.0.0.py',
            '-v', '3.0.0',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisHIGH-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisLOW-L2_20130921_v3.0.0.cdf',
            '/n/space_data/cda/rbsp/MagEphem/predicted/b/'
            'rbspb_pre_MagEphem_OP77Q_20130921_v1.0.0.txt',
            '/n/space_data/cda/rbsp/rbspb/mageis_vc/level2/int/'
            'rbspb_int_ect-mageisM75-L2_20130921_v3.0.0.cdf',
            os.path.join(rm.tempdir,
                         'rbspb_int_ect-mageis-L2_20130921_v3.0.1.cdf')
        ], rm.cmdline)

    def testNoInputFiles(self):
        """Check command line for a process with no input files"""
        # Create a product to serve as the output of the process
        prodid = self.addProduct(
            product_name='triggered_output',
            instrument_id=1,
            format='trigger_{Y}{m}{d}_v{VERSION}.out',
            level=2)
        procid, codeid = self.addProcess('no_input', output_product_id=prodid)
        # Fake the processqueue (None)
        # Use no inputs
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), procid,
            [], None, version_bump=None)
        #Make sure this is runnable
        self.assertTrue(rm.ableToRun)
        #Components of the command line
        self.assertEqual(['no_input_args'], rm.args)
        self.assertEqual([], rm.extra_params)
        self.assertEqual(
            '/n/space_data/cda/rbsp/scripts/junk.py',
            rm.codepath)
        self.assertEqual(
            'trigger_20130921_v1.0.0.out',
            rm.filename)
        #And now the command line itself
        rm.make_command_line()
        shutil.rmtree(rm.tempdir) #remove the dir made for the command
        self.assertEqual([
            '/n/space_data/cda/rbsp/scripts/junk.py',
            'no_input_args',
            os.path.join(rm.tempdir,
                         'trigger_20130921_v1.0.0.out')
        ], rm.cmdline)

    def testNoInputOutputExist(self):
        """Check command line for a process with no input, output exists"""
        # Create a product to serve as the output of the process
        prodid = self.addProduct(
            product_name='triggered_output',
            instrument_id=1,
            format='trigger_{Y}{m}{d}_v{VERSION}.out',
            level=2)
        procid, codeid = self.addProcess('no_input', output_product_id=prodid)
        fid = self.addFile('trigger_20130921_v1.0.0.out', prodid)
        self.dbu.addFilecodelink(fid, codeid)
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), procid,
            [], None, version_bump=None)
        # Should not be runnable, because no change (would conflict)
        self.assertFalse(rm.ableToRun)
        # But if force, that marks runnable even with conflict
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), procid,
            [], None, version_bump=None, force=True)
        self.assertTrue(rm.ableToRun)
        # And now the command line itself
        rm.make_command_line()
        shutil.rmtree(rm.tempdir) #remove the dir made for the command
        # Version bump not requested, so it would conflict
        self.assertEqual([
            '/n/space_data/cda/rbsp/scripts/junk.py',
            'no_input_args',
            os.path.join(rm.tempdir,
                         'trigger_20130921_v1.0.0.out')
        ], rm.cmdline)
        # Same thing with version bump
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), procid,
            [], None, version_bump=1)
        # Make sure this is runnable
        self.assertTrue(rm.ableToRun)
        rm.make_command_line()
        shutil.rmtree(rm.tempdir)
        self.assertEqual([
            '/n/space_data/cda/rbsp/scripts/junk.py',
            'no_input_args',
            os.path.join(rm.tempdir,
                         'trigger_20130921_v1.1.0.out')
        ], rm.cmdline)

    def testNoInputUpdateCode(self):
        """Process with no input, output exists, code has been updated"""
        prodid = self.addProduct(
            product_name='triggered_output',
            instrument_id=1,
            format='trigger_{Y}{m}{d}_v{VERSION}.out',
            level=2)
        procid, codeid = self.addProcess('no_input', output_product_id=prodid)
        fid = self.addFile('trigger_20130921_v1.0.0.out', prodid)
        self.dbu.addFilecodelink(fid, codeid)
        # Meet the new code, same as the old code
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
        # Now see if runnable (with a version bump)
        rm = dbprocessing.runMe.runMe(
            self.dbu, datetime.date(2013, 9, 21), procid,
            [], None, version_bump=1)
        # Make sure this is runnable
        self.assertTrue(rm.ableToRun)
        rm.make_command_line()
        shutil.rmtree(rm.tempdir)
        self.assertEqual([
            '/n/space_data/cda/rbsp/scripts/new_junk.py',
            'no_input_args',
            os.path.join(rm.tempdir,
                         'trigger_20130921_v1.1.0.out')
        ], rm.cmdline)


if __name__ == '__main__':
    unittest.main()

