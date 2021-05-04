#!/usr/bin/env python

import os.path
import sys
import tempfile
import unittest

import dbp_testing
dbp_testing.add_scripts_to_path()

import dbprocessing.Utils
from addFromConfig import configCheck, _sectionCheck, _keysCheck, _keysRemoveExtra, _keysPresentCheck, _fileTest

class addFromConfig(unittest.TestCase):
    """Tests for addFromConfig script"""

    longMessage = True
    maxDiff = None

    def test_readconfig(self):
        """Does readconfig match the expected output"""

        conf = dbprocessing.Utils.readconfig(os.path.join(
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

    def test_sectionCheck_Valid(self):
        """Make sure no Exceptions are thrown for a valid config"""
        try:
            conf = dbprocessing.Utils.readconfig(os.path.join(
                dbp_testing.testsdir, 'data', 'configs', 'testDB.conf'))
            _sectionCheck(conf)
        except Exception as e:
            self.fail(e)

    def test_sectionCheck_MissingMission(self):
        """Make sure it notices Mission is missing"""
        conf = dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs', 'testDB_noMission.conf'))
        self.assertRaises(ValueError, _sectionCheck, conf)

    def test_sectionCheck_FakeSection(self):
        """Make sure it notices there's a fake section"""
        conf = dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs', 'testDB_fakeSection.conf'))
        self.assertRaises(ValueError, _sectionCheck, conf)

    def test_keysCheck_Valid(self):
        """Make sure no Exceptions are thrown for a valid config"""
        conf = dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs', 'testDB.conf'))
        try:
            for key in conf.keys():
                _keysCheck(conf, key)
        except Exception as e:
            self.fail(e)

    def test_keysCheck_MissingRequired(self):
        """Make sure it notices missing required keys"""
        conf = dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs', 'testDB_missingKey.conf'))
        self.assertRaises(ValueError, _keysCheck, conf, 'mission')

    def test_keysRemoveExtra(self):
        """Make sure it removes extra keys"""
        validResult = dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs', 'testDB.conf'))

        conf = _keysRemoveExtra(dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs', 'testDB_extraKey.conf')), 'mission')
        self.assertEqual(validResult['mission'], conf)

    def test_keysPresentCheck_Valid(self):
        try:
            conf = dbprocessing.Utils.readconfig(os.path.join(
                dbp_testing.testsdir, 'data', 'configs', 'testDB.conf'))
            _keysPresentCheck(conf)
        except Exception as e:
            self.fail(e)

    def test_keysPresentCheck_Invalid(self):
        conf = dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs',
            'testDB_missingProduct.conf'))
        self.assertRaises(ValueError, _keysPresentCheck, conf)

    def test_keysPresentCheck_Trigger(self):
        try:
            conf = dbprocessing.Utils.readconfig(os.path.join(
                dbp_testing.testsdir, 'data', 'configs', 'testDB_trigger.conf'))
            _keysPresentCheck(conf)
        except Exception as e:
            self.fail(e)

    def test_fileTest_Valid(self):
        try:
            _fileTest(os.path.join(dbp_testing.testsdir, 'data', 'configs',
                                   'testDB.conf'))
        except Exception as e:
            self.fail(e)

    def test_fileTest_Invalid(self):
        self.assertRaises(ValueError, _fileTest, os.path.join(
            dbp_testing.testsdir, 'data', 'configs',
            'testDB_duplicateSection.conf'))

    def test_NoInputs(self):
        """Create a process with no inputs"""
        conf = dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs',
            'testDB_processNoInputs.conf'))
        self.assertEqual(
            {'code_active': 'True',
             'code_arguments': '',
             'code_cpu': '1',
             'code_date_written': '2016-05-31',
             'code_description': 'Creates magic output',
             'code_filename': 'create_output.py',
             'code_newest_version': 'True',
             'code_output_interface': '1',
             'code_ram': '1',
             'code_relative_path': 'scripts',
             'code_start_date': '2010-09-01',
             'code_stop_date': '2020-01-01',
             'code_version': '1.0.0',
             'extra_params': '',
             'output_product': 'product_triggered_output',
             'output_timebase': 'DAILY',
             'process_name': 'no_input'
            },
            conf['process_no_input'])
        configCheck(conf)

    def test_NoOutputs(self):
        """Create a process with no outputs"""
        conf = dbprocessing.Utils.readconfig(os.path.join(
            dbp_testing.testsdir, 'data', 'configs',
            'testDB_processNoOutputs.conf'))
        self.assertEqual(
            {'code_active': 'True',
             'code_arguments': '',
             'code_cpu': '1',
             'code_date_written': '2016-05-31',
             'code_description': 'Creates no output',
             'code_filename': 'no_output.py',
             'code_newest_version': 'True',
             'code_output_interface': '1',
             'code_ram': '1',
             'code_relative_path': 'scripts',
             'code_start_date': '2010-09-01',
             'code_stop_date': '2020-01-01',
             'code_version': '1.0.0',
             'extra_params': '',
             'output_product': '',
             'output_timebase': 'RUN',
             'process_name': 'no_output',
             'required_input1': ('product_input_only', 0, 0),
            },
            conf['process_no_output'])
        configCheck(conf)


if __name__ == "__main__":
    unittest.main()
