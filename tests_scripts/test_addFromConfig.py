#!/usr/bin/env python

import sys
import tempfile
import unittest

sys.path.append('../scripts') # Add scripts dir to the python path for the import
from addFromConfig import readconfig, _sectionCheck, _keysCheck, _keysRemoveExtra, _keysPresentCheck, _fileTest

class addFromConfig(unittest.TestCase):
    """Tests for addFromConfig script"""

    def test_readconfig(self):
        """Does readconfig match the expected output"""

        conf = readconfig('testing_configs/testDB.conf')
        # Regression testing, just copy-pasted from the actual output
        ans = {'satellite': {'satellite_name': '{MISSION}-a'}, 'product_concat': {'inspector_output_interface': '1', 'inspector_version': '1.0.0', 'inspector_arguments': '-q', 'format': 'testDB_{nnn}.cat', 'level': '1.0', 'product_description': '', 'relative_path': 'L1', 'inspector_newest_version': 'True', 'inspector_relative_path': 'codes/inspectors', 'inspector_date_written': '2016-05-31', 'inspector_filename': 'rot13_L1.py', 'inspector_description': 'Level 1', 'inspector_active': 'True', 'product_name': '{MISSION}_rot13_L1'}, 'product_rot13': {'inspector_output_interface': '1', 'inspector_version': '1.0.0', 'inspector_arguments': '-q', 'format': 'testDB_{nnn}.rot', 'level': '2.0', 'product_description': '', 'relative_path': 'L2', 'inspector_newest_version': 'True', 'inspector_relative_path': 'codes/inspectors', 'inspector_date_written': '2016-05-31', 'inspector_filename': 'rot13_L2.py', 'inspector_description': 'Level 2', 'inspector_active': 'True', 'product_name': '{MISSION}_rot13_L2'}, 'mission': {'incoming_dir': 'L0', 'rootdir': '/home/myles/dbprocessing/test_DB', 'mission_name': 'testDB'}, 'instrument': {'instrument_name': 'rot13'}, 'process_rot13_L1-L2': {'code_cpu': '1', 'code_start_date': '2010-09-01', 'code_stop_date': '2020-01-01', 'code_filename': 'run_rot13_L1toL2.py', 'code_relative_path': 'scripts', 'required_input1': 'product_concat', 'code_version': '1.0.0', 'process_name': 'rot_L1toL2', 'code_output_interface': '1', 'code_newest_version': 'True', 'code_date_written': '2016-05-31', 'code_description': 'Python L1->L2', 'output_product': 'product_rot13', 'code_active': 'True', 'code_arguments': '', 'extra_params': '', 'output_timebase': 'FILE', 'code_ram': '1'}}
        self.assertEqual(ans, conf)

    def test_sectionCheck_Valid(self):
        """Make sure no Exceptions are thrown for a valid config"""
        try:
            conf = readconfig('testing_configs/testDB.conf')
            _sectionCheck(conf)
        except Exception as e:
            self.fail(e)

    def test_sectionCheck_MissingMission(self):
        """Make sure it notices Mission is missing"""
        conf = readconfig('testing_configs/testDB_noMission.conf')
        self.assertRaises(ValueError, _sectionCheck, conf)

    def test_sectionCheck_FakeSection(self):
        """Make sure it notices there's a fake section"""
        conf = readconfig('testing_configs/testDB_fakeSection.conf')
        self.assertRaises(ValueError, _sectionCheck, conf)

    def test_keysCheck_Valid(self):
        """Make sure no Exceptions are thrown for a valid config"""
        conf = readconfig('testing_configs/testDB.conf')
        try:
            for key in conf.keys():
                _keysCheck(conf, key)
        except Exception as e:
            self.fail(e)

    def test_keysCheck_MissingRequired(self):
        """Make sure it notices missing required keys"""
        conf = readconfig('testing_configs/testDB_missingKey.conf')
        self.assertRaises(ValueError, _keysCheck, conf, 'mission')

    def test_keysRemoveExtra(self):
        """Make sure it removes extra keys"""
        validResult = readconfig('testing_configs/testDB.conf')

        conf = _keysRemoveExtra(readconfig('testing_configs/testDB_extraKey.conf'), 'mission')
        self.assertEqual(validResult['mission'], conf)

    def test_keysPresentCheck_Valid(self):
        try:
            conf = readconfig('testing_configs/testDB.conf')
            _keysPresentCheck(conf)
        except Exception as e:
            self.fail(e)

    def test_keysPresentCheck_Invalid(self):
        conf = readconfig('testing_configs/testDB_missingProduct.conf')
        self.assertRaises(ValueError, _keysPresentCheck, conf)

    def test_fileTest_Valid(self):
        try:
            _fileTest('testing_configs/testDB.conf')
        except Exception as e:
            self.fail(e)

    def test_fileTest_Invalid(self):
        self.assertRaises(ValueError, _fileTest, 'testing_configs/testDB_duplicateSection.conf')

if __name__ == "__main__":
    unittest.main()
