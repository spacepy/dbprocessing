#!/usr/bin/env python2.6

import os
import os.path
import tempfile
import unittest
import subprocess
import sys

import dbp_testing
dbp_testing.add_scripts_to_path()

from dbprocessing import DButils
import CreateDB


class CreateDB(unittest.TestCase):
    """Tests for CreateDB script"""

    def setUp(self):
        super(CreateDB, self).setUp()
        # run the script
        self.tfile = tempfile.NamedTemporaryFile(delete=False)
        self.tfile.close()
        self.tfile = self.tfile.name
        os.remove(self.tfile)
        subprocess.check_call([
            sys.executable, os.path.abspath(os.path.join(
                dbp_testing.testsdir, '..', 'scripts', 'CreateDB.py')),
            self.tfile])

    def tearDown(self):
        super(CreateDB, self).tearDown()
        os.remove(self.tfile)

    def test1(self):
        dbu = DButils.DButils(self.tfile)
        del dbu


if __name__ == "__main__":
    unittest.main()
