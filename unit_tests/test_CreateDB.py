#!/usr/bin/env python

import os.path
import tempfile
import unittest
import shutil

import dbp_testing
dbp_testing.add_scripts_to_path()

from dbprocessing import DButils
import CreateDB


class CreateDBTests(unittest.TestCase):
    """Tests for CreateDB script"""

    def test1(self):
        td = tempfile.mkdtemp()
        try:
            testfile = os.path.join(td, 'test.sqlite')
            CreateDB.main([testfile])
            dbu = DButils.DButils(testfile)
            del dbu
        finally:
            shutil.rmtree(td)


if __name__ == "__main__":
    unittest.main()
