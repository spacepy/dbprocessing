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
            pg = 'PGDATABASE' in os.environ
            testdb = os.environ['PGDATABASE'] if pg\
                else os.path.join(td, 'emptyDB.sqlite')
            argv = (['--dialect', 'postgresql'] if pg else []) + [testdb]
            CreateDB.main(argv)
            dbu = DButils.DButils(testdb)
            if pg:
                dbu.session.close()
                dbu.metadata.drop_all()
            del dbu
        finally:
            shutil.rmtree(td)


if __name__ == "__main__":
    unittest.main()
