#!/usr/bin/env python2.6

import datetime
import os
import tempfile
import unittest
import subprocess

from dbprocessing import DBfile
from dbprocessing import DButils
from ..scripts import CreateDB

filename = 'test_file.txt'



class CreateDB(unittest.TestCase):
    """Tests for CreateDB script"""

    def setUp(self):
        super(CreateDB, self).setUp()
        # run the script
        self.tfile = tempfile.NamedTemporaryFile(delete=False)
        self.tfile.close()
        self.tfile = self.tfile.name
        os.remove(self.tfile)
        subprocess.check_call( [ 'python', os.path.expanduser(os.path.join('~', 'dbUtils', 'CreateDB.py')), self.tfile ] )

    def tearDown(self):
        print('444444444')
        super(CreateDB, self).tearDown()
        print('5555555555')
        os.remove(self.tfile)
        print('666666666')


    def test1(self):
        print('111111111')
        dbu = DButils.DButils(self.tfile)
        print('222222222')
        del dbu
        print('333333333')




if __name__ == "__main__":
    unittest.main()
