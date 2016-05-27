#!/usr/bin/env python
from __future__ import print_function

import datetime
import unittest
import os
import tempfile


from dbprocessing import DBfile
from dbprocessing import DButils
from dbprocessing import Diskfile

filename = 'test_file.txt'


class dummyDBU(object):
    def __init__(self):
        self.mission = filename


class DBfileTests(unittest.TestCase):
    """Tests for DBfile class"""
    
    def setUp(self):
        super(DBfileTests, self).setUp()
        with open(filename, 'w') as fp:
            fp.write('I am some test data\n')
        self.dbu = dummyDBU()
        self.diskfile = Diskfile.Diskfile(filename, self.dbu)
        self.diskfile.params['utc_file_date'] = datetime.date(2012, 4, 12)
        self.diskfile.params['interface_version'] = 1 
        self.diskfile.params['quality_version'] = 2
        self.diskfile.params['revision_version'] = 3


    def tearDown(self):
        super(DBfileTests, self).tearDown()
        os.remove(filename)


if __name__ == "__main__":
    unittest.main()
