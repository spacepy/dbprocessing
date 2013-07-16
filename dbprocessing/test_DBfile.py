#!/usr/bin/env python2.6

import datetime
import unittest
import os
import tempfile


import DBfile
import DBUtils
import Diskfile

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

    def test_doDirSubs(self):
        """the substitutions should work"""
        dbf = DBfile.DBfile(self.diskfile, self.dbu)
        path = '{Y}{m}{d}'
        self.assertEqual('20120412', dbf._doDirSubs(path))
        path = '{DATE}'
        self.assertEqual('20120412', dbf._doDirSubs(path))
        path = '{Y}{b}{d}'
        self.assertEqual('2012Apr12', dbf._doDirSubs(path))
        path = '{Y}{j}'
        self.assertEqual('2012103', dbf._doDirSubs(path))
        path = '{VERSION}'
        self.assertEqual('1.2.3', dbf._doDirSubs(path))


if __name__ == "__main__":
    unittest.main()
