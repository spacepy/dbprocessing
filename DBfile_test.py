#!/usr/bin/env python2.6

import unittest
import DBfile
import os
import Diskfile

class DBfileTests(unittest.TestCase):
    def setUp(self):
        super(DBfileTests, self).setUp()
        self.test_filename = 'Test-one_R0_evinst_20100112_v2.0.0.cdf'
        with open(self.test_filename, 'wb') as f:
            f.write('I am some text in a file')
        self.df = Diskfile.Diskfile(self.test_filename)
        self.df.parseAll()

    def tearDown(self):
        super(DBfileTests, self).tearDown()
        os.remove(self.test_filename)

    def test_badInput(self):
        """DBfile object will only take a Diskfile as input"""
        self.assertRaises(DBfile.DBfileError, DBfile.DBfile, 'wrong input')





if __name__ == "__main__":
    unittest.main()

