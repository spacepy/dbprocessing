#!/usr/bin/env python
from __future__ import print_function

import datetime
import unittest
import shutil
import os

from dbprocessing.Diskfile import calcDigest

class TestFunctional(unittest.TestCase):
    def setUp(self):
        super(TestFunctional, self).setUp()
        shutil.copy('../testDB.sqlite.bak', '../testDB.sqlite')
        os.system('./ProcessQueue.py -i -m ../testDB.sqlite')
        os.system('./ProcessQueue.py -p -m ../testDB.sqlite')

    def tearDown(self):
        super(TestFunctional, self).tearDown()
        os.remove('../testDB.sqlite')

    def test_FileChecksums(self):
        self.assertEqual('4b3c2153586d3518238c0d601cc516d16054fd74', calcDigest('../L1/testDB_000.cat'))
        self.assertEqual('1409f6cc7ca6b44be1ddd16edfe8fa1606c2ef4a', calcDigest('../L1/testDB_001.cat'))
        self.assertEqual('e11d18f1f5b56edc782bb6bb54dc72b92b392c14', calcDigest('../L2/testDB_000.rot'))
        self.assertEqual('95f8786e3fa4fb5798a663a0c3b019b6c99a0d8b', calcDigest('../L2/testDB_001.rot'))

if __name__ == '__main__':
    unittest.main()