#!/usr/bin/env python
from __future__ import print_function

import datetime
import unittest
from distutils.dir_util import copy_tree, remove_tree
import os
import tempfile

import dbp_testing
from dbprocessing import DBfile
from dbprocessing import DButils
from dbprocessing import Diskfile
from dbprocessing import Version

class DBfileTests(unittest.TestCase):
    """Tests for DBfile class"""
    
    def setUp(self):
        super(DBfileTests, self).setUp()
        self.tempD = tempfile.mkdtemp()
        copy_tree(os.path.join(dbp_testing.testsdir, '..', 'functional_test'), self.tempD)
        copy_tree(os.path.join(dbp_testing.testsdir, 'tars'), self.tempD)

        self.dbu = DButils.DButils(os.path.join(self.tempD, 'testDB.sqlite'))
        #Update the mission path to the tmp dir
        self.dbu.getEntry('Mission', 1).rootdir = self.tempD
        self.dbu.commitDB()
        self.dbu.MissionDirectory = self.dbu.getMissionDirectory()

    def tearDown(self):
        super(DBfileTests, self).tearDown()
        remove_tree(self.tempD)

    def createDummyDBF(self, fname):
        dbf = DBfile.DBfile(self.tempD + fname, self.dbu, makeDiskFile=True)
        dbf.diskfile.params['utc_file_date'] = datetime.date.today()
        dbf.diskfile.params['utc_start_time'] = datetime.date.today()
        dbf.diskfile.params['utc_stop_time'] = datetime.date.today() + datetime.timedelta(days=1)
        dbf.diskfile.params['data_level'] = 0
        dbf.diskfile.params['file_create_date'] = datetime.date.today()
        dbf.diskfile.params['exists_on_disk'] = 1
        dbf.diskfile.params['product_id'] = 1
        dbf.diskfile.params['shasum'] = Diskfile.calcDigest(self.tempD + fname)
        dbf.diskfile.params['version'] = Version.Version(1, 2, 3)

        return dbf

    def test_invalidInput(self):
        self.assertRaises(DBfile.DBfileError, DBfile.DBfile, self.tempD + '/L0/testDB_000_000.raw', self.dbu)

    def test_repr(self):
        dbf = DBfile.DBfile(self.tempD + '/L0/testDB_000_000.raw', self.dbu, makeDiskFile=True)
        self.assertTrue(dbf.__repr__().startswith("<DBfile.DBfile object: "))

    def test_getDirectory(self):
        dbf = DBfile.DBfile(self.tempD + '/L0/testDB_000_000.raw', self.dbu, makeDiskFile=True)
        
        #Fails because product_id is not set
        self.assertRaises(DBfile.DBfileError, dbf.getDirectory )

        dbf.diskfile.params['product_id'] = 4
        self.assertEqual( self.tempD + '/L0', dbf.getDirectory() )

    def test_addFileToDB(self):
        with open(self.tempD + '/file.file', 'w') as fp:
            fp.write('I am some test data\n')
        dbf = self.createDummyDBF('/file.file')

        self.assertTrue(dbf.addFileToDB())

    def test_move_NormalFile(self):
        with open(self.tempD + '/file.file', 'w') as fp:
            fp.write('I am some test data\n')
        dbf = self.createDummyDBF('/file.file')

        real_ans = (self.tempD + '/file.file', self.tempD + '/L1/file.file')
        self.assertEqual(real_ans, dbf.move())

    def test_move_SymLink(self):
        with open(self.tempD + '/file.file', 'w') as fp:
            fp.write('I am some test data\n')
        os.symlink(self.tempD + '/file.file', self.tempD + '/sym.file')
        dbf = self.createDummyDBF('/sym.file')

        real_ans = (self.tempD + '/sym.file', self.tempD + '/L1/sym.file')
        self.assertTrue(os.path.isfile(self.tempD + '/sym.file'))
        self.assertEqual(real_ans, dbf.move())
        self.assertFalse(os.path.isfile(self.tempD + '/sym.file'))
        self.assertFalse(os.path.isfile(self.tempD + '/L1/sym.file'))

    def test_move_NormalFileTargetDoesntExist(self):
        with open(self.tempD + '/file.file', 'w') as fp:
            fp.write('I am some test data\n')
        dbf = self.createDummyDBF('/file.file')

        remove_tree(self.tempD+'/L1')

        real_ans = (self.tempD + '/file.file', self.tempD + '/L1/file.file')
        self.assertFalse(os.path.isdir(self.tempD + '/L1'))
        self.assertEqual(real_ans, dbf.move())
        self.assertTrue(os.path.isdir(self.tempD + '/L1'))

    def test_move_goodtgzfile(self):
        """Test moving a valid .tgz file"""
        dbf = self.createDummyDBF('/goodtar.tgz')

        remove_tree(self.tempD + '/L1')
        real_ans = (self.tempD + '/goodtar.tgz', self.tempD + '/L1/goodtar.tgz')
        self.assertFalse(os.path.isdir(os.path.join(self.tempD, 'L1')))
        self.assertEqual(real_ans, dbf.move())
        self.assertTrue(os.path.isdir(os.path.join(self.tempD, 'L1')))
        # Verify that archive was expanded
        self.assertTrue(os.path.isfile(os.path.join(self.tempD, 'tar1.txt')))
        self.assertTrue(os.path.isfile(os.path.join(self.tempD, 'tar2.txt')))

    def test_move_badtgzfile(self):
        """Test moving a corrupted .tgz file"""
        dbf = self.createDummyDBF('/badtar.tgz')

        remove_tree(self.tempD + '/L1')
        real_ans = (self.tempD + '/badtar.tgz', self.tempD + '/L1/badtar.tgz')
        self.assertFalse(os.path.isdir(os.path.join(self.tempD, 'L1')))
        # Method return may not be helpful but this is it for now
        self.assertEqual(real_ans, dbf.move())
        self.assertTrue(os.path.isdir(os.path.join(self.tempD, 'L1')))

    def test_move_nulltgzfile(self):
        """Test moving an empty .tgz file"""
        dbf = self.createDummyDBF('/emptytar.tgz')

        remove_tree(self.tempD + '/L1')
        real_ans = (self.tempD + '/emptytar.tgz', self.tempD + '/L1/emptytar.tgz')
        self.assertFalse(os.path.isdir(os.path.join(self.tempD, 'L1')))
        # Method return may not be helpful but this is it for now
        self.assertEqual(real_ans, dbf.move())
        self.assertTrue(os.path.isdir(os.path.join(self.tempD, 'L1')))


if __name__ == "__main__":
    unittest.main()
