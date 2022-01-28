#!/usr/bin/env python
from __future__ import print_function

import datetime
import unittest
import os
import os.path
import shutil
import tempfile

import dbp_testing
from dbprocessing import DBfile
from dbprocessing import DButils
from dbprocessing import Diskfile
from dbprocessing import Version

class DBfileTests(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """Tests for DBfile class"""
    
    def setUp(self):
        super(DBfileTests, self).setUp()
        self.makeTestDB()
        sourcedir = os.path.join(dbp_testing.testsdir, '..',
                                 'functional_test', 'L0')
        shutil.copytree(sourcedir, os.path.join(self.td, 'L0'))
        sourcepath = os.path.join(dbp_testing.testsdir, 'tars')
        for f in os.listdir(sourcepath):
            shutil.copy2(os.path.join(sourcepath, f), os.path.join(self.td, f))
        self.loadData(os.path.join(dbp_testing.testsdir, 'data', 'db_dumps',
                                   'testDB_dump.json'))
        #Update the mission path to the tmp dir
        self.dbu.getEntry('Mission', 1).rootdir = self.td
        self.dbu.commitDB()
        self.dbu.MissionDirectory = self.dbu.getMissionDirectory()

    def tearDown(self):
        super(DBfileTests, self).tearDown()
        self.removeTestDB()

    def createDummyDBF(self, fname):
        fullpath = os.path.join(self.td, fname)
        dbf = DBfile.DBfile(fullpath, self.dbu, makeDiskFile=True)
        dbf.diskfile.params['utc_file_date'] = datetime.date.today()
        dbf.diskfile.params['utc_start_time'] = datetime.date.today()
        dbf.diskfile.params['utc_stop_time'] = datetime.date.today() + datetime.timedelta(days=1)
        dbf.diskfile.params['data_level'] = 0
        dbf.diskfile.params['file_create_date'] = datetime.date.today()
        dbf.diskfile.params['exists_on_disk'] = True
        dbf.diskfile.params['product_id'] = 1
        dbf.diskfile.params['shasum'] = Diskfile.calcDigest(fullpath)
        dbf.diskfile.params['version'] = Version.Version(1, 2, 3)

        return dbf

    def test_invalidInput(self):
        self.assertRaises(DBfile.DBfileError, DBfile.DBfile, self.td + '/L0/testDB_000_000.raw', self.dbu)

    def test_repr(self):
        dbf = DBfile.DBfile(self.td + '/L0/testDB_000_000.raw', self.dbu, makeDiskFile=True)
        self.assertTrue(dbf.__repr__().startswith("<DBfile.DBfile object: "))

    def test_getDirectory(self):
        dbf = DBfile.DBfile(self.td + '/L0/testDB_000_000.raw', self.dbu, makeDiskFile=True)
        
        #Fails because product_id is not set
        self.assertRaises(DBfile.DBfileError, dbf.getDirectory )

        dbf.diskfile.params['product_id'] = 4
        self.assertEqual(os.path.join(self.td, 'L0'), dbf.getDirectory())

    def test_addFileToDB(self):
        with open(os.path.join(self.td, 'file.file'), 'w') as fp:
            fp.write('I am some test data\n')
        dbf = self.createDummyDBF('file.file')

        self.assertTrue(dbf.addFileToDB())

    def test_move_NormalFile(self):
        with open(os.path.join(self.td, 'file.file'), 'w') as fp:
            fp.write('I am some test data\n')
        dbf = self.createDummyDBF('file.file')

        real_ans = (os.path.join(self.td, 'file.file'),
                    os.path.join(self.td, 'L1', 'file.file'))
        self.assertEqual(real_ans, dbf.move())

    def test_move_SymLink(self):
        targetpath = os.path.join(self.td, 'file.file')
        linkpath = os.path.join(self.td, 'sym.file')
        with open(targetpath, 'w') as fp:
            fp.write('I am some test data\n')
        os.symlink(targetpath, linkpath)
        dbf = self.createDummyDBF('sym.file')

        real_ans = (linkpath,
                    os.path.join(self.td, 'L1', 'sym.file'))
        self.assertTrue(os.path.isfile(linkpath))
        self.assertEqual(real_ans, dbf.move())
        self.assertFalse(os.path.isfile(linkpath))
        self.assertFalse(os.path.isfile(os.path.join(
            self.td, 'L1', 'sym.file')))

    def test_move_NormalFileTargetDoesntExist(self):
        with open(os.path.join(self.td, 'file.file'), 'w') as fp:
            fp.write('I am some test data\n')
        dbf = self.createDummyDBF('file.file')

        real_ans = (os.path.join(self.td, 'file.file'),
                    os.path.join(self.td, 'L1', 'file.file'))
        self.assertFalse(os.path.isdir(os.path.join(self.td, 'L1')))
        self.assertEqual(real_ans, dbf.move())
        self.assertTrue(os.path.isdir(os.path.join(self.td, 'L1')))

    def test_move_goodtgzfile(self):
        """Test moving a valid .tgz file"""
        dbf = self.createDummyDBF('goodtar.tgz')

        real_ans = (os.path.join(self.td, 'goodtar.tgz'),
                    os.path.join(self.td, 'L1', 'goodtar.tgz'))
        self.assertFalse(os.path.isdir(os.path.join(self.td, 'L1')))
        self.assertEqual(real_ans, dbf.move())
        self.assertTrue(os.path.isdir(os.path.join(self.td, 'L1')))
        # Verify that archive was expanded
        self.assertTrue(os.path.isfile(os.path.join(self.td, 'tar1.txt')))
        self.assertTrue(os.path.isfile(os.path.join(self.td, 'tar2.txt')))

    def test_move_badtgzfile(self):
        """Test moving a corrupted .tgz file"""
        dbf = self.createDummyDBF('badtar.tgz')

        real_ans = (os.path.join(self.td, 'badtar.tgz'),
                    os.path.join(self.td, 'L1', 'badtar.tgz'))
        self.assertFalse(os.path.isdir(os.path.join(self.td, 'L1')))
        # Method return may not be helpful but this is it for now
        self.assertEqual(real_ans, dbf.move())
        self.assertTrue(os.path.isdir(os.path.join(self.td, 'L1')))

    def test_move_nulltgzfile(self):
        """Test moving an empty .tgz file"""
        dbf = self.createDummyDBF('emptytar.tgz')

        real_ans = (os.path.join(self.td, 'emptytar.tgz'),
                    os.path.join(self.td, 'L1', 'emptytar.tgz'))
        self.assertFalse(os.path.isdir(os.path.join(self.td, 'L1')))
        # Method return may not be helpful but this is it for now
        self.assertEqual(real_ans, dbf.move())
        self.assertTrue(os.path.isdir(os.path.join(self.td, 'L1')))


if __name__ == "__main__":
    unittest.main()
