#!/usr/bin/env python2.6

import datetime
import os
import stat
import unittest

import DBUtils2
import Diskfile
import Version


__version__ = '2.0.3'


class DiskfileStaticTests(unittest.TestCase):
    """Tests for the static methods in Diskfile"""

    def test_calcDigest(self):
        """ calcDigest  should behave correctly"""
        self.assertRaises(Diskfile.DigestError, Diskfile.calcDigest, 'idontexist.file')
        with open('IamAfileThatExists.file', 'wb') as f:
            f.write('I am some text in a file')
        real_ans = 'aa42c02f50c92203be933747670bdd512848385e'
        ans = Diskfile.calcDigest('IamAfileThatExists.file')
        self.assertEqual(real_ans, ans)
        with open('IamAfileThatExists.file', 'wb+') as f:
            f.write('I m more text')
        ans = Diskfile.calcDigest('IamAfileThatExists.file')
        self.assertNotEqual (real_ans, ans)
        f.close()
        os.remove('IamAfileThatExists.file')


class DiskfileTests(unittest.TestCase):
    """Tests for Diskfile class"""

    def setUp(self):
        super(DiskfileTests, self).setUp()
        dbo = DBUtils2.DBUtils2()
        dbo._openDB()
        dbo._createTableObjects()
        self.dbo = dbo

    def tearDown(self):
        super(DiskfileTests, self).tearDown()
        #Shouldn't this do something to close the db object created in setup?

    def test_read_error(self):
        """given a file input that is not readable raise ReadError:"""
        self.assertRaises(Diskfile.ReadError, Diskfile.Diskfile, 'wrong input',
                          self.dbo)

    def test_repr(self):
        """repr retuens a known string"""
        self.assertEqual("DBProcessing class instance for mission Test, version: 2.0.3", self.dbo.__repr__())


    def test_write_error(self):
        """given a file input that is not writeable WriteError"""
        with open('IamAfileThatExists.file', 'wb') as f:
            f.write('I am some text in a file')
        os.chmod('IamAfileThatExists.file', stat.S_IRUSR)
        self.assertRaises(Diskfile.WriteError, Diskfile.Diskfile,
                          'IamAfileThatExists.file', self.dbo)

        os.chmod('IamAfileThatExists.file', stat.S_IWUSR|stat.S_IRUSR)
        os.remove('IamAfileThatExists.file')

    def test_init(self):
        """init does some checking"""
        with open('IamAfileThatExists.file', 'wb') as f:
            f.write('I am some text in a file')
        try:
            a = Diskfile.Diskfile('IamAfileThatExists.file', self.dbo)
        finally:
            os.remove('IamAfileThatExists.file')

    def test_makeProductFilename(self):
        """makeProductFilename shoould make a known filename"""
        with open('Test-Test_R0_evinst_20090117_v1.0.0.cdf', 'wb') as f:
            f.write('I am some text in a file')
        try:
            a = Diskfile.Diskfile('Test-Test_R0_evinst_20090117_v1.0.0.cdf',
                                  self.dbo)
            now = datetime.datetime.utcnow()
            expect = u'Test-Test_L1_evinst_{:04}{:02}{:02}_v1.0.0.cdf'.format(
                now.year, now.month, now.day)

            self.assertEqual(a.makeProductFilename(17, datetime.datetime.utcnow(),
                                    Version.Version(1, 0, 0)),
                                    expect)
        finally:
            os.remove('Test-Test_R0_evinst_20090117_v1.0.0.cdf')

    def test_makeProductFilenameChecks(self):
        """makeProductFilename should do some checking"""
        with open('Test-Test_R0_evinst_20090117_v1.0.0.cdf', 'wb') as f:
            f.write('I am some text in a file')
        try:
            a = Diskfile.Diskfile('Test-Test_R0_evinst_20090117_v1.0.0.cdf',
                                  self.dbo)
            self.assertRaises(Diskfile.InputError, a.makeProductFilename, 17,
                              datetime.datetime.utcnow(), '1.0.0')
            self.assertRaises(Diskfile.InputError, a.makeProductFilename, 17,
                              'bad in', Version.Version(1, 0, 0))
            self.assertRaises(Diskfile.InputError, a.makeProductFilename, 17,
                              datetime.datetime.utcnow(), Version.Version(1,0,0),
                              'bad in')
        finally:
            os.remove('Test-Test_R0_evinst_20090117_v1.0.0.cdf')


if __name__ == "__main__":
    unittest.main()
