#!/usr/bin/env python2.6

import unittest
import DBUtils2
import datetime
import Diskfile
import os
import Version

class StaticTests(unittest.TestCase):
    """ tests for the static methods in DBfile"""
    def setUp(self):
        super(StaticTests, self).setUp()
        pass

    def tearDown(self):
        super(StaticTests, self).tearDown()
        pass


    def test_calcDigest(self):
        """ calcDigest  should behave correctly"""
        self.assertRaises(Diskfile.DigestError, Diskfile.calcDigest, 'idontexist.file')
        with open('IamAfileThatExists.file', 'wb') as f:
            f.write('I am some text in a file')
        real_ans = '5fcab280bae1c870ddc3ca6c899bb29c'
        ans = Diskfile.calcDigest('IamAfileThatExists.file')
        self.assertEqual(real_ans, ans)
        with open('IamAfileThatExists.file', 'wb+') as f:
            f.write('I m more text')
        ans = Diskfile.calcDigest('IamAfileThatExists.file')
        self.assertNotEqual (real_ans, ans)
        f.close()
        os.remove('IamAfileThatExists.file')

    def test_make_Testfilename(self):
        """make_Testfilename should give known output for known input"""
        in_val = [ ('one', 'Product1',  datetime.datetime(2000, 12, 12), Version.Version(1,0,0)),
                        ('one', 'Product2',  datetime.date(2000, 12, 12), Version.Version(2,1,0)) ]
        real_ans = [ 'Test-one_Product1_20001212_v1.0.0.cdf', 'Test-one_Product2_20001212_v2.1.0.cdf' ]
        for i, val in enumerate(real_ans):
            self.assertEqual(val, Diskfile.make_Testfilename(*in_val[i]) )

class DiskfileTests(unittest.TestCase):
    def setUp(self):
        super(DiskfileTests, self).setUp()
        dbo = DBUtils2.DBUtils2()
        dbo._openDB()
        dbo._createTableObjects()
        self.dbo = dbo

    def tearDown(self):
        super(DiskfileTests, self).tearDown()
        pass

    def test_read_error(self):
        """given a file input that is not readable raise ReadError:"""
        self.assertRaises(Diskfile.ReadError, Diskfile.Diskfile, 'wrong input')


    def test_write_error(self):
        """given a file input that is not writeable WriteError"""
#        with open('IamAfileThatExists.file', 'wb') as f:
#            f.write('I am some text in a file')
        pass   #TODO how do I set a file to readonly?



    def test_isTestFile(self):
        """__isTestFile should return True for a Test file and False otherwise:"""
        self.assertRaises(Diskfile.ReadError, Diskfile.Diskfile, 'wrong input')
        test_filename = 'Test-one_R0_evinst_20100112_v1.0.0.cdf'
        with open(test_filename, 'wb') as f:
            f.write('I am some text in a file')

        fn2 = Diskfile.Diskfile(test_filename)
        self.assertTrue(fn2.isTestFile() )
        os.remove(test_filename)


    def test_parseTestFile(self):
        """ parseTestFile should parse a test file into a dict"""
        test_filename = 'Test-one_R0_evinst_20100112_v1.0.0.cdf'
        with open(test_filename, 'wb') as f:
            f.write('I am some text in a file')
        fn = Diskfile.Diskfile(test_filename)
        fn.parse_TestFile()
        params = {}
        params['filename'] = test_filename
        params['utc_file_date'] = datetime.date(2010, 1, 12)
        params['utc_start_time'] = datetime.datetime(2010, 1, 12)
        params['utc_stop_time'] = datetime.datetime(2010, 1, 12, 23, 59, 59, 999999)
        params['data_level'] = 0.0
        params['check_date'] = None
        params['verbose_provenance'] = None
        params['quality_comment'] = None
        params['caveats'] = None
        params['release_number'] = None
        params['file_create_date'] = datetime.datetime.now()
        params['met_start_time'] = None
        params['met_stop_time'] = None
        params['exists_on_disk'] = True
        params['quality_checked'] = None
        params['product_id'] = 16
        params['file_create_date'] = datetime.datetime(2010, 1, 12, 23, 59, 59)
        params['version'] = Version.Version(1, 0, 0)
        for val in params:
            if val == 'file_create_date': continue #dont test this
            if val == 'md5sum': continue  # dont test this
            self.assertEqual(params[val], fn.params[val])
        os.remove(test_filename)





if __name__ == "__main__":
    unittest.main()


