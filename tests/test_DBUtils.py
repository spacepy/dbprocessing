#!/usr/bin/env python2.6

import datetime
import os
import os.path
import shutil
import stat
import sys
import unittest
import tempfile
import time

from dbprocessing import DBUtils
from dbprocessing import Version


__version__ = '2.0.3'



class DBUtilsDBTests(unittest.TestCase):
    """Tests for database access through DBUtils"""

    def setUp(self):
        super(DBUtilsDBTests, self).setUp()
        sqpath = os.path.join(os.path.dirname(__file__), 'RBSP_MAGEIS.sqlite')
        self.sqlworking = sqpath.replace('RBSP_MAGEIS.sqlite', 'working.sqlite')
        shutil.copy(sqpath, self.sqlworking)
        os.chmod(self.sqlworking, stat.S_IRUSR|stat.S_IWUSR)
        self.dbu = DBUtils.DBUtils(self.sqlworking)

    def tearDown(self):
        super(DBUtilsDBTests, self).tearDown()
        self.dbu._closeDB()
        del self.dbu
        os.remove(self.sqlworking)

    def test_init(self):
        """__init__ has an exception to test"""
        self.assertRaises(DBUtils.DBError, DBUtils.DBUtils, None)


class ProcessqueueTests(unittest.TestCase):
    """Test all the processqueue functionality"""

    def setUp(self):
        super(ProcessqueueTests, self).setUp()
        sqpath = os.path.join(os.path.dirname(__file__), 'RBSP_MAGEIS.sqlite')
        self.sqlworking = sqpath.replace('RBSP_MAGEIS.sqlite', 'working.sqlite')
        shutil.copy(sqpath, self.sqlworking)
        os.chmod(self.sqlworking, stat.S_IRUSR|stat.S_IWUSR)
        self.dbu = DBUtils.DBUtils(self.sqlworking, echo=False)

    def tearDown(self):
        super(ProcessqueueTests, self).tearDown()
        self.dbu._closeDB()
        del self.dbu
        os.remove(self.sqlworking)

    def add_files(self):
        self.dbu.Processqueue.push([17,18,19,20,21])
    
    def test_pq_flush(self):
        """test self.Processqueue.flush"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.dbu.Processqueue.flush()
        self.assertEqual(0, self.dbu.Processqueue.len())

    def test_pq_remove(self):
        """test self.Processqueue.remove"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.dbu.Processqueue.remove(20)
        self.assertEqual(4, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [17,18,19,21]:
            self.assertTrue(v in pq)
        self.dbu.Processqueue.remove([17,18])
        self.assertEqual(2, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [19,21]:
            self.assertTrue(v in pq)
        self.dbu.Processqueue.remove('ect_rbspb_0377_381_03.ptp.gz')
        self.assertEqual(1, self.dbu.Processqueue.len())
        self.assertEqual([21],  self.dbu.Processqueue.getAll())

    def test_pq_push(self):
        """test self.Processqueue.push"""
        t0 = time.time()
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.dbu.Processqueue.push(20)
        self.assertEqual(1, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        self.assertTrue(20 in pq)
        # push a value that is not there
        self.assertFalse(self.dbu.Processqueue.push(214442))
        self.assertFalse(self.dbu.Processqueue.push(20))
        self.assertEqual([17,18,19,21], self.dbu.Processqueue.push([17,18,19,20,21]))

    
    def test_pq_len(self):
        """test self.Processqueue.len"""
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())

    def test_pq_pop(self):
        """test self.Processqueue.pop"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.dbu.Processqueue.pop(0)
        self.assertEqual(4, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [18,19,20,21]:
            self.assertTrue(v in pq)
        self.dbu.Processqueue.pop(2)
        self.assertEqual(3, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [18,19,21]:
            self.assertTrue(v in pq)

    def test_pq_get(self):
        """test self.Processqueue.get"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.assertEqual(17, self.dbu.Processqueue.get(0))
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.assertEqual(19, self.dbu.Processqueue.get(2))
        self.assertEqual(5, self.dbu.Processqueue.len())

    def test_pq_clean(self):
        """test self.Processqueue.clean"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.dbu.Processqueue.clean()
        self.assertEqual(1, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        self.assertTrue(17 in pq)

    def test_pq_rawadd(self):
        """test self.Processqueue.rawadd"""
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.dbu.Processqueue.rawadd(20)
        self.assertEqual(1, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        self.assertTrue(20 in pq)
        self.dbu.Processqueue.rawadd(20000)
        pq = self.dbu.Processqueue.pop(1)
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFileID, pq) 



class DBUtilsClassMethodTests(unittest.TestCase):
    """Tests for class methods of DBUtils"""

#    def test_test_SQLAlchemy_version(self):
#        """The testing of the SQLAlchemy version should work"""
#        self.assertTrue(DBUtils.DBUtils._test_SQLAlchemy_version())
#        errstr = 'SQLAlchemy version wrong_Ver was not expected, expected 0.7.x'
#        try:
#            DBUtils.DBUtils._test_SQLAlchemy_version('wrong_Ver')
#        except DBUtils.DBError:
#            self.assertEqual(sys.exc_info()[1].__str__(),
#                             errstr)
#        else:
#            self.fail('Should have raised DBError: ' +
#                      errstr)

    def test_daterange_to_dates(self):
        """daterange_to_dates"""
        daterange = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 6)]
        expected = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5), datetime.datetime(2000, 1, 6)]
        self.assertEqual(expected, DBUtils.DBUtils.daterange_to_dates(daterange))
        daterange = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5, 23)]
        expected = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5)]
        self.assertEqual(expected, DBUtils.DBUtils.daterange_to_dates(daterange))



if __name__ == "__main__":
    unittest.main()
