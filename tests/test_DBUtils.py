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

try: # new version changed this annoyingly
    from sqlalchemy.exceptions import IntegrityError
    from sqlalchemy.orm.exceptions import NoResultFound
except ImportError:
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm.exc import NoResultFound

from dbprocessing import DBUtils
from dbprocessing import Version


__version__ = '2.0.3'



class DBUtilsGetTests(unittest.TestCase):
    """Tests for database gets through DBUtils"""

    def setUp(self):
        super(DBUtilsGetTests, self).setUp()
        sqpath = os.path.join(os.path.dirname(__file__), 'RBSP_MAGEIS.sqlite')
        self.sqlworking = sqpath.replace('RBSP_MAGEIS.sqlite', 'working.sqlite')
        shutil.copy(sqpath, self.sqlworking)
        os.chmod(self.sqlworking, stat.S_IRUSR|stat.S_IWUSR)
        self.dbu = DBUtils.DBUtils(self.sqlworking)

    def tearDown(self):
        super(DBUtilsGetTests, self).tearDown()
        self.dbu._closeDB()
        del self.dbu
        os.remove(self.sqlworking)

    def test_init(self):
        """__init__ has an exception to test"""
        self.assertRaises(DBUtils.DBError, DBUtils.DBUtils, None)

    def test_getAllSatellites(self):
        """getAllSatellites"""
        ans = self.dbu.getAllSatellites()
        # check that this is what we expect
        self.assertEqual(2, len(ans))
        self.assertEqual([('satellite', 'satellite'), ('mission', 'mission')], zip(*ans))
        self.assertEqual(ans[0]['mission'], ans[1]['mission'])
        self.assertEqual(ans[0]['satellite'].satellite_name[:-1], 
                         ans[1]['satellite'].satellite_name[:-1])

    def test_getAllInstruments(self):
        """getAllInstruments"""
        ans = self.dbu.getAllInstruments()
        # check that this is what we expect
        self.assertEqual(2, len(ans))
        self.assertEqual([('instrument', 'instrument'), 
                          ('satellite', 'satellite'), 
                          ('mission', 'mission')], zip(*ans))
        self.assertEqual(ans[0]['mission'], ans[1]['mission'])
        self.assertEqual(ans[0]['satellite'].satellite_name[:-1], 
                         ans[1]['satellite'].satellite_name[:-1])     
        self.assertEqual(ans[0]['instrument'].instrument_name, 
                         ans[1]['instrument'].instrument_name)

    def test_getAllFilenames(self):
        """getAllFilenames"""
        files = self.dbu.getAllFilenames(fullPath=False)
        self.assertEqual(6681, len(files))
        files = self.dbu.getAllFilenames(fullPath=False, level=2)
        self.assertEqual(184, len(files))
        files = self.dbu.getAllFilenames(fullPath=False, level=2, product=190)
        self.assertEqual(15, len(files))
        ans = set([u'rbspb_int_ect-mageis-L2_20130909_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130907_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130908_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130910_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130921_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130922_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130920_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130918_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130916_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130917_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130914_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130915_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130913_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130912_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130911_v3.0.0.cdf'])
        self.assertFalse(ans.difference(set(files)))

    def test_getAllFileIds(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds()
        self.assertEqual(6681, len(files))
        self.assertEqual(range(1, 6682), files)

    def test_getFileFullPath(self):
        """getFileFullPath"""
        self.assertEqual(u'/n/space_data/cda/rbsp/MagEphem/predicted/b/rbspb_pre_MagEphem_OP77Q_20130909_v1.0.0.txt', 
                         self.dbu.getFileFullPath(1))
        self.assertEqual(u'/n/space_data/cda/rbsp/MagEphem/predicted/b/rbspb_pre_MagEphem_OP77Q_20130909_v1.0.0.txt', 
                         self.dbu.getFileFullPath('rbspb_pre_MagEphem_OP77Q_20130909_v1.0.0.txt'))

        self.assertEqual(u'/n/space_data/cda/rbsp/rbspb/mageis_vc/level0/ect_rbspb_0377_364_02.ptp.gz', 
                         self.dbu.getFileFullPath(100))
        self.assertEqual(u'/n/space_data/cda/rbsp/rbspb/mageis_vc/level0/ect_rbspb_0377_364_02.ptp.gz', 
                         self.dbu.getFileFullPath('ect_rbspb_0377_364_02.ptp.gz'))

    def test_getProcessFromInputProduct(self):
        """getProcessFromInputProduct"""
        self.assertEqual([6, 13, 19, 26], self.dbu.getProcessFromInputProduct(1))
        self.assertEqual([3], self.dbu.getProcessFromInputProduct(2))
        self.assertFalse(self.dbu.getProcessFromInputProduct(3))
        self.assertFalse(self.dbu.getProcessFromInputProduct(124324))

    def test_getProcessFromOutputProduct(self):
        """getProcessFromOutputProduct"""
        self.assertFalse(self.dbu.getProcessFromOutputProduct(1))
        self.assertEqual(None, self.dbu.getProcessFromOutputProduct(1))
        self.assertEqual(1, self.dbu.getProcessFromOutputProduct(4))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getProcessFromOutputProduct, 40043)

    def test_getProcessID(self):
        """getProcessID"""
        self.assertEqual(1, self.dbu.getProcessID(1))
        self.assertEqual(61, self.dbu.getProcessID('rbspb_int_ect-mageis-M75_L1toL2'))
        self.assertRaises(NoResultFound, self.dbu.getProcessID, 'badval')
        self.assertRaises(NoResultFound, self.dbu.getProcessID, 10000 )

    def test_getSatelliteID(self):
        """getSatelliteID"""
        self.assertEqual(1, self.dbu.getSatelliteID(1))
        self.assertEqual(1, self.dbu.getSatelliteID('rbspa'))
        self.assertEqual(2, self.dbu.getSatelliteID('rbspb'))
        self.assertRaises(NoResultFound, self.dbu.getSatelliteID, 'badval')
        self.assertRaises(NoResultFound, self.dbu.getSatelliteID, 3)

    def test_getSatelliteMission(self):
        """getSatelliteMission"""
        val = self.dbu.getSatelliteMission(1)
        self.assertEqual(1, val.mission_id)
        self.assertEqual(u'mageis_incoming', val.incoming_dir)
        self.assertEqual(u'/n/space_data/cda/rbsp', val.rootdir)
        self.assertRaises(NoResultFound, self.dbu.getSatelliteMission, 100)
        self.assertRaises(NoResultFound, self.dbu.getSatelliteMission, 'badval')

    def test_getInstrumentID(self):
        """getInstrumentID"""
        self.assertRaises(ValueError, self.dbu.getInstrumentID, 'mageis')
        self.assertEqual(1, self.dbu.getInstrumentID('mageis', 1))
        self.assertEqual(2, self.dbu.getInstrumentID('mageis', 2))
        self.assertEqual(1, self.dbu.getInstrumentID('mageis', 'rbspa'))
        self.assertEqual(2, self.dbu.getInstrumentID('mageis', 'rbspb'))
        self.assertRaises(NoResultFound, self.dbu.getInstrumentID, 'mageis', 'badval')
        self.assertRaises(DBUtils.DBNoData, self.dbu.getInstrumentID, 'badval')

    def test_getMissions(self):
        """getMissions"""
        self.assertEqual([u'rbsp'], self.dbu.getMissions())
        
    def test_getFileID(self):
        """getFileID"""
        self.assertEqual(1, self.dbu.getFileID(1))
        self.assertEqual(2, self.dbu.getFileID(2))
        self.assertEqual(11, self.dbu.getFileID('rbspa_pre_MagEphem_OP77Q_20130907_v1.0.0.txt'))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFileID, 'badval')
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFileID, 343423)

    def test_getCodeID(self):
        """getCodeID"""
        self.assertEqual(1, self.dbu.getCodeID(1))
        self.assertEqual(2, self.dbu.getCodeID(2))
        self.assertEqual([1, 4, 10, 16, 17, 20, 21, 24, 25, 29, 30, 33, 34, 37,
                          43, 49, 50, 53, 54, 57, 58, 62, 63, 66],
                         self.dbu.getCodeID('l05_to_l1.py'))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getCodeID, 'badval')
        self.assertRaises(DBUtils.DBNoData, self.dbu.getCodeID, 343423)

    def test_getFileDates(self):
        """getFileDates"""
        self.assertEqual([datetime.date(2013, 9, 9), datetime.date(2013, 9, 9)], 
                         self.dbu.getFileDates(1))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFileDates, 343423)
        self.assertEqual([datetime.date(2013, 9, 8), datetime.date(2013, 9, 8)], 
                         self.dbu.getFileDates(2))

    def test_getInputProductID(self):
        """getInputProductID"""
        self.assertEqual([(60, False)], self.dbu.getInputProductID(1))
        self.assertEqual([(22, False), (43, False), (84, False), (90, True)], 
                         self.dbu.getInputProductID(2))
        self.assertFalse(self.dbu.getInputProductID(2343))
        self.assertEqual([], self.dbu.getInputProductID(2343))
        
    def test_getFilesByProductDate(self):
        """getFilesByProductDate"""
        self.assertFalse(self.dbu.getFilesByProductDate(1, [datetime.date(2013, 12, 12)]*2))
        val = self.dbu.getFilesByProductDate(187, [datetime.date(2013, 9, 10)]*2)
        self.assertEqual(5, len(val))
        ans = ['ect_rbspb_0377_381_05.ptp.gz',
               'ect_rbspb_0377_381_04.ptp.gz',
               'ect_rbspb_0377_381_03.ptp.gz',
               'ect_rbspb_0377_381_02.ptp.gz',
               'ect_rbspb_0377_381_01.ptp.gz']
        self.assertEqual(ans, [v.filename for v in val] )
        val = self.dbu.getFilesByProductDate(187, [datetime.date(2013, 9, 10)]*2, newest_version=True)
        self.assertEqual(1, len(val))
        self.assertEqual(['ect_rbspb_0377_381_05.ptp.gz'], [v.filename for v in val] )

    def test_getFilesByProduct(self):
        """getFilesByProduct"""
        self.assertFalse(self.dbu.getFilesByProduct(2))
        self.assertEqual([], self.dbu.getFilesByProduct(2))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFilesByProduct, 343423)
        val = self.dbu.getFilesByProduct(1)
        self.assertEqual(30, len(val))
        val = self.dbu.getFilesByProduct(187)
        self.assertEqual(90, len(val))
        val = self.dbu.getFilesByProduct(187, newest_version=True)
        self.assertEqual(24, len(val))
        filenames = [v.filename for v in self.dbu.getFilesByProduct(187, newest_version=True)]
        self.assertTrue('ect_rbspb_0380_381_02.ptp.gz' in filenames)
        
    def test_getFilesByInstrument(self):
        """getFilesByInstrument"""
        files = self.dbu.getFilesByInstrument(1)
        self.assertEqual(3220, len(files))
        filenames = [v.filename for v in files]
        self.assertTrue('rbsp-a_magnetometer_uvw_emfisis-Quick-Look_20130909_v1.3.1.cdf' in
                        filenames)
        files = self.dbu.getFilesByInstrument(1, id_only=True)
        self.assertEqual(3220, len(files))
        self.assertTrue(582 in files)
        files = self.dbu.getFilesByInstrument(1, id_only=True, level=2)
        self.assertEqual(94, len(files))
        self.assertTrue(5880 in files)
        self.assertFalse(self.dbu.getFilesByInstrument(1, id_only=True, level=6))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFilesByInstrument, 'badval')
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFilesByInstrument, 100)
        ids = [int(v) for v in files]

    def test_getActiveInspectors(self):
        """getActiveInspectors"""
        val = self.dbu.getActiveInspectors()
        self.assertEqual(190, len(val))
        v2 = set([v[0] for v in val])
        ans = set([u'/n/space_data/cda/rbsp/codes/inspectors/ect_L05_V1.0.0.py',
                   u'/n/space_data/cda/rbsp/codes/inspectors/ect_L0_V1.0.0.py',
                   u'/n/space_data/cda/rbsp/codes/inspectors/ect_L1_V1.0.0.py',
                   u'/n/space_data/cda/rbsp/codes/inspectors/ect_L2_V1.0.0.py',
                   u'/n/space_data/cda/rbsp/codes/inspectors/emfisis_V1.0.0.py',
                   u'/n/space_data/cda/rbsp/codes/inspectors/rbsp_pre_MagEphem_insp.py'])
        self.assertEqual(ans, v2)
        v3 = set([v[-1] for v in val])
        self.assertEqual(set(range(1, 191)), v3)

    def test_getChildrenProducts(self):
        """getChildrenProducts"""
        self.assertEqual([35, 38, 39, 46, 47, 51, 52, 59, 61], 
                         self.dbu.getChildrenProducts(1))
        self.assertEqual([44], self.dbu.getChildrenProducts(123))        
        self.assertFalse(self.dbu.getChildrenProducts(5998))
        self.assertEqual([], self.dbu.getChildrenProducts(5998))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getChildrenProducts, 59498)

    def test_getProductID(self):
        """getProductID"""
        self.assertEqual(1, self.dbu.getProductID(1))
        self.assertEqual(2, self.dbu.getProductID(2))
        self.assertEqual(163, self.dbu.getProductID('rbspb_mageis-M75-sp-hg-L0'))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getProductID, 'badval')
        self.assertRaises(DBUtils.DBNoData, self.dbu.getProductID, 343423)
        
    def test_getSatelliteID(self):
        """getSatelliteID"""
        self.assertEqual(1, self.dbu.getSatelliteID(1))
        self.assertEqual(2, self.dbu.getSatelliteID(2))
        self.assertEqual(1, self.dbu.getSatelliteID('rbspa'))
        self.assertEqual(2, self.dbu.getSatelliteID('rbspb'))
        self.assertRaises(NoResultFound, self.dbu.getSatelliteID, 'badval')
        self.assertRaises(NoResultFound, self.dbu.getSatelliteID, 343423)
        
    def test_getCodePath(self):
        """getCodePath"""
        self.assertEqual('/n/space_data/cda/rbsp/codes/l05_to_l1.py',
                         self.dbu.getCodePath(1))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getCodePath, 'badval')

    def test_getCodeVersion(self):
        """getCodeVersion"""
        self.assertEqual(Version.Version(3,0,0), self.dbu.getCodeVersion(1))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getCodeVersion, 'badval')
        
    def test_getCodeFromProcess(self):
        """getCodeFromProcess"""
        self.assertEqual(1, self.dbu.getCodeFromProcess(1, datetime.date(2013, 9, 10)))
        self.assertFalse(self.dbu.getCodeFromProcess(1, datetime.date(1900, 9, 11)))
        self.assertTrue(self.dbu.getCodeFromProcess(1, datetime.date(1900, 9, 11)) is None)

    def test_getMissionDirectory(self):
        """getMissionDirectory"""
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu.getMissionDirectory(1))
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu.getMissionDirectory())
        self.assertRaises(DBUtils.DBNoData, self.dbu.getMissionDirectory, 3)

    def test_getIncomingPath(self):
        """getIncomingPath"""
        self.assertEqual('/n/space_data/cda/rbsp/mageis_incoming', self.dbu.getIncomingPath(1))
        self.assertEqual('/n/space_data/cda/rbsp/mageis_incoming', self.dbu.getIncomingPath())
        self.assertRaises(DBUtils.DBNoData, self.dbu.getIncomingPath, 3)
                        
    def test_getErrorPath(self):
        """getErrorPath"""
        self.assertEqual('/n/space_data/cda/rbsp/errors/', self.dbu.getErrorPath())
        self.assertRaises(TypeError, self.dbu.getErrorPath, 3)

    def test_getFilecodelink_byfile(self):
        """getFilecodelink_byfile"""
        self.assertEqual(26, self.dbu.getFilecodelink_byfile(5974))
        self.assertFalse(self.dbu.getFilecodelink_byfile(1))
        self.assertTrue(self.dbu.getFilecodelink_byfile(1) is None)

    def test_getMissionID(self):
        """getMissionID"""
        self.assertEqual(1, self.dbu.getMissionID(1))
        self.assertEqual(1, self.dbu.getMissionID('rbsp'))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getMissionID, 'badval')
        self.assertRaises(DBUtils.DBNoData, self.dbu.getMissionID, 343423)
                     


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

    def test_pq_getall(self):
        """test self.Processqueue.getAll"""
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.assertEqual([17,18,19,20,21], self.dbu.Processqueue.getAll())
        self.assertEqual(zip([17,18,19,20,21], [None]*5), self.dbu.Processqueue.getAll(version_bump=True))
    
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

    def test_pq_pop_reverse(self):
        """test self.Processqueue.pop with negative indices"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.dbu.Processqueue.pop(-1)
        self.assertEqual(4, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [17,18,19,20]:
            self.assertTrue(v in pq)
        self.dbu.Processqueue.pop(-2)
        self.assertEqual(3, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [17,18,20]:
            self.assertTrue(v in pq)

    def test_pq_get(self):
        """test self.Processqueue.get"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.assertEqual((17, None), self.dbu.Processqueue.get(0))
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.assertEqual((19, None), self.dbu.Processqueue.get(2))
        self.assertEqual(5, self.dbu.Processqueue.len())

    def test_pq_get_reverse(self):
        """test self.Processqueue.get with negative indices"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.assertEqual((21, None), self.dbu.Processqueue.get(-1))
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.assertEqual((20, None), self.dbu.Processqueue.get(-2))
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
