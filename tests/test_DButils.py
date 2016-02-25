#!/usr/bin/env python2.6
from __future__ import print_function

import datetime
import os
import os.path
import shutil
import stat
import tempfile
import unittest

try:  # new version changed this annoyingly
    from sqlalchemy.exceptions import IntegrityError
    from sqlalchemy.exceptions import ArgumentError
    from sqlalchemy.orm.exceptions import NoResultFound
except ImportError:
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.exc import ArgumentError
    from sqlalchemy.orm.exc import NoResultFound

from dbprocessing import DButils
from dbprocessing import Version


def make_tmpfile():
    tf = tempfile.NamedTemporaryFile(delete=False)
    tf.close()
    return tf.name


def remove_tmpfile(fname):
    os.remove(fname)


class TestSetup(unittest.TestCase):
    """
    master class for the setup and teardown
    """

    def setUp(self):
        super(TestSetup, self).setUp()
        sqpath = os.path.join(os.path.dirname(__file__), 'RBSP_MAGEIS.sqlite')
        self.sqlworking = sqpath.replace('RBSP_MAGEIS.sqlite', 'working.sqlite')
        shutil.copy(sqpath, self.sqlworking)
        os.chmod(self.sqlworking, stat.S_IRUSR | stat.S_IWUSR)
        self.dbu = DButils.DButils(self.sqlworking)

    def tearDown(self):
        super(TestSetup, self).tearDown()
        self.dbu.closeDB()
        del self.dbu
        os.remove(self.sqlworking)


class DBUtilsStaticTests(unittest.TestCase):
    def test_test_SQLAlchemy_version(self):
        self.assertRaises(DButils.DBError, DButils.DButils._test_SQLAlchemy_version, '1.0.11')
        self.assertTrue(DButils.DButils._test_SQLAlchemy_version('0.7'))


class DBUtilsOtherTests(TestSetup):
    """Tests that are not processqueue or get or add"""

    def test_checkFiles(self):
        """checkFiles"""
        ans = set([(u'ect_rbspa_0220_344_01.ptp.gz', '(200) file not found'),
                   (u'ect_rbspa_0220_344_02.ptp.gz', '(200) file not found'),
                   (u'ect_rbspa_0220_345_01.ptp.gz', '(200) file not found')])
        self.assertFalse(set(self.dbu.checkFiles(limit=3)).difference(ans))

    def test_checkIncoming(self):
        """checkIncoming"""
        """checkIncoming"""
        self.assertFalse(self.dbu.checkIncoming())
        e = self.dbu.getEntry('Mission', 1)
        e.incoming_dir = os.path.abspath(os.path.dirname(__file__))
        self.dbu.session.add(e)
        self.dbu.commitDB()
        inc_files = self.dbu.checkIncoming()
        self.assertTrue(inc_files)
        self.assertTrue(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test_DButils.py') in inc_files)

    def test_currentlyProcessing(self):
        """currentlyProcessing"""
        self.assertFalse(self.dbu.currentlyProcessing())
        log = self.dbu.Logging()
        log.currently_processing = True
        log.pid = 123
        log.processing_start_time = datetime.datetime.now()
        log.mission_id = self.dbu.getMissionID('rbsp')
        log.user = 'user'
        log.hostname = 'hostname'
        self.dbu.session.add(log)
        self.dbu.commitDB()
        self.assertEqual(123, self.dbu.currentlyProcessing())
        log = self.dbu.Logging()
        log.currently_processing = True
        log.processing_start_time = datetime.datetime.now()
        log.mission_id = self.dbu.getMissionID('rbsp')
        log.user = 'user'
        log.hostname = 'hostname'
        log.pid = 1234
        self.dbu.session.add(log)
        self.dbu.commitDB()
        self.assertRaises(DButils.DBError, self.dbu.currentlyProcessing)

    def test_startLogging(self):
        """startLogging"""
        self.assertFalse(self.dbu.currentlyProcessing())
        self.dbu.startLogging()
        self.assertTrue(self.dbu.currentlyProcessing())
        self.assertRaises(DButils.DBError, self.dbu.startLogging)
        self.assertTrue(self.dbu.currentlyProcessing())

    def test_stopLogging(self):
        """stopLogging"""
        self.assertFalse(self.dbu.currentlyProcessing())
        self.assertRaises(DButils.DBProcessingError, self.dbu.stopLogging, 'comment')
        self.dbu.startLogging()
        self.assertTrue(self.dbu.currentlyProcessing())
        self.dbu.stopLogging('comment')
        self.assertFalse(self.dbu.currentlyProcessing())

    def test_resetProcessingFlag(self):
        """resetProcessingFlag"""
        self.dbu.startLogging()
        self.assertTrue(self.dbu.currentlyProcessing())
        self.assertRaises(ValueError, self.dbu.resetProcessingFlag)  # no comment
        self.dbu.resetProcessingFlag('testing comment')
        self.assertFalse(self.dbu.currentlyProcessing())

    def test_repr(self):
        """repr"""
        self.assertTrue(self.dbu.__repr__().startswith('DBProcessing class instance for mission /Users/blarsen/git/dbprocessing/tests/working.sqlite, version:'))

    def test_purgeFileFromDB(self):
        """purgeFileFromDB"""
        self.assertEqual(self.dbu.session.query(self.dbu.File).count(), 6681)
        file_id = self.dbu.getFileID(123)
        self.dbu.purgeFileFromDB(file_id)
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, file_id)
        self.assertEqual(self.dbu.session.query(self.dbu.File).count(), 6680)

    def test_purgeFileFromDB(self):
        """purgeFileFromDB"""
        # file_id = self.dbu.getFileID(123356)
        file_id = 123356
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, file_id)
        self.dbu.purgeFileFromDB(file_id)

    def test_nameSubProduct(self):
        """_nameSubProduct"""
        self.assertTrue(self.dbu._nameSubProduct(None, 1) is None)
        self.assertEqual('Nothing to do', self.dbu._nameSubProduct('Nothing to do', 1))
        # repl = ['{INSTRUMENT}', '{SPACECRAFT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}'] 
        self.assertEqual('rbsp-a_magnetometer_uvw_emfisis-Quick-Look', self.dbu._nameSubProduct('{PRODUCT}', 1))
        self.assertEqual('mageis', self.dbu._nameSubProduct('{INSTRUMENT}', 10))
        self.assertEqual('rbspa', self.dbu._nameSubProduct('{SATELLITE}', 1))
        self.assertEqual('rbspa', self.dbu._nameSubProduct('{SPACECRAFT}', 1))
        self.assertEqual('rbsp', self.dbu._nameSubProduct('{MISSION}', 1))
        self.assertEqual('0.0', self.dbu._nameSubProduct('{LEVEL}', 1))
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu._nameSubProduct('{ROOTDIR}', 1))

    def test_nameSubInspector(self):
        """_nameSubInspector"""
        self.assertTrue(self.dbu._nameSubInspector(None, 1) is None)
        self.assertEqual('Nothing to do', self.dbu._nameSubInspector('Nothing to do', 1))
        # repl = ['{INSTRUMENT}', '{SPACECRAFT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}'] 
        self.assertEqual('rbsp-a_magnetometer_uvw_emfisis-Quick-Look', self.dbu._nameSubInspector('{PRODUCT}', 1))
        self.assertEqual('mageis', self.dbu._nameSubInspector('{INSTRUMENT}', 10))
        self.assertEqual('rbspa', self.dbu._nameSubInspector('{SATELLITE}', 1))
        self.assertEqual('rbspa', self.dbu._nameSubInspector('{SPACECRAFT}', 1))
        self.assertEqual('rbsp', self.dbu._nameSubInspector('{MISSION}', 1))
        self.assertEqual('0.0', self.dbu._nameSubInspector('{LEVEL}', 1))
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu._nameSubInspector('{ROOTDIR}', 1))

    def test_nameSubProcess(self):
        """_nameSubProcess"""
        self.assertTrue(self.dbu._nameSubProcess(None, 1) is None)
        self.assertEqual('Nothing to do', self.dbu._nameSubProcess('Nothing to do', 1))
        # repl = ['{INSTRUMENT}', '{SPACECRAFT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}'] 
        self.assertEqual('rbspa_int_ect-mageisM35-hr-L05', self.dbu._nameSubProcess('{PRODUCT}', 1))
        self.assertEqual('mageis', self.dbu._nameSubProcess('{INSTRUMENT}', 10))
        self.assertEqual('rbspa', self.dbu._nameSubProcess('{SATELLITE}', 1))
        self.assertEqual('rbsp', self.dbu._nameSubProcess('{MISSION}', 1))
        self.assertEqual('0.5', self.dbu._nameSubProcess('{LEVEL}', 1))
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu._nameSubProcess('{ROOTDIR}', 1))

    def test_nameSubFile(self):
        """_nameSubFile"""
        self.assertTrue(self.dbu._nameSubFile(None, 1) is None)
        self.assertEqual('Nothing to do', self.dbu._nameSubFile('Nothing to do', 1))
        # repl = ['{INSTRUMENT}', '{SPACECRAFT}', '{SATELLITE}', '{MISSION}', '{PRODUCT}', '{LEVEL}', '{ROOTDIR}'] 
        self.assertEqual('rbspb_pre_MagEphem_OP77Q', self.dbu._nameSubFile('{PRODUCT}', 1))
        self.assertEqual('mageis', self.dbu._nameSubFile('{INSTRUMENT}', 10))
        self.assertEqual('rbspb', self.dbu._nameSubFile('{SATELLITE}', 1))
        self.assertEqual('rbsp', self.dbu._nameSubFile('{MISSION}', 1))
        self.assertEqual('0.0', self.dbu._nameSubFile('{LEVEL}', 1))
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu._nameSubFile('{ROOTDIR}', 1))

    def test_codeIsActive(self):
        """codeIsActive"""
        self.assertTrue(self.dbu.codeIsActive(1, datetime.date(2013, 1, 1)))
        self.assertFalse(self.dbu.codeIsActive(1, datetime.date(1900, 1, 1)))
        self.assertFalse(self.dbu.codeIsActive(1, datetime.date(2100, 1, 1)))
        self.assertTrue(self.dbu.codeIsActive(1, datetime.datetime(2013, 1, 1)))
        self.assertFalse(self.dbu.codeIsActive(1, datetime.datetime(1900, 1, 1)))
        self.assertFalse(self.dbu.codeIsActive(1, datetime.datetime(2100, 1, 1)))

    def test_codeIsActive2(self):
        """codeIsActive"""
        c = self.dbu.getEntry('Code', 1)
        self.assertTrue(self.dbu.codeIsActive(1, datetime.datetime(2013, 1, 1)))
        c.active_code = False
        self.dbu.session.add(c)
        self.dbu.commitDB()
        self.assertFalse(self.dbu.codeIsActive(1, datetime.datetime(2013, 1, 1)))

    def test_codeIsActive3(self):
        """codeIsActive"""
        c = self.dbu.getEntry('Code', 1)
        self.assertTrue(self.dbu.codeIsActive(1, datetime.datetime(2013, 1, 1)))
        c.newest_version = False
        self.dbu.session.add(c)
        self.dbu.commitDB()
        self.assertFalse(self.dbu.codeIsActive(1, datetime.datetime(2013, 1, 1)))

    def test_renameFile(self):
        """renameFile"""
        self.dbu.renameFile('ect_rbspb_0388_34c_01.ptp.gz', 'ect_rbspb_0388_34c_01.ptp.gz_newname')
        self.assertEqual(2051, self.dbu.getFileID('ect_rbspb_0388_34c_01.ptp.gz_newname'))

    def test_checkDiskForFile(self):
        """checkDiskForFile"""
        self.assertFalse(self.dbu.checkDiskForFile(123))


class DBUtilsAddTests(TestSetup):
    """Tests for database adds through DButils"""

    def test_addMission(self):
        """addMission"""
        self.assertEqual(self.dbu.addMission('name', '/rootdir/', '/root/incoming'), 2)
        self.assertEqual(self.dbu.getEntry('Mission', 2).mission_id, 2)

    def test_addSatellite(self):
        """addMission"""
        self.assertEqual(self.dbu.addSatellite('name', 1), 3)
        self.assertEqual(self.dbu.getEntry('Satellite', 3).satellite_id, 3)

    def test_addProcess1(self):
        """addProcess"""
        self.assertRaises(ValueError, self.dbu.addProcess, 'proc_name', 1, 'bad_base')

    def test_addProcess2(self):
        """addProcess"""
        self.assertEqual(67, self.dbu.addProcess('proc_name', 1, 'DAILY'))


class DBUtilsGetTests(TestSetup):
    """Tests for database gets through DButils"""

    def test_init(self):
        """__init__ has an exception to test"""
        self.assertRaises(DButils.DBError, DButils.DButils, None)

    def test_openDB1(self):
        """__init__ has an exception to test"""
        self.assertRaises(ValueError, DButils.DButils, 'i do not exist')

    def test_openDB2(self):
        """__init__ bad engine"""
        self.assertRaises(DButils.DBError, DButils.DButils, self.sqlworking, engine='i am bogus')

    def test_openDB3(self):
        """__init__ bad engine"""
        self.assertRaises(DButils.DBError, DButils.DButils, self.sqlworking, engine='i am bogus')

    def test_openDB4(self):
        """__init__ bad engine"""
        tfile = make_tmpfile()
        try:
            self.assertRaises(AttributeError, DButils.DButils, tfile),
        finally:
            remove_tmpfile(tfile)

    def test_file_id_Clean(self):
        """file_id_Clean"""
        files = [self.dbu.getEntry('File', v) for v in [1, 2, 3]]
        tmp = self.dbu.file_id_Clean(files)
        self.assertEqual(len(tmp), 3)

    def test_file_id_Clean2(self):
        """file_id_Clean"""
        tmp = self.dbu.file_id_Clean(['1', '2', '3'])
        self.assertEqual(len(tmp), 3)

    def test_openDB5(self):
        """__init__ already open"""
        self.assertTrue(self.dbu.openDB('sqlite') is None)

    def test_getRunProcess(self):
        """getRunProcess"""
        self.assertEqual([], self.dbu.getRunProcess())

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

    def test_getAllFilenames_limit(self):
        """getAllFilenames"""
        files = self.dbu.getAllFilenames(fullPath=False, limit=10)
        self.assertEqual(10, len(files))
        files = self.dbu.getAllFilenames(fullPath=False, level=2, limit=10)
        self.assertEqual(10, len(files))
        files = self.dbu.getAllFilenames(fullPath=False, level=2, product=190, limit=4)
        self.assertEqual(4, len(files))
        ans = set([u'rbspb_int_ect-mageis-L2_20130907_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130909_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130908_v3.0.0.cdf',
                   u'rbspb_int_ect-mageis-L2_20130910_v3.0.0.cdf'])
        self.assertFalse(ans.difference(set(files)))

    def test_getAllFilenames_level(self):
        """getAllFilenames"""
        files = self.dbu.getAllFilenames(fullPath=False, product=1)
        self.assertEqual(len(files), 30)

    def test_getAllFilenames_level_limit(self):
        """getAllFilenames"""
        files = self.dbu.getAllFilenames(fullPath=False, product=1, limit=10)
        self.assertEqual(len(files), 10)

    def test_getAllFilenames_fullpath(self):
        """getAllFilenames"""
        files = self.dbu.getAllFilenames(fullPath=True, product=1)
        self.assertEqual(len(files), 30)
        self.assertTrue(all(['/n/space_data' in v for v in files]))

    def test_getAllFilenames_fullpath_limit(self):
        """getAllFilenames"""
        files = self.dbu.getAllFilenames(fullPath=True, product=1, limit=10)
        self.assertEqual(len(files), 10)
        self.assertTrue(all(['/n/space_data' in v for v in files]))

    def test_getAllFileIds(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds()
        self.assertEqual(6681, len(files))
        self.assertEqual(range(1, 6682), sorted(files))

    def test_getAllFileIds2(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds(newest_version=True)
        self.assertEqual(2752, len(files))
        self.assertEqual(len(files), len(set(files)))

    def test_getAllFileIds_limit(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds(limit=10)
        self.assertEqual(10, len(files))
        self.assertEqual(range(1, 11), sorted(files))

    def test_getAllFileIds2_limit(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds(newest_version=True, limit=10)
        self.assertEqual(10, len(files))
        self.assertEqual(len(files), len(set(files)))

    def test_getAllCodes(self):
        """getAllCodes"""
        codes = self.dbu.getAllCodes()
        self.assertEqual(len(codes), 66)
        self.assertTrue(isinstance(codes[0], dict))
        self.assertEqual(set(codes[0].keys()), set(
            ['product', 'code', 'process', 'satellite', 'mission', 'instrument', 'instrumentproductlink']))

    def test_getAllCodes_active(self):
        """getAllCodes"""
        codes = self.dbu.getAllCodes(active=False)
        codes[0]['code'].newest_version = False
        self.dbu.session.add(codes[0]['code'])
        self.dbu.commitDB()
        codes = self.dbu.getAllCodes()
        self.assertEqual(len(codes), 65)
        codes = self.dbu.getAllCodes(active=False)
        self.assertEqual(len(codes), 66)

    def test_commitDB1(self):
        """commitDB"""
        f = self.dbu.session.query(self.dbu.File).first()
        f.filename += '_test'
        tmp = f.filename
        id = f.file_id
        self.dbu.session.add(f)
        self.dbu.commitDB()
        self.assertEqual(id, self.dbu.getFileID(tmp))

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
        self.assertRaises(DButils.DBNoData, self.dbu.getProcessFromOutputProduct, 40043)

    def test_getProcessID(self):
        """getProcessID"""
        self.assertEqual(1, self.dbu.getProcessID(1))
        self.assertEqual(61, self.dbu.getProcessID('rbspb_int_ect-mageis-M75_L1toL2'))
        self.assertRaises(NoResultFound, self.dbu.getProcessID, 'badval')
        self.assertRaises(NoResultFound, self.dbu.getProcessID, 10000)

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
        self.assertRaises(NoResultFound, self.dbu.getInstrumentID, 'mageis', satellite_id='badval')
        self.assertRaises(DButils.DBNoData, self.dbu.getInstrumentID, 'badval')

    def test_getMissions(self):
        """getMissions"""
        self.assertEqual([u'rbsp'], self.dbu.getMissions())

    def test_getFileID(self):
        """getFileID"""
        self.assertEqual(1, self.dbu.getFileID(1))
        self.assertEqual(2, self.dbu.getFileID(2))
        self.assertEqual(11, self.dbu.getFileID('rbspa_pre_MagEphem_OP77Q_20130907_v1.0.0.txt'))
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, 'badval')
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, 343423)
        f = self.dbu.getFileID(2)
        self.assertEqual(2, self.dbu.getFileID(f))

    def test_getCodeID(self):
        """getCodeID"""
        self.assertEqual(1, self.dbu.getCodeID(1))
        self.assertEqual([1], self.dbu.getCodeID([1]))
        self.assertEqual(2, self.dbu.getCodeID(2))
        self.assertEqual([1, 4, 10, 16, 17, 20, 21, 24, 25, 29, 30, 33, 34, 37,
                          43, 49, 50, 53, 54, 57, 58, 62, 63, 66],
                         self.dbu.getCodeID('l05_to_l1.py'))
        self.assertRaises(DButils.DBNoData, self.dbu.getCodeID, 'badval')
        self.assertRaises(DButils.DBNoData, self.dbu.getCodeID, 343423)

    def test_getFileDates(self):
        """getFileDates"""
        self.assertEqual([datetime.date(2013, 9, 9), datetime.date(2013, 9, 9)],
                         self.dbu.getFileDates(1))
        self.assertRaises(DButils.DBNoData, self.dbu.getFileDates, 343423)
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
        self.assertFalse(self.dbu.getFilesByProductDate(1, [datetime.date(2013, 12, 12)] * 2))
        val = self.dbu.getFilesByProductDate(187, [datetime.date(2013, 9, 10)] * 2)
        self.assertEqual(5, len(val))
        ans = ['ect_rbspb_0377_381_05.ptp.gz',
               'ect_rbspb_0377_381_04.ptp.gz',
               'ect_rbspb_0377_381_03.ptp.gz',
               'ect_rbspb_0377_381_02.ptp.gz',
               'ect_rbspb_0377_381_01.ptp.gz']
        self.assertEqual(ans, [v.filename for v in val])
        # WARNING: calling newest_version=True returns FILENAME, not DButils.File
        val = self.dbu.getFilesByProductDate(187, [datetime.date(2013, 9, 10)] * 2, newest_version=True)
        self.assertEqual(1, len(val))
        self.assertEqual(['ect_rbspb_0377_381_05.ptp.gz'], val)

    def test_getFilesByDate(self):
        """getFilesByDate"""
        self.assertFalse(self.dbu.getFilesByDate([datetime.date(2013, 12, 12)] * 2))
        val = self.dbu.getFilesByDate([datetime.date(2013, 9, 10)] * 2)
        self.assertEqual(256, len(val))
        ans = ['ect_rbspa_0377_344_01.ptp.gz',
               'ect_rbspa_0377_344_02.ptp.gz',
               'ect_rbspa_0377_345_01.ptp.gz',
               'ect_rbspa_0377_346_01.ptp.gz',
               'ect_rbspa_0377_349_01.ptp.gz']
        filenames = sorted([v.filename for v in val])
        self.assertEqual(ans, filenames[:len(ans)])
        val = self.dbu.getFilesByDate([datetime.date(2013, 9, 10)] * 2, newest_version=True)
        self.assertEqual(2, len(val))
        filenames = sorted([v.filename for v in val])
        ans = [u'rbsp-a_magnetometer_uvw_emfisis-Quick-Look_20130910_v1.3.1.cdf',
               u'rbsp-b_magnetometer_uvw_emfisis-Quick-Look_20130910_v1.3.1.cdf']
        self.assertEqual(ans, filenames[:len(ans)])

    def test_getFilesByProduct(self):
        """getFilesByProduct"""
        self.assertFalse(self.dbu.getFilesByProduct(2))
        self.assertEqual([], self.dbu.getFilesByProduct(2))
        self.assertRaises(DButils.DBNoData, self.dbu.getFilesByProduct, 343423)
        val = self.dbu.getFilesByProduct(1)
        self.assertEqual(30, len(val))
        val = self.dbu.getFilesByProduct(187)
        self.assertEqual(90, len(val))
        val = self.dbu.getFilesByProduct(187, newest_version=True)
        self.assertEqual(23, len(val))
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
        files = self.dbu.getFilesByInstrument(2, id_only=True)
        self.assertEqual(3461, len(files))
        files = self.dbu.getFilesByInstrument(1, id_only=True, level=2)
        self.assertEqual(94, len(files))
        self.assertTrue(5880 in files)
        self.assertFalse(self.dbu.getFilesByInstrument(1, id_only=True, level=6))
        self.assertRaises(DButils.DBNoData, self.dbu.getFilesByInstrument, 'badval')
        self.assertRaises(DButils.DBNoData, self.dbu.getFilesByInstrument, 100)
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

    def test_getChildrenProcesses(self):
        """getChildrenProcesses"""
        self.assertEqual([35, 38, 39, 46, 47, 51, 52, 59, 61],
                         self.dbu.getChildrenProcesses(1))
        self.assertEqual([44], self.dbu.getChildrenProcesses(123))
        self.assertFalse(self.dbu.getChildrenProcesses(5998))
        self.assertEqual([], self.dbu.getChildrenProcesses(5998))
        self.assertRaises(DButils.DBNoData, self.dbu.getChildrenProcesses, 59498)

    def test_getProductID(self):
        """getProductID"""
        self.assertEqual(1, self.dbu.getProductID(1))
        self.assertEqual(2, self.dbu.getProductID(2))
        self.assertEqual(163, self.dbu.getProductID('rbspb_mageis-M75-sp-hg-L0'))
        self.assertRaises(DButils.DBNoData, self.dbu.getProductID, 'badval')
        self.assertRaises(DButils.DBNoData, self.dbu.getProductID, 343423)

    def test_getProductID2(self):
        """getProductID"""
        newid = self.dbu.addProduct('rbspb_mageis-M75-sp-hg-L0', 2, 'relpath', 'format', 3, 'desc')
        self.assertEqual(newid, 191)
        self.assertEqual(self.dbu.getProductID('rbspb_mageis-M75-sp-hg-L0'), 163)

    def test_getSatelliteID(self):
        """getSatelliteID"""
        self.assertEqual(1, self.dbu.getSatelliteID(1))
        self.assertEqual(2, self.dbu.getSatelliteID(2))
        self.assertEqual(1, self.dbu.getSatelliteID('rbspa'))
        self.assertEqual(2, self.dbu.getSatelliteID('rbspb'))
        self.assertRaises(NoResultFound, self.dbu.getSatelliteID, 'badval')
        self.assertRaises(NoResultFound, self.dbu.getSatelliteID, 343423)
        self.assertEqual([1, 2], self.dbu.getSatelliteID([1, 2]))

    def test_getCodePath(self):
        """getCodePath"""
        self.assertEqual('/n/space_data/cda/rbsp/codes/l05_to_l1.py',
                         self.dbu.getCodePath(1))
        self.assertRaises(DButils.DBNoData, self.dbu.getCodePath, 'badval')

    def test_getCodePath2(self):
        """getCodePath"""
        cd = self.dbu.getEntry('Code', 1)
        cd.active_code = False
        self.dbu.session.add(cd)
        self.dbu.commitDB()
        self.assertTrue(self.dbu.getCodePath(1) is None)

    def test_getAllCodesFromProcess(self):
        """getAllCodesFromProcess"""
        self.assertEqual(self.dbu.getAllCodesFromProcess(1),
                         [(1, datetime.date(2000, 1, 1), datetime.date(2050, 12, 31))])
        self.assertEqual(self.dbu.getAllCodesFromProcess(6),
                         [(6, datetime.date(2000, 1, 1), datetime.date(2050, 12, 31))])

    def test_getCodeVersion(self):
        """getCodeVersion"""
        self.assertEqual(Version.Version(3, 0, 0), self.dbu.getCodeVersion(1))
        self.assertRaises(DButils.DBNoData, self.dbu.getCodeVersion, 'badval')

    def test_getCodeFromProcess(self):
        """getCodeFromProcess"""
        self.assertEqual(1, self.dbu.getCodeFromProcess(1, datetime.date(2013, 9, 10)))
        self.assertFalse(self.dbu.getCodeFromProcess(1, datetime.date(1900, 9, 11)))
        self.assertTrue(self.dbu.getCodeFromProcess(1, datetime.date(1900, 9, 11)) is None)

    def test_getMissionDirectory(self):
        """getMissionDirectory"""
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu.getMissionDirectory(1))
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu.getMissionDirectory())
        self.assertRaises(DButils.DBNoData, self.dbu.getMissionDirectory, 3)

    def test_getIncomingPath(self):
        """getIncomingPath"""
        self.assertEqual('/n/space_data/cda/rbsp/mageis_incoming', self.dbu.getIncomingPath(1))
        self.assertEqual('/n/space_data/cda/rbsp/mageis_incoming', self.dbu.getIncomingPath())
        self.assertRaises(DButils.DBNoData, self.dbu.getIncomingPath, 3)

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
        self.assertRaises(DButils.DBNoData, self.dbu.getMissionID, 'badval')
        self.assertRaises(DButils.DBNoData, self.dbu.getMissionID, 343423)

    def test_getProductsByInstrument(self):
        """getProductsByInstrument"""
        p1 = self.dbu.getProductsByInstrument(1)
        p2 = self.dbu.getProductsByInstrument(2)
        self.assertEqual(95, len(p1))
        self.assertEqual(95, len(p2))
        self.assertFalse(set(p1).intersection(p2))

    def test_getProductsByInstrument2(self):
        """getProductsByInstrument"""
        id = self.dbu.addInstrument('Inst_name', 1)
        self.assertTrue(self.dbu.getProductsByInstrument(id) is None)

    def test_getAllProcesses(self):
        """getAllProcesses"""
        self.assertEqual(66, len(self.dbu.getAllProcesses()))
        self.assertEqual(42, len(self.dbu.getAllProcesses('DAILY')))
        self.assertEqual(24, len(self.dbu.getAllProcesses('FILE')))

    def test_getAllProducts(self):
        """getAllProducts"""
        self.assertEqual(95 + 95, len(self.dbu.getAllProducts()))

    def test_getProductsByLevel(self):
        """getProductsByLevel"""
        pr = self.dbu.getProductsByLevel(0)
        self.assertEqual(118, len(pr))
        self.assertTrue(self.dbu.getProductsByLevel(10) is None)

    def test_getProcessTimebase(self):
        """getProcessTimebase"""
        self.assertEqual("DAILY", self.dbu.getProcessTimebase(1))

    def test_getFilesByCode(self):
        """getFilesByCode"""
        f = self.dbu.getFilesByCode(2)
        self.assertEqual(20, len(f))
        ids = self.dbu.getFilesByCode(2, id_only=True)
        self.assertEqual(set([576, 1733, 1735, 1741, 1745, 1872, 5814, 5817, 5821,
                              5824, 5831, 5834, 5838, 5842, 5845, 5849, 5855, 5858,
                              5861, 5865]), set(ids))

    def test_getVersion(self):
        """getVersion"""
        self.assertEqual(Version.Version(1, 0, 0), self.dbu.getVersion(1))
        self.assertEqual(Version.Version(1, 1, 0), self.dbu.getVersion(123))

    def test_getChildTree(self):
        """getChildTree"""
        tmp = self.dbu.getChildTree(1)
        ans = set([10, 8, 39, 76])
        self.assertFalse(set(tmp).difference(ans))

    def test_getFileParents(self):
        """getFileParents"""
        ids = self.dbu.getFileParents(1879, id_only=True)
        files = self.dbu.getFileParents(1879, id_only=False)
        self.assertEqual(3, len(ids))
        self.assertEqual(3, len(files))
        for vv in files:
            self.assertTrue(self.dbu.getFileID(vv) in ids)
        self.assertEqual([1846, 1802, 1873], ids)

    def test_getProductParentTree(self):
        """getProductParentTree"""
        tmp = self.dbu.getProductParentTree()
        self.assertEqual(190, len(tmp))
        self.assertTrue([1, [10, 8, 39, 76]] in tmp)


class ProcessqueueTests(TestSetup):
    """Test all the processqueue functionality"""

    def add_files(self):
        self.dbu.Processqueue.push([17, 18, 19, 20, 21])

    def test_pq_getall(self):
        """test self.Processqueue.getAll"""
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.assertEqual([17, 18, 19, 20, 21], self.dbu.Processqueue.getAll())
        self.assertEqual(zip([17, 18, 19, 20, 21], [None] * 5), self.dbu.Processqueue.getAll(version_bump=True))

    def test_pq_getall2(self):
        """test self.Processqueue.getAll"""
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.assertFalse(self.dbu.Processqueue.getAll())
        self.assertFalse(self.dbu.Processqueue.getAll(version_bump=True))

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
        for v in [17, 18, 19, 21]:
            self.assertTrue(v in pq)
        self.dbu.Processqueue.remove([17, 18])
        self.assertEqual(2, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [19, 21]:
            self.assertTrue(v in pq)
        self.dbu.Processqueue.remove('ect_rbspb_0377_381_03.ptp.gz')
        self.assertEqual(1, self.dbu.Processqueue.len())
        self.assertEqual([21], self.dbu.Processqueue.getAll())

    def test_pq_push(self):
        """test self.Processqueue.push"""
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.dbu.Processqueue.push(20)
        self.assertEqual(1, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        self.assertTrue(20 in pq)
        # push a value that is not there
        self.assertFalse(self.dbu.Processqueue.push(214442))
        self.assertFalse(self.dbu.Processqueue.push(20))
        self.assertEqual([17, 18, 19, 21], self.dbu.Processqueue.push([17, 18, 19, 20, 21]))

    def test_pq_push_MAX_ADD(self):
        """test self.Processqueue.push"""
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.dbu._processqueuePush([17, 18, 19, 20, 21], MAX_ADD=2)
        self.assertEqual(5, self.dbu.Processqueue.len())

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
        for v in [18, 19, 20, 21]:
            self.assertTrue(v in pq)
        self.dbu.Processqueue.pop(2)
        self.assertEqual(3, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [18, 19, 21]:
            self.assertTrue(v in pq)

    def test_pq_pop_reverse(self):
        """test self.Processqueue.pop with negative indices"""
        self.add_files()
        self.assertEqual(5, self.dbu.Processqueue.len())
        self.dbu.Processqueue.pop(-1)
        self.assertEqual(4, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [17, 18, 19, 20]:
            self.assertTrue(v in pq)
        self.dbu.Processqueue.pop(-2)
        self.assertEqual(3, self.dbu.Processqueue.len())
        pq = self.dbu.Processqueue.getAll()
        for v in [17, 18, 20]:
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
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, pq)


if __name__ == "__main__":
    unittest.main()
