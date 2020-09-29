#!/usr/bin/env python
from __future__ import print_function

import datetime
import os
import os.path
import shutil
import sqlite3
import stat
import tempfile
import unittest
from distutils.dir_util import copy_tree, remove_tree

from sqlalchemy.orm.exc import NoResultFound

import dbp_testing

from dbprocessing import DButils
from dbprocessing import Version


def make_tmpfile():
    tf = tempfile.NamedTemporaryFile(delete=False)
    tf.close()
    return tf.name


def remove_tmpfile(fname):
    os.remove(fname)


class TestSetupNoOpen(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """
    master class for the setup and teardown, without opening db
    (so it can be altered first)
    """
    dbfile = 'RBSP_MAGEIS.sqlite'
    def setUp(self):
        super(TestSetupNoOpen, self).setUp()
        sqpath = os.path.join(os.path.dirname(__file__), self.dbfile)
        self.sqlworking = sqpath.replace(self.dbfile, 'working.sqlite')
        shutil.copy(sqpath, self.sqlworking)
        os.chmod(self.sqlworking, stat.S_IRUSR | stat.S_IWUSR)

    def tearDown(self):
        super(TestSetupNoOpen, self).tearDown()
        self.dbu.closeDB()
        del self.dbu
        os.remove(self.sqlworking)


class TestSetup(TestSetupNoOpen):
    def setUp(self):
        super(TestSetup, self).setUp()
        self.dbu = DButils.DButils(self.sqlworking)


class DBUtilsEmptyTests(TestSetup):
    """Tests on an empty database"""
    dbfile = 'emptyDB.sqlite'

    def test_empty_mission_dir(self):
        self.assertIsNone(self.dbu.getMissionDirectory())

    def test_addMissionOptionals(self):
        """Test addMission excluding optional inputs"""
        self.assertEqual(self.dbu.addMission(
            'name', '/rootdir/', '/root/incoming'), 1)
        miss = self.dbu.getEntry('Mission', 1)
        self.assertEqual(1, miss.mission_id)
        self.assertTrue(miss.codedir is None)
        self.assertTrue(miss.errordir is None)
        self.assertTrue(miss.inspectordir is None)
        self.assertEqual('/rootdir/', miss.rootdir)
        self.assertEqual('/root/incoming', miss.incoming_dir)

        # These are not exactly test of just the add, but they behave
        # differently with this different add.
        # Reload the mission
        self.dbu.closeDB()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual('/rootdir', self.dbu.getCodeDirectory())
        self.assertEqual('/rootdir/errors', self.dbu.getErrorPath())
        self.assertEqual('/rootdir', self.dbu.getInspectorDirectory())


class DBUtilsOtherTests(TestSetup):
    """Tests that are not processqueue or get or add"""

    def test_newest_version(self):
        """Test for newest_version"""
        ans = set([v.filename for v in self.dbu.getFilesByProduct(13, newest_version=True)])
        self.assertEqual(len(ans), 22)
        newest_files = set([
                         u'ect_rbspa_0220_377_02.ptp.gz',
                         u'ect_rbspa_0221_377_04.ptp.gz',
                         u'ect_rbspa_0370_377_06.ptp.gz',
                         u'ect_rbspa_0371_377_03.ptp.gz',
                         u'ect_rbspa_0372_377_03.ptp.gz',
                         u'ect_rbspa_0373_377_04.ptp.gz',
                         u'ect_rbspa_0374_377_02.ptp.gz',
                         u'ect_rbspa_0375_377_03.ptp.gz',
                         u'ect_rbspa_0376_377_07.ptp.gz',
                         u'ect_rbspa_0377_377_01.ptp.gz',
                         u'ect_rbspa_0378_377_03.ptp.gz',
                         u'ect_rbspa_0379_377_04.ptp.gz',
                         u'ect_rbspa_0380_377_02.ptp.gz',
                         u'ect_rbspa_0381_377_02.ptp.gz',
                         u'ect_rbspa_0382_377_07.ptp.gz',
                         u'ect_rbspa_0383_377_04.ptp.gz',
                         u'ect_rbspa_0384_377_02.ptp.gz',
                         u'ect_rbspa_0385_377_03.ptp.gz',
                         u'ect_rbspa_0386_377_03.ptp.gz',
                         u'ect_rbspa_0387_377_02.ptp.gz',
                         u'ect_rbspa_0388_377_03.ptp.gz',
                         u'ect_rbspa_0389_377_05.ptp.gz'])
        self.assertEqual(ans, newest_files)

    def test_checkIncoming(self):
        """checkIncoming"""
        e = self.dbu.getEntry('Mission', 1)
        td = tempfile.mkdtemp()
        try:
            e.incoming_dir = td
            self.assertFalse(self.dbu.checkIncoming())
            fnames = [os.path.join(td, f) for f in ['test', 'test2', '3test']]
            for f in fnames:
                open(f, 'w').close()
            inc_files = self.dbu.checkIncoming()
            self.assertTrue(inc_files)
            self.assertEqual(sorted(fnames), sorted(inc_files))
        finally:
            shutil.rmtree(td)

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
        self.assertRaises(ValueError, self.dbu.resetProcessingFlag, comment=None)  # no comment
        self.dbu.resetProcessingFlag('testing comment')
        self.assertFalse(self.dbu.currentlyProcessing())

    def test_repr(self):
        """repr"""
        self.assertTrue('DBProcessing class instance for mission' in self.dbu.__repr__())

    def test_purgeFileFromDB(self):
        """purgeFileFromDB"""
        self.assertEqual(self.dbu.session.query(self.dbu.File).count(), 6681)
        file_id = self.dbu.getFileID(123)
        self.dbu._purgeFileFromDB(file_id)
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, file_id)
        self.assertEqual(self.dbu.session.query(self.dbu.File).count(), 6680)

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


class DBUtilsAddTests(TestSetup):
    """Tests for database adds through DButils"""

    def test_addMission(self):
        """addMission"""
        self.assertEqual(self.dbu.addMission('name', '/rootdir/', '/root/incoming', '/code/', '/inspectors/', '/errors/'), 2)
        self.assertEqual(self.dbu.getEntry('Mission', 2).mission_id, 2)

    def test_addSatellite(self):
        """test_addSatellite"""
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

    def test_getAllFileIds(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds()
        self.assertEqual(6681, len(files))
        self.assertEqual(range(1, 6682), sorted(files))

    def test_getAllFileIds2(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds(newest_version=True)
        self.assertEqual(2848, len(files))
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

    def test_getFileDatesSpan(self):
        """getFileDates for a file that spans days"""
        fid = self.dbu.addFile(
            filename='rbsp-a_magnetometer_uvw_emfisis-Quick-Look'
            '_20120101_v99.99.99.cdf',
            data_level=0,
            version=Version.Version(99, 99, 99),
            product_id=1,
            # Pretend this is a product where the actual timespan is shifted
            # by an hour from "characteristic" date
            utc_file_date=datetime.datetime(2012, 1, 1),
            utc_start_time=datetime.datetime(2012, 1, 1, 1),
            utc_stop_time=datetime.datetime(2012, 1, 2, 1),
            file_create_date=datetime.datetime.now(),
            exists_on_disk=True)
        self.assertEqual(
            [datetime.date(2012, 1, 1), datetime.date(2012, 1, 2)],
            self.dbu.getFileDates(fid))

    def test_getInputProductID(self):
        """getInputProductID"""
        self.assertEqual([(60, False)], self.dbu.getInputProductID(1))
        self.assertEqual([(22, False), (43, False), (84, False), (90, True)],
                         self.dbu.getInputProductID(2))
        self.assertFalse(self.dbu.getInputProductID(2343))
        self.assertEqual([], self.dbu.getInputProductID(2343))

    def test_getInputProductIDOldDB(self):
        """getInputProductID, asking for yesterday/tomorrow on old DB"""
        res = self.dbu.getInputProductID(2, True)
        self.assertEqual(
            [(22, False, 0, 0), (43, False, 0, 0),
             (84, False, 0, 0), (90, True, 0, 0)],
            res)

    def test_getFilesEndDate(self):
        """getFiles with only end date specified"""
        val = self.dbu.getFiles(endDate='2013-09-14', product=138)
        expected = ['rbspb_pre_MagEphem_OP77Q_201309{:02d}_v1.0.0.txt'.format(i)
                    for i in range(1, 15)]
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesStartDate(self):
        """getFiles with only start date specified"""
        val = self.dbu.getFiles(startDate='2013-09-14', product=138)
        expected = ['rbspb_pre_MagEphem_OP77Q_201309{:02d}_v1.0.0.txt'.format(i)
                    for i in range(14, 31)]
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesBeyondMagicDate(self):
        """getFiles with a date past magic values for date"""
        self.dbu.addFile(
            filename='rbspb_pre_MagEphem_OP77Q_20990101_v1.0.0.txt',
            data_level=0, version=Version.Version(1, 0, 0),
            file_create_date=datetime.date(2010, 1, 1),
            exists_on_disk=False, utc_file_date=datetime.date(2099, 1, 1),
            utc_start_time=datetime.datetime(2099, 1, 1),
            utc_stop_time=datetime.datetime(2099, 1, 1, 23, 59, 59),
            product_id=138, shasum='0')
        val = self.dbu.getFiles(startDate='2013-09-14', product=138)
        expected = ['rbspb_pre_MagEphem_OP77Q_201309{:02d}_v1.0.0.txt'.format(i)
                    for i in range(14, 31)]
        expected += ['rbspb_pre_MagEphem_OP77Q_20990101_v1.0.0.txt']
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesBeforeMagicDate(self):
        """getFiles with a date before magic values for date"""
        self.dbu.addFile(
            filename='rbspb_pre_MagEphem_OP77Q_19590101_v1.0.0.txt',
            data_level=0, version=Version.Version(1, 0, 0),
            file_create_date=datetime.date(2010, 1, 1),
            exists_on_disk=False, utc_file_date=datetime.date(1959, 1, 1),
            utc_start_time=datetime.datetime(1959, 1, 1),
            utc_stop_time=datetime.datetime(1959, 1, 1, 23, 59, 59),
            product_id=138, shasum='0')
        val = self.dbu.getFiles(endDate='2013-09-14', product=138)
        expected = ['rbspb_pre_MagEphem_OP77Q_19590101_v1.0.0.txt'] + [
            'rbspb_pre_MagEphem_OP77Q_201309{:02d}_v1.0.0.txt'.format(i)
            for i in range(1, 15)]
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesUTCDay(self):
        """getFiles with a single UTC day time"""
        expected = ['ect_rbspb_0186_381_01.ptp.gz',
                    'ect_rbspb_0186_381_02.ptp.gz']
        val = self.dbu.getFiles(
            startTime='2013-03-02', endTime='2013-03-02', product=187)
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)
        expected = ['ect_rbspb_0186_381_02.ptp.gz']
        val = self.dbu.getFiles(
            startTime='2013-03-02', endTime='2013-03-02',
            product=187, newest_version=True)
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesUTCDayUnixTime(self):
        """getFiles with a single UTC day time, lookup by Unix time"""
        self.dbu.addUnixTimeTable()
        # Run all the same checks
        self.test_getFilesUTCDay()

    def test_getFilesStartTime(self):
        """getFiles with a start time"""
        expected = [
            # V01 ends earlier in the day than the start time
            'ect_rbspb_0388_381_02.ptp.gz',
            'ect_rbspb_0388_381_03.ptp.gz',
            'ect_rbspb_0389_381_01.ptp.gz',
            'ect_rbspb_0389_381_02.ptp.gz',
            'ect_rbspb_0389_381_03.ptp.gz',
            ]
        val = self.dbu.getFiles(
            startTime=datetime.datetime(2013, 9, 21, 12), product=187)
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesStartTimeUnixTime(self):
        """getFiles with a start time, lookup by Unix time"""
        self.dbu.addUnixTimeTable()
        self.test_getFilesStartTime()

    def test_getFilesByProductTime(self):
        """getFiles by the UTC date of data"""
        expected = ['ect_rbspb_0382_381_04.ptp.gz',
                    'ect_rbspb_0383_381_03.ptp.gz',
        ]
        val = self.dbu.getFilesByProductTime(187, ['2013-9-15', '2013-9-15'],
                                             newest_version=True)
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesByProductTimeUnixTime(self):
        """getFiles by the UTC date of data, lookup by Unix time"""
        self.dbu.addUnixTimeTable()
        self.test_getFilesByProductTime()

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

        val = self.dbu.getFilesByProductDate(187, [datetime.date(2013, 9, 10)] * 2, newest_version=True)
        self.assertEqual(1, len(val))
        self.assertEqual('ect_rbspb_0377_381_05.ptp.gz', val[0].filename)

    def test_getFilesByDate1(self):
        """getFilesByDate, newest_version=False"""
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

    def test_getFilesByDate2(self):
        """getFilesByDate, newest_version=True"""
        val = self.dbu.getFilesByDate([datetime.date(2013, 9, 10)] * 2, newest_version=True)
        self.assertEqual(129, len(val))
        filenames = sorted([v.filename for v in val])
        ans = [u'ect_rbspa_0377_344_02.ptp.gz', 
               u'ect_rbspa_0377_345_01.ptp.gz']
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
        self.assertEqual([163, 2], self.dbu.getProductID(('rbspb_mageis-M75-sp-hg-L0', 2)))
        self.assertEqual([163, 1], self.dbu.getProductID(['rbspb_mageis-M75-sp-hg-L0', 1]))
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
        self.assertEqual('/n/space_data/cda/rbsp', self.dbu.getMissionDirectory())

    def test_getCodeDirectory(self):
        """getCodeDirectory"""
        self.assertEqual(self.dbu.getCodeDirectory(), '/n/space_data/cda/rbsp')
        
    def test_getInspectorDirectory(self):
        """getInspectorDirectory"""
        self.assertEqual(self.dbu.getInspectorDirectory(),
                         '/n/space_data/cda/rbsp')

    def test_getDirectory(self):
        self.assertEqual(self.dbu.getDirectory('codedir'), None)
        self.assertEqual(self.dbu.getDirectory('inspector_dir'), None)
        self.assertEqual(self.dbu.getDirectory('incoming_dir'), '/n/space_data/cda/rbsp/mageis_incoming') 
        
    def test_getIncomingPath(self):
        """getIncomingPath"""
        self.assertEqual(self.dbu.getIncomingPath(), '/n/space_data/cda/rbsp/mageis_incoming')

    def test_getErrorPath(self):
        """getErrorPath"""
        self.assertEqual(self.dbu.getErrorPath(),
                         '/n/space_data/cda/rbsp/errors')

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
        self.assertEqual("DAILY", self.dbu.getProcessTimebase('rbspa_int_ect-mageis-M35-hr_L05toL1'))

    def test_getFilesByCode(self):
        """getFilesByCode"""
        f = self.dbu.getFilesByCode(2)
        self.assertEqual(20, len(f))
        ids = self.dbu.getFilesByCode(2, id_only=True)
        self.assertEqual(set([576, 1733, 1735, 1741, 1745, 1872, 5814, 5817, 5821,
                              5824, 5831, 5834, 5838, 5842, 5845, 5849, 5855, 5858,
                              5861, 5865]), set(ids))

    def test_getVersion(self):
        """getFileVersion"""
        self.assertEqual(Version.Version(1, 0, 0), self.dbu.getFileVersion(1))
        self.assertEqual(Version.Version(1, 1, 0), self.dbu.getFileVersion(123))

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
        self.assertEqual([], self.dbu.getFileParents(3000))

    def test_getProductParentTree(self):
        """getProductParentTree"""
        tmp = self.dbu.getProductParentTree()
        self.assertEqual(190, len(tmp))
        self.assertTrue([1, [10, 8, 39, 76]] in tmp)

    def test_getProcessTraceback(self):
        """Traceback for a process"""
        result = self.dbu.getTraceback('Process', 4)
        self.assertEqual(4, result['process'].process_id)
        self.assertEqual(4, result['code'].code_id)
        input_product = result['input_product']
        self.assertEqual(1, len(input_product))
        input_product, optional = input_product[0]
        self.assertEqual(83, input_product.product_id)
        self.assertFalse(optional)

    def test_getProcessTracebackNoInput(self):
        """Traceback for a process without any input product"""
        # Create a product to serve as the output of the process
        prodid = self.addProduct(
            product_name='triggered_output',
            instrument_id=1,
            format='trigger_{Y}{m}{d}_v{VERSION}.out',
            level=2)
        procid, codeid = self.addProcess('no_input', output_product_id=prodid)
        res = self.dbu.getTraceback('Process', procid)
        self.assertEqual(procid, res['process'].process_id)
        self.assertEqual(codeid, res['code'].code_id)
        self.assertEqual(0, len(res['input_product']))

    def test_getFileTraceback(self):
        """Get the full traceback for a file"""
        res = self.dbu.getTraceback('File', 573)
        self.assertEqual(
            573, res['file'].file_id)
        self.assertEqual(
            137, res['product'].product_id)
        self.assertEqual(
            137, res['inspector'].inspector_id)
        self.assertEqual(
            2, res['instrument'].instrument_id)
        self.assertEqual(
            2, res['satellite'].satellite_id)
        self.assertEqual(
            1, res['mission'].mission_id)

    def test_getFileTracebackNoInput(self):
        """Traceback for a file resulting from inputless product"""
        prodid = self.addProduct(
            product_name='triggered_output',
            instrument_id=1,
            format='trigger_{Y}{m}{d}_v{VERSION}.out',
            level=2)
        procid, codeid = self.addProcess('no_input', output_product_id=prodid)
        fid = self.addFile('trigger_20130921_v1.0.0.out', prodid)
        self.dbu.addFilecodelink(fid, codeid)
        res = self.dbu.getTraceback('File', fid)
        self.assertEqual(
            fid, res['file'].file_id)
        self.assertEqual(
            prodid, res['product'].product_id)
        self.assertEqual(
            1, res['instrument'].instrument_id)
        self.assertEqual(
            1, res['satellite'].satellite_id)
        self.assertEqual(
            1, res['mission'].mission_id)


class DBUtilsGetTestsNoOpen(TestSetupNoOpen):

    def test_getCodeDirectoryAbsSpecified(self):
        #https://stackoverflow.com/questions/7300948/add-column-to-sqlalchemy-table
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column codedir VARCHAR(50)")
        connection.commit()
        cursor.execute("UPDATE mission SET codedir = '/n/space_data/cda/rbsp/codedir' WHERE mission_id = 1")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getCodeDirectory(),
                         '/n/space_data/cda/rbsp/codedir')
        
    def test_getCodeDirectoryRelSpecified(self):
        #https://stackoverflow.com/questions/7300948/add-column-to-sqlalchemy-table
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column codedir VARCHAR(50)")
        connection.commit()
        cursor.execute("UPDATE mission SET codedir = 'codedir' WHERE mission_id = 1")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getCodeDirectory(),
                         '/n/space_data/cda/rbsp/codedir')
     
    def test_getCodeDirectorySpecifiedBlank(self):
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column codedir VARCHAR(50)")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getCodeDirectory(), '/n/space_data/cda/rbsp')

    def test_getInspectorDirectoryAbsSpecified(self):
        #https://stackoverflow.com/questions/7300948/add-column-to-sqlalchemy-table
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column inspectordir VARCHAR(50)")
        connection.commit()
        cursor.execute("UPDATE mission SET inspectordir = '/n/space_data/cda/rbsp/inspector_dir' WHERE mission_id = 1")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getInspectorDirectory(),
                         '/n/space_data/cda/rbsp/inspector_dir')

    def test_getInspectorDirectoryRelSpecified(self):
        #https://stackoverflow.com/questions/7300948/add-column-to-sqlalchemy-table
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column inspectordir VARCHAR(50)")
        connection.commit()
        cursor.execute("UPDATE mission SET inspectordir = 'inspector_dir' WHERE mission_id = 1")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getInspectorDirectory(),
                         '/n/space_data/cda/rbsp/inspector_dir')

    def test_getInspectorDirectorySpecifiedBlank(self):
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column inspector_dir VARCHAR(50)")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getInspectorDirectory(), '/n/space_data/cda/rbsp')

    def test_getErrorDirectoryAbsSpecified(self):
        #https://stackoverflow.com/questions/7300948/add-column-to-sqlalchemy-table
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column errordir VARCHAR(50)")
        connection.commit()
        cursor.execute("UPDATE mission SET errordir = '/n/space_data/cda/rbsp/errors' WHERE mission_id = 1")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getErrorPath(), '/n/space_data/cda/rbsp/errors')

    def test_getErrorDirectoryRelSpecified(self):
        #https://stackoverflow.com/questions/7300948/add-column-to-sqlalchemy-table
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column errordir VARCHAR(50)")
        connection.commit()
        cursor.execute("UPDATE mission SET errordir = 'errors' WHERE mission_id = 1")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getErrorPath(),
                         '/n/space_data/cda/rbsp/errors')

    def test_getErrorDirectorySpecifiedBlank(self):
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column errordir VARCHAR(50)")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getInspectorDirectory(), '/n/space_data/cda/rbsp')

    def test_getDirectorySpecified(self):
        #https://stackoverflow.com/questions/7300948/add-column-to-sqlalchemy-table
        connection = sqlite3.connect(self.sqlworking)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE mission ADD column codedir VARCHAR(50)")
        cursor.execute("ALTER TABLE mission ADD column inspector_dir VARCHAR(50)")
        connection.commit()
        cursor.execute("UPDATE mission SET inspector_dir = 'inspector_dir' WHERE mission_id = 1")
        cursor.execute("ALTER TABLE mission ADD column errordir VARCHAR(50)")
        connection.commit()
        cursor.execute("UPDATE mission SET errordir = '/n/space_data/cda/rbsp/errors' WHERE mission_id = 1")
        connection.commit()
        connection.close()
        self.dbu = DButils.DButils(self.sqlworking)
        self.assertEqual(self.dbu.getDirectory('errordir'),'/n/space_data/cda/rbsp/errors')
        self.assertEqual(self.dbu.getDirectory('inspector_dir'), '/n/space_data/cda/rbsp/inspector_dir')
        self.assertEqual(self.dbu.getDirectory('codedir'), None)


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


class TestWithtestDB(unittest.TestCase):
    """Tests that require the new testDB (or were written after it was made)"""

    def setUp(self):
        super(TestWithtestDB, self).setUp()
        self.tempD = tempfile.mkdtemp()
        self.path = os.path.dirname(os.path.realpath(__file__))

        copy_tree(self.path + '/../functional_test/', self.tempD)
        self.dbu = DButils.DButils(self.tempD + '/testDB.sqlite')
        self.dbu.getEntry('Mission', 1).rootdir = self.tempD  # Set the mission's dir to the tmp so we can work with it
        self.dbu.commitDB()
        self.dbu.MissionDirectory = self.tempD

    def tearDown(self):
        super(TestWithtestDB, self).tearDown()
        self.dbu.closeDB()
        # print(self.tempD)
        remove_tree(self.tempD)

    def test_checkDiskForFile_DBTrue_FileTrue(self):
        """Check file in database exists on disk"""
        self.assertTrue(self.dbu.checkDiskForFile(1))

    def test_checkDiskForFile_DBTrue_FileFalse_FixTrue(self):
        """Check consistency between FS and DB, correct DB"""
        os.remove(self.tempD + '/L0/testDB_001_001.raw')
        self.assertTrue(self.dbu.checkDiskForFile(1, True))

    def test_checkDiskForFile_DBTrue_FileFalse(self):
        """checkDiskForFile returns false if the file in DB does not exist"""
        os.remove(self.tempD + '/L0/testDB_001_001.raw')
        self.assertFalse(self.dbu.checkDiskForFile(1))

    def test_checkDiskForFile_DBFalse_FileTrue(self):
        """checkDiskForFile returns true for nonexistent file, real and in DB"""
        self.dbu.getEntry('File', 1).exists_on_disk = False
        self.assertTrue(self.dbu.checkDiskForFile(1))

    def test_checkFileSHA(self):
        """Compare DB and real checksum, both matching and nonmatching"""
        file_id = self.dbu.getFileID("testDB_001_001.raw")
        self.assertTrue(self.dbu.checkFileSHA(file_id))

        with open(self.tempD + '/L0/testDB_001_001.raw', 'w') as fp:
            fp.write('I am some text that will change the SHA\n')
        self.assertFalse(self.dbu.checkFileSHA(file_id))

    def test_checkFiles(self):
        """Checks if checkFiles will detect both missing files and bad checksums"""
        with open(self.tempD + '/L0/testDB_001_000.raw', 'w') as fp:
            fp.write('I am some text that will change the SHA\n')
        os.remove(self.tempD + '/L0/testDB_000_000.raw')

        ans = [('testDB_001_000.raw', 1), ('testDB_000_000.raw', 2)]
        self.assertEqual(ans, self.dbu.checkFiles())

    def addGenericCode(self, processID=1):
        """Adds a dummy code."""
        cID = self.dbu.addCode(filename="run_test.py",
                               relative_path="scripts",
                               code_start_date="2010-09-01",
                               code_stop_date="2099-01-01",
                               code_description="Desc",
                               process_id=processID,
                               version="1.2.3",
                               active_code=1,
                               date_written="2016-06-08",
                               output_interface_version=1,
                               newest_version=1
                               )
        return cID

    def addGenericInspector(self, productID):
        """Adds a dummy inspector. productID is the index of the associated product"""
        iID = self.dbu.addInspector(filename="testing_L0.py",
                                    relative_path="codes/inspectors",
                                    description="Level 0",
                                    version="1.2.3",
                                    active_code=1,
                                    date_written="2016-06-08",
                                    output_interface_version=1,
                                    newest_version=1,
                                    product=productID,
                                    arguments="-q"
                                    )
        return iID

    def addGenericProduct(self):
        """Adds a dummy product."""
        pID = self.dbu.addProduct(product_name="testing_Product",
                                  instrument_id=1,
                                  relative_path="L0",
                                  format="testing_frmt",
                                  level=0,
                                  product_description="desc"
                                  )
        return pID

    def addGenericFile(self, productID, version=(1,2,3)):
        """Adds a dummy file"""
        version = Version.Version(*version)
        fID = self.dbu.addFile(filename="testing_file_{0}.file".format(str(version)),
                               data_level=0,
                               version=version,
                               file_create_date=datetime.date(2010, 1, 1),
                               exists_on_disk=1,
                               utc_file_date=datetime.date(2010, 1, 1),
                               utc_start_time=datetime.datetime(2010, 1, 1, 0, 0, 0),
                               utc_stop_time=datetime.datetime(2010, 1, 2, 0, 0, 0),
                               product_id=productID,
                               shasum='0'
                               )
        return fID

    def addGenericInstrument(self):
        """Adds a dummy instrument"""
        iID = self.dbu.addInstrument(instrument_name="testing_Instrument",
                                     satellite_id=1
                                     )
        return iID

    def test_fileIsNewest(self):
        """test is a specific file is the newest version"""
        fID1 = self.addGenericFile(1, version=(1, 0, 0))
        fID2 = self.addGenericFile(1, version=(1, 1, 0))
        fID3 = self.addGenericFile(1, version=(1, 2, 0))
        fID4 = self.addGenericFile(1, version=(1, 3, 0))
        self.assertFalse(self.dbu.fileIsNewest('testing_file_1.0.0.file'))
        self.assertFalse(self.dbu.fileIsNewest('testing_file_1.1.0.file'))
        self.assertFalse(self.dbu.fileIsNewest('testing_file_1.2.0.file'))
        self.assertTrue(self.dbu.fileIsNewest('testing_file_1.3.0.file'))
        self.assertFalse(self.dbu.fileIsNewest(fID1))
        self.assertFalse(self.dbu.fileIsNewest(fID2))
        self.assertFalse(self.dbu.fileIsNewest(fID1))
        self.assertTrue(self.dbu.fileIsNewest(fID4))

    def test_addCode(self):
        """Tests if addCode is succesful"""
        cID = self.addGenericCode()
        c = self.dbu.getEntry('Code', cID)

        self.assertEqual("run_test.py", c.filename)
        self.assertEqual("scripts", c.relative_path)
        self.assertEqual(datetime.date(2010, 9, 1), c.code_start_date)
        self.assertEqual(datetime.date(2099, 1, 1), c.code_stop_date)
        self.assertEqual("Desc", c.code_description)
        self.assertEqual(1, c.process_id)
        self.assertEqual(1, c.interface_version)
        self.assertEqual(2, c.quality_version)
        self.assertEqual(3, c.revision_version)
        self.assertEqual(1, c.active_code)
        self.assertEqual(datetime.date(2016, 6, 8), c.date_written)
        self.assertEqual(1, c.output_interface_version)
        self.assertEqual(1, c.newest_version)

    def test_addInspector(self):
        """Tests if addInspector is succesful"""
        iID = self.addGenericInspector(1)
        i = self.dbu.getEntry('Inspector', iID)

        self.assertEqual("testing_L0.py", i.filename)
        self.assertEqual("codes/inspectors", i.relative_path)
        self.assertEqual("Level 0", i.description)
        self.assertEqual(1, i.interface_version)
        self.assertEqual(2, i.quality_version)
        self.assertEqual(3, i.revision_version)
        self.assertEqual(1, i.active_code)
        self.assertEqual(datetime.date(2016, 6, 8), i.date_written)
        self.assertEqual(1, i.output_interface_version)
        self.assertEqual(1, i.newest_version)
        self.assertEqual(1, i.product)
        self.assertEqual("-q", i.arguments)

    def test_addFile(self):
        """Tests if addFile is succesful"""
        fID = self.addGenericFile(1)

        i = self.dbu.getEntry('File', fID)
        self.assertEqual("testing_file_1.2.3.file", i.filename)
        self.assertEqual(0, i.data_level)
        self.assertEqual(1, i.interface_version)
        self.assertEqual(2, i.quality_version)
        self.assertEqual(3, i.revision_version)
        self.assertEqual(datetime.datetime(2010, 1, 1, 0, 0), i.file_create_date)
        self.assertEqual(1, i.exists_on_disk)
        self.assertEqual(datetime.date(2010, 1, 1), i.utc_file_date)
        self.assertEqual(datetime.datetime(2010, 1, 1, 0, 0), i.utc_start_time)
        self.assertEqual(datetime.datetime(2010, 1, 2, 0, 0), i.utc_stop_time)
        self.assertEqual(1, i.product_id)
        self.assertEqual('0', i.shasum)

    def test_addFileUnixTime(self):
        """Tests if addFile populates Unix time"""
        self.dbu.addUnixTimeTable()
        fID = self.addGenericFile(1)
        r = self.dbu.getEntry('Unixtime', fID)
        self.assertEqual(1262304000, r.unix_start)
        self.assertEqual(1262390400, r.unix_stop)

    def test_addInstrument(self):
        """Tests if addInstrument is succesful"""
        iID = self.dbu.addInstrument(instrument_name="testing_{MISSION}_{SPACECRAFT}_Instrument",
                                     satellite_id=1
                                     )

        i = self.dbu.getEntry('Instrument', iID)
        self.assertEqual('testing_testDB_testDB-a_Instrument', i.instrument_name)

    def test_addProduct(self):
        """Tests if addProduct is succesful"""
        pID = self.addGenericProduct()

        i = self.dbu.getEntry('Product', pID)
        self.assertEqual('testing_Product', i.product_name)
        self.assertEqual(1, i.instrument_id)
        self.assertEqual('L0', i.relative_path)
        self.assertEqual('testing_frmt', i.format)
        self.assertEqual(0, i.level)
        self.assertEqual('desc', i.product_description)

    def test_addInstrumentproductlink(self):
        """Tests if addInstrumentproductlink is succesful"""
        pID = self.addGenericProduct()
        self.addGenericInspector(pID)
        ID = self.dbu.addInstrumentproductlink(instrument_id=1,
                                               product_id=pID
                                               )

        i = self.dbu.getEntry('Instrumentproductlink', ID)
        self.assertEqual(1, i.instrument_id)
        self.assertEqual(pID, i.product_id)

    def test_addproductprocesslink(self):
        """Tests if addproductprocesslink is succesful"""
        pID = self.addGenericProduct()
        self.addGenericInspector(pID)
        ID = self.dbu.addproductprocesslink(input_product_id=pID,
                                            process_id=1,
                                            optional=0
                                            )

        i = self.dbu.session.query(self.dbu.Productprocesslink).filter_by(input_product_id=pID).first()
        self.assertEqual(pID, i.input_product_id)
        self.assertEqual(1, i.process_id)
        self.assertEqual(0, i.optional)

    def test_addFilecodelink(self):
        """Tests if addFilecodelink is succesful"""
        cID = self.addGenericCode()
        fID = self.addGenericFile(1)
        self.dbu.addFilecodelink(resulting_file_id=fID,
                                 source_code=cID
                                 )

        i = self.dbu.session.query(self.dbu.Filecodelink).filter_by(resulting_file=fID).first()
        self.assertEqual(fID, i.resulting_file)
        self.assertEqual(cID, i.source_code)

    def test_addFilefilelink(self):
        """Tests if addFilefilelink is succesful"""
        f1ID = self.addGenericFile(1)
        self.dbu.addFilefilelink(resulting_file_id=f1ID,
                                 source_file=f1ID - 1
                                 )

        i = self.dbu.session.query(self.dbu.Filefilelink).filter_by(resulting_file=f1ID).first()
        self.assertEqual(f1ID, i.resulting_file)
        self.assertEqual(f1ID - 1, i.source_file)

    def test_delInspector(self):
        """Tests if delInspector is succesful"""
        iID = self.addGenericInspector(1)
        i = self.dbu.getEntry('Inspector', iID)
        # Make sure the add worked
        self.assertEqual("testing_L0.py", i.filename)

        self.dbu.delInspector(iID)
        self.assertEqual(None, self.dbu.getEntry('Inspector', iID))

    def test_delFilecodelink(self):
        """Tests if delFilecodelink is succesful"""
        cID = self.addGenericCode()
        fID = self.addGenericFile(1)
        self.dbu.addFilecodelink(resulting_file_id=fID,
                                 source_code=cID
                                 )

        i = self.dbu.session.query(self.dbu.Filecodelink).filter_by(resulting_file=fID).first()
        # Make sure the add worked
        self.assertEqual(fID, i.resulting_file)

        self.dbu.delFilecodelink(fID)
        i = self.dbu.session.query(self.dbu.Filecodelink).filter_by(resulting_file=fID).first()
        self.assertEqual(None, i)

    def test_delFilefilelink(self):
        """Tests if delFilefilelink is succesful"""
        f1ID = self.addGenericFile(1)
        # f2ID = self.addGenericFile(2, datetime.date(2010, 1, 2))
        self.dbu.addFilefilelink(resulting_file_id=f1ID,
                                 source_file=f1ID - 1
                                 )

        i = self.dbu.session.query(self.dbu.Filefilelink).filter_by(resulting_file=f1ID).first()
        # Make sure the add worked
        self.assertEqual(f1ID, i.resulting_file)

        self.dbu.delFilefilelink(f1ID)
        i = self.dbu.session.query(self.dbu.Filefilelink).filter_by(resulting_file=f1ID).first()
        self.assertEqual(None, i)

    def test_updateInspectorSubs(self):
        """Tests if updateInspectorSubs is succesful"""
        pID = self.addGenericProduct()

        iID = self.dbu.addInspector(filename="testing_L0.py",
                                    relative_path="codes/{SPACECRAFT}_{SATELLITE}_{MISSION}_{PRODUCT}_{LEVEL}",
                                    description="Level 0",
                                    version=Version.Version(1, 0, 0),
                                    active_code=1,
                                    date_written="2016-06-08",
                                    output_interface_version=1,
                                    newest_version=1,
                                    product=pID,
                                    arguments="-q"
                                    )
        self.dbu.addInstrumentproductlink(instrument_id=1,
                                          product_id=pID
                                          )

        self.dbu.updateInspectorSubs(iID)
        i = self.dbu.getEntry('Inspector', iID)
        self.assertEqual('codes/testDB-a_testDB-a_testDB_testing_Product_0.0', i.relative_path)

    def test_updateProductSubs(self):
        """Tests if updateProductSubs is succesful"""
        pID = self.dbu.addProduct(product_name="testing_{INSTRUMENT}",
                                  instrument_id=1,
                                  relative_path="L0",
                                  format="testing_{SPACECRAFT}_{SATELLITE}_{MISSION}_{PRODUCT}_{LEVEL}",
                                  level=0,
                                  product_description=None
                                  )

        self.addGenericInspector(pID)
        self.dbu.addInstrumentproductlink(instrument_id=1,
                                          product_id=pID
                                          )

        self.dbu.updateProductSubs(pID)
        p = self.dbu.getEntry('Product', pID)
        self.assertEqual('testing_rot13', p.product_name)
        self.assertEqual('testing_testDB-a_testDB-a_testDB_testing_rot13_0.0', p.format)

    def test_updateProcessSubs(self):
        """Tests if updateProcessSubs is succesful"""
        pID = self.addGenericProduct()
        self.addGenericInspector(pID)
        self.dbu.addInstrumentproductlink(instrument_id=1,
                                          product_id=pID
                                          )
        prID = self.dbu.addProcess(process_name="testing_process_{PRODUCT}_{INSTRUMENT}_{SATELLITE}_{MISSION}",
                                   output_product=pID,
                                   output_timebase="FILE")
        cID = self.addGenericCode(prID)
        self.dbu.addproductprocesslink(input_product_id=1,
                                       process_id=prID,
                                       optional=0
                                       )
        #Make sure the code was appropriately associated with the process
        #(test of the test before we rely on it)
        code = self.dbu.getEntry('Code', cID)
        self.assertEqual(prID, code.process_id)
        self.assertEqual(
            cID,
            self.dbu.getCodeFromProcess(prID, datetime.date.today()))
        self.dbu.updateProcessSubs(prID)
        p = self.dbu.getEntry('Process', prID)
        self.assertEqual('testing_process_testDB_rot13_L1_rot13_testDB-a_testDB', p.process_name)

    def test_list_release(self):
        """Tests all of the release stuff, it's all intertwined anyway"""
        self.dbu.tag_release(1)

        ans = set(['testDB_001_001.raw', 'testDB_000_001.raw', 
                   'testDB_001_000.raw', 'testDB_000_000.raw',
                   'testDB_2016-01-02.cat', 'testDB_2016-01-04.cat', 
                   'testDB_2016-01-03.cat', 'testDB_2016-01-01.cat', 
                   'testDB_2016-01-04.rot', 'testDB_2016-01-05.cat', 
                   'testDB_2016-01-05.rot', 'testDB_2016-01-02.rot', 
                   'testDB_2016-01-01.rot', 'testDB_2016-01-03.rot', 
                   'testDB_000_002.raw', 'testDB_000_003.raw'])
        self.assertEqual(ans, set(self.dbu.list_release(1, fullpath=False)))
        # Test additional release options
        self.dbu.addRelease('testDB_2016-01-01.cat', 2, commit=True)
        self.assertEqual([self.tempD + '/L1/testDB_2016-01-01.cat'], self.dbu.list_release(2, fullpath=True))

    def test_getAllFilenames_all(self):
        """getAllFilenames should return all files in the db when passed no filters"""
        ans = sorted(['testDB_001_001.raw', 'testDB_000_001.raw', 
                   'testDB_001_000.raw', 'testDB_000_000.raw',
                   'testDB_2016-01-02.cat', 'testDB_2016-01-04.cat', 
                   'testDB_2016-01-03.cat', 'testDB_2016-01-01.cat', 
                   'testDB_2016-01-04.rot', 'testDB_2016-01-05.cat', 
                   'testDB_2016-01-05.rot', 'testDB_2016-01-02.rot', 
                   'testDB_2016-01-01.rot', 'testDB_2016-01-03.rot', 
                   'testDB_000_002.raw', 'testDB_000_003.raw'])

        self.assertEqual(ans, sorted(self.dbu.getAllFilenames(fullPath = False)))

    def test_getAllFilenames_product(self):
        """getAllFilenames should return the files with product_id 1"""
        ans = sorted(['testDB_2016-01-01.cat', 'testDB_2016-01-02.cat', 
               'testDB_2016-01-03.cat', 'testDB_2016-01-04.cat',
               'testDB_2016-01-05.cat'])

        self.assertEqual(ans, sorted(self.dbu.getAllFilenames(fullPath = False,
                                                       product = 1)))

    def test_getAllFilenames_level(self):
        """getAllFilenames should return the files with level 0"""
        ans = sorted(['testDB_001_001.raw', 'testDB_001_000.raw', 
                'testDB_000_001.raw', 'testDB_000_000.raw',
                'testDB_000_002.raw', 'testDB_000_003.raw'])

        self.assertEqual(ans, sorted(self.dbu.getAllFilenames(fullPath = False,
                                                       level = 0)))

    def test_getAllFilenames_code(self):
        """getAllFilenames should return the files with code 1"""
        ans = sorted(['testDB_2016-01-02.cat', 'testDB_2016-01-04.cat', 
               'testDB_2016-01-03.cat', 'testDB_2016-01-01.cat', 
               'testDB_2016-01-05.cat'])

        self.assertEqual(ans, sorted(self.dbu.getAllFilenames(fullPath = False,
                                                       code = 1)))

    def test_getAllFilenames_instrument(self):
        """getAllFilenames should return the files with instrument 1"""
        ans = sorted(['testDB_001_001.raw', 'testDB_000_001.raw', 
                'testDB_001_000.raw', 'testDB_000_000.raw',
                'testDB_2016-01-02.cat', 'testDB_2016-01-04.cat', 
                'testDB_2016-01-03.cat', 'testDB_2016-01-01.cat', 
                'testDB_2016-01-04.rot', 'testDB_2016-01-05.cat', 
                'testDB_2016-01-05.rot', 'testDB_2016-01-02.rot', 
                'testDB_2016-01-01.rot', 'testDB_2016-01-03.rot', 
                'testDB_000_002.raw', 'testDB_000_003.raw'])

        self.assertEqual(ans, sorted(self.dbu.getAllFilenames(fullPath = False,
                                                       instrument = 1)))

    def test_getAllFilenames_date1(self):
        """getAllFilenames, date, string"""
        ans = sorted(['testDB_001_000.raw', 'testDB_000_000.raw',
               'testDB_2016-01-01.cat', 'testDB_2016-01-01.rot'])

        self.assertEqual(ans, sorted(self.dbu.getAllFilenames(fullPath = False,
                                                       startDate = "2016-01-01",
                                                       endDate = "2016-01-01")))

    def test_getAllFilenames_date2(self):
        """getAllFilenames, date, datetime.date"""
        ans = sorted(['testDB_001_000.raw', 'testDB_000_000.raw',
               'testDB_2016-01-01.cat', 'testDB_2016-01-01.rot'])

        self.assertEqual(ans, sorted(self.dbu.getAllFilenames(fullPath = False,
                                                       startDate = datetime.date(2016, 1, 1),
                                                       endDate = datetime.date(2016, 1, 1))))

    def test_getAllFilenames_allFilters(self):
        """getAllFilenames should return the files with all the filters"""
        ans = ['testDB_2016-01-01.cat', 'testDB_2016-01-02.cat',
               'testDB_2016-01-03.cat', 'testDB_2016-01-04.cat',
               'testDB_2016-01-05.cat']

        self.assertEqual(ans, self.dbu.getAllFilenames(fullPath = False,
                                                       level = 1,
                                                       product = 1,
                                                       code = 1,
                                                       instrument = 1,
                                                       exists = True))

    def test_getAllFilenames_limit(self):
        """getAllFilenames should only return 4 items with limit=4"""
        ans = set(['testDB_2016-01-01.cat', 'testDB_001_001.raw',
                   'testDB_001_000.raw', 'testDB_000_003.raw',
                   'testDB_000_002.raw', 'testDB_000_001.raw',
                   'testDB_000_000.raw', 'testDB_2016-01-02.cat',
                   'testDB_2016-01-03.cat', 'testDB_2016-01-04.cat',
                   'testDB_2016-01-05.cat', 'testDB_2016-01-01.rot',
                   'testDB_2016-01-02.rot', 'testDB_2016-01-03.rot',
                   'testDB_2016-01-04.rot', 'testDB_2016-01-05.rot'])
        out = set(self.dbu.getAllFilenames(fullPath = False, limit = 4))

        self.assertEqual(4, len(out))
        self.assertTrue(out.issubset(ans))

    def test_getAllFilenames_fullPath(self):
        """getAllFilenames should return the fullPath"""
        out = self.dbu.getAllFilenames()

        self.assertTrue(all([self.tempD in v for v in out]))

    def testUpdateCodeNewestVersion(self):
        """Set the newest version flag on a code"""
        #Test precondition
        code = self.dbu.getEntry('Code', 1)
        self.assertTrue(code.newest_version)
        self.assertTrue(code.active_code)
        #Test default
        self.dbu.updateCodeNewestVersion(1)
        self.assertFalse(code.newest_version)
        self.assertFalse(code.active_code)
        #Specify
        self.dbu.updateCodeNewestVersion(1, True)
        self.assertTrue(code.newest_version)
        self.assertTrue(code.active_code)
        #Test the no-op version
        self.dbu.updateCodeNewestVersion(1, True)
        self.assertTrue(code.newest_version)
        self.assertTrue(code.active_code)
        #And specify false
        self.dbu.updateCodeNewestVersion(1, False)
        self.assertFalse(code.newest_version)
        self.assertFalse(code.active_code)
        #Test with the name instead of ID
        self.dbu.updateCodeNewestVersion('run_rot13_L0toL1.py', True)
        self.assertTrue(code.newest_version)
        self.assertTrue(code.active_code)

    def testEditTableReplace(self):
        """Test editTable with simple string replace"""
        self.dbu.editTable('code', 1, 'relative_path',
                           my_str='newscripts', replace_str='scripts')
        code = self.dbu.getEntry('Code', 1)
        self.assertEqual('newscripts', code.relative_path)

    def testEditTableReplaceProcess(self):
        """Test editTable with simple string replace on process table"""
        self.dbu.editTable('process', 1, 'process_name',
                           my_str='L5', replace_str='L1')
        process = self.dbu.getEntry('Process', 1)
        self.assertEqual('rot_L0toL5', process.process_name)

    def testEditTableReplaceMultipleCodes(self):
        """Test editTable with multiple matches for the code"""
        #Make multiple codes with same script name
        self.addGenericCode()
        self.addGenericCode()
        with self.assertRaises(RuntimeError) as cm:
            self.dbu.editTable('code', 'run_test.py', 'relative_path',
                               my_str='newscripts', replace_str='scripts')
        self.assertEqual(
            'Multiple rows match run_test.py', cm.exception.message)

    def testEditTableReplaceAfter(self):
        """Test editTable with a replace only after a flag"""
        code = self.dbu.getEntry('Code', 1)
        code.arguments = '-i foobar -j foobar -k foobar'
        self.dbu.editTable('code', 1, 'arguments',
                           my_str='baz', replace_str='bar',
                           after_flag='-j')
        self.assertEqual('-i foobar -j foobaz -k foobar',
                         code.arguments)

    def testEditTableReplaceAfterMultiple(self):
        """Test editTable with a replace-after-flag, flag happens many times"""
        code = self.dbu.getEntry('Code', 1)
        code.arguments = '-i foobar -j foobar -k foobar -j goobar'
        self.dbu.editTable('code', 1, 'arguments',
                           my_str='baz', replace_str='bar',
                           after_flag='-j')
        self.assertEqual('-i foobar -j foobaz -k foobar -j goobaz',
                         code.arguments)

    def testEditTableCombine(self):
        """Test editTable argument combination"""
        code = self.dbu.getEntry('Code', 1)
        code.arguments = '-i foo -i bar -j baz'
        self.dbu.editTable('code', 1, 'arguments', combine=True,
                           after_flag='-i')
        self.assertEqual('-i foo,bar -j baz', code.arguments)
        code.arguments = '-i foo -i bar -j baz'
        self.dbu.editTable('code', 1, 'arguments', combine=True,
                           after_flag='-j')
        self.assertEqual('-i foo -i bar -j baz', code.arguments)

    def testEditTableInsertBefore(self):
        """Test editTable inserting before a string"""
        code = self.dbu.getEntry('Code', 1)
        code.arguments = '-i foo -j bar -k baz'
        self.dbu.editTable('code', 1, 'arguments', my_str='test_',
                           ins_before='foo')
        self.assertEqual('-i test_foo -j bar -k baz', code.arguments)

    def testEditTableInsertAfter(self):
        """Test editTable inserting after a string"""
        code = self.dbu.getEntry('Code', 1)
        code.arguments = '-i foo -j foobar -k baz'
        self.dbu.editTable('code', 1, 'arguments', my_str='2',
                           ins_after='foo')
        self.assertEqual('-i foo2 -j foo2bar -k baz', code.arguments)

    def testEditTableNoChange(self):
        """Test editTable with no actual change"""
        code = self.dbu.getEntry('Code', 1)
        code.arguments = '-i foo -j foobar -k baz'
        self.dbu.editTable('code', 1, 'arguments', my_str='2',
                           replace_str='nothing')
        self.assertEqual('-i foo -j foobar -k baz', code.arguments)

    def testEditTableNoChange2(self):
        """Test editTable with identical replacement"""
        code = self.dbu.getEntry('Code', 1)
        code.arguments = '-i foo -j foobar -k baz'
        self.dbu.editTable('code', 1, 'arguments', my_str='foo',
                           replace_str='foo')
        self.assertEqual('-i foo -j foobar -k baz', code.arguments)

    def testEditTableNULL(self):
        """Test editTable with NULL string"""
        code = self.dbu.getEntry('Code', 1)
        code.arguments = None
        self.dbu.editTable('code', 1, 'arguments', my_str='2',
                           replace_str='nothing')
        self.assertEqual(None, code.arguments)

    def testEditTableExceptions(self):
        """Test editTable with bad arguments"""
        #Each test case is a tuple of kwargs for the call and expected
        #error message from the exception.
        test_cases = [
            ({},
             'Nothing to be done.'),
            ({'ins_after': 'foo', 'ins_before': 'bar', 'my_str': 'baz'},
             'Only use one of ins_after, ins_before,'
                 ' and replace_str.'),
            ({'ins_after': 'foo'},
             'Need my_str.'),
            ({'combine': True, 'ins_after': 'foo', 'my_str': 'bar',
              'after_flag': '-f' },
             'Combine flag cannot be used with ins_after,'
                 ' ins_before, or replace_str.'),
            ({'combine': True, 'my_str': 'bar', 'after_flag': '-f'},
             'Do not need my_str with combine.'),
            ({'combine': True},
             'Must specify after_flag with combine.'),
            ]
        for kwargs, msg in test_cases:
            with self.assertRaises(ValueError) as cm:
                self.dbu.editTable('code', 1, 'arguments', **kwargs)
            self.assertEqual(msg, cm.exception.message)

        #Tests that don't fit exactly the same pattern
        with self.assertRaises(ValueError) as cm:
            self.dbu.editTable('code', 1, 'filename', combine=True,
                               after_flag='-f')
        self.assertEqual(
            'Only use after_flag with arguments column in Code table.',
            cm.exception.message)
        with self.assertRaises(ValueError) as cm:
            self.dbu.editTable('process', 1, 'arguments', combine=True,
                               after_flag='-f')
        self.assertEqual(
            'Only use after_flag with arguments column in Code table.',
            cm.exception.message)

        with self.assertRaises(AttributeError) as cm:
            self.dbu.editTable('nonexistent', 1, 'process_name',
                               ins_after='L1', my_str='_new')
        self.assertEqual(
            "'DButils' object has no attribute 'Nonexistent'",
            cm.exception.message)

    def testAddUnixTimeTable(self):
        """Add the table with Unix time"""
        self.dbu.addUnixTimeTable()
        r = self.dbu.getEntry('Unixtime', 1)
        f = self.dbu.getEntry('File', 1)
        # Verify the preconditions, UTC times are what expect
        self.assertEqual(
            datetime.datetime(2016, 1, 2),
            f.utc_start_time)
        self.assertEqual(
            datetime.datetime(2016, 1, 3),
            f.utc_stop_time)
        # Verify the Unix time conversions
        self.assertEqual(1451692800, r.unix_start)
        self.assertEqual(1451779200, r.unix_stop)
        with self.assertRaises(RuntimeError) as cm:
            self.dbu.addUnixTimeTable()
        self.assertEqual('Unixtime table already seems to exist.',
                         str(cm.exception))


if __name__ == "__main__":
    unittest.main()
