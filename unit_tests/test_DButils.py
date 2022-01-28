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


class TestSetup(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """
    master class for the setup and teardown
    """
    def setUp(self):
        super(TestSetup, self).setUp()
        self.makeTestDB()
        self.loadData(os.path.join(dbp_testing.testsdir, 'data', 'db_dumps',
                                   'RBSP_MAGEIS_dump.json'))

    def tearDown(self):
        super(TestSetup, self).tearDown()
        self.removeTestDB()


class DBUtilsEmptyTests(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """Tests on an empty database"""

    def setUp(self):
        super(DBUtilsEmptyTests, self).setUp()
        self.td = tempfile.mkdtemp()
        self.pg = 'PGDATABASE' in os.environ
        self.dbname = os.environ.get(
            'PGDATABASE',  os.path.join(self.td, 'working.sqlite'))
        DButils.create_tables(self.dbname,
                              dialect = 'postgresql' if self.pg else 'sqlite')
        self.dbu = DButils.DButils(self.dbname)

    def tearDown(self):
        super(DBUtilsEmptyTests, self).tearDown()
        if self.pg:
            self.dbu.session.close()
            self.dbu.metadata.drop_all()
        self.dbu.closeDB()
        del self.dbu
        shutil.rmtree(self.td)

    def test_create_tables(self):
        """Chunk check that table creation in setUp works."""
        self.assertTrue(hasattr(self.dbu, 'File'))

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
        self.dbu = DButils.DButils(self.dbname)
        rootdir = os.path.join(dbp_testing.driveroot, 'rootdir')
        self.assertEqual(rootdir, self.dbu.getCodeDirectory())
        self.assertEqual(os.path.join(rootdir, 'errors'),
                         self.dbu.getErrorPath())
        self.assertEqual(rootdir, self.dbu.getInspectorDirectory())

    def test_addProcessNoOutput(self):
        """Add a process with no output product"""
        # This needs to be on "empty" db because our test non-empty
        # doesn't allow null output product (old database)
        pid = self.dbu.addProcess('no_output1', '', 'RUN')
        self.assertIsNone(self.dbu.getEntry('Process', pid).output_product)
        pid = self.dbu.addProcess('no_output2', None, 'RUN')
        self.assertIsNone(self.dbu.getEntry('Process', pid).output_product)
        # Verify product ID of zero isn't smashed
        # Need to create explicit product ID 0, since auto-inc starts at 1
        mission = self.dbu.addMission('mission', '/', '/incoming')
        sat = self.dbu.addSatellite('sat', mission)
        inst = self.dbu.addInstrument('inst', sat)
        prod = self.dbu.Product()
        prod.product_id = 0
        prod.instrument_id = inst
        prod.product_name = 'product0'
        prod.relative_path = 'data'
        prod.format = 'foo.txt'
        prod.level = 1.
        prod.description = 'product zero'
        self.dbu.session.add(prod)
        self.dbu.commitDB()
        pid = self.dbu.addProcess('output_pid_zero', 0, 'DAILY')
        self.assertEqual(0, self.dbu.getEntry('Process', pid).output_product)


class DBUtilsOtherTests(TestSetup):
    """Tests that are not processqueue or get or add"""

    def test_newest_version(self):
        """Test for newest_version"""
        ans = set([v.filename for v in self.dbu.getFilesByProduct(13, newest_version=True)])
        self.assertEqual(len(ans), 10)
        newest_files = set([
                         'ect_rbspa_0220_377_02.ptp.gz',
                         'ect_rbspa_0221_377_04.ptp.gz',
                         'ect_rbspa_0370_377_06.ptp.gz',
                         'ect_rbspa_0371_377_03.ptp.gz',
                         'ect_rbspa_0372_377_03.ptp.gz',
                         'ect_rbspa_0373_377_04.ptp.gz',
                         'ect_rbspa_0374_377_02.ptp.gz',
                         'ect_rbspa_0375_377_03.ptp.gz',
                         'ect_rbspa_0376_377_07.ptp.gz',
                         'ect_rbspa_0377_377_01.ptp.gz'])
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
        self.assertEqual(self.dbu.session.query(self.dbu.File).count(), 922)
        file_id = self.dbu.getFileID(123)
        self.dbu._purgeFileFromDB(file_id)
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, file_id)
        self.assertEqual(self.dbu.session.query(self.dbu.File).count(), 921)

    def test_purgeFileFromDBByName(self):
        """purgeFileFromDB, given a filename"""
        self.assertEqual(self.dbu.session.query(self.dbu.File).count(), 922)
        self.dbu._purgeFileFromDB('ect_rbspb_0377_356_01.ptp.gz')
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, 123)
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID,
                          'ect_rbspb_0377_356_01.ptp.gz')
        self.assertEqual(self.dbu.session.query(self.dbu.File).count(), 921)

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
        self.dbu.renameFile('ect_rbspb_0374_34c_01.ptp.gz', 'ect_rbspb_0374_34c_01.ptp.gz_newname')
        self.assertEqual(938, self.dbu.getFileID('ect_rbspb_0374_34c_01.ptp.gz_newname'))

    def test_getEntry(self):
        """getEntry works for non-PK input (e.g. names)"""
        res = self.dbu.getEntry(
            'File', 'rbspb_pre_MagEphem_OP77Q_20130909_v1.0.0.txt')
        self.assertEqual(1, res.file_id)

    def test_getEntryNoresult(self):
        """getEntry always raises DBNoData on no match"""
        with self.assertRaises(DButils.DBNoData) as cm:
            res = self.dbu.getEntry(
                'File', 'rbspb_pre_MagEphem_OP77Q_20130909_v1.0.1.txt')
        self.assertEqual(
            'No filename rbspb_pre_MagEphem_OP77Q_20130909_v1.0.1.txt'
            ' found in the DB', str(cm.exception))
        self.assertIs(None, self.dbu.getEntry('Inspector', 0))
# Desired behaviour in the future
#        with self.assertRaises(DButils.DBNoData) as cm:
#            res = self.dbu.getEntry(
#                'Inspector', 0)
#        self.assertEqual(
#            'No entry found for table Inspector, key 0.',
#            str(cm.exception))


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
        self.assertEqual(39, self.dbu.addProcess('proc_name', 1, 'DAILY'))


class DBUtilsGetTests(TestSetup):
    """Tests for database gets through DButils"""

    def test_init(self):
        """__init__ has an exception to test"""
        self.assertRaises(DButils.DBError, DButils.DButils, None)

    def test_openDB1(self):
        """__init__ has an exception to test"""
        self.assertRaises(ValueError, DButils.DButils, 'i do not exist',
                          engine='sqlite')

    def test_openDB2(self):
        """__init__ bad engine"""
        self.assertRaises(DButils.DBError, DButils.DButils, self.dbname, engine='i am bogus')

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
        # Checking keys
        self.assertEqual(
            sorted([('satellite', 'satellite'), ('mission', 'mission')]),
            sorted(list(zip(*ans))))
        # And for the same contents
        self.assertEqual(ans[0]['mission'], ans[1]['mission'])
        self.assertEqual(ans[0]['satellite'].satellite_name[:-1],
                         ans[1]['satellite'].satellite_name[:-1])

    def test_getAllInstruments(self):
        """getAllInstruments"""
        ans = self.dbu.getAllInstruments()
        # check that this is what we expect
        self.assertEqual(2, len(ans))
        # Expected keys...
        self.assertEqual(
            sorted([('instrument', 'instrument'),
                    ('satellite', 'satellite'),
                    ('mission', 'mission')]),
            sorted(zip(*ans)))
        # ...and matching values
        self.assertEqual(ans[0]['mission'], ans[1]['mission'])
        self.assertEqual(ans[0]['satellite'].satellite_name[:-1],
                         ans[1]['satellite'].satellite_name[:-1])
        self.assertEqual(ans[0]['instrument'].instrument_name,
                         ans[1]['instrument'].instrument_name)

    def test_getAllFileIds(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds()
        self.assertEqual(922, len(files))
        self.assertEqual(1, sorted(files)[0])

    def test_getAllFileIds2(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds(newest_version=True)
        self.assertEqual(362, len(files))
        self.assertEqual(len(files), len(set(files)))

    def test_getAllFileIds_limit(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds(limit=10)
        self.assertEqual(10, len(files))
        self.assertEqual(list(range(1, 11)), sorted(files))

    def test_getAllFileIds2_limit(self):
        """getAllFileIds"""
        files = self.dbu.getAllFileIds(newest_version=True, limit=10)
        self.assertEqual(10, len(files))
        self.assertEqual(len(files), len(set(files)))

    def test_getAllCodes(self):
        """getAllCodes"""
        codes = self.dbu.getAllCodes()
        self.assertEqual(len(codes), 17)
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
        self.assertEqual(len(codes), 16)
        codes = self.dbu.getAllCodes(active=False)
        self.assertEqual(len(codes), 17)

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
        rootdir = os.path.join(
            dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp')
        self.assertEqual(
            os.path.join(rootdir, 'MagEphem', 'predicted', 'b',
                         'rbspb_pre_MagEphem_OP77Q_20130909_v1.0.0.txt'),
            self.dbu.getFileFullPath(1))
        self.assertEqual(
            os.path.join(rootdir, 'MagEphem', 'predicted', 'b',
                         'rbspb_pre_MagEphem_OP77Q_20130909_v1.0.0.txt'),
            self.dbu.getFileFullPath(
                'rbspb_pre_MagEphem_OP77Q_20130909_v1.0.0.txt'))
        self.assertEqual(
            os.path.join(rootdir, 'rbspb', 'mageis_vc', 'level0',
                         'ect_rbspb_0377_381_05.ptp.gz'),
            self.dbu.getFileFullPath(17))
        self.assertEqual(
            os.path.join(rootdir, 'rbspb', 'mageis_vc', 'level0',
                         'ect_rbspb_0377_381_05.ptp.gz'),
            self.dbu.getFileFullPath('ect_rbspb_0377_381_05.ptp.gz'))

    def test_getProcessFromInputProduct(self):
        """getProcessFromInputProduct"""
        self.assertEqual([6, 13], self.dbu.getProcessFromInputProduct(1))
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
        self.assertEqual(6, self.dbu.getProcessID('rbspa_int_ect-mageis-M35_L2toL3'))
        self.assertRaises(NoResultFound, self.dbu.getProcessID, 'badval')
        self.assertRaises(NoResultFound, self.dbu.getProcessID, 10000)

    def test_getSatelliteMission(self):
        """getSatelliteMission"""
        val = self.dbu.getSatelliteMission(1)
        self.assertEqual(1, val.mission_id)
        self.assertEqual('mageis_incoming', val.incoming_dir)
        self.assertEqual('/n/space_data/cda/rbsp', val.rootdir)
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
        self.assertEqual(['rbsp'], self.dbu.getMissions())

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
        self.assertEqual(22, self.dbu.getCodeID(22))
        self.assertEqual([1, 4, 10, 20, 29, 30],
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
                         sorted(self.dbu.getInputProductID(2)))
        self.assertFalse(self.dbu.getInputProductID(2343))
        self.assertEqual([], self.dbu.getInputProductID(2343))

    def test_getInputProductIDOldDB(self):
        """getInputProductID, asking for yesterday/tomorrow on old DB"""
        res = self.dbu.getInputProductID(22, True)
        self.assertEqual(
            [(17, False, 0, 0)],
            res)

    def test_getFilesEndDate(self):
        """getFiles with only end date specified"""
        val = self.dbu.getFiles(endDate='2013-09-09', product=138)
        expected = ['rbspb_pre_MagEphem_OP77Q_201309{:02d}_v1.0.0.txt'.format(i)
                    for i in range(1, 10)]
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesStartDate(self):
        """getFiles with only start date specified"""
        val = self.dbu.getFiles(startDate='2013-09-08', product=138)
        expected = ['rbspb_pre_MagEphem_OP77Q_201309{:02d}_v1.0.0.txt'.format(i)
                    for i in range(8, 11)]
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
        val = self.dbu.getFiles(startDate='2013-09-08', product=138)
        expected = ['rbspb_pre_MagEphem_OP77Q_201309{:02d}_v1.0.0.txt'.format(i)
                    for i in range(8, 11)]
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
        val = self.dbu.getFiles(endDate='2013-09-08', product=138)
        expected = ['rbspb_pre_MagEphem_OP77Q_19590101_v1.0.0.txt'] + [
            'rbspb_pre_MagEphem_OP77Q_201309{:02d}_v1.0.0.txt'.format(i)
            for i in range(1, 9)]
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
            # V01, V02 end earlier in the day than the start time
            'ect_rbspb_0377_381_03.ptp.gz',
            'ect_rbspb_0377_381_04.ptp.gz',
            'ect_rbspb_0377_381_05.ptp.gz',
            ]
        val = self.dbu.getFiles(
            startTime=datetime.datetime(2013, 9, 10, 12), product=187)
        actual = sorted([v.filename for v in val])
        self.assertEqual(expected, actual)

    def test_getFilesStartTimeUnixTime(self):
        """getFiles with a start time, lookup by Unix time"""
        self.dbu.addUnixTimeTable()
        self.test_getFilesStartTime()

    def test_getFilesStartTimeFractional(self):
        """getFiles looked up by Unix time with a fractional second"""
        self.dbu.addUnixTimeTable()
        kwargs = {
            'filename': "rbspa_int_ect-mageisM35-hr-L1_20100101_v1.0.0.cdf",
            'data_level': 1.0,
            'version': Version.Version(1, 0, 0),
            'file_create_date': datetime.date(2010, 1, 1),
            'exists_on_disk': True,
            'utc_file_date': datetime.date(2010, 1, 1),
            'utc_start_time': datetime.datetime(2010, 1, 1),
            # Weird stop time to trigger rounding errors
            'utc_stop_time': datetime.datetime(2010, 1, 1, 23, 0, 0, 600000),
            'product_id': 4,
            'shasum': '0'
        }
        fID = self.dbu.addFile(**kwargs)
        start_time = datetime.datetime(2010, 1, 1, 23, 0, 0, 200000)
        val = self.dbu.getFiles(startTime=start_time, product=4)
        self.assertEqual([fID], [v.file_id for v in val])

    def test_getFilesByProductTime(self):
        """getFiles by the UTC date of data"""
        expected = ['ect_rbspb_0377_381_05.ptp.gz',
        ]
        val = self.dbu.getFilesByProductTime(187, ['2013-9-10', '2013-9-10'],
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
        self.assertEqual(sorted(ans), sorted([v.filename for v in val]))

        val = self.dbu.getFilesByProductDate(187, [datetime.date(2013, 9, 10)] * 2, newest_version=True)
        self.assertEqual(1, len(val))
        self.assertEqual('ect_rbspb_0377_381_05.ptp.gz', val[0].filename)

    def test_getFilesByDate1(self):
        """getFilesByDate, newest_version=False"""
        self.assertFalse(self.dbu.getFilesByDate([datetime.date(2013, 12, 12)] * 2))
        val = self.dbu.getFilesByDate([datetime.date(2013, 9, 10)] * 2)
        self.assertEqual(59, len(val))
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
        self.assertEqual(39, len(val))
        filenames = sorted([v.filename for v in val])
        ans = ['ect_rbspa_0377_344_02.ptp.gz', 
               'ect_rbspa_0377_345_01.ptp.gz']
        self.assertEqual(ans, filenames[:len(ans)])

    def test_getFilesByProduct(self):
        """getFilesByProduct"""
        self.assertFalse(self.dbu.getFilesByProduct(2))
        self.assertEqual([], self.dbu.getFilesByProduct(2))
        self.assertRaises(DButils.DBNoData, self.dbu.getFilesByProduct, 343423)
        val = self.dbu.getFilesByProduct(1)
        self.assertEqual(10, len(val))
        val = self.dbu.getFilesByProduct(187)
        self.assertEqual(44, len(val))
        val = self.dbu.getFilesByProduct(187, newest_version=True)
        self.assertEqual(12, len(val))
        filenames = [v.filename for v in self.dbu.getFilesByProduct(187, newest_version=True)]
        self.assertTrue('ect_rbspb_0371_381_03.ptp.gz' in filenames)

    def test_getFilesByInstrument(self):
        """getFilesByInstrument"""
        files = self.dbu.getFilesByInstrument(1)
        self.assertEqual(782, len(files))
        filenames = [v.filename for v in files]
        self.assertTrue('rbsp-a_magnetometer_uvw_emfisis-Quick-Look_20130909_v1.3.1.cdf' in
                        filenames)
        files = self.dbu.getFilesByInstrument(1, id_only=True)
        self.assertEqual(782, len(files))
        self.assertTrue(591 in files)
        files = self.dbu.getFilesByInstrument(2, id_only=True)
        self.assertEqual(140, len(files))
        files = self.dbu.getFilesByInstrument(1, id_only=True, level=2)
        self.assertEqual(16, len(files))
        self.assertTrue(576 in files)
        self.assertFalse(self.dbu.getFilesByInstrument(1, id_only=True, level=6))
        self.assertRaises(DButils.DBNoData, self.dbu.getFilesByInstrument, 'badval')
        self.assertRaises(DButils.DBNoData, self.dbu.getFilesByInstrument, 100)
        ids = [int(v) for v in files]

    def test_getActiveInspectors(self):
        """getActiveInspectors"""
        val = self.dbu.getActiveInspectors()
        self.assertEqual(57, len(val))
        v2 = set([v[0] for v in val])
        ans = set([
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp', 'codes', 'inspectors', o)
            for o in ('ect_L05_V1.0.0.py', 'ect_L0_V1.0.0.py',
                      'ect_L1_V1.0.0.py', 'ect_L2_V1.0.0.py',
                      'emfisis_V1.0.0.py', 'rbsp_pre_MagEphem_insp.py')])
        self.assertEqual(ans, v2)
        v3 = set([v[-1] for v in val])
        self.assertIn(1, v3)

    def test_getChildrenProcesses(self):
        """getChildrenProcesses"""
        self.assertEqual([38],
                         self.dbu.getChildrenProcesses(1))
        self.assertFalse(self.dbu.getChildrenProcesses(5754))
        self.assertEqual([], self.dbu.getChildrenProcesses(5754))
        self.assertRaises(DButils.DBNoData, self.dbu.getChildrenProcesses, 59498)

    def test_getProductID(self):
        """getProductID"""
        self.assertEqual(1, self.dbu.getProductID(1))
        self.assertEqual(2, self.dbu.getProductID(2))
        self.assertEqual(5, self.dbu.getProductID('rbspa_int_ect-mageisM75-ns-L1'))
        self.assertEqual([5, 2], self.dbu.getProductID(('rbspa_int_ect-mageisM75-ns-L1', 2)))
        self.assertEqual([5, 1], self.dbu.getProductID(['rbspa_int_ect-mageisM75-ns-L1', 1]))
        self.assertRaises(DButils.DBNoData, self.dbu.getProductID, 'badval')
        self.assertRaises(DButils.DBNoData, self.dbu.getProductID, 343423)

    def test_getProductID2(self):
        """getProductID"""
        newid = self.dbu.addProduct('rbsp-a_magnetometer_uvw_emfisis-Quick-Look', 1, 'relpath', 'format', 3, 'desc')
        self.assertEqual(newid, 191)
        self.assertEqual(self.dbu.getProductID('rbsp-a_magnetometer_uvw_emfisis-Quick-Look'), 1)

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
        self.assertEqual(
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp', 'codes', 'l05_to_l1.py'),
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
        self.assertEqual(
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp'),
            self.dbu.getMissionDirectory())

    def test_getCodeDirectory(self):
        """getCodeDirectory"""
        self.assertEqual(
            self.dbu.getCodeDirectory(),
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp'))
        
    def test_getInspectorDirectory(self):
        """getInspectorDirectory"""
        self.assertEqual(
            self.dbu.getInspectorDirectory(),
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp'))

    def test_getDirectory(self):
        self.assertEqual(self.dbu.getDirectory('codedir'), None)
        self.assertEqual(self.dbu.getDirectory('inspector_dir'), None)
        self.assertEqual(
            self.dbu.getDirectory('incoming_dir'),
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp',
                'mageis_incoming'))
        
    def test_getIncomingPath(self):
        """getIncomingPath"""
        self.assertEqual(
            self.dbu.getIncomingPath(),
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp',
                'mageis_incoming'))

    def test_getErrorPath(self):
        """getErrorPath"""
        self.assertEqual(
            self.dbu.getErrorPath(),
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp',
                'errors'))

    def test_getFilecodelink_byfile(self):
        """getFilecodelink_byfile"""
        self.assertEqual(3, self.dbu.getFilecodelink_byfile(517))
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
        self.assertEqual(52, len(p1))
        self.assertEqual(5, len(p2))
        self.assertFalse(set(p1).intersection(p2))

    def test_getProductsByInstrument2(self):
        """getProductsByInstrument"""
        id = self.dbu.addInstrument('Inst_name', 1)
        self.assertTrue(self.dbu.getProductsByInstrument(id) is None)

    def test_getAllProcesses(self):
        """getAllProcesses"""
        self.assertEqual(17, len(self.dbu.getAllProcesses()))
        self.assertEqual(11, len(self.dbu.getAllProcesses('DAILY')))
        self.assertEqual(6, len(self.dbu.getAllProcesses('FILE')))

    def test_getAllProducts(self):
        """getAllProducts"""
        self.assertEqual(57, len(self.dbu.getAllProducts()))

    def test_getProductsByLevel(self):
        """getProductsByLevel"""
        pr = self.dbu.getProductsByLevel(0)
        self.assertEqual(39, len(pr))
        self.assertTrue(self.dbu.getProductsByLevel(10) is None)

    def test_getProcessTimebase(self):
        """getProcessTimebase"""
        self.assertEqual("DAILY", self.dbu.getProcessTimebase(1))
        self.assertEqual("DAILY", self.dbu.getProcessTimebase('rbspa_int_ect-mageis-M35-hr_L05toL1'))

    def test_getFilesByCode(self):
        """getFilesByCode"""
        f = self.dbu.getFilesByCode(2)
        self.assertEqual(8, len(f))
        ids = self.dbu.getFilesByCode(2, id_only=True)
        self.assertEqual(set([576, 1733, 1735, 1741, 1745, 1872,
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
        ids = self.dbu.getFileParents(517, id_only=True)
        files = self.dbu.getFileParents(517, id_only=False)
        self.assertEqual(3, len(ids))
        self.assertEqual(3, len(files))
        for vv in files:
            self.assertTrue(self.dbu.getFileID(vv) in ids)
        self.assertEqual([235, 240, 233], ids)
        self.assertEqual([], self.dbu.getFileParents(255))

    def test_getProductParentTree(self):
        """getProductParentTree"""
        tmp = self.dbu.getProductParentTree()
        self.assertEqual(57, len(tmp))
        self.assertTrue([1, [10, 8]] in tmp)

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
        res = self.dbu.getTraceback('File', 517)
        self.assertEqual(
            517, res['file'].file_id)
        self.assertEqual(
            49, res['product'].product_id)
        self.assertEqual(
            49, res['inspector'].inspector_id)
        self.assertEqual(
            1, res['instrument'].instrument_id)
        self.assertEqual(
            1, res['satellite'].satellite_id)
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

    def test_getCodeDirectoryAbsSpecified(self):
        m = self.dbu.getEntry('Mission', 1)
        m.codedir = '/n/space_data/cda/rbsp/codedir'
        self.dbu.commitDB()
        self.assertEqual(
            self.dbu.getCodeDirectory(),
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp',
                'codedir'))
        
    def test_getCodeDirectoryRelSpecified(self):
        m = self.dbu.getEntry('Mission', 1)
        m.codedir = 'codedir'
        self.dbu.commitDB()
        self.assertEqual(
            self.dbu.getCodeDirectory(),
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda', 'rbsp',
                'codedir'))
     
    def test_getCodeDirectorySpecifiedBlank(self):
        self.assertEqual(self.dbu.getCodeDirectory(),
            os.path.join(
                dbp_testing.driveroot, 'n', 'space_data', 'cda',
                'rbsp'))

    def test_getInspectorDirectoryAbsSpecified(self):
        m = self.dbu.getEntry('Mission', 1)
        m.inspectordir = '/n/space_data/cda/rbsp/inspector_dir'
        self.dbu.commitDB()
        self.assertEqual(
            self.dbu.getInspectorDirectory(),
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp', 'inspector_dir'))

    def test_getInspectorDirectoryRelSpecified(self):
        m = self.dbu.getEntry('Mission', 1)
        m.inspectordir = 'inspector_dir'
        self.dbu.commitDB()
        self.assertEqual(
            self.dbu.getInspectorDirectory(),
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp', 'inspector_dir'))

    def test_getInspectorDirectorySpecifiedBlank(self):
        self.assertEqual(
            self.dbu.getInspectorDirectory(),
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp'))

    def test_getErrorDirectoryAbsSpecified(self):
        m = self.dbu.getEntry('Mission', 1)
        m.errordir = '/n/space_data/cda/rbsp/errors'
        self.dbu.commitDB()
        self.assertEqual(
            self.dbu.getErrorPath(),
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp', 'errors'))

    def test_getErrorDirectoryRelSpecified(self):
        m = self.dbu.getEntry('Mission', 1)
        m.errordir = 'errors'
        self.dbu.commitDB()
        self.assertEqual(
            self.dbu.getErrorPath(),
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp', 'errors'))

    def test_getErrorDirectorySpecifiedBlank(self):
        self.assertEqual(
            self.dbu.getInspectorDirectory(),
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp'))

    def test_getDirectorySpecified(self):
        m = self.dbu.getEntry('Mission', 1)
        m.inspector_dir = 'inspector_dir'
        m.errordir = '/n/space_data/cda/rbsp/errors'
        self.dbu.commitDB()
        self.assertEqual(
            self.dbu.getDirectory('errordir'),
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp', 'errors'))
        self.assertEqual(
            self.dbu.getDirectory('inspector_dir'),
            os.path.join(dbp_testing.driveroot, 'n', 'space_data', 'cda',
                         'rbsp', 'inspector_dir'))
        self.assertEqual(self.dbu.getDirectory('codedir'), None)


class ProcessqueueTests(TestSetup):
    """Test all the processqueue functionality"""

    def add_files(self):
        self.dbu.ProcessqueuePush([17, 18, 19, 20, 21])

    def test_pq_getall(self):
        """test self.ProcessqueueGetAll"""
        self.assertEqual(0, self.dbu.ProcessqueueLen())
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.assertEqual([17, 18, 19, 20, 21], self.dbu.ProcessqueueGetAll())
        self.assertEqual(list(zip([17, 18, 19, 20, 21], [None] * 5)),
                         self.dbu.ProcessqueueGetAll(version_bump=True))

    def test_pq_getall2(self):
        """test self.ProcessqueueGetAll"""
        self.assertEqual(0, self.dbu.ProcessqueueLen())
        self.assertFalse(self.dbu.ProcessqueueGetAll())
        self.assertFalse(self.dbu.ProcessqueueGetAll(version_bump=True))

    def test_pq_flush(self):
        """test self.ProcessqueueFlush"""
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.dbu.ProcessqueueFlush()
        self.assertEqual(0, self.dbu.ProcessqueueLen())

    def test_pq_remove(self):
        """test self.ProcessqueueRemove"""
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.dbu.ProcessqueueRemove(20)
        self.assertEqual(4, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        for v in [17, 18, 19, 21]:
            self.assertTrue(v in pq)
        self.dbu.ProcessqueueRemove([17, 18])
        self.assertEqual(2, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        for v in [19, 21]:
            self.assertTrue(v in pq)
        self.dbu.ProcessqueueRemove('ect_rbspb_0377_381_03.ptp.gz')
        self.assertEqual(1, self.dbu.ProcessqueueLen())
        self.assertEqual([21], self.dbu.ProcessqueueGetAll())

    def test_pq_push(self):
        """test self.ProcessqueuePush"""
        self.assertEqual(0, self.dbu.ProcessqueueLen())
        self.dbu.ProcessqueuePush(20)
        self.assertEqual(1, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        self.assertTrue(20 in pq)
        # push a value that is not there
        self.assertFalse(self.dbu.ProcessqueuePush(214442))
        self.assertFalse(self.dbu.ProcessqueuePush(20))
        self.assertEqual([17, 18, 19, 21], self.dbu.ProcessqueuePush([17, 18, 19, 20, 21]))

    def test_pq_push_MAX_ADD(self):
        """test self.ProcessqueuePush"""
        self.assertEqual(0, self.dbu.ProcessqueueLen())
        self.dbu.ProcessqueuePush([17, 18, 19, 20, 21], MAX_ADD=2)
        self.assertEqual(5, self.dbu.ProcessqueueLen())

    def test_pq_len(self):
        """test self.ProcessqueueLen"""
        self.assertEqual(0, self.dbu.ProcessqueueLen())
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())

    def test_pq_pop(self):
        """test self.ProcessqueuePop"""
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.dbu.ProcessqueuePop(0)
        self.assertEqual(4, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        for v in [18, 19, 20, 21]:
            self.assertTrue(v in pq)
        self.dbu.ProcessqueuePop(2)
        self.assertEqual(3, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        for v in [18, 19, 21]:
            self.assertTrue(v in pq)

    def test_pq_pop_reverse(self):
        """test self.ProcessqueuePop with negative indices"""
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.dbu.ProcessqueuePop(-1)
        self.assertEqual(4, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        for v in [17, 18, 19, 20]:
            self.assertTrue(v in pq)
        self.dbu.ProcessqueuePop(-2)
        self.assertEqual(3, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        for v in [17, 18, 20]:
            self.assertTrue(v in pq)

    def test_pq_get(self):
        """test self.ProcessqueueGet"""
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.assertEqual((17, None), self.dbu.ProcessqueueGet(0))
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.assertEqual((19, None), self.dbu.ProcessqueueGet(2))
        self.assertEqual(5, self.dbu.ProcessqueueLen())

    def test_pq_get_reverse(self):
        """test self.ProcessqueueGet with negative indices"""
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.assertEqual((21, None), self.dbu.ProcessqueueGet(-1))
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.assertEqual((20, None), self.dbu.ProcessqueueGet(-2))
        self.assertEqual(5, self.dbu.ProcessqueueLen())

    def test_pq_clean(self):
        """test self.ProcessqueueClean"""
        self.add_files()
        self.assertEqual(5, self.dbu.ProcessqueueLen())
        self.dbu.ProcessqueueClean()
        self.assertEqual(1, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        self.assertTrue(17 in pq)

    def test_pq_rawadd(self):
        """test self.ProcessqueueRawadd"""
        self.assertEqual(0, self.dbu.ProcessqueueLen())
        self.dbu.ProcessqueueRawadd(20)
        self.assertEqual(1, self.dbu.ProcessqueueLen())
        pq = self.dbu.ProcessqueueGetAll()
        self.assertTrue(20 in pq)
        try:
            self.dbu.ProcessqueueRawadd(20000)
        except DButils.DBError:
            # Database doesn't support adding an ID which doesn't exist
            return
        pq = self.dbu.ProcessqueuePop(1)
        self.assertRaises(DButils.DBNoData, self.dbu.getFileID, pq)


class TestWithtestDB(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """Tests that require the new testDB (or were written after it was made)"""

    def setUp(self):
        super(TestWithtestDB, self).setUp()
        self.makeTestDB()
        sourcepath = os.path.join(dbp_testing.testsdir, '..', 'functional_test')
        for f in os.listdir(sourcepath):
            sourcedir = os.path.join(sourcepath, f)
            if os.path.isdir(sourcedir):
                shutil.copytree(sourcedir, os.path.join(self.td, f))
        self.loadData(os.path.join(dbp_testing.testsdir, 'data', 'db_dumps',
                                   'testDB_dump.json'))
        self.dbu.getEntry('Mission', 1).rootdir = self.td  # Set the mission's dir to the tmp so we can work with it
        self.dbu.commitDB()
        self.dbu.MissionDirectory = self.td

    def tearDown(self):
        super(TestWithtestDB, self).tearDown()
        self.removeTestDB()

    def test_checkDiskForFile_DBTrue_FileTrue(self):
        """Check file in database exists on disk"""
        self.assertTrue(self.dbu.checkDiskForFile(1))

    def test_checkDiskForFile_DBTrue_FileFalse_FixTrue(self):
        """Check consistency between FS and DB, correct DB"""
        os.remove(self.td + '/L0/testDB_001_001.raw')
        self.assertTrue(self.dbu.checkDiskForFile(1, True))

    def test_checkDiskForFile_DBTrue_FileFalse(self):
        """checkDiskForFile returns false if the file in DB does not exist"""
        os.remove(self.td + '/L0/testDB_001_001.raw')
        self.assertFalse(self.dbu.checkDiskForFile(1))

    def test_checkDiskForFile_DBFalse_FileTrue(self):
        """checkDiskForFile returns true for nonexistent file, real and in DB"""
        self.dbu.getEntry('File', 1).exists_on_disk = False
        self.assertTrue(self.dbu.checkDiskForFile(1))

    def test_checkFileSHA(self):
        """Compare DB and real checksum, both matching and nonmatching"""
        file_id = self.dbu.getFileID("testDB_001_001.raw")
        self.assertTrue(self.dbu.checkFileSHA(file_id))

        with open(self.td + '/L0/testDB_001_001.raw', 'w') as fp:
            fp.write('I am some text that will change the SHA\n')
        self.assertFalse(self.dbu.checkFileSHA(file_id))

    def test_checkFiles(self):
        """Checks if checkFiles will detect both missing files and bad checksums"""
        with open(self.td + '/L0/testDB_001_000.raw', 'w') as fp:
            fp.write('I am some text that will change the SHA\n')
        os.remove(self.td + '/L0/testDB_000_000.raw')

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
                               exists_on_disk=True,
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

    def test_addFileNonDatetimeStart(self):
        """Add a file with a non-datetime utc_start_time"""
        v = Version.Version(1, 0, 0)
        kwargs = {
            'filename': "testing_file_1.0.0.file",
            'data_level': 0,
            'version': v,
            'file_create_date': datetime.date(2010, 1, 1),
            'exists_on_disk': True,
            'utc_file_date': datetime.date(2010, 1, 1),
            'utc_start_time': datetime.date(2010, 1, 1),
            'utc_stop_time': datetime.datetime(2010, 1, 2, 0, 0, 0),
            'product_id': 1,
            'shasum': '0'
        }
        # Make with a start date instead of datetime
        fID = self.dbu.addFile(**kwargs)
        f = self.dbu.getEntry('File', fID)
        self.assertEqual(datetime.datetime(2010, 1, 1),
                         f.utc_start_time)
        # Do the same thing with the Unix time table
        self.dbu.addUnixTimeTable()
        kwargs.update({
            'filename': "testing_file_1.1.0.file",
            'version': Version.Version(1, 1, 0)
        })
        fID = self.dbu.addFile(**kwargs)
        r = self.dbu.getEntry('Unixtime', fID)
        self.assertEqual(1262304000, r.unix_start)

    def test_addFileUnixTime(self):
        """Tests if addFile populates Unix time"""
        self.dbu.addUnixTimeTable()
        fID = self.addGenericFile(1)
        r = self.dbu.getEntry('Unixtime', fID)
        self.assertEqual(1262304000, r.unix_start)
        self.assertEqual(1262390400, r.unix_stop)

    def test_addFileNearDay(self):
        """Make sure conversion to unix time doesn't change day"""
        self.dbu.addUnixTimeTable()
        kwargs = {
            'filename': "testing_file_1.0.0.file",
            'data_level': 0,
            'version': Version.Version(1, 0, 0),
            'file_create_date': datetime.date(2010, 1, 1),
            'exists_on_disk': True,
            'utc_file_date': datetime.date(2010, 1, 1),
            'utc_start_time': datetime.datetime(2010, 1, 1),
            'utc_stop_time': datetime.datetime(2010, 1, 1, 23, 59, 59, 600000),
            'product_id': 1,
            'shasum': '0'
        }
        fID = self.dbu.addFile(**kwargs)
        r = self.dbu.getEntry('Unixtime', fID)
        # Convert back from Unix time and make sure same date
        self.assertEqual(datetime.date(2010, 1, 1),
                         (datetime.datetime(1970, 1, 1)
                          + datetime.timedelta(seconds=r.unix_stop)).date())

    def test_addInstrument(self):
        """Tests if addInstrument is succesful"""
        iID = self.dbu.addInstrument(instrument_name="t_{MISSION}_{SPACECRAFT}_In",
                                     satellite_id=1
                                     )

        i = self.dbu.getEntry('Instrument', iID)
        self.assertEqual('t_testDB_testDB-a_In', i.instrument_name)

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
                                            optional=False
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
        prID = self.dbu.addProcess(process_name="test_{PRODUCT}_{INSTRUMENT}_{SATELLITE}_{MISSION}",
                                   output_product=pID,
                                   output_timebase="FILE")
        cID = self.addGenericCode(prID)
        self.dbu.addproductprocesslink(input_product_id=1,
                                       process_id=prID,
                                       optional=False
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
        self.assertEqual('test_testDB_rot13_L1_rot13_testDB-a_testDB', p.process_name)

    def test_list_release(self):
        """Tests all of the release stuff, it's all intertwined anyway"""
        self.dbu.tag_release('1')

        ans = set(['testDB_001_001.raw', 'testDB_000_001.raw', 
                   'testDB_001_000.raw', 'testDB_000_000.raw',
                   'testDB_2016-01-02.cat', 'testDB_2016-01-04.cat', 
                   'testDB_2016-01-03.cat', 'testDB_2016-01-01.cat', 
                   'testDB_2016-01-04.rot', 'testDB_2016-01-05.cat', 
                   'testDB_2016-01-05.rot', 'testDB_2016-01-02.rot', 
                   'testDB_2016-01-01.rot', 'testDB_2016-01-03.rot', 
                   'testDB_000_002.raw', 'testDB_000_003.raw'])
        self.assertEqual(ans, set(self.dbu.list_release('1', fullpath=False)))
        # Test additional release options
        self.dbu.addRelease('testDB_2016-01-01.cat', '2', commit=True)
        self.assertEqual(
            [os.path.join(self.td , 'L1', 'testDB_2016-01-01.cat')],
            self.dbu.list_release('2', fullpath=True))

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
        actual = self.dbu.getAllFilenames(
            fullPath=False, level=1, product=1, code=1, instrument=1,
            exists=True)
        self.assertEqual(ans, sorted(actual))

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

        self.assertTrue(all([self.td in v for v in out]))

    def test_getChildTreeNoOutput(self):
        """getChildTree for processes w/o output"""
        tmp = self.dbu.getChildTree(3)
        ans = set([])
        self.assertFalse(set(tmp).difference(ans))
        # Explicitly make it null (it's empty string in the db)
        self.dbu.getEntry('Process', 2).output_product = None
        tmp = self.dbu.getChildTree(3)
        self.assertFalse(set(tmp).difference(ans))

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
            'Multiple rows match run_test.py', str(cm.exception))

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
            self.assertEqual(msg, str(cm.exception))

        #Tests that don't fit exactly the same pattern
        with self.assertRaises(ValueError) as cm:
            self.dbu.editTable('code', 1, 'filename', combine=True,
                               after_flag='-f')
        self.assertEqual(
            'Only use after_flag with arguments column in Code table.',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            self.dbu.editTable('process', 1, 'arguments', combine=True,
                               after_flag='-f')
        self.assertEqual(
            'Only use after_flag with arguments column in Code table.',
            str(cm.exception))

        with self.assertRaises(AttributeError) as cm:
            self.dbu.editTable('nonexistent', 1, 'process_name',
                               ins_after='L1', my_str='_new')
        self.assertEqual(
            "'DButils' object has no attribute 'Nonexistent'",
            str(cm.exception))

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
