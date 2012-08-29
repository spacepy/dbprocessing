#!/usr/bin/env python2.6

import datetime
import os
import os.path
import shutil
import sys
import unittest
import tempfile

import CreateDB
import DBUtils2
import Version


__version__ = '2.0.3'



class DBUtils2DBTests(unittest.TestCase):
    """Tests for database access through DBUtils2"""

    def setUp(self):
        super(DBUtils2DBTests, self).setUp()
        db = CreateDB.dbprocessing_db(filename = ':memory:', create=True)
        self.dbu = DBUtils2.DBUtils2(mission='unittest')
        self.dbu._openDB(db_var=db)
        self.dbu._createTableObjects()

    def tearDown(self):
        super(DBUtils2DBTests, self).tearDown()
        self.dbu._closeDB()
        del self.dbu

    def test_opencloseDB(self):
        """_open should open a db in memory and _close should close it"""
        self.dbu._openDB()
        self.assertTrue(self.dbu._openDB() is None)
        try:
            self.dbu.engine
        except:
            self.fail("engine was not created")
        try:
            self.dbu.metadata
        except:
            self.fail("Metadata not created")
        try:
            self.dbu.session
        except:
            self.fail("Session not created")
        ## should think up a better test here
        self.dbu._closeDB()
        self.assertTrue(self.dbu._closeDB() is None)

    def test_init(self):
        """__init__ has an exception to test"""
        self.assertRaises(DBUtils2.DBError, DBUtils2.DBUtils2, None)

    def test_repr(self):
        """__repr__ should print a known output"""
        self.dbu._openDB()
        self.assertEqual('DBProcessing class instance for mission ' + self.dbu.mission + ', version: ' + DBUtils2.__version__,
                         self.dbu.__repr__())

    def test_createTableObjects(self):
        """_createTableObjects should do just that"""
        from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, BigInteger, Date, DateTime, BigInteger, Boolean
        self.dbu._openDB()
        metadata = MetaData()
        data_file_table = Table('data_files', metadata,
                                Column('f_id', Integer, primary_key=True),
                                Column('filename', String(50)),
                                Column('utc_file_date', Date),
                                Column('utc_start_time', DateTime),
                                Column('utc_end_time', DateTime),
                                Column('data_level', Integer),
                                Column('consistency_check', DateTime),
                                Column('interface_version', Integer),
                                Column('verbose_provenance', String(500)),
                                Column('quality_check', String(1)),
                                Column('quality_comment', String(100)),
                                Column('caveats', String(20)),
                                Column('release_number', String(2)),
                                Column('ds_id', Integer),
                                Column('quality_version', Integer),
                                Column('revision_version', Integer),
                                Column('file_create_date', DateTime),
                                Column('dp_id', Integer),
                                Column('met_start_time', Integer),
                                Column('met_stop_time', Integer),
                                Column('exists_on_disk', Boolean),
                                Column('base_filename', String(50))
                                )
        metadata.create_all(self.dbu.engine)
        ## now the actual test
        try:
            self.dbu._createTableObjects()
        except DBUtils2.DBError:
            self.fail('Error is setting up table->class mapping')
        try:
            self.dbu.File
        except AttributeError:
            self.fail('Class not created')

    def test_addMission(self):
        """add a mission to the DB"""
        m = self.dbu.addMission('unittest', 'rootdir')
        self.assertEqual(m, 1)
        self.assertEqual(self.dbu.getMissionID('unittest'), m)
        self.assertEqual(self.dbu.getMissionDirectory(), 'rootdir')
        self.assertEqual(self.dbu.getMissionName(), 'unittest')
        self.assertRaises(DBUtils2.DBError, self.addMission)
        self.assertEqual(self.dbu.getMissionName(id=1), ['unittest'])

    def addMission(self):
        """utility to add a mission"""
        self.mission = self.dbu.addMission('unittest', 'rootdir')

    def test_getMissions(self):
        """test _getInputProductID"""
        self.assertEqual([], self.dbu.getMissions())
        self.addMission()
        self.assertEqual(['unittest'], self.dbu.getMissions())

    def test_addSatellite(self):
        """add a satellite to the DB"""
        self.addMission()
        id = self.dbu.addSatellite('satname')
        self.assertEqual(id, 1)
        self.assertRaises(DBUtils2.DBError, self.addSatellite)

    def addSatellite(self):
        """add satellite utility"""
        self.satellite = self.dbu.addSatellite('satname')

    def test_addInstrument(self):
        """testing addInstrument"""
        self.addMission()
        self.addSatellite()
        id = self.dbu.addInstrument('instname', 1)
        self.assertEqual(id, 1)
        self.assertRaises(DBUtils2.DBError, self.addInstrument)

    def addInstrument(self):
        """addInstrument utility"""
        self.instrument = self.dbu.addInstrument('instname', 1)

    def test_getInstrumentID(self):
        """test getInstrumentID"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.assertEqual(self.dbu.getInstrumentID('instname'), 1)
        self.assertEqual(self.dbu.getInstrumentID(1), 1)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getInstrumentID, 23462345)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getInstrumentID, 'noexist')
        self.dbu.addInstrument('instname', self.dbu.addSatellite('satname2'))
        self.assertRaises(ValueError, self.dbu.getInstrumentID, 'instname')
        self.assertEqual(self.dbu.getInstrumentID('instname', 1), 1)

    def test_addProduct(self):
        """test addProduct"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        id = self.dbu.addProduct('prod1', 1, 'prod1_path', None, 'format', 0)
        self.assertEqual(id, 1)
        self.assertRaises(DBUtils2.DBError, self.addProduct)

    def addProduct(self):
        """addProduct utility"""
        self.product = self.dbu.addProduct('prod1', 1, 'prod1_path', None, 'format', 0)
        self.dbu.addInstrumentproductlink(1, 1)

    def test_getProducts(self):
        """getProducts"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.assertEqual(1, self.dbu.getProducts()[0].product_id)

    def addProductOutput(self):
        """addProductOutput utility"""
        self.productOutput = self.dbu.addProduct('prod2', 1, 'prod2_path', None, 'format', 0)
        self.dbu.addInstrumentproductlink(1, 2)

    def test_getProductID(self):
        """test getProductID"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.assertEqual(self.dbu.getProductID('prod1'), 1)
        self.addProductOutput()
        self.assertEqual(self.dbu.getProductID('prod2'), 2)
        self.assertEqual(self.dbu.getProductID(2), 2)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getProductID, 23452)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getProductID, 'nofile')

    def test_getProductName(self):
        """test getProductName"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.assertEqual(self.dbu.getProductName('prod1'), 'prod1')
        self.assertEqual(self.dbu.getProductName(1), 'prod1')
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getProductName, 23452)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getProductName, 'nofile')

    def test_getProductLevel(self):
        """test getProductLevel"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.assertEqual(self.dbu.getProductLevel(1), 0)

    def test_getProductNames(self):
        """test getProductNames"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.assertEqual(self.dbu.getProductNames(), [(u'unittest', u'satname', u'instname', u'prod1', 1),
                         (u'unittest', u'satname', u'instname', u'prod2', 2)])
        self.assertEqual(self.dbu.getProductNames(1), (u'unittest', u'satname', u'instname', u'prod1', 1))

    def test_getProductFormats(self):
        """getProductFormats"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        val = self.dbu.getProductFormats()
        self.assertEqual(val, [(u'format', 1), (u'format', 2)])
        val = self.dbu.getProductFormats(1)
        self.assertEqual(val, 'format')

    def test_addProcess(self):
        """test addProcess"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.assertRaises(ValueError, self.dbu.addProcess, 'process1', 1, 'BADVAL', extra_params='Extra=extra', super_process_id=None)
        id = self.dbu.addProcess('process1', 1, 'DAILY', extra_params='Extra=extra', super_process_id=None)
        self.assertEqual(id, 1)
        self.assertRaises(DBUtils2.DBError, self.addProcess)

    def addProcess(self):
        """addProcess utility"""
        self.process = self.dbu.addProcess('process1', 1, 'DAILY', extra_params='Extra=extra', super_process_id=None)

    def test_getChildrenProducts(self):
        """getChildrenProducts"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addFile()
        self.addproductprocesslink()
        val = self.dbu.getChildrenProducts(1)
        self.assertEqual(val, [1])

    def test_getAllProcesses(self):
        """getAllProcesses"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addproductprocesslink()
        self.addProcess()
        val = self.dbu.getAllProcesses()
        self.assertEqual(val[0].process_id, 1)
        self.assertEqual(len(val), 1)

    def test_getProcessTimebase(self):
        """test getProcessTimebase"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProcess()
        self.addproductprocesslink()
        self.assertEqual(self.dbu.getProcessTimebase(1), 'DAILY')
        self.assertEqual(self.dbu.getProcessTimebase('process1'), 'DAILY')
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getProcessTimebase, 23)

    def test_getProcessID(self):
        """test getProcessID"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProcess()
        self.assertEqual(self.dbu.getProcessID('process1'), 1)

    def test_addproductprocesslink(self):
        """test addproductprocesslink"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        in_id, proc_id = self.dbu.addproductprocesslink(1, 1, False)
        self.assertEqual(in_id, 1)
        self.assertEqual(proc_id, 1)
        self.assertRaises(DBUtils2.DBError, self.addproductprocesslink)

    def addproductprocesslink(self):
        """addproductprocesslink utility"""
        self.productprocesslink = self.dbu.addproductprocesslink(1, 1, False)

    def test_addCode(self):
        """test addCode"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        code_id = self.dbu.addCode('code_filename',
                                   'code_path',
                                   datetime.datetime(2000, 1, 1),
                                   datetime.datetime(2013, 1, 1),
                                   'code description',
                                   1,
                                   Version.Version(1,0,0),
                                   True,
                                   datetime.datetime(2012, 5, 3),
                                   1,
                                   True,
                                   arguments='arguments')
        self.assertEqual(code_id, 1)
        self.assertRaises(DBUtils2.DBError, self.addCode)

    def addCode(self):
        """addCode utility"""
        self.code = self.dbu.addCode('code_filename',
                                   'code_path',
                                   datetime.datetime(2000, 1, 1),
                                   datetime.datetime(2013, 1, 1),
                                   'code description',
                                   1,
                                   Version.Version(1,0,0),
                                   True,
                                   datetime.datetime(2012, 5, 3),
                                   1,
                                   True,
                                   arguments='arguments')

    def test_codeIsActive(self):
        """ test of _codeIsActive()"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.assertTrue(self.dbu._codeIsActive(1, datetime.datetime(2001, 1, 1) ) )
        self.assertFalse(self.dbu._codeIsActive(1, datetime.datetime(1999, 1, 1) ) )
        self.assertFalse(self.dbu._codeIsActive(1, datetime.datetime(2015, 1, 1) ) )
        self.assertFalse(self.dbu._codeIsActive(1, datetime.date(1999, 1, 1) ) )
        self.assertFalse(self.dbu._codeIsActive(1, datetime.date(2015, 1, 1) ) )
        sq = self.dbu.session.query(self.dbu.Code).all()
        sq[0].active_code = False
        self.dbu._commitDB()
        self.assertFalse(self.dbu._codeIsActive(1, datetime.datetime(2001, 1, 1) ) )

    def test_getCodeArgs(self):
        """test getCodeArgs"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.assertEqual(self.dbu.getCodeArgs(1), 'arguments')
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getCodeArgs, 342243)

    def test_getProcessFromOutputProduct(self):
        """getProcessFromOutputProduct"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        val = self.dbu.getProcessFromOutputProduct(self.product)
        self.assertEqual(val, self.process)

    def test_getOutputProductFromProcess(self):
        """getOutputProductFromProcess"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        val = self.dbu.getOutputProductFromProcess(self.product)
        self.assertEqual(val, self.product)

    def test_getCodePath(self):
        """test getCodePath"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.assertEqual(self.dbu.getCodePath(1), 'rootdir/code_path/code_filename')
        self.dbu.addCode('code_filename2',
                           'code_path2',
                           datetime.datetime(2000, 1, 1),
                           datetime.datetime(2013, 1, 1),
                           'code description',
                           1,
                           Version.Version(1,0,0),
                           False,
                           datetime.datetime(2012, 5, 3),
                           1,
                           True,
                           arguments='arguments')
        self.assertTrue(self.dbu.getCodePath(2) is None)

    def test_getCodeVersion(self):
        """getCodeVersion"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        val = self.dbu.getCodeVersion(1)
        self.assertEqual(Version.Version(1,0,0), val)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getCodeVersion, 342243)

    def test_getCodeID(self):
        """test getCodeID"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.assertEqual(self.dbu.getCodeID('code_filename'), 1)

    def test_addFile(self):
        """test addFile"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        file_id = self.dbu.addFile('file_filename',
                                    0,
                                    Version.Version(1,0,0),
                                    file_create_date = datetime.datetime.utcnow(),
                                    exists_on_disk=False,
                                    utc_file_date=datetime.date.today(),
                                    utc_start_time=datetime.datetime(2012, 1, 1),
                                    utc_stop_time=datetime.datetime(2012, 1, 1),
                                    product_id = 1,
                                    newest_version=True,
                                    )
        self.assertEqual(file_id, 1)

    def addFile(self):
        """utility to addFile to the db"""
        self.file = self.dbu.addFile('file_filename',
                                    0,
                                    Version.Version(1,0,0),
                                    file_create_date = datetime.datetime.utcnow(),
                                    exists_on_disk=False,
                                    utc_file_date=datetime.date(2012, 5, 4),
                                    utc_start_time=datetime.datetime(2012, 1, 1),
                                    utc_stop_time=datetime.datetime(2012, 1, 1),
                                    product_id = 1,
                                    newest_version=True,
                                    process_keywords='process_keywords=foo',
                                    )

    def addFile2(self):
        """utility to addFile to the db"""
        self.file2 = self.dbu.addFile('file_filename2',
                                    0,
                                    Version.Version(1,0,0),
                                    file_create_date = datetime.datetime.utcnow(),
                                    exists_on_disk=False,
                                    utc_file_date=datetime.date(2012, 5, 4),
                                    utc_start_time=datetime.datetime(2012, 1, 2),
                                    utc_stop_time=datetime.datetime(2012, 1, 2),
                                    product_id = 1,
                                    newest_version=True,
                                    process_keywords='process_keywords=foo',
                                    )

    def testaddFilefilelink(self):
        """addFilefilelink should work"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.addFile2()
        ans = self.dbu.addFilefilelink(self.file, self.file2)
        self.assertEqual(ans, (2,1))
        self.dbu.delFilefilelink(2)
        self.assertEqual(2, self.dbu.session.query(self.dbu.File).count())
        self.assertEqual(0, self.dbu.session.query(self.dbu.Filefilelink).count())
        self.assertRaises(DBUtils2.DBNoData, self.dbu.delFilefilelink, 2)

    def test_getFilefilelink_byresult(self):
        """getFilefilelink_byresult"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.addFile2()
        self.dbu.addFilefilelink(self.file, self.file2)
        val = self.dbu.getFilefilelink_byresult(self.file2)
        self.assertTrue(val is None)
        val = self.dbu.getFilefilelink_byresult(self.file)
        self.assertEqual(val, (self.file2,))

    def test_getFilecodelink_byfile(self):
        """getFilecodelink_byfile"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.addFile2()
        self.dbu.addFilefilelink(self.file, self.file2)
        self.dbu.addFilecodelink(self.file, self.code)
        val = self.dbu.getFilecodelink_byfile(self.file2)
        self.assertTrue(val is None)
        val = self.dbu.getFilecodelink_byfile(self.file)
        self.assertEqual(val, self.code)

    def testaddFilecodelink(self):
        """addFilecodelink should work"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        ans = self.dbu.addFilecodelink(self.file, self.code)
        self.assertEqual(ans, (1,1))
        self.dbu.delFilecodelink(1)
        self.assertEqual(1, self.dbu.session.query(self.dbu.File).count())
        self.assertEqual(0, self.dbu.session.query(self.dbu.Filecodelink).count())
        self.assertRaises(DBUtils2.DBNoData, self.dbu.delFilecodelink, 2)

    def test_deleteAllEntries(self):
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.dbu.deleteAllEntries()
        self.assertEqual(0, self.dbu.session.query(self.dbu.File).count())
        self.assertEqual(0, self.dbu.session.query(self.dbu.Code).count())
        self.assertEqual(0, self.dbu.session.query(self.dbu.Process).count())
        self.assertEqual(0, self.dbu.session.query(self.dbu.Product).count())

    def test_purgeFileFromDB(self):
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.dbu._purgeFileFromDB(1)
        self.assertEqual(0, self.dbu.session.query(self.dbu.File).count())

    def test_getAllFilenames(self):
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.addFile2()
        expected = [(u'file_filename', u'rootdir/prod1_path/file_filename'),
                    (u'file_filename2', u'rootdir/prod1_path/file_filename2')]
        self.assertEqual(expected, self.dbu.getAllFilenames())

    def test_processqueue(self):
        """Test all the process queue functions"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.dbu.processqueuePush(1)
        self.assertEqual(1, self.dbu.processqueueLen())
        self.assertEqual(1, self.dbu.processqueueFlush())
        self.assertEqual(0, self.dbu.processqueueLen())
        self.addFile2()
        self.dbu.processqueuePush([1,2])
        self.assertEqual(None, self.dbu.processqueuePop(100))
        self.assertEqual((1,2), self.dbu.processqueueGetAll())
        self.assertEqual(2, self.dbu.processqueueLen())
        self.assertEqual(1, self.dbu.processqueueGet())
        self.assertEqual(2, self.dbu.processqueueLen())
        self.assertEqual(2, self.dbu.processqueueGet(1))
        self.assertEqual(None, self.dbu.processqueueGet(100))
        self.assertEqual(1, self.dbu.processqueuePop())
        self.assertEqual(2, self.dbu.processqueuePop(0))
        self.assertEqual(None, self.dbu.processqueuePop())
        self.assertEqual(None, self.dbu.processqueueGet())
        # test remove item
        self.dbu.processqueuePush([1,2])
        self.dbu.processqueueRemoveItem(1)
        self.assertEqual(2, self.dbu.processqueueGet())
        self.assertRaises(DBUtils2.DBNoData, self.dbu.processqueueRemoveItem, 1)
        self.dbu.processqueueRemoveItem('file_filename2')
        self.assertEqual(None, self.dbu.processqueueGet())

    def test_getFileProduct(self):
        """test getFileProduct"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFileProduct(1), 1)
        self.assertEqual(self.dbu.getFileProduct('file_filename'), 1)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getFileProduct, 5)

    def test_getFilename(self):
        """getFilename"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFilename('file_filename'), 'file_filename')
        self.assertEqual(self.dbu.getFilename(1), 'file_filename')
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getFilename, 5)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getFilename, 'nofile')

    def test_getFileID(self):
        """ test getFileID"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFileID(1), 1)
        self.assertEqual(self.dbu.getFileID('file_filename'), 1)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getFileID, 34523)
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getFileID, 'noexist')

    def test_getFileUTCfileDate(self):
        """test getFileUTCfileDate"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFileUTCfileDate(1), datetime.date(2012, 5, 4))

    def test_getFileProcess_keywords(self):
        """test getFileProcess_keywords"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFileProcess_keywords(1), 'process_keywords=foo')
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getFileID, 'noexist')

    def test_getFileDates(self):
        """getFileDates"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFileDates(1), [datetime.date(2012, 1, 1)])

    def test_getFiles_product_utc_file_date(self):
        """getFiles_product_utc_file_date"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        ans = self.dbu.getFiles_product_utc_file_date(1, datetime.date(2012, 1, 1))
        self.assertEqual(ans, [(1, Version.Version(1,0,0), 1, datetime.date(2012, 5, 4))])
        ans = self.dbu.getFiles_product_utc_file_date(1, datetime.datetime(2012, 1, 1))
        self.assertEqual(ans, [(1, Version.Version(1,0,0), 1, datetime.date(2012, 5, 4))])
        ans = self.dbu.getFiles_product_utc_file_date(1, [datetime.datetime(2012, 1, 1), datetime.datetime(2012, 1, 2)])
        self.assertEqual(ans, [(1, Version.Version(1,0,0), 1, datetime.date(2012, 5, 4))])

    def test_getFileFullPath(self):
        """getFileFullPath tests"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFileFullPath(1), 'rootdir/prod1_path/file_filename')
        self.assertEqual(self.dbu.getFileFullPath('file_filename'), 'rootdir/prod1_path/file_filename')
        self.assertRaises(DBUtils2.DBNoData, self.dbu.getFileFullPath, 3)

    def test_file_id_Clean(self):
        """file_id_Clean"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        val = self.dbu.getFiles_product_utc_file_date(1, datetime.date(2012, 1, 1))
        self.assertEqual(val, self.dbu.file_id_Clean(val))
        self.dbu.addFile('file_filename2',
                            0,
                            Version.Version(1,1,0),
                            file_create_date = datetime.datetime.utcnow(),
                            exists_on_disk=False,
                            utc_file_date=datetime.date(2012, 5, 4),
                            utc_start_time=datetime.datetime(2012, 1, 1),
                            utc_stop_time=datetime.datetime(2012, 1, 1),
                            product_id = 1,
                            newest_version=True,
                            process_keywords='process_keywords=foo',
                            )
        val = self.dbu.getFiles_product_utc_file_date(1, datetime.date(2012, 1, 1))
        self.assertEqual([(2, Version.Version(1,1,0), 1, datetime.date(2012, 5, 4))], self.dbu.file_id_Clean(val))
        val = list(reversed(self.dbu.getFiles_product_utc_file_date(1, datetime.date(2012, 1, 1))))
        self.assertEqual([(2, Version.Version(1,1,0), 1, datetime.date(2012, 5, 4))], self.dbu.file_id_Clean(val))

    def test_getFilesByProductDate(self):
        """getFilesByProductDate"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.addFile2()
        val = self.dbu.getFilesByProductDate(1, [datetime.datetime(2000, 1,1), datetime.datetime(2014, 1,1)])
        self.assertEqual(val, [])
        f1 = self.dbu.session.query(self.dbu.File).get(1)
        f2 = self.dbu.session.query(self.dbu.File).get(2)
        f1.exists_on_disk = True
        f2.exists_on_disk = True
        self.dbu._commitDB()
        val = self.dbu.getFilesByProductDate(1, [datetime.datetime(2000, 1,1), datetime.datetime(2014, 1,1)])
        self.assertEqual(val, [1,2])

    def test_getFilesByProduct(self):
        """getFilesByProduct"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.addFile2()
        val = self.dbu.getFilesByProduct(1)
        self.assertEqual([val[0].file_id,val[1].file_id], [1,2])

    def test_getFileVersion(self):
        """test getFileVersion"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFileVersion(1), Version.Version(1,0,0))
        self.assertEqual(self.dbu.getFileVersion('file_filename'), Version.Version(1,0,0))

    def test_getFileMission(self):
        """test getFileMission"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual(self.dbu.getFileMission(1).mission_id, 1)

    def test_getErrorPath(self):
        """test getErrorPath"""
        self.addMission()
        self.assertEqual(self.dbu.getErrorPath(), 'rootdir/errors/')

    def test_getIncomingPath(self):
        """test getIncomingPath"""
        self.addMission()
        self.assertEqual(self.dbu.getIncomingPath(), os.path.join('rootdir', 'incoming') + '/')

    def test_checkIncoming(self):
        """_checkIncoming"""
        tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpdir, 'incoming'))
        with open(os.path.join(tmpdir, 'incoming', 'file1'), 'w') as fp:
            fp.write('tmpfile')

        self.dbu.addMission('unittest', tmpdir)
        val = self.dbu._checkIncoming()
        self.assertEqual(val, [os.path.join(tmpdir, 'incoming', 'file1')])
        shutil.rmtree(tmpdir)

    def test_getNewestFiles(self):
        """getNewestFiles"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        val = self.dbu.getNewestFiles()
        self.assertEqual(val, (1,))
        self.addFile2()
        val = self.dbu.getNewestFiles()
        self.assertEqual(val, (1,2) )

    def test_tag_release(self):
        """tag_release"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        val = self.dbu.tag_release('rel1')
        self.assertEqual(val, 1)
        n = self.dbu.session.query(self.dbu.Release).count()
        self.assertEqual(n, 1)

        self.addFile2()
        val = self.dbu.tag_release('rel2')
        self.assertEqual(val, 2)
        n = self.dbu.session.query(self.dbu.Release).count()
        self.assertEqual(n, 3)

    def test_addRelease(self):
        """addRelease"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.addFile2()
        self.dbu.addRelease(1, 'rel1', commit=True)
        n = self.dbu.session.query(self.dbu.Release).count()
        self.assertEqual(n, 1)

        self.dbu.addRelease(2, 'rel1', commit=True)
        n = self.dbu.session.query(self.dbu.Release).count()
        self.assertEqual(n, 2)

    def test_list_release(self):
        """list_release"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.addFile2()
        self.dbu.tag_release('rel1')
        val = self.dbu.list_release('rel1')
        self.assertEqual(val, [u'rootdir/prod1_path/file_filename', u'rootdir/prod1_path/file_filename2'])
        val = self.dbu.list_release('rel1', fullpath=False)
        self.assertEqual(val, [u'file_filename', u'file_filename2'])

    def test_checkFileMD5(self):
        """checkFileMD5"""
        import Diskfile
        tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpdir, 'incoming'))
        with open(os.path.join(tmpdir, 'incoming', 'file1'), 'w') as fp:
            fp.write('tmpfile')
        sha = Diskfile.calcDigest(os.path.join(tmpdir, 'incoming', 'file1'))

        self.dbu.addMission('unittest', '/')
        self.addSatellite()
        self.addInstrument()
        self.dbu.addProduct('prod1', self.instrument, os.path.join(tmpdir, 'incoming'), None, 'format', 0)
        self.dbu.addProduct('prod2', self.instrument, os.path.join(tmpdir, 'incoming'), None, 'format', 1)
        self.dbu.addInstrumentproductlink(1, 1)
        self.dbu.addInstrumentproductlink(1, 2)

        self.addProcess()
        self.addproductprocesslink()

        self.file = self.dbu.addFile('file1',
                                    0,
                                    Version.Version(1,0,0),
                                    file_create_date = datetime.datetime.utcnow(),
                                    exists_on_disk=True,
                                    utc_file_date=datetime.date(2012, 5, 4),
                                    utc_start_time=datetime.datetime(2012, 1, 1),
                                    utc_stop_time=datetime.datetime(2012, 1, 1),
                                    product_id = 1,
                                    newest_version=True,
                                    process_keywords='process_keywords=foo',
                                    md5sum=sha,
                                    )

        self.assertTrue(self.dbu.checkFileMD5(1))
        self.assertEqual(self.dbu.checkFiles(), [])
        with open(os.path.join(tmpdir, 'incoming', 'file1'), 'a') as fp:
            fp.write('--tmpfile')
        self.assertFalse(self.dbu.checkFileMD5(1))
        self.assertEqual(self.dbu.checkFiles(), [(u'file1', '(100) bad checksum')])
        self.file = self.dbu.addFile('file2',
                                    0,
                                    Version.Version(1,0,0),
                                    file_create_date = datetime.datetime.utcnow(),
                                    exists_on_disk=True,
                                    utc_file_date=datetime.date(2012, 5, 4),
                                    utc_start_time=datetime.datetime(2012, 1, 1),
                                    utc_stop_time=datetime.datetime(2012, 1, 1),
                                    product_id = 1,
                                    newest_version=True,
                                    process_keywords='process_keywords=foo',
                                    md5sum=sha,
                                    )

        self.assertEqual(self.dbu.checkFiles(), [(u'file1', '(100) bad checksum'), (u'file2', '(200) file not found')])
        shutil.rmtree(tmpdir)


#    def test_startLogging(self):
#        """_startLogging should enter data into db"""
#        self.dbu._startLogging()
#        sq = self.dbu.session.query(self.dbu.Logging.logging_id).all() # should be just one entry
#        print "########################", sq
#        self.assertEqual(sq[0], 1)


class DBUtils2AddTests(unittest.TestCase):
    """Tests for add methods (?) of DBUtils2"""

    def setUp(self):
        super(DBUtils2AddTests, self).setUp()
        self.dbu = DBUtils2.DBUtils2(mission='unittest')
        self.dbu._openDB()
        self.dbu._createTableObjects()

    def tearDown(self):
        super(DBUtils2AddTests, self).tearDown()
        self.dbu._closeDB()
        del self.dbu

    def test_addMissionInput(self):
        """_addMission should only accept strig input"""
        self.assertRaises(ValueError, self.dbu.addMission, 1234, 12345)
        self.assertRaises(ValueError, self.dbu.addMission, 1234, 'path')
        self.assertRaises(ValueError, self. dbu.addMission, 'filename', 12345)

    def test_addMissionOrder(self):
        """_addMission wont work until the Mission class is created from the DB"""
        self.assertRaises(DBUtils2.DBError, self.dbu.addMission, 'filename', 'path')

    def test_addSatelliteInput(self):
        """_addSatellite should only accept string input"""
        self.assertRaises(ValueError, self.dbu.addSatellite, 1234)

    def test_addSatelliteOrder(self):
        """_addSatellite wont work until the Mission class is created from the DB"""
        self.assertRaises(DBUtils2.DBError, self.dbu.addSatellite, 'satname', )


class DBUtils2ClassMethodTests(unittest.TestCase):
    """Tests for class methods of DBUtils2"""

    def test_build_fname(self):
        """_build_fname should give known outout for known input"""
        dat_in =( ('/root/file/', 'relative/', 'Test', 'test1', 'Prod1', '20100614', 1, 1, 1),
                  ('/root/file/', 'relative/', 'Test', 'test1', 'Prod1', '20100614', 1, 1, 1, '.txt') )
        real_ans = ( '/root/file/relative/Test-test1_Prod1_20100614_v1.1.1.cdf',
                     '/root/file/relative/Test-test1_Prod1_20100614_v1.1.1.txt' )
        for i, val in enumerate(dat_in):
            self.assertEqual(real_ans[i], DBUtils2.DBUtils2._build_fname(*val))

    def test_test_SQLAlchemy_version(self):
        """The testing of the SQLAlchemy version should work"""
        self.assertTrue(DBUtils2.DBUtils2._test_SQLAlchemy_version())
        errstr = 'SQLAlchemy version wrong_Ver was not expected, expected 0.7.x'
        try:
            DBUtils2.DBUtils2._test_SQLAlchemy_version('wrong_Ver')
        except DBUtils2.DBError:
            self.assertEqual(sys.exc_info()[1].__str__(),
                             errstr)
        else:
            self.fail('Should have raised DBError: ' +
                      errstr)

    def test_daterange_to_dates(self):
        """daterange_to_dates"""
        daterange = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 6)]
        expected = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5), datetime.datetime(2000, 1, 6)]
        self.assertEqual(expected, DBUtils2.DBUtils2.daterange_to_dates(daterange))
        daterange = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5, 23)]
        expected = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5)]
        self.assertEqual(expected, DBUtils2.DBUtils2.daterange_to_dates(daterange))

class DBUtils2DBUseTests(unittest.TestCase):
    """Tests for DBUtils2"""

    def __init__(self, *args):
        # TODO change this to a test DB not the real one

        super(DBUtils2DBUseTests, self).__init__(*args)

    def setUp(self):
        self.dbu = DBUtils2.DBUtils2(mission='unittest')
        self.dbu._openDB()
        self.dbu._createTableObjects()
        super(DBUtils2DBUseTests, self).setUp()
        pass

    def tearDown(self):
        self.dbu._closeDB()
        del self.dbu
        super(DBUtils2DBUseTests, self).tearDown()


if __name__ == "__main__":
    unittest.main()
