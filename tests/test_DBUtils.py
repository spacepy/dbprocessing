#!/usr/bin/env python2.6

import datetime
import os
import os.path
import shutil
import sys
import unittest
import tempfile

import CreateDB
import DBUtils
import Version


__version__ = '2.0.3'



class DBUtilsDBTests(unittest.TestCase):
    """Tests for database access through DBUtils"""

    def setUp(self):
        super(DBUtilsDBTests, self).setUp()
        db = CreateDB.dbprocessing_db(filename = ':memory:', create=True)
        self.dbu = DBUtils.DBUtils(mission='unittest', db_var=db)

    def tearDown(self):
        super(DBUtilsDBTests, self).tearDown()
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
        self.assertRaises(DBUtils.DBError, DBUtils.DBUtils, None)

    def test_repr(self):
        """__repr__ should print a known output"""
        self.dbu._openDB()
        self.assertEqual('DBProcessing class instance for mission ' + self.dbu.mission + ', version: ' + DBUtils.__version__,
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
        except DBUtils.DBError:
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
        self.assertRaises(DBUtils.DBError, self.addMission)

    def test_getMissionID(self):
        """getMissionID"""
        self.addMission()
        self.assertEqual(1, self.dbu.getMissionID(1))
        self.assertEqual(1, self.dbu.getMissionID('unittest'))
        self.assertRaises(DBUtils.DBNoData, self.dbu.getMissionID, 2345)
        self.assertRaises(DBUtils.DBNoData, self.dbu.getMissionID, 'noname')

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
        self.assertRaises(DBUtils.DBError, self.addSatellite)

    def addSatellite(self):
        """add satellite utility"""
        self.satellite = self.dbu.addSatellite('satname')

    def test_addInstrument(self):
        """testing addInstrument"""
        self.addMission()
        self.addSatellite()
        id = self.dbu.addInstrument('instname', 1)
        self.assertEqual(id, 1)
        self.assertRaises(DBUtils.DBError, self.addInstrument)

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
        self.assertRaises(DBUtils.DBNoData, self.dbu.getInstrumentID, 23462345)
        self.assertRaises(DBUtils.DBNoData, self.dbu.getInstrumentID, 'noexist')
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
        self.assertRaises(DBUtils.DBError, self.addProduct)
        # _nameSubProduct
        self.dbu.addInstrumentproductlink(1, 1)
        id = self.dbu.addProduct('prod2_{MISSION}', 1, 'prod2_path', None, 'format', 0)
        self.dbu.addInstrumentproductlink(1, 2)
        self.assertEqual('unittest_satname_instname_prod2_unittest_0_rootdir',
                         self.dbu._nameSubProduct('{MISSION}_{SATELLITE}_{INSTRUMENT}_{PRODUCT}_{LEVEL}_{ROOTDIR}', 2))
        self.assertTrue(self.dbu._nameSubProduct(None, 1) is None)

    def addProduct(self):
        """addProduct utility"""
        self.product = self.dbu.addProduct('prod1', 1, 'prod1_path', None, 'format', 0)
        self.dbu.addInstrumentproductlink(1, 1)

    def test_updateProductSubs(self):
        """updateProductSubs"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.dbu.addProduct('prod1_{MISSION}', 1, 'prod1_path_{MISSION}', None, 'format_{MISSION}', 0)
        self.dbu.addInstrumentproductlink(1,1)
        self.dbu.updateProductSubs(1)
        prod = self.dbu.getEntry('Product', 1)
        self.assertEqual(prod.product_name, 'prod1_unittest')
        self.assertEqual(prod.relative_path, 'prod1_path_unittest')
        self.assertEqual(prod.format, 'format_unittest')

    def test_addInspector(self):
        """addInspector, delInspector"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        val = self.dbu.addInspector('insp_name', 'rel_path', 'desc',
                                    Version.Version(1,0,0), True,
                                    datetime.date(2012, 2, 3), 1, True, self.product, 'args')
        self.assertEqual(val, 1)
        # getActiveInspectors
        self.assertEqual([(u'rootdir/rel_path/insp_name', u'args', 1)], self.dbu.getActiveInspectors())
        # delInspector
        self.dbu.delInspector(1)
        self.assertEqual(0, self.dbu.session.query(self.dbu.Inspector).count())
        self.assertRaises(DBUtils.DBNoData, self.dbu.delInspector, 1)

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
        self.assertRaises(DBUtils.DBNoData, self.dbu.getProductID, 23452)
        self.assertRaises(DBUtils.DBNoData, self.dbu.getProductID, 'nofile')

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
        self.addProductOutput()
        self.assertRaises(ValueError, self.dbu.addProcess, 'process1', 1, 'BADVAL', extra_params='Extra=extra', super_process_id=None)
        id = self.dbu.addProcess('process1', 1, 'DAILY', extra_params='Extra=extra', super_process_id=None)
        self.assertEqual(id, 1)
        self.assertRaises(DBUtils.DBError, self.addProcess)

    def test_nameSubProcess(self):
        # nameSubProcess
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()

        pid = self.dbu.addProcess('process1_{MISSION}', self.product, 'DAILY', extra_params='Extra=extra', super_process_id=None)
        self.dbu.addproductprocesslink(self.product, pid, False)
        self.assertEqual('unittest_satname_instname_prod1_0_rootdir',
                         self.dbu._nameSubProcess('{MISSION}_{SATELLITE}_{INSTRUMENT}_{PRODUCT}_{LEVEL}_{ROOTDIR}', 1))
        self.assertTrue(self.dbu._nameSubProcess(None, 1) is None)

    def addProcess(self):
        """addProcess utility"""
        self.process = self.dbu.addProcess('process1', self.product, 'DAILY', extra_params='Extra=extra', super_process_id=None)

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
        self.assertRaises(DBUtils.DBError, self.addproductprocesslink)

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
        self.assertRaises(DBUtils.DBError, self.addCode)

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
        self.assertRaises(DBUtils.DBNoData, self.dbu.getCodeVersion, 342243)

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
        self.assertRaises(DBUtils.DBNoData, self.dbu.getCodeID, 'badname')

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
        # nameSubFile
        self.assertEqual('unittest_satname_instname_0',
                         self.dbu._nameSubFile('{MISSION}_{SATELLITE}_{INSTRUMENT}_{LEVEL}', 1))
        self.assertTrue(self.dbu._nameSubFile(None, 1) is None)

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
                                    Version.Version(1,1,0),
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
        self.assertRaises(DBUtils.DBNoData, self.dbu.delFilefilelink, 2)

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
        self.assertRaises(DBUtils.DBNoData, self.dbu.delFilecodelink, 2)

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
        self.addFile()
        self.addFile2()
        self.dbu.addFilefilelink(1, 2)
        self.dbu.Processqueue.push(1)
        self.dbu.addFilecodelink(1, 1)
        self.dbu._purgeFileFromDB(1)
        self.assertEqual(1, self.dbu.session.query(self.dbu.File).count())

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
        self.dbu.Processqueue.push(1)
        self.assertEqual(1, self.dbu.Processqueue.len())
        self.assertEqual(1, self.dbu.Processqueue.flush())
        self.assertEqual(0, self.dbu.Processqueue.len())
        self.addFile2()
        self.dbu.Processqueue.push([1,2])
        self.assertEqual(None, self.dbu.Processqueue.pop(100))
        self.assertEqual((1,2), self.dbu.Processqueue.getAll())
        self.assertEqual(2, self.dbu.Processqueue.len())
        self.assertEqual(1, self.dbu.Processqueue.get())
        self.assertEqual(2, self.dbu.Processqueue.len())
        self.assertEqual(2, self.dbu.Processqueue.get(1))
        self.assertEqual(None, self.dbu.Processqueue.get(100))
        self.assertEqual(1, self.dbu.Processqueue.pop())
        self.assertEqual(2, self.dbu.Processqueue.pop(0))
        self.assertEqual(None, self.dbu.Processqueue.pop())
        self.assertEqual(None, self.dbu.Processqueue.get())
        # test remove item
        self.dbu.Processqueue.push([1,2])
        self.dbu.Processqueue.remove(1)
        self.assertEqual(2, self.dbu.Processqueue.get())
        self.assertRaises(DBUtils.DBNoData, self.dbu.Processqueue.remove, 1)
        self.dbu.Processqueue.remove('file_filename2')
        self.assertEqual(None, self.dbu.Processqueue.get())
        # test clean
        self.assertEqual(None, self.dbu.Processqueue.clean())
        self.dbu.Processqueue.push([1,2])
        self.dbu.Processqueue.clean()
        self.assertEqual(1, self.dbu.Processqueue.len())
        self.assertEqual(2, self.dbu.Processqueue.get())
        self.dbu.Processqueue.flush()
        self.dbu.Processqueue.push([2,1])
        self.dbu.Processqueue.clean()
        self.assertEqual(1, self.dbu.Processqueue.len())
        self.assertEqual(2, self.dbu.Processqueue.get())

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
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFileID, 34523)
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFileID, 'noexist')

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
        self.assertRaises(DBUtils.DBNoData, self.dbu.getFileFullPath, 3)

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

    def test_Logging(self):
        """Logging methods"""
        self.addMission()
        self.assertFalse(self.dbu._currentlyProcessing())
        cp = self.dbu.Logging()
        cp.currently_processing = True
        cp.mission_id = 1
        cp.processing_start_time = datetime.datetime(2012, 3, 4)
        cp.user = 'username'
        cp.hostname = 'hostname'
        cp.pid = 123
        self.dbu.session.add(cp)
        self.dbu._commitDB()
        self.assertEqual(123, self.dbu._currentlyProcessing())
        self.assertRaises(DBUtils.DBError, self.dbu._startLogging)
        cp = self.dbu.Logging()
        cp.currently_processing = True
        cp.mission_id = 1
        cp.processing_start_time = datetime.datetime(2012, 3, 4)
        cp.user = 'username'
        cp.hostname = 'hostname'
        cp.pid = 1234
        self.dbu.session.add(cp)
        self.dbu._commitDB()
        self.assertRaises(DBUtils.DBError, self.dbu._currentlyProcessing)
        self.assertRaises(ValueError, self.dbu._resetProcessingFlag)
        self.dbu._resetProcessingFlag('testing comment')
        self.assertFalse(self.dbu._currentlyProcessing())
        self.dbu._startLogging()
        self.assertEqual(os.getpid(), self.dbu._currentlyProcessing())
        self.dbu._stopLogging('stop comment')
        self.assertFalse(self.dbu._currentlyProcessing())
        self.assertRaises(DBUtils.DBProcessingError, self.dbu._stopLogging, 'stop comment')

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
        self.product = self.dbu.addProduct('prod1', self.instrument, os.path.join(tmpdir, 'incoming'), None, 'format', 0)
        self.dbu.addProduct('prod2', self.instrument, os.path.join(tmpdir, 'incoming'), None, 'format', 1)
        self.dbu.addInstrumentproductlink(1, 1)
        self.dbu.addInstrumentproductlink(1, 2)

        self.addProcess()
        self.dbu.addproductprocesslink(1, 1, False)

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
                                    shasum=sha,
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
                                    shasum=sha,
                                    )

        self.assertEqual(self.dbu.checkFiles(), [(u'file1', '(100) bad checksum'), (u'file2', '(200) file not found')])
        shutil.rmtree(tmpdir)

    def test_checkDiskForFile(self):
        """_checkDiskForFile"""
        tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpdir, 'incoming'))
        with open(os.path.join(tmpdir, 'incoming', 'file1'), 'w') as fp:
            fp.write('tmpfile')

        self.dbu.addMission('unittest', '/')
        self.addSatellite()
        self.addInstrument()
        self.product = self.dbu.addProduct('prod1', self.instrument, os.path.join(tmpdir, 'incoming'), None, 'format', 0)
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
                                    shasum=None,
                                    )

        self.assertTrue(self.dbu._checkDiskForFile(self.file))
        fle = self.dbu.getEntry('File', self.file)
        fle.exists_on_disk=False
        self.dbu.session.add(fle)
        self.dbu._commitDB()
        self.assertTrue(self.dbu._checkDiskForFile(self.file))
        shutil.rmtree(tmpdir)
        fle = self.dbu.getEntry('File', self.file)
        fle.exists_on_disk=True
        self.dbu.session.add(fle)
        self.dbu._commitDB()
        self.assertFalse(self.dbu._checkDiskForFile(self.file))
        self.assertTrue(self.dbu._checkDiskForFile(self.file, fix=True))
        fle = self.dbu.getEntry('File', self.file)
        self.assertFalse(fle.exists_on_disk)

    def test_getEntry(self):
        """getEntry"""
        self.addMission()
        self.addSatellite()
        self.addInstrument()
        self.addProduct()
        self.addProductOutput()
        self.addProcess()
        self.addCode()
        self.addFile()
        self.assertEqual('unittest', self.dbu.getEntry('Mission', 1).mission_name)
        self.assertRaises(ValueError, self.dbu.getEntry, 'BAD NAME')
        self.assertEqual('code_filename', self.dbu.getEntry('Code', 1).filename)
        self.assertRaises(DBUtils.DBNoData, self.dbu.getEntry, 'Logging', 1)
        self.dbu._startLogging()
        self.assertEqual(1, self.dbu.getEntry('Logging', 1).logging_id)
        self.assertRaises(ValueError, self.dbu.getEntry, 'Logging', 'badval')



class DBUtilsAddTests(unittest.TestCase):
    """Tests for add methods (?) of DBUtils"""
    def setUp(self):
        super(DBUtilsAddTests, self).setUp()
        db = CreateDB.dbprocessing_db(filename = ':memory:', create=True)
        self.dbu = DBUtils.DBUtils(mission='unittest', db_var=db)

    def tearDown(self):
        super(DBUtilsAddTests, self).tearDown()
        self.dbu._closeDB()
        del self.dbu

    def test_addMissionInput(self):
        """_addMission should only accept strig input"""
        self.assertRaises(ValueError, self.dbu.addMission, 1234, 12345)
        self.assertRaises(ValueError, self.dbu.addMission, 1234, 'path')
        self.assertRaises(ValueError, self. dbu.addMission, 'filename', 12345)

    def test_addSatelliteInput(self):
        """_addSatellite should only accept string input"""
        self.assertRaises(ValueError, self.dbu.addSatellite, 1234)

    def test_addSatelliteOrder(self):
        """_addSatellite wont work until the Mission class is created from the DB"""
        self.assertRaises(DBUtils.DBNoData, self.dbu.addSatellite, 'satname', )


class DBUtilsClassMethodTests(unittest.TestCase):
    """Tests for class methods of DBUtils"""

    def test_test_SQLAlchemy_version(self):
        """The testing of the SQLAlchemy version should work"""
        self.assertTrue(DBUtils.DBUtils._test_SQLAlchemy_version())
        errstr = 'SQLAlchemy version wrong_Ver was not expected, expected 0.7.x'
        try:
            DBUtils.DBUtils._test_SQLAlchemy_version('wrong_Ver')
        except DBUtils.DBError:
            self.assertEqual(sys.exc_info()[1].__str__(),
                             errstr)
        else:
            self.fail('Should have raised DBError: ' +
                      errstr)

    def test_daterange_to_dates(self):
        """daterange_to_dates"""
        daterange = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 6)]
        expected = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5), datetime.datetime(2000, 1, 6)]
        self.assertEqual(expected, DBUtils.DBUtils.daterange_to_dates(daterange))
        daterange = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5, 23)]
        expected = [datetime.datetime(2000, 1, 4), datetime.datetime(2000, 1, 5)]
        self.assertEqual(expected, DBUtils.DBUtils.daterange_to_dates(daterange))

    def test_processRunning(self):
        """processRunning"""
        self.assertTrue(DBUtils.DBUtils.processRunning(os.getpid()))
        self.assertFalse(DBUtils.DBUtils.processRunning(32768)) # one more than allowed on ect-soc-s1 (/proc/sys/kernel/pid_max)


class DBUtilsDBUseTests(unittest.TestCase):
    """Tests for DBUtils"""

    def __init__(self, *args):
        # TODO change this to a test DB not the real one

        super(DBUtilsDBUseTests, self).__init__(*args)

    def setUp(self):
        self.dbu = DBUtils.DBUtils(mission='unittest')
        self.dbu._openDB()
        self.dbu._createTableObjects()
        super(DBUtilsDBUseTests, self).setUp()
        pass

    def tearDown(self):
        self.dbu._closeDB()
        del self.dbu
        super(DBUtilsDBUseTests, self).tearDown()


if __name__ == "__main__":
    unittest.main()
