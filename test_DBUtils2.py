#!/usr/bin/env python2.6

import datetime
import os
import os.path
import sys
import unittest

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
            self.dbu.Data_files
        except AttributeError:
            self.fail('Class not created')

    def test_addMission(self):
        """add a mission to the DB"""
        m = self.dbu.addMission('unittest', 'rootdir')
        self.assertEqual(m, 1)
        self.assertEqual(self.dbu._getMissionID(), m)
        self.assertEqual(self.dbu._getMissionDirectory(), 'rootdir')
        self.assertEqual(self.dbu._getMissionName(), 'unittest')
        self.assertRaises(DBUtils2.DBError, self.addMission)

    def addMission(self):
        """utility to add a mission"""
        self.mission = self.dbu.addMission('unittest', 'rootdir')

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

    def test_addProcessInput(self):
        """_addProcess should only accept correct input"""
        self.assertRaises(ValueError, self.dbu.addProcess, 1234, 'id string', 123, 'string')
        self.assertRaises(ValueError, self.dbu.addProcess, 'process', 1234, 1234)
        self.assertRaises(ValueError, self.dbu.addProcess, 'process', 1234, 'string', 'string')
        self.assertRaises(ValueError, self.dbu.addProcess, 'process', 1234, None, 'string')

    def test_addProcessOrder(self):
        """_addProcess wont work until the Process class is created from the DB"""
        self.assertRaises(TypeError, self.dbu.addProcess, 'instname', 1234)
        self.assertRaises(ValueError, self.dbu.addProcess, 'instname', 1234, 'string')

    def test_addCodeOrder(self):
        """_addCode wont work until the Code class is created from the DB"""
        ver =Version.Version(1, 0, 0)
        startt = datetime.datetime(2010, 4, 5)
        stopt = datetime.datetime(2010, 4, 7)
        writet = datetime.datetime(2010, 6, 7)
        self.assertRaises(DBUtils2.DBError, self.dbu.addCode, 'string',
                          'string', startt, stopt, 'string', 1234, ver,
                          False, startt, 1234, True)

    def test_code_dats(self):
        """Stop date must be after start date"""
        startt = datetime.datetime(2010, 4, 5)
        stopt = datetime.datetime(2010, 4, 7)
        ver =Version.Version(1, 0, 0)
        self.assertRaises(DBUtils2.DBError, self.dbu.addCode, 'string', 'string', stopt, startt, 'string', 1234, ver, False, startt, 1, True)

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
