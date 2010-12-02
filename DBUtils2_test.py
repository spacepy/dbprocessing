#!/usr/bin/env python2.6

import unittest
import DBUtils2
import sys
import Version
import datetime
import os
import os.path

__version__ = '2.0.1'



class AddTests(unittest.TestCase):
    def setUp(self):
        super(AddTests, self).setUp()
        self.dbu = DBUtils2.DBUtils2()
        self.dbu._openDB('Test')
        self.dbu._createTableObjects()



    def tearDown(self):
        super(AddTests, self).tearDown()
        self.dbu._closeDB()
        del self.dbu





    def test_addMissionInput(self):
        """_addMission should only accept strig input"""
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addMission, 1234, 12345)
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addMission, 1234, 'path')
        self.assertRaises(DBUtils2.DBInputError,self. dbu._addMission, 'filename', 12345)

    def test_addMissionOrder(self):
        """_addMission wont work until the Mission class is created from the DB"""
        self.assertRaises(DBUtils2.DBError, self.dbu._addMission, 'filename', 'path')


    def test_addSatelliteInput(self):
        """_addSatellite should only accept strig input"""
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addSatellite, 1234, 'id string')
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addSatellite, 1234, 1234)
        self.assertRaises(DBUtils2.DBInputError,self. dbu._addSatellite, 'filename', 'id string')

    def test_addSatelliteOrder(self):
        """_addSatellite wont work until the Mission class is created from the DB"""
        self.assertRaises(DBUtils2.DBError, self.dbu._addSatellite, 'satname', 1234)


    def test_addInstrumentInput(self):
        """_addInstrument should only accept strig input"""
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addInstrument, 1234, 'id string')
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addInstrument, 1234, 1234)
        self.assertRaises(DBUtils2.DBInputError,self. dbu._addInstrument, 'instname', 'id string')

    def test_addInstrumentOrder(self):
        """_addInstrument wont work until the Mission class is created from the DB"""
        self.assertRaises(DBUtils2.DBError, self.dbu._addInstrument, 'instname', 1234)

    def test_addProcessInput(self):
        """_addProcess should only accept correct input"""
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addProcess, 1234, 'id string', 123, 'string')
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addProcess, 'process', 1234, 1234)
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addProcess, 'process', 1234, 'string', 'string')
        self.assertRaises(DBUtils2.DBInputError, self.dbu._addProcess, 'process', 1234, None, 'string')

    def test_addProcessOrder(self):
        """_addProcess wont work until the Process class is created from the DB"""
        self.assertRaises(DBUtils2.DBError, self.dbu._addProcess, 'instname', 1234)
        self.assertRaises(DBUtils2.DBError, self.dbu._addProcess, 'instname', 1234, 'string')
        self.assertRaises(DBUtils2.DBError, self.dbu._addProcess, 'instname', 1234, 'string', 1234)

    ## def test_addProductOrder(self):
    ##     """_addProduct wont work until the Product class is created from the DB"""
    ##     self.assertRaises(DBUtils2.DBError, self.dbu._addProduct, 'pname' , 11, 'rel_path', 1234, 'path')

    def test_addCodeOrder(self):
        """_addCode wont work until the Code class is created from the DB"""
        ver =Version.Version(1,0,0)
        startt = datetime.datetime(2010, 4, 5)
        stopt = datetime.datetime(2010, 4, 7)
        writet = datetime.datetime(2010, 6, 7)
        self.assertRaises(DBUtils2.DBError, self.dbu._addCode, 'string', 'string', startt, stopt, 'string', 1234, ver, False, startt, 1234, True)

    def test_code_dats(self):
        """Stop date must be after start date"""
        startt = datetime.datetime(2010, 4, 5)
        stopt = datetime.datetime(2010, 4, 7)
        ver =Version.Version(1,0,0)
        self.assertRaises(DBUtils2.DBError, self.dbu._addCode, 'string', 'string', stopt, startt, 'string', 1234, ver, False, startt, 1, True)

class ClassMethodTests(unittest.TestCase):

    def setUp(self):
        #        super(PickleTests, self).setUp()
        pass


    def tearDown(self):
        # super(PickleTests, self).tearDown()
        pass

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
        errstr = 'SQLAlchemy version wrong_Ver was not expected, expected 0.6.2'
        try:
            DBUtils2.DBUtils2._test_SQLAlchemy_version('wrong_Ver')
        except DBUtils2.DBError:
            self.assertEqual(sys.exc_info()[1].__str__(),
                             errstr)
        else:
            self.fail('Should have raised DBError: ' +
                      errstr)


class DBTests(unittest.TestCase):
    def __init__(self, *args):
        self.dbp = DBUtils2.DBUtils2('unittest')
        super(DBTests, self).__init__(*args)


    def setUp(self):
        #        super(PickleTests, self).setUp()
        pass

    def tearDown(self):
        # super(PickleTests, self).tearDown()
        pass

    def test_opencloseDB(self):
        """_open should open a db in memory and _close should close it"""
        self.dbp._openDB()
        try:
            self.dbp.engine
        except:
            self.fail("engine was not created")
        try:
            self.dbp.metadata
        except:
            self.fail("Metadata not created")
        try:
            self.dbp.session
        except:
            self.fail("Session not created")
        ## should think up a better test here
        self.dbp._closeDB()


    def test_repr(self):
        """__repr__ should print a known output"""
        self.dbp._openDB()
        self.assertEqual('DBProcessing class instance for mission ' + self.dbp.mission + ', version: ' + DBUtils2.__version__,
                         self.dbp.__repr__())

    def test_createTableObjects(self):
        """_createTableObjects should do just that"""
        from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, BigInteger, Date, DateTime, BigInteger, Boolean
        self.dbp._openDB()
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
        metadata.create_all(self.dbp.engine)
        ## now the actual test
        try:
            self.dbp._createTableObjects()
        except DBError:
            self.fail('Error is setting up table->class mapping')
        try:
            self.dbp.Data_files
        except AttributeError:
            self.fail('Class not created')

    def test_createViews(self):
        """_createViews should do just that"""
        # TODO define a test here, not sure how to crate views from within SQLAlchemy
        pass

class DBUseTests(unittest.TestCase):
    def __init__(self, *args):
        # TODO change this to a test DB not the real one

        super(DBUseTests, self).__init__(*args)


    def setUp(self):
        self.dbu = DBUtils2.DBUtils2()
        self.dbu._openDB()
        self.dbu._createTableObjects()
        super(DBUseTests, self).setUp()
        pass

    def tearDown(self):
        self.dbu._closeDB()
        del self.dbu
        super(DBUseTests, self).tearDown()

    def test_getMissionDirectory(self):
        """ _getMissionDirectory should return known ouput for a known mission"""
        self.assertEqual(u'/n/projects/cda/Test', self.dbu._getMissionDirectory())

    def test_checkIncomming(self):
        """ adding a file to incoming should be found by this method """
        fd = open(self.dbu._getMissionDirectory() + '/incoming/unittest.tst', 'w')
        fd.write('Test')
        fd.close()
        self.assertTrue(self.dbu._getMissionDirectory() + '/incoming/unittest.tst' in self.dbu._checkIncoming())
        os.remove(self.dbu._getMissionDirectory() + '/incoming/unittest.tst')




#    def test_addDataFile(self):
#        """_addDataFile should add a file entry to the db"""
#        import datetime
#        in_val = ('Filename',
#                  datetime.date(2010, 4, 5),
#                  datetime.datetime(2010,4,5),
#                  datetime.datetime(2010,4,5, 23, 59, 59),
#                  0,
#                  None,
#                  1,
#                  None,
#                  False,
#                  None,
#                  None,
#                  None,
#                  1,
#                  1,
#                  1,
#                  datetime.datetime(2010,7,5, 23, 59, 59),
#                  4,
#                  None,
#                  None,
#                  True,
#                  'File'
#                  )
#        self.assertTrue(self.dbp._addDataFile(*in_val))
#


if __name__ == "__main__":
    unittest.main()






