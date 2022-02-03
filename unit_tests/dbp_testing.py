#!/usr/bin/env python

"""Helper functions for dbprocessing unit tests

Simply importing this will redirect the logs and change pathing, so import
before importing any dbprocessing modules.
"""

import datetime
import json
import os
import os.path
import shutil
import sys
import sysconfig
import tempfile

import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.schema


#The log is opened on import, so need to quarantine the log directory
#right away (before other dbp imports)
os.environ['DBPROCESSING_LOG_DIR'] = os.path.join(os.path.dirname(__file__),
                                                  'unittestlogs')


testsdir = os.path.dirname(__file__)  # Used by add_build_to_path


# Define this before using it for importing dbprocessing modules...
def add_build_to_path():
    """Adds the python build directory to the search path.

    Locates the build directory in the same repository as this test module
    and adds the (version-specific) library directories to the Python
    module search path, so the unit tests can be run against the built
    instead of installed version.

    This is run on import of this module.
    """
    # Prioritize version-specific path; py2 tends to be version-specific
    # and py3 tends to use just "lib". But only use first-matching.
    for pth in ('lib',  # Prepending, so add low-priority paths first.
                'lib.{0}-{1}.{2}'.format(sysconfig.get_platform(),
                                         *sys.version_info[:2]),
                ):
        buildpath = os.path.abspath(os.path.join(testsdir, '..', 'build', pth))
        if os.path.isdir(buildpath):
            if not buildpath in sys.path:
                sys.path.insert(0, buildpath)
            break


# Get the "build" version of dbp for definitions in this module.
add_build_to_path()
import dbprocessing.DButils
import dbprocessing.tables
import dbprocessing.Version


__all__ = ['AddtoDBMixin', 'add_build_to_path', 'add_scripts_to_path',
           'driveroot', 'testsdir']


driveroot = os.path.join(os.path.splitdrive(os.getcwd())[0], os.path.sep)\
            if sys.platform == 'win32' else os.path.sep
"""Root of the current drive (or filesystem)"""

class AddtoDBMixin(object):
    """Mixin class providing helper functions for adding to database

    Useful for testing when db tables need to be populated with some
    simplified parameters and/or defaults.

    Assumes the existence of a ``dbu`` data member, which is a DBUtils
    instance to be used for adding to a database.
    """

    def addProduct(self, product_name, instrument_id=None, level=0,
                   format=None):
        """Add a product to database (incl. inspector)

        Won't actually work, just getting the record in
        """
        if instrument_id is None:
            ids = self.dbu.session.query(self.dbu.Instrument).all()
            instrument_id = ids[0].instrument_id
        if format is None:
            format = product_name.replace(' ', '_') + '_{Y}{m}{d}_v{VERSION}'
        pid = self.dbu.addProduct(
            product_name=product_name,
            instrument_id=instrument_id,
            relative_path='junk',
            format=format,
            level=level,
            product_description='Test product {}'.format(product_name)
            )
        self.dbu.addInstrumentproductlink(instrument_id, pid)
        self.dbu.addInspector(
            filename='fake.py',
            relative_path='inspectors',
            description='{} inspector'.format(product_name),
            version=dbprocessing.Version.Version(1, 0, 0),
            active_code=True,
            date_written='2010-01-01',
            output_interface_version=1,
            newest_version=True,
            product=pid,
            arguments="foo=bar")
        return pid

    def addProcess(self, process_name, output_product_id,
                   output_timebase='DAILY'):
        """Add a process + code record to the database

        Again, just the minimum to get the records in
        """
        process_id = self.dbu.addProcess(
            process_name,
            output_product=output_product_id,
            output_timebase=output_timebase)
        code_id = self.dbu.addCode(
            filename='junk.py',
            relative_path='scripts',
            code_start_date='2010-01-01',
            code_stop_date='2099-01-01',
            code_description='{} code'.format(process_name),
            process_id=process_id,
            version='1.0.0',
            active_code=1,
            date_written='2010-01-01',
            output_interface_version=1,
            newest_version=1,
            arguments=process_name.replace(' ', '_') + '_args')
        return process_id, code_id

    def addProductProcessLink(self, product_id, process_id, optional=False,
                              yesterday=0, tomorrow=0):
        """Minimum record for product-process link in db"""
        self.dbu.addproductprocesslink(product_id, process_id, optional,
                                       yesterday, tomorrow)

    def addFile(self, filename, product_id, utc_date=None, version=None,
                utc_start=None, utc_stop=None, exists=True):
        """Add a file to the database"""
        if utc_date is None:
            utc_date = datetime.datetime.strptime(
                filename.split('_')[-2], '%Y%m%d')
        if version is None:
            version = filename.split('_v')[-1]
            while version.count('.') > 2:
                version = version[:version.rfind('.')]
        level = self.dbu.getEntry('Product', product_id).level
        if utc_start is None:
            utc_start = utc_date.replace(
                hour=0, minute=0, second=0, microsecond=0)
        if utc_stop is None:
            utc_stop = utc_date.replace(
                hour=23, minute=59, second=59, microsecond=999999)
        fid = self.dbu.addFile(
            filename=filename,
            data_level=level,
            version=dbprocessing.Version.Version.fromString(version),
            product_id=product_id,
            utc_file_date=utc_date,
            utc_start_time=utc_start,
            utc_stop_time=utc_stop,
            file_create_date=datetime.datetime.now(),
            exists_on_disk=exists,
        )
        return fid

    def addSkeletonMission(self):
        """Starting with empty database, add a skeleton mission

        Should be called before opening a DButils instance so that
        the mission, etc. tables can be created.

        Makes one mission, one satellite, and two instruments

        Assumes self.td has the test/temp directory path (str),
        and self.dbname has the name of the database (including full
        path if sqlite).
        Will populate self.instrument_ids for a list of instruments.
        """
        dbu = dbprocessing.DButils.DButils(self.dbname)
        if dbu.session.query(sqlalchemy.func.count(
                dbu.Mission.mission_id)).scalar():
            raise RuntimeError('Unit test database is not empty!')
        mission_id = dbu.addMission(
            'Test mission',
            os.path.join(self.td, 'data'),
            os.path.join(self.td, 'incoming'),
            os.path.join(self.td, 'codes'),
            os.path.join(self.td, 'inspectors'),
            os.path.join(self.td, 'errors'))
        satellite_id = dbu.addSatellite('Satellite', mission_id)
        # Make two instruments (so can test interactions between them)
        self.instrument_ids = [
            dbu.addInstrument(instrument_name='Instrument {}'.format(i),
                              satellite_id=satellite_id)
            for i in range(1, 3)]
        del dbu

    def makeTestDB(self):
        """Create a test database and working directory

        Creates three attributes:
            * self.td: a temporary directory
            * self.pg: is the database postgres (if False, sqlite)
            * self.dbname: name of database (sqlite path or postgresql db)

        Does not open the database
        """
        self.td = tempfile.mkdtemp()
        self.pg = 'PGDATABASE' in os.environ
        self.dbname = os.environ['PGDATABASE'] if self.pg\
            else os.path.join(self.td, 'testDB.sqlite')
        dbprocessing.DButils.create_tables(
            self.dbname, dialect = 'postgresql' if self.pg else 'sqlite')

    def removeTestDB(self):
        """Remove test database and working directory

        Assumes has a working (open) DBUtils instance in self.dbu, which
        will be closed.
        """
        if self.pg:
            self.dbu.session.close()
            self.dbu.metadata.drop_all()
        self.dbu.closeDB() # Before the database is removed...
        del self.dbu
        shutil.rmtree(self.td)

    def loadData(self, filename):
        """Load data into db from a JSON file

        Assumes existence of:
            * self.dbname: name of database (sqlite path or postgresql db)
              Must exist, with tables.
            * self.pg: if database is postgresql

        Creates:
            * self.dbu: open DButils instance

        Parameters
        ----------
        filename : :class:`str`
            Full path to the JSON file
        """
        with open(filename, 'rt') as f:
            data = json.load(f)
        self.dbu = dbprocessing.DButils.DButils(self.dbname)
        for k, v in data.items():
            for row in v:
                for column in row:
                    if column in ('code_start_date',
                                  'code_stop_date',
                                  'date_written',
                                  'utc_file_date',
                    ):
                        row[column] \
                            = None if row[column] is None\
                            else datetime.datetime.strptime(
                                    row[column], '%Y-%m-%dT%H:%M:%S.%f').date()
                    elif column in ('utc_start_time',
                                    'utc_stop_time',
                                    'check_date',
                                    'file_create_date',
                                    'processing_start_time',
                                    'processing_end_time',
                    ):
                        row[column] \
                            = None if row[column] is None\
                            else datetime.datetime.strptime(
                                    row[column], '%Y-%m-%dT%H:%M:%S.%f')
        if 'unixtime' not in data:
            # Dump from old database w/o the Unixtime table
            insp = sqlalchemy.inspect(self.dbu.Unixtime)
            # persist_selectable added 1.3 (mapped_table deprecated)
            tbl = insp.persist_selectable\
                  if hasattr(insp, 'persist_selectable') else insp.mapped_table
            tbl.drop()
            self.dbu.metadata.remove(tbl)
            del self.dbu.Unixtime
        if data['productprocesslink']\
           and 'yesterday' not in data['productprocesslink'][0]:
            # Dump from old database w/o yesterday/tomorrow,
            # set defaults.
            for row in data['productprocesslink']:
                row['yesterday'] = row['tomorrow'] = 0
        for t in dbprocessing.tables.names:
            if t not in data or not data[t]:
                # Data not in dump, nothing to insert
                continue
            insp = sqlalchemy.inspect(getattr(self.dbu, t.title()))
            table = insp.persist_selectable\
                  if hasattr(insp, 'persist_selectable') else insp.mapped_table
            ins = table.insert()
            self.dbu.session.execute(ins, data[t])
            idcolumn = '{}_id'.format(t)
            if self.pg and idcolumn in data[t][0]:
                maxid = max(row[idcolumn] for row in data[t])
                sel = "SELECT pg_catalog.setval(pg_get_serial_sequence("\
                      "'{table}', '{column}'), {maxid})".format(
                          table=t, column=idcolumn, maxid=maxid)
                self.dbu.session.execute(sel)
        self.dbu.commitDB()
        # Re-reference directories since new data loaded
        self.dbu.MissionDirectory = self.dbu.getMissionDirectory()
        self.dbu.CodeDirectory = self.dbu.getCodeDirectory()
        self.dbu.InspectorDirectory = self.dbu.getInspectorDirectory()


def add_scripts_to_path():
    """Add the script build directory to Python path

    This allows unit testing of scripts.
    """
    scriptpath = os.path.abspath(os.path.join(
        testsdir, '..', 'build', 'scripts-{}.{}'.format(*sys.version_info[:2])))
    if not scriptpath in sys.path and os.path.isdir(scriptpath):
        sys.path.insert(0, scriptpath)
