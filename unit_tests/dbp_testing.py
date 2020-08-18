#!/usr/bin/env python

"""Helper functions for dbprocessing unit tests

Simply importing this will redirect the logs, so import before importing
any dbprocessing modules.
"""

import datetime
import os
import os.path
import sys

#The log is opened on import, so need to quarantine the log directory
#right away (before other dbp imports)
os.environ['DBPROCESSING_LOG_DIR'] = os.path.join(os.path.dirname(__file__),
                                                  'unittestlogs')

import dbprocessing.Version

__all__ = ['AddtoDBMixin', 'add_scripts_to_path', 'testsdir']


testsdir = os.path.dirname(__file__)


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
                utc_start=None, utc_stop=None):
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
            exists_on_disk=True,
        )
        return fid

def add_scripts_to_path():
    """Add the script source directory to Python path

    This allows unit testing of scripts.
    """
    scriptpath = os.path.abspath(os.path.join(
        testsdir, '..', 'scripts'))
    if not scriptpath in sys.path and os.path.isdir(scriptpath):
        sys.path.insert(0, scriptpath)
