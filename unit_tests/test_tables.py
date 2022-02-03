#!/usr/bin/env python
"""Unit test of table definitions

This is a few quick checks and particularly examining tricky stuff; testing
all the tables amounts to a check that SQLalchemy works rather than that
our definitions work.
"""

import os
import os.path
import shutil
import sqlite3
import tempfile
import unittest

import sqlalchemy
import sqlalchemy.inspection

import dbp_testing

import dbprocessing.tables


class TableDefnTests(unittest.TestCase):
    """Test table definitions"""

    def setUp(self):
        """Create the test database"""
        self.td = tempfile.mkdtemp()
        self.engine = sqlalchemy.create_engine(
            'sqlite:///{}'.format(os.path.join(self.td, 'test.sqlite')),
            echo=False)
        self.metadata = sqlalchemy.schema.MetaData(bind=self.engine)

    def tearDown(self):
        """Delete test database"""
        self.engine.dispose()
        shutil.rmtree(self.td)

    def makeTables(self, *tables):
        """Helper functions, makes all tables named in args, in that order

        Parameters
        ----------
        tables : list of str
            names of tables to make

        Returns
        -------
        dict
            Created table objects, keyed by name
        """
        created = {
            name: sqlalchemy.schema.Table(
                name, self.metadata, *dbprocessing.tables.definition(name))
            for name in tables}
        self.metadata.create_all()
        actual = sqlalchemy.inspection.inspect(self.engine)\
                 .get_table_names()
        self.assertEqual(sorted(tables), sorted(actual))
        return created

    def testFile(self):
        """Test file table definition"""
        # file requires product requires instrument requires satellite
        # requires mission
        t = self.makeTables(
            'file', 'product', 'instrument', 'satellite', 'mission')['file']
        # Check that got desired columns (in order)
        self.assertEqual(
            ['file_id', 'filename',
             'utc_file_date', 'utc_start_time', 'utc_stop_time',
             'data_level',
             'interface_version', 'quality_version', 'revision_version',
             'verbose_provenance', 'check_date', 'quality_comment', 'caveats',
             'file_create_date',
             'met_start_time', 'met_stop_time', 'exists_on_disk',
             'quality_checked', 'product_id', 'shasum', 'process_keywords'],
            [c.name for c in t.columns])
        # Check all indices in place
        self.assertEqual(
            ['ix_file_big',
             'ix_file_data_level',
             'ix_file_file_id',
             'ix_file_filename',
             'ix_file_utc_file_date',
             'ix_file_utc_start_time',
             'ix_file_utc_stop_time'],
            sorted([i.name for i in t.indexes]))


if __name__ == "__main__":
    unittest.main()
