#!/usr/bin/env python2.6

"""Unit tests for string handling for dbprocessing"""

__author__ = 'Jonathan Niehof <jniehof@lanl.gov>'
__version__ = '0.3pre'

import datetime
import unittest

import DBStrings


class DBFormatterTests(unittest.TestCase):
    """Tests of the revised Formatter class

    @ivar fmtr: instance of the revised formatter
    @type fmtr: DBStrings.DBFormatter
    """

    def __init__(self, *args, **kwargs):
        """Create a formatter"""
        super(DBFormatterTests, self).__init__(*args, **kwargs)
        self.fmtr = DBStrings.DBFormatter()

    def testNormalFormatting(self):
        """Format some strings that are same as normal formatter"""
        self.assertEqual('hi there',
                         self.fmtr.format('hi {a}', a='there'))
        self.assertEqual('hi there',
                         self.fmtr.format('hi {0}', 'there'))
        self.assertEqual('0003.20 hi',
                         self.fmtr.format('{1:07.2f} {0}', 'hi', 3.2))

    def testExpandFormat(self):
        """Add formatting codes to special fields"""
        self.assertEqual('{Y:04d}',
                         self.fmtr.expand_format('{Y}'))
        self.assertEqual(
            'stuff{morestuff|r:55.4f}{Y:02d}{Y:04d}',
            self.fmtr.expand_format('stuff{morestuff|r:55.4f}{Y:02d}{Y}'))

    def testExpandFormatRE(self):
        """Add formatting codes to special fields, with regex"""
        self.assertEqual('((19|2\d)\d\d)',
                         self.fmtr.expand_format('{Y}', {}))
        self.assertEqual(
            'stuff{morestuff|r:55.4f}{Y:02d}((19|2\d)\d\d){m:02d}',
            self.fmtr.expand_format('stuff{morestuff|r:55.4f}{Y:02d}{Y}{m}',
                                    {'m': 20}))
        self.assertEqual(
            'stuff(\d{{3}})([0-3]\d\d){d:2d}',
            self.fmtr.expand_format('stuff{MILLI}{j:03d}{d:2d}',
                                    {'d': 89}))

    def testRegex(self):
        """Replace special fields with regular expression"""
        self.assertEqual('stuff(\d{3})([0-3]\d\d)89',
                         self.fmtr.re('stuff{MILLI}{j:03d}{d:2d}', d=89))

    def testExpandDatetime(self):
        """Expand a single datetime object to a set of keywords"""
        dt = datetime.datetime(2010, 1, 2, 3, 44, 59, 123456)
        kwargs = {'Y': 1999, 'datetime': dt}
        self.fmtr.expand_datetime(kwargs)
        expected = {'Y': 1999, 'm': 1, 'd': 2, 'y': 10, 'j': 2,
                    'H': 3, 'M': 44, 'S': 59, 'MILLI': 123, 'MICRO': 456,
                    'datetime': dt, 'DATE': '20100102', 'b': 'Jan'}
        self.assertEqual(expected, kwargs)

    def testFormat(self):
        """Format a string from a datetime"""
        dt = datetime.datetime(2010, 1, 2, 3, 44, 59, 123456)
        self.assertEqual('2010/01/02 hi',
                         self.fmtr.format('{Y}/{m}/{d} hi', datetime=dt))

    def testAssemble(self):
        """Assemble components of a field spec"""
        self.assertEqual('{04d}', self.fmtr.assemble('', '', '04d', ''))
        self.assertEqual('stuff{name[0]!s:4.2f}',
                         self.fmtr.assemble('stuff', 'name[0]', '4.2f', 's'))
        self.assertEqual('hi', self.fmtr.assemble('hi', None, None, None))

    def testMissingKey(self):
        """Format strings with unspecified keys"""
        self.assertRaises(KeyError, self.fmtr.format,
                          '{hi} {there}', hi='hi')


if __name__ == '__main__':
    unittest.main()

