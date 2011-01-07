#!/usr/bin/env python2.6

"""Unit tests for string handling for dbprocessing"""

__author__ = 'Jonathan Niehof <jniehof@lanl.gov>'
__version__ = '0.1'

import datetime
import unittest

import DBStrings


class XFormatterTests(unittest.TestCase):
    """Test of the extended Formatter"""

    def testAssemble(self):
        """Assemble components of a field spec"""
        self.assertEqual(
            '{04d}',
            DBStrings.XFormatter.assemble('', '', '04d', ''))
        self.assertEqual(
            'stuff{name[0]!s:4.2f}',
            DBStrings.XFormatter.assemble('stuff', 'name[0]', '4.2f', 's'))

    def testParseFormat(self):
        """Parse components of a format string"""
        #There should be more tests here if this is ever used
        fmtr = DBStrings.XFormatter()
        inputs = ['04d',
                  ]
        outputs = [(None, None, None, None, True, 4, None, 'd'),
                   ]
        for (inp, outp) in zip(inputs, outputs):
            self.assertEqual(outp, fmtr.parse_format(inp))


class UnfoundFieldTests(unittest.TestCase):
    """Tests of the UnfoundField object"""

    def testSetKey(self):
        """Record key name on not-found object"""
        a = DBStrings._UnfoundField('key')
        self.assertEqual('key', a.key)

    def testGetattr(self):
        """Record an attribute lookup on the not-found object"""
        a = DBStrings._UnfoundField('key')
        b = a.attr
        self.assertEqual(a, b)
        self.assertEqual(a.lookups, '.attr')

    def testGetItem(self):
        """Record an item lookup on the not-found object"""
        a = DBStrings._UnfoundField('key')
        b = a['keyname']
        self.assertEqual(a, b)
        self.assertEqual(a.lookups, "['keyname']")
        c = b[0]
        self.assertEqual(a, c)
        self.assertEqual(a.lookups, "['keyname'][0]")

    def testFieldSpec(self):
        """Reproduce original spec of the field"""
        a = DBStrings._UnfoundField('key')
        b = a['keyname']
        c = b.attribute
        d = c[0]
        a.conversion = 's'
        self.assertEqual("{key['keyname'].attribute[0]!s:04d}",
                         a.field_spec('04d'))

class QuietFormatterTests(unittest.TestCase):
    """Tests of the QuietFormatter"""

    def __init__(self, *args, **kwargs):
        """Create a formatter"""
        super(QuietFormatterTests, self).__init__(*args, **kwargs)
        self.fmtr = DBStrings.QuietFormatter()

    def testNormalFormatting(self):
        """Format some strings that are same as normal formatter"""
        self.assertEqual('hi there',
                         self.fmtr.format('hi {a}', a='there'))
        self.assertEqual('hi there',
                         self.fmtr.format('hi {0}', 'there'))
        self.assertEqual('0003.20 hi',
                         self.fmtr.format('{1:07.2f} {0}', 'hi', 3.2))

    def testMissingKey(self):
        """Format strings with unspecified keys"""
        self.assertEqual('hi {there}',
                         self.fmtr.format('{hi} {there}', hi='hi'))
        self.assertEqual(
            'hi {there.here!s:100s}',
            self.fmtr.format('{hi} {there.here!s:100s}', hi='hi'))


class DBFormatterTests(unittest.TestCase):
    """Tests of the revised Formatter class

    @ivar fmtr: instance of the revised formatter
    @type fmtr: DBStrings.DBFormatter
    """

    def __init__(self, *args, **kwargs):
        """Create a formatter"""
        super(DBFormatterTests, self).__init__(*args, **kwargs)
        self.fmtr = DBStrings.DBFormatter()

    def testExpandFormat(self):
        """Add formatting codes to special fields"""
        self.assertEqual('{Y:04d}',
                         self.fmtr.expand_format('{Y}'))
        self.assertEqual(
            'stuff{morestuff|r:55.4f}{Y:02d}{Y:04d}',
            self.fmtr.expand_format('stuff{morestuff|r:55.4f}{Y:02d}{Y}'))

    def testRegex(self):
        """Replace special fields with regular expression"""
        self.assertEqual('stuff(\d{3})([0-3]\d\d){d:2d}',
                         self.fmtr.regex('stuff{MILLI}{j:03d}{d:2d}'))

    def testExpandDatetime(self):
        """Expand a single datetime object to a set of keywords"""
        dt = datetime.datetime(2010, 1, 2, 3, 44, 59, 123456)
        kwargs = {'Y': 1999, 'datetime': dt}
        self.fmtr.expand_datetime(kwargs)
        expected = {'Y': 1999, 'm': 1, 'd': 2, 'y': 10, 'j': 2,
                    'H': 3, 'M': 44, 'S': 59, 'MILLI': 123, 'MICRO': 456,
                    'datetime': dt}
        self.assertEqual(expected, kwargs)

    def testFormat(self):
        """Format a string from a datetime"""
        dt = datetime.datetime(2010, 1, 2, 3, 44, 59, 123456)
        self.assertEqual('2010/1/2 hi',
                         self.fmtr.format('{Y}/{m}/{d} hi', datetime=dt))


if __name__ == '__main__':
    unittest.main()

