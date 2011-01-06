#!/usr/bin/env python2.6

"""Unit tests for string handling for dbprocessing"""

__author__ = 'Jonathan Niehof <jniehof@lanl.gov>'
__version__ = '0.0'

import unittest

import DBStrings


class DBFormatterTests(unittest.TestCase):
    """Tests of the revised Formatter class

    @ivar fmtr: instance of the revised formatter
    @type fmtr: DBStrings.DBFormatter
    """

    def __init__(self, *args, **kwargs):
        """Create a formatter object"""
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

    def testMissingKey(self):
        """Format strings with unspecified keys"""
        self.assertEqual('hi {there}',
                         self.fmtr.format('{hi} {there}', hi='hi'))


if __name__ == '__main__':
    unittest.main()

