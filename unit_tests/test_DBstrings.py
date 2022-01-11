#!/usr/bin/env python
from __future__ import print_function

"""Unit tests for string handling for dbprocessing"""

__author__ = 'Jonathan Niehof <jniehof@lanl.gov>'

import datetime
import re
import unittest

import dbp_testing
from dbprocessing import DBstrings


class DBFormatterTests(unittest.TestCase):
    """Tests of the revised Formatter class

    @ivar fmtr: instance of the revised formatter
    @type fmtr: DBstrings.DBformatter
    """

    def __init__(self, *args, **kwargs):
        """Create a formatter"""
        super(DBFormatterTests, self).__init__(*args, **kwargs)
        self.fmtr = DBstrings.DBformatter()

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

    def testDatetimeRe(self):
        """Expand a simple datetime reference to regex"""
        self.assertEqual('stuff((19|2\d)\d\d(0\d|1[0-2])[0-3]\d)',
                         self.fmtr.re('stuff{datetime}'))

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

    def testHopeRegressExpandDatetime(self):
        """Check on the datetime expansion for a simple HOPE regression"""
        fmtstring = 'rbspa_ect-hope-hk-L05_0095_v{VERSION}.cdf'
        fmtkeywords = {'INSTRUMENT': 'hope', 'SATELLITE': 'rbspa',
                       'VERSION': '2.0.0', 'PRODUCT': 'rbsp_ect-hope-hk-L05',
                       'datetime': datetime.date(2012, 12, 2)}
        self.fmtr.expand_datetime(fmtkeywords)
        self.assertEqual({'DATE': '20121202',
                          'INSTRUMENT': 'hope',
                          'PRODUCT': 'rbsp_ect-hope-hk-L05',
                          'SATELLITE': 'rbspa',
                          'VERSION': '2.0.0',
                          'Y': 2012,
                          'b': 'Dec',
                          'd': 2,
                          'datetime': datetime.date(2012, 12, 2),
                          'j': 337,
                          'm': 12,
                          'y': 12},
                         fmtkeywords)

    def testHopeRegressExpandFormat(self):
        """Check on the basic format expansion for a simple HOPE regression"""
        self.assertEqual('rbspa_ect-hope-hk-L05_0095_v{VERSION}.cdf',
                         self.fmtr.expand_format(
                             'rbspa_ect-hope-hk-L05_0095_v{VERSION}.cdf'))

    def testHopeRegressions(self):
        """Use input/outputs from HOPE processing to catch regressions"""
        #each tuple of (format string, kwargs dict)
        inputs = [
            ('rbspa_ect-hope-hk-L05_0095_v{VERSION}.cdf',
             {'INSTRUMENT': 'hope', 'SATELLITE': 'rbspa',
              'VERSION': '2.0.0', 'PRODUCT': 'rbsp_ect-hope-hk-L05',
              'datetime': datetime.date(2012, 12, 2)}),
            ('rbspa_ect-hope-sci-L05_0091_v{VERSION}.cdf',
             {'INSTRUMENT': 'hope', 'SATELLITE': 'rbspa',
              'VERSION': '2.0.0', 'PRODUCT': 'rbsp_ect-hope-sci-L05',
              'datetime': datetime.date(2012, 11, 28)}),
            ('rbspa_ect-hope-hk-L1_{DATE}_v{VERSION}.cdf',
             {'INSTRUMENT': 'hope', 'SATELLITE': 'rbspa',
              'VERSION': '2.0.0', 'PRODUCT': 'rbsp_ect-hope-hk-L1',
              'datetime': datetime.date(2012, 10, 3)}),
            ('rbspa_ect-hope-sci-L1_{DATE}_v{VERSION}.cdf',
             {'INSTRUMENT': 'hope', 'SATELLITE': 'rbspa',
              'VERSION': '2.0.0', 'PRODUCT': 'rbsp_ect-hope-sci-L1',
              'datetime': datetime.date(2012, 10, 3)}),
            ]
        #output string
        outputs = [
            'rbspa_ect-hope-hk-L05_0095_v2.0.0.cdf',
            'rbspa_ect-hope-sci-L05_0091_v2.0.0.cdf',
            'rbspa_ect-hope-hk-L1_20121003_v2.0.0.cdf',
            'rbspa_ect-hope-sci-L1_20121003_v2.0.0.cdf',
            ]
        for i, o in zip(inputs, outputs):
            self.assertEqual(o, self.fmtr.format(i[0], **i[1]))

    def testAPID(self):
        """Test regex and format expansion with an APID in the string"""
        fmt = 'ect_rbspa_{nnnn}_{APID}_{nn}.ptp.gz'
        expected = 'ect_rbspa_(\d\d\d\d)_([\da-fA-F]+)_(\d\d).ptp.gz'
        self.assertEqual(expected, self.fmtr.re(fmt))
        # Does this regex actually match a filename?
        self.assertTrue(re.match(
            '^' + expected + '$',
            'ect_rbspa_0377_34b_02.ptp.gz'))
        self.assertEqual(
            'ect_rbspa_{nnnn}_{APID:x}_{nn}.ptp.gz', self.fmtr.expand_format(fmt))

    def testMissionDayRegex(self):
        """Test regex and format expansion  with mission day in the string"""
        fmt = 'rbspa_int_ect-mageisLOW-ns-L05_{mday}_v{VERSION}.cdf'
        expected = 'rbspa_int_ect-mageisLOW-ns-L05_(-?\d+)_v(\d+\.\d+\.\d+).cdf'
        self.assertEqual(expected, self.fmtr.re(fmt))
        self.assertTrue(re.match(
            '^' + expected + '$',
            'rbspa_int_ect-mageisLOW-ns-L05_0376_v3.0.0.cdf'))
        self.assertEqual(
            'rbspa_int_ect-mageisLOW-ns-L05_{mday:d}_v{VERSION}.cdf',
            self.fmtr.expand_format(fmt))


if __name__ == '__main__':
    unittest.main()

