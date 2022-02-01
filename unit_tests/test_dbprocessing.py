#!/usr/bin/env python
from __future__ import print_function

"""Unit tests for dbprocessing.dbprocessing module"""

__author__ = 'Jonathan Niehof <Jonathan.Niehof@unh.edu>'


import datetime
import os.path
import unittest

import dbp_testing

import dbprocessing.DButils
import dbprocessing.dbprocessing


class ProcessQueueTestsBase(unittest.TestCase, dbp_testing.AddtoDBMixin):
    """Base class for tests that require ProcessQueue setup"""

    def setUp(self):
        """Make a db and open it so have something to work with"""
        super(ProcessQueueTestsBase, self).setUp()
        self.makeTestDB()
        # Set up the baseline mission environment, BEFORE making processqueue
        self.addSkeletonMission()
        self.pq = dbprocessing.dbprocessing.ProcessQueue(self.dbname)
        self.dbu = self.pq.dbu

    def tearDown(self):
        """Remove the db and working tree"""
        self.removeTestDB()
        # Unfortunately all the cleanup is in the destructor
        del self.pq
        super(ProcessQueueTestsBase, self).tearDown()


# Many of these tests do not work as expected. The tests that fail with
# the current code (but are expected to succeed) are commented out, with
# notation on the expected and actual behavior.
# Thus these tests "succeeding" indicates consistency, and as the desired
# behavior is fixed, should be updated to match.
class BuildChildrenTests(ProcessQueueTestsBase):
    """Tests of ProcessQueue.buildChildren, checking what runMes are made"""
    longMessage = True

    def checkCommandLines(self, fid, expected):
        """Check the command line built for a file ID

        fid is the (single) file ID

        expected is a list-of-lists, all the expected commands to be called.
        """
        # Clear any pending runMes from prior subtests
        del self.pq.runme_list[:]
        self.pq.buildChildren([fid, None])
        self.assertEqual(
            len(self.pq.runme_list), len(expected))
        actual = []
        for rm in self.pq.runme_list:
            rm.make_command_line(rundir='')
            actual.append(rm.cmdline)
        for i, (e, a) in enumerate(zip(expected, actual)):
            # Sort the input files, identified by having 'data'
            # in there (so not argument or output); the order
            # of input files doesn't matter. (Don't change function inputs.)
            datapart = os.path.join('x', 'data', 'x')[1:-1]
            idx_exp = [j for j in range(len(e)) if datapart in e[j]]
            idx_act = [j for j in range(len(a)) if datapart in a[j]]
            e = e[:idx_exp[0]] + sorted(e[idx_exp[0]:idx_exp[-1]+1]) \
                + e[idx_exp[-1]+1:]
            a = a[:idx_act[0]] + sorted(a[idx_act[0]:idx_act[-1]+1]) \
                + a[idx_act[-1]+1:]
            self.assertEqual(e, a, 'Command {}'.format(i))

    def testSimple(self):
        """Single daily file making another single daily file"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        expected = [[
            os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
            os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
            'level_1_20120101_v1.0.0']]
        self.checkCommandLines(fid, expected)

    def testSingleDailyUpdate(self):
        """Single daily file making another, new version appears"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        l0fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        l1fid = self.addFile('level_1_20120101_v1.0.0', l1pid)
        self.dbu.addFilefilelink(l1fid, l0fid)
        expected = []
        # Should be up to date
        self.checkCommandLines(l0fid, expected)
        #Updated version of L0
        fid = self.addFile('level_0_20120101_v1.1.0', l0pid)
        expected = [[
            os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
            os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.1.0'),
            'level_1_20120101_v1.1.0']]

    def testYesterdayNewFile(self):
        """Daily file, use yesterday

        Does not make the day with only yesterday as input
        """
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process, yesterday=1)
        l0fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
            os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
            'level_1_20120101_v1.0.0'
            ],
# Yesterday-only is not made
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),,
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
#             'level_1_20120102_v1.0.0'
#            ],
        ]
        self.checkCommandLines(l0fid, expected)

    def testYesterdayNewFileTwoDays(self):
        """Two daily files, use yesterday

        Both days are made; second day has yesterday input
        """
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process, yesterday=1)
        l0fid1 = self.addFile('level_0_20120101_v1.0.0', l0pid)
        l0fid2 = self.addFile('level_0_20120102_v1.0.0', l0pid)
        # Precondition: two subsequent L0 days, L1 not made yet.
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120101_v1.0.0'
            ],
# 2012-01-02 not triggered on "yesterday" even though it has "today"
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
#             'level_1_20120102_v1.0.0'
#            ],
        ]
        self.checkCommandLines(l0fid1, expected)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
             # Yesterday is included in the command build
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120102_v1.0.0'
            ],
# 2012-01-03 yesterday-only, not triggered
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
#             'level_1_20120103_v1.0.0'
#            ],
        ]
        self.checkCommandLines(l0fid2, expected)

    def testYesterdayUpdate(self):
        """Daily file, use yesterday, new file yesterday, no today yet

        Does not make the day with only yesterday as input
        """
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process, yesterday=1)
        l0fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        l1fid = self.addFile('level_1_20120101_v1.0.0', l1pid)
        self.dbu.addFilefilelink(l1fid, l0fid)
        newfid = self.addFile('level_0_20120101_v1.1.0', l0pid)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.1.0'),
             'level_1_20120101_v1.1.0'
            ],
# Yesterday-only is not made
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.1.0'),
#             'level_1_20120102_v1.0.0'
#            ],
        ]
        self.checkCommandLines(newfid, expected)

    def testYesterdayUpdateTodayExists(self):
        """Daily file, use yesterday, new file yesterday, today exists

        Does not make the day with only yesterday updated
        """
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process, yesterday=1)
        l0fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        l1fid = self.addFile('level_1_20120101_v1.0.0', l1pid)
        self.dbu.addFilefilelink(l1fid, l0fid)
        # This file has "yesterday" and "today" inputs
        l1fid = self.addFile('level_1_20120102_v1.0.0', l1pid)
        self.dbu.addFilefilelink(l1fid, l0fid)
        l0fid = self.addFile('level_0_20120102_v1.0.0', l0pid)
        self.dbu.addFilefilelink(l1fid, l0fid)
        # Precondition: both yesterday and today have L0 and L1, and up to date
        # Perturbation: Add new "yesterday"
        newfid = self.addFile('level_0_20120101_v1.1.0', l0pid)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.1.0'),
             'level_1_20120101_v1.1.0'
            ],
# Date with only yesterday changed is not updated.
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.1.0'),
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
#             'level_1_20120102_v1.1.0'
#            ],
        ]
        self.checkCommandLines(newfid, expected)

    def testTomorrowNewFile(self):
        """Daily file, use tomorrow

        Does not make the day with only tomorrow as input
        """
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process, tomorrow=1)
        l0fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120101_v1.0.0'
            ],
# Tomorrow-only is not made
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
#             'level_1_20111231_v1.0.0'
#            ],
        ]
        self.checkCommandLines(l0fid, expected)

    def testTomorrowNewFileTwoDays(self):
        """Two daily files, use tomorrow

        Both days are made; first day has tomorrow input
        """
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process, tomorrow=1)
        l0fid1 = self.addFile('level_0_20120101_v1.0.0', l0pid)
        l0fid2 = self.addFile('level_0_20120102_v1.0.0', l0pid)
        # Precondition: two subsequent L0 days, L1 not made yet.
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             # Tomorrow is included in the command build
             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120101_v1.0.0'
            ],
# 2011-12-31 tomorrow-only, not triggered
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
#             'level_1_20111231_v1.0.0'
#            ],
        ]
        self.checkCommandLines(l0fid1, expected)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
             'level_1_20120102_v1.0.0'
            ],
# 2012-01-01 not triggered by "tomorrow" even though have "today"
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
#             'level_1_20120101_v1.0.0'
#            ],
        ]
        self.checkCommandLines(l0fid2, expected)

    def testTomorrowUpdate(self):
        """Daily file, use tomorrow, new file tomorrow, no today yet

        Does not make the day with only tomorrow as input
        """
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process, tomorrow=1)
        l0fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        l1fid = self.addFile('level_1_20120101_v1.0.0', l1pid)
        self.dbu.addFilefilelink(l1fid, l0fid)
        newfid = self.addFile('level_0_20120101_v1.1.0', l0pid)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.1.0'),
             'level_1_20120101_v1.1.0'
            ],
# Tomorrow-only is not made
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.1.0'),
#             'level_1_20111231_v1.0.0'
#            ],
        ]
        self.checkCommandLines(newfid, expected)

    def testTomorrowUpdateTodayExists(self):
        """Daily file, use tomorow, new file tomorrow, today exists

        Does not make the day with only tomorrow updated
        """
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process, tomorrow=1)
        l0fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        l1fid = self.addFile('level_1_20120101_v1.0.0', l1pid)
        # This file has "tomorrow" and "today" inputs
        self.dbu.addFilefilelink(l1fid, l0fid)
        l0fid = self.addFile('level_0_20120102_v1.0.0', l0pid)
        self.dbu.addFilefilelink(l1fid, l0fid)
        l1fid = self.addFile('level_1_20120102_v1.0.0', l1pid)
        self.dbu.addFilefilelink(l1fid, l0fid)
        # Precondition: both tomorrow and today have L0 and L1, and up to date
        # Perturbation: Add new "tomorrow"
        newfid = self.addFile('level_0_20120102_v1.1.0', l0pid)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
            'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.1.0'),
             'level_1_20120102_v1.1.0'
            ],
# Date with only tomorrow changed is not updated.
#            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
#            'level_0-1_args',
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
#             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.1.0'),
#             'level_1_20120101_v1.1.0'
#            ],
        ]
        self.checkCommandLines(newfid, expected)

    def testChangeTimebase(self):
        """L0 file that spans days should have L1 that spans days"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        fid = self.addFile('level_0_20120101_v1.0.0', l0pid,
                           utc_start=datetime.datetime(2012, 1, 1, 1),
                           utc_stop=datetime.datetime(2012, 1, 2, 1))
        self.assertEqual(
            [datetime.date(2012, 1, 1), datetime.date(2012, 1, 2)],
            self.dbu.getFileDates(fid))
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
             'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120101_v1.0.0'],
# l1 "tomorrow" built because l0 "today" includes data for it
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
             'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120102_v1.0.0']
        ]
        self.checkCommandLines(fid, expected)

    def testChangeTimebaseMultiDays(self):
        """L0 files straddle days, should give both files input to L1 day"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        fid1 = self.addFile('level_0_20120101_v1.0.0', l0pid,
                           utc_start=datetime.datetime(2012, 1, 1, 1),
                           utc_stop=datetime.datetime(2012, 1, 2, 1))
        self.assertEqual(
            [datetime.date(2012, 1, 1), datetime.date(2012, 1, 2)],
            self.dbu.getFileDates(fid1))
        fid2 = self.addFile('level_0_20120102_v1.0.0', l0pid,
                            utc_start=datetime.datetime(2012, 1, 2, 1),
                            utc_stop=datetime.datetime(2012, 1, 3, 1))
        self.assertEqual(
            [datetime.date(2012, 1, 2), datetime.date(2012, 1, 3)],
            self.dbu.getFileDates(fid2))
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
             'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120101_v1.0.0'],
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
             'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
# l0 "previous day" with data for l1 "today" is included
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120102_v1.0.0']
        ]
        self.checkCommandLines(fid1, expected)
        expected = [
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
             'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
# l0 "next day" with data for l1 "today" is included
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120102_v1.0.0'],
# l1 "tomorrow" built because l0 "today" includes data for it
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
             'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120102_v1.0.0'),
             'level_1_20120103_v1.0.0']
        ]
        self.checkCommandLines(fid2, expected)

    def testChangeTimebaseYesterday(self):
        """L0 file has a little "yesterday" should have L1 that spans days"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        fid = self.addFile('level_0_20120101_v1.0.0', l0pid,
                           utc_start=datetime.datetime(2011, 12, 31, 23),
                           utc_stop=datetime.datetime(2012, 1, 1, 23))
        self.assertEqual(
            [datetime.date(2011, 12, 31), datetime.date(2012, 1, 1)],
            self.dbu.getFileDates(fid))
        expected = [
# l1 "yesterday" built because l0 "today" includes data for it
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
             'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20111231_v1.0.0'],
            [os.path.join(self.td, 'codes', 'scripts', 'junk.py'),
             'level_0-1_args',
             os.path.join(self.td, 'data', 'junk', 'level_0_20120101_v1.0.0'),
             'level_1_20120101_v1.0.0'],
        ]
        self.checkCommandLines(fid, expected)


class ProcessQueueTests(ProcessQueueTestsBase):
    """Other tests of ProcessQueue"""

    def testReprocessByNoDate(self):
        """Do _reprocessBy without a specific date"""
        l0pid = self.addProduct('level 0')
        fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        self.assertEqual(1, self.pq._reprocessBy())
        self.assertEqual((fid, None), self.dbu.ProcessqueuePop())


class GetRequiredProductsTests(ProcessQueueTestsBase):
    """Tests of the _getRequiredProducts method"""

    def testSimple(self):
        """Single file, only itself as input"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        fid = self.addFile('level_0_20120101_v1.0.0', l0pid)
        files, input_products = self.pq._getRequiredProducts(
            l01process, fid, datetime.datetime(2012, 1, 1))
        self.assertEqual(1, len(input_products))
        prodid, optional, yesterday, tomorrow = input_products[0]
        self.assertEqual(l0pid, prodid)
        self.assertEqual(False, optional)
        self.assertEqual(0, yesterday)
        self.assertEqual(0, tomorrow)
        self.assertEqual(1, len(files))
        self.assertEqual(fid, files[0].file_id)

    def testOverlapDays(self):
        """Single file, input crosses two days"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        fid0 = self.addFile('level_0_20120101_v1.0.0', l0pid,
                            utc_start=datetime.datetime(2012, 1, 1, 1),
                            utc_stop=datetime.datetime(2012, 1, 2, 0, 59))
        fid1 = self.addFile('level_0_20120102_v1.0.0', l0pid,
                            utc_start=datetime.datetime(2012, 1, 2, 1),
                            utc_stop=datetime.datetime(2012, 1, 3, 0, 59))
        files, input_products = self.pq._getRequiredProducts(
            l01process, fid1, datetime.datetime(2012, 1, 2))
        self.assertEqual(1, len(input_products))
        self.assertEqual(l0pid,
                         input_products[0][0])
        self.assertEqual(2, len(files))
        self.assertEqual([fid0, fid1],
                         sorted([f.file_id for f in files]))

    def testOverlapDaysRun(self):
        """Single file, input crosses two days, but RUN timebase"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid,
                                              output_timebase='RUN')
        self.addProductProcessLink(l0pid, l01process)
        fid0 = self.addFile('level_0_20120101_v1.0.0', l0pid,
                            utc_start=datetime.datetime(2012, 1, 1, 1),
                            utc_stop=datetime.datetime(2012, 1, 2, 0, 59))
        fid1 = self.addFile('level_0_20120102_v1.0.0', l0pid,
                            utc_start=datetime.datetime(2012, 1, 2, 1),
                            utc_stop=datetime.datetime(2012, 1, 3, 0, 59))
        files, input_products = self.pq._getRequiredProducts(
            l01process, fid1, datetime.datetime(2012, 1, 2))
        self.assertEqual(1, len(input_products))
        self.assertEqual(l0pid, input_products[0][0])
        self.assertEqual(1, len(files))
        self.assertEqual(fid1, files[0].file_id)

    def testOverlapDaysNotOnDisc(self):
        """Single file, version not on disc crosses days"""
        l0pid = self.addProduct('level 0')
        l1pid = self.addProduct('level 1', level=1)
        l01process, l01code = self.addProcess('level 0-1', l1pid)
        self.addProductProcessLink(l0pid, l01process)
        fid0 = self.addFile('level_0_20120101_v1.0.0', l0pid,
                            utc_start=datetime.datetime(2012, 1, 1, 1),
                            utc_stop=datetime.datetime(2012, 1, 2, 0, 59),
                            exists=False)
        fid0 = self.addFile('level_0_20120101_v1.1.0', l0pid,
                            utc_start=datetime.datetime(2012, 1, 1, 1),
                            utc_stop=datetime.datetime(2012, 1, 1, 23, 59))
        fid1 = self.addFile('level_0_20120102_v1.0.0', l0pid,
                            utc_start=datetime.datetime(2012, 1, 2, 1),
                            utc_stop=datetime.datetime(2012, 1, 2, 23, 59))
        files, input_products = self.pq._getRequiredProducts(
            l01process, fid1, datetime.datetime(2012, 1, 2))
        self.assertEqual(1, len(input_products))
        self.assertEqual(l0pid,
                         input_products[0][0])
        self.assertEqual(1, len(files))
        self.assertEqual([fid1],
                         sorted([f.file_id for f in files]))


if __name__ == '__main__':
    unittest.main()

