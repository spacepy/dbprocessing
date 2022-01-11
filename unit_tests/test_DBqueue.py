#!/usr/bin/env python
from __future__ import print_function

import unittest

import dbp_testing
from dbprocessing import DBqueue


class DBqueueTests(unittest.TestCase):
    """Tests for DBqueue class"""

    def setUp(self):
        super(DBqueueTests, self).setUp()
        self.queue = DBqueue.DBqueue([1, 2, 3])

    def test_popleftiter(self):
        """ pop left should pop from the left and iterate"""
        expected = [1, 2, 3]
        for i, val in enumerate(self.queue.popleftiter()):
            self.assertEqual(expected[i], val)
        self.assertFalse(self.queue)

    def test_popiter(self):
        """ pop should pop from the right and iterate"""
        expected = [3, 2, 1]
        for i, val in enumerate(self.queue.popiter()):
            self.assertEqual(expected[i], val)
        self.assertFalse(self.queue)



if __name__ == "__main__":
    unittest.main()
