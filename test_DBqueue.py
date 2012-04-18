#!/usr/bin/env python2.6

import unittest

import DBqueue


__version__ = '2.0.3'


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

    def test_popiter(self):
        """ pop should pop from the right and iterate"""
        expected = [3, 2, 1]
        for i, val in enumerate(self.queue.popiter()):
            self.assertEqual(expected[i], val)


if __name__ == "__main__":
    unittest.main()