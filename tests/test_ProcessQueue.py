#!/usr/bin/env python2.6

import collections
import unittest

import ProcessQueue


__version__ = '2.0.3'


class ProcessQueueSimpleTests(unittest.TestCase):
     """Simple test cases for the ProcessQueue class"""

     def test_emptyQueue(self):
        """ should raise an EmptyQueue exception if it is empty"""
        pass


if __name__ == "__main__":
    unittest.main()
