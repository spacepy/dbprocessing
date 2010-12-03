#!/usr/bin/env python2.6

import unittest
#import DBUtils2
#import sys
#import Version
#import datetime
#import os
import ProcessQueue

from collections import deque

__version__ = '2.0.3'


class SimpleTests(unittest.TestCase):
    def setUp(self):
        super(SimpleTests, self).setUp()
        pass


    def tearDown(self):
        super(SimpleTests, self).tearDown()
        pass



    def test_emptyQueue(self):
        """ should raise an EmptyQueue exception if it is empty"""
        pass








if __name__ == "__main__":
    unittest.main()
