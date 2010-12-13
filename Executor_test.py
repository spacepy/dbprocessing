#!/usr/bin/env python2.6

import os
import stat
import sys
import unittest

import Executor


__version__ = '2.0.3'


class ExecutorTests(unittest.TestCase):
    """Tests of the Executor class"""
    
    def setUp(self):
        super(ExecutorTests, self).setUp()
        with open('code_run_tmp', 'w') as f:
            f.writelines('#!/bin/sh\necho $1')
        os.chmod('code_run_tmp', stat.S_IXUSR|stat.S_IWUSR|stat.S_IRUSR)
        with open('code_dat_tmp', 'w') as f:
            f.writelines('Hello World')
        self.path = os.path.abspath('.')

    def tearDown(self):
        super(ExecutorTests, self).tearDown()
        os.remove('code_run_tmp')
        os.remove('code_dat_tmp')

    def test_init(self):
        """Init does some input checking"""
        self.assertRaises(Executor.ExecutorError, Executor.Executor, 123, 'input', 'output')
        self.assertRaises(Executor.ExecutorError, Executor.Executor, 'code', 'input', 123)

    def test_checkExists(self):
        """Exception on missing file to run"""
        ex = Executor.Executor('code_run_tmp_bad', '', '')
        self.assertRaises(Executor.ExecutorError, ex.checkExists)
        ex = Executor.Executor('code_run_tmp_bad', 'blabla', '')
        self.assertRaises(Executor.ExecutorError, ex.checkExists)

    def test_runfile(self):
        """Should run an echo command"""

        ex = Executor.Executor(self.path + '/code_run_tmp', self.path + '/code_dat_tmp', None)
        ex.checkExists()
        ex.doIt()

    def test_outputBadDir(self):
        """Exception on bad output directory"""
        ex = Executor.Executor(self.path + '/code_run_tmp', self.path + '/code_dat_tmp', '/tmp/IDONOTEXIST/file')
        self.assertRaises(Executor.ExecutorError, ex.checkExists)


if __name__ == "__main__":
    unittest.main()
