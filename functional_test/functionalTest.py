#!/usr/bin/env python

"""Functional test for dbprocessing, somewhat separate from unit tests"""

#This runs from the CHECKOUT of dbprocessing/dbutils, not installed

import os
import os.path
import shutil
import subprocess
import sys
import tempfile


class FunctionalTest(object):
    """Run functional test"""

    def __init__(self):
        """Figure out file locations, set up temp dir"""
        checkout = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                '..'))
        tmpdir = tempfile.mkdtemp()
        env = os.environ.copy()
        scripts = os.path.join(checkout, 'scripts')
        env['PYTHONPATH'] = '{}:{}'.format(env['PYTHONPATH'], checkout) \
                            if 'PYTHONPATH' in env else checkout
        env['DBPROCESSING_LOG_DIR'] = os.path.join(
            tmpdir, 'dbp_logs')
        
        self.checkout = checkout
        """Directory with the dbp checkout"""
        self.env = env
        """Environment to use for calls in the test dbp setup"""
        self.scripts = scripts
        """Directory containing scripts in the checkout"""
        self.tmpdir = tmpdir
        """Temporary directory with the full dbp setup (rootdir)"""

    def main(self):
        """Perform the test"""
        try:
            self.setup()
            self.ingest_and_process()
            self.error_check()
        except:
            self.fail()
            raise
        else:
            self.success()

    def execute(self, script, *args):
        """Execute a Python script in the dbp environment"""
        #Explicitly call the Python interpreter in case script isn't
        #marked executable
        subprocess.check_call([
            sys.executable, os.path.join(self.scripts, script)] + list(args),
            shell=False, cwd=self.tmpdir, env=self.env)

    def setup(self):
        """Create the test db environment"""
        #TODO: copy codes, make errors directory?
        for d in ('codes', 'scripts'):
            shutil.copytree(os.path.join(self.checkout, 'functional_test', d),
                        os.path.join(self.tmpdir, d))
        shutil.copytree(os.path.join(self.checkout, 'functional_test', 'L0'),
                        os.path.join(self.tmpdir, 'incoming'))
        self.dbname = os.path.join(self.tmpdir, 'testDB.sqlite')
        self.execute('CreateDB.py', self.dbname)
        self.execute('addFromConfig.py', '-m', self.dbname,
                     os.path.join(self.checkout, 'functional_test', 'config',
                                  'testDB.conf'))

    def ingest_and_process(self):
        """Run ProcessQueue -i and -p"""
        self.execute('ProcessQueue.py', '-i', '-m', self.dbname)
        self.execute('ProcessQueue.py', '-p', '--num-proc', '1',
                     '-m', self.dbname)

    def error_check(self):
        """Check for errors in the logs"""
        logdir = os.path.join(self.tmpdir, 'dbp_logs')
        for logfile in os.listdir(logdir):
            with open(os.path.join(logdir, logfile), 'r') as f:
                for l in f:
                    if 'ERROR' in l or 'CRITICAL' in l:
                        raise RuntimeError('dbp logged error {}'.format(l))

    def fail(self):
        """Test failed, print message"""
        print('Failed. Environment still in {}'.format(self.tmpdir))        

    def success(self):
        """Remove the temp directory"""
        print('Successful, deleting {}'.format(self.tmpdir))
        shutil.rmtree(self.tmpdir)


if __name__ == '__main__':
    FunctionalTest().main()
