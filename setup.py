#!/usr/bin/env python
"""Setup script for dbprocessing"""

__author__ = 'Brian Larsen <balarsen@lanl.gov>'
__version__ = '0.0'

from distutils.core import setup
import os

scripts = ('scripts/ProcessQueue.py', 'scripts/addProducts.py',
               'scripts/writeDBhtml.py', 'scripts/writeProductsConf.py',
               'scripts/updateProducts.py', 'scripts/addProcess.py',
               'scripts/writeProcessConf.py', 'scripts/deleteAllDBFiles.py',
               'scripts/README.txt')

scripts_dir = os.path.expanduser('~/dbUtils')

setup(name='dbprocessing',
      version='0.0',
      description='RBSP database file processing',
      author='Brian Larsen',
      author_email='balarsen@lanl.gov',
      packages=['dbprocessing'],
      provides=['dbprocessing'],
      #package_data={'dbprocessing': ['rbsp_config.txt', 'xstartup']},
      data_files=[ (scripts_dir, scripts) ]
      )
