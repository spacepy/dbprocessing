#!/usr/bin/env python
"""Setup script for dbprocessing"""

__author__ = 'Brian Larsen <balarsen@lanl.gov>'
__version__ = '0.0'

from distutils.core import setup

setup(name='dbprocessing',
      version='0.0',
      description='RBSP database file processing',
      author='Brian Larsen',
      author_email='balarsen@lanl.gov',
      packages=['dbprocessing'],
      provides=['dbprocessing'],
      #package_data={'dbprocessing': ['rbsp_config.txt', 'xstartup']},
      scripts=['scripts/ProcessQueue.py', 'scripts/addProducts.py'] ,
      )
