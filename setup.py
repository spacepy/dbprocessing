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
      scripts=['dbprocessing/ProcessQueue.py'] ,
#               'scripts/parse_telemetry.py', 'scripts/sync_inst_data.py',
#               'scripts/sync_moc_data.py', 'scripts/sync_queue_data.py',
#	       'scripts/repeating_sync.py', 'scripts/strip_moc_headers.py',
#               'scripts/l05_to_l1.py', 'scripts/md_to_utc.py'],
      )
