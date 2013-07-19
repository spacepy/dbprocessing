#!/usr/bin/env python
"""Setup script for dbprocessing"""

__author__ = 'Brian Larsen <balarsen@lanl.gov>'
__version__ = '0.0'

from distutils.core import setup
import os

scripts = ('scripts/ProcessQueue.py',
               'scripts/writeDBhtml.py', 'scripts/writeProductsConf.py',
               'scripts/writeProcessConf.py', 'scripts/deleteAllDBFiles.py',
               'scripts/README.txt', 'scripts/deleteDBFile.py',
               'scripts/flushProcessQueue.py', 'scripts/printProcessQueue.py',
               'scripts/processQueueHTML.py', 'scripts/deleteAllDBProducts.py',
               'scripts/weeklyReport.py', 'scripts/qualityControlFileDates.py',
               'scripts/qualityControlEmail.py', 'scripts/QCEmailer_conf.txt',
	           'scripts/dataToIncoming.py', 'scripts/reprocessByCode.py',
               'scripts/reprocessByProduct.py', 'scripts/reprocessByInstrument.py',
               'scripts/hope_dataToIncoming.py', 'scripts/reprocessByAll.py',
               'scripts/hopeCoverageHTML.py', 'scripts/makeLatestSymlinks.py',
               'scripts/hope_query.py', 'scripts/magephem_dataToIncoming.py',
               'scripts/magephem-pre-CoverageHTML.py', 'scripts/magephem_def_dataToIncoming.py',
               'scripts/addVerboseProvenance.py', 'scripts/updateSHAsum.py',
               'scripts/printInfo.py', 'scripts/addFromConfig.py', 'scripts/CreateDB.py',
               'scripts/clearProcessingFlag.py')

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
