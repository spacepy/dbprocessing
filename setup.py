#!/usr/bin/env python
"""Setup script for dbprocessing"""

__author__ = 'Brian Larsen <balarsen@lanl.gov>'
__version__ = '0.0'

from distutils.core import setup
import os

scripts = ('scripts/ProcessQueue.py',
           'scripts/writeDBhtml.py', 'scripts/writeProductsConf.py',
           'scripts/writeProcessConf.py', 'scripts/deleteAllDBFiles.py',
           'scripts/README.txt', 'scripts/flushProcessQueue.py',
           'scripts/printProcessQueue.py', 'scripts/hope_query.py', 
           'scripts/deleteAllDBProducts.py', 'scripts/weeklyReport.py', 
           'scripts/QCEmailer_conf.txt', 'scripts/reprocessByCode.py',
           'scripts/reprocessByProduct.py', 'scripts/reprocessByInstrument.py',
           'scripts/reprocessByAll.py', 'scripts/makeLatestSymlinks.py',
           'scripts/addVerboseProvenance.py', 'scripts/updateSHAsum.py',
           'scripts/printInfo.py', 'scripts/addFromConfig.py', 'scripts/CreateDB.py',
           'scripts/clearProcessingFlag.py', 'scripts/possibleProblemDates.py',
           'scripts/missingFilesByProduct.py', 'scripts/histogramCodes.py',
           'scripts/htmlCoverage.py', 'scripts/magephem_dataToIncoming.py',
           'scripts/magephem_def_dataToIncoming.py',
           'scripts/missingFiles.py', 'scripts/configFromDB.py',
           'scripts/reprocessByDate.py', 'scripts/dbOnlyFiles.py',
           'scripts/coveragePlot.py', 'scripts/DBRunner.py',
           'scripts/dataToIncoming.py', 'scripts/updateCode.py', 
           'scripts/purgeFileFromDB.py', 'scripts/deleteFromDBifNotOnDisk.py')

scripts_dir = os.path.expanduser('~/dbUtils')

setup(name='dbprocessing',
      version='0.0',
      description='RBSP database file processing',
      author='Brian Larsen',
      author_email='balarsen@lanl.gov',
      packages=['dbprocessing'],
      provides=['dbprocessing'],
      #package_data={'dbprocessing': ['rbsp_config.txt', 'xstartup']},
      # data_files=[ (scripts_dir, scripts) ]
      scripts = scripts
      )
