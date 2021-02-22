#!/usr/bin/env python
"""Setup script for dbprocessing"""

__author__ = 'Brian Larsen <balarsen@lanl.gov>'
__version__ = '0.0'

from distutils.core import setup

scripts = ('scripts/addFromConfig.py', 'scripts/missingFilesByProduct.py',
           'scripts/missingFiles.py',
           'scripts/clearProcessingFlag.py', 'scripts/possibleProblemDates.py',
           'scripts/configFromDB.py', 'scripts/printInfo.py',
           'scripts/coveragePlot.py', 'scripts/printProcessQueue.py',
           'scripts/CreateDB.py', 'scripts/ProcessQueue.py',
           'scripts/purgeFileFromDB.py',
           'scripts/dbOnlyFiles.py',
           'scripts/DBRunner.py',
           'scripts/deleteAllDBFiles.py', 'scripts/reprocessByCode.py',
           'scripts/reprocessByDate.py',
           'scripts/flushProcessQueue.py', 'scripts/reprocessByInstrument.py',
           'scripts/histogramCodes.py', 'scripts/reprocessByProduct.py',
           'scripts/htmlCoverage.py', 'scripts/updateSHAsum.py',
           'scripts/makeLatestSymlinks.py', 'scripts/testInspector.py',
           'scripts/replaceArgsWithRootdir.py', 'scripts/printRequired.py',
           'scripts/changeProductDir.py', 'scripts/compareDB.py')

setup(name='dbprocessing',
      version='0.0',
      description='RBSP database file processing',
      author='Brian Larsen',
      author_email='balarsen@lanl.gov',
      packages=['dbprocessing'],
      provides=['dbprocessing'],
      scripts=scripts
      )
