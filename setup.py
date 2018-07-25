#!/usr/bin/env python
"""Setup script for dbprocessing"""

__author__ = 'Brian Larsen <balarsen@lanl.gov>'
__version__ = '0.0'

from distutils.core import setup

scripts = ('scripts/addFromConfig.py', 'scripts/missingFilesByProduct.py',
           'scripts/addVerboseProvenance.py', 'scripts/missingFiles.py',
           'scripts/clearProcessingFlag.py', 'scripts/possibleProblemDates.py',
           'scripts/configFromDB.py', 'scripts/printInfo.py',
           'scripts/coveragePlot.py', 'scripts/printProcessQueue.py',
           'scripts/CreateDB.py', 'scripts/ProcessQueue.py',
           'scripts/dataToIncoming.py', 'scripts/purgeFileFromDB.py',
           'scripts/dbOnlyFiles.py', 'scripts/QCEmailer_conf.txt',
           'scripts/DBRunner.py',
           'scripts/deleteAllDBFiles.py', 'scripts/reprocessByCode.py',
           'scripts/deleteAllDBProducts.py', 'scripts/reprocessByDate.py',
           'scripts/flushProcessQueue.py', 'scripts/reprocessByInstrument.py',
           'scripts/histogramCodes.py', 'scripts/reprocessByProduct.py',
           'scripts/hopeCoverageHTML.py', 'scripts/updateCode.py',
           'scripts/hope_query.py', 'scripts/updateProducts.py',
           'scripts/htmlCoverage.py', 'scripts/updateSHAsum.py',
           'scripts/link_missing_ql_mag_l2_mag.py', 'scripts/weeklyReport.py',
           'scripts/magephem_dataToIncoming.py', 'scripts/writeDBhtml.py',
           'scripts/magephem_def_dataToIncoming.py', 'scripts/writeProcessConf.py',
           'scripts/magephem-pre-CoverageHTML.py', 'scripts/writeProductsConf.py',
           'scripts/makeLatestSymlinks.py', 'scripts/testInspector.py')

setup(name='dbprocessing',
      version='0.0',
      description='RBSP database file processing',
      author='Brian Larsen',
      author_email='balarsen@lanl.gov',
      packages=['dbprocessing'],
      provides=['dbprocessing'],
      scripts=scripts
      )
