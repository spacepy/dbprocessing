# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 14:10:27 2012

@author: balarsen
"""
import os

from spacepy import pycdf

import DBlogging
import inspector
import Version

class Inspector(inspector.inspector):
    code_name = 'R0_evinst.py'
    
    def inspect(self, kwargs):
        """
        look for a filename that matches this product
        and example is "Test-Test_R0_evinst_20020602_v1.0.0.cdf"
        """
#==============================================================================
#         convince yourself this is your file
#==============================================================================
        fname = os.path.basename(self.filename)
        # {MISSION}-{SPACECRAFT}_{PRODUCT}_{Y}{m}{d}_v{VERSION}.cdf
        if fname[0:19] != 'Test-Test_R0_evinst':
            return None
        if not inspector.valid_YYYYMMDD(fname[20:28]):
            return None
        if fname.split(os.extsep)[-1] != 'cdf':
            return None
            
#==============================================================================
#         ## now convinced that this is really a R0_evinst file
#         # fill in the diskfile stuff
#         #    - still check for errors and return None, logging is allowed
#==============================================================================
        #        * self.diskfile.params['utc_file_date'] : date object (user)
        #        * self.diskfile.params['utc_start_time'] : datetime object (user)
        #        * self.diskfile.params['utc_stop_time'] : datetime object (user)
        #        * self.diskfile.params['data_level'] : float (user)
        #        * self.diskfile.params['verbose_provenance'] : string (user) (optional)
        #        * self.diskfile.params['quality_comment'] : string (user) (optional)
        #        * self.diskfile.params['caveats'] : string (user) (optional)
        #        * self.diskfile.params['release_number'] : int (user) (optional)
        #        * self.diskfile.params['met_start_time'] : long (user) (optional)
        #        * self.diskfile.params['met_stop_time'] : long (user) (optional)
        #        * self.diskfile.params['version'] : Version object (user)
        self.diskfile.params['utc_file_date'] = self.extract_YYYYMMDD()
        
        ## assume it is a istp cdf
        try:
            cdf = pycdf.CDF(self.filename)
        except pycdf.CDFError:
            DBlogging.dblogger.error("File {0} is not a cdf".format(self.filename)) # error since this should be a cdf by now
            return None
        ## get the start cdf time from the file
        try:
            self.diskfile.params['utc_start_time'] = cdf['Epoch'][0]
            self.diskfile.params['utc_stop_time']  = cdf['Epoch'][-1]   
        except pycdf.CDFError:
            return None
        self.diskfile.params['data_level'] = 0
        
        ver = fname.split('v')[-1]
        try:
            self.diskfile.params['version'] = Version.Version(ver.split('.')[0], ver.split('.')[1], ver.split('.')[2])
        except:
            return None

        return "I work" # anything that is non None is good enough

        