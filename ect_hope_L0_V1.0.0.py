# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 14:10:27 2012

@author: balarsen
"""
import re

from rbsp import ptp

import DBlogging
import inspector
import Version

class Inspector(inspector.inspector):
    code_name = 'ect_hope_L0_V1.0.0.py'

    def inspect(self, kwargs):  ## TODO pass in the args
        """
        look for a filename that matches this product
        and example is "Test-Test_R0_evinst_20020602_v1.0.0.cdf"
        """
#==============================================================================
#         convince yourself this is your file
#==============================================================================
        # example ect_rbspa_0272_221_01.ptp.gz  format:  ect_{SPACECRAFT}_{????}_{APID}_{??}.ptp.gz
        ## TODO pass in the args (rbspa, apid)
        try:
            if re.search("ect_rbspa_\d\d\d\d_\d\d\d_\d\d.ptp.gz", self.basename).group() is None:
                return None
        except (ValueError, AttributeError): # there is not one
            return None

#==============================================================================
#         ## now convinced that this is really a R0_evinst file
#         # fill in the diskfile stuff
#         #    - still check for errors and return None, logging is allowed
#         #    - some are optional
#==============================================================================
        #        * self.diskfile.params['utc_file_date'] : date object
        #        * self.diskfile.params['utc_start_time'] : datetime object
        #        * self.diskfile.params['utc_stop_time'] : datetime object
        #        * self.diskfile.params['data_level'] : float
        #        * self.diskfile.params['version'] : Version object
        ################### OPTIONAL ##################
        #        * self.diskfile.params['verbose_provenance'] : string (optional)
        #        * self.diskfile.params['quality_comment'] : string (optional)
        #        * self.diskfile.params['caveats'] : string (optional)
        #        * self.diskfile.params['release_number'] : int (optional)
        #        * self.diskfile.params['met_start_time'] : long (optional)
        #        * self.diskfile.params['met_stop_time'] : long (optional)
        l0 = ptp.ptp(self.filename)
        min_max_met = l0.min_max_met()
        min_max_utc = l0.min_max_utc()
        self.diskfile.params['utc_file_date'] = min_max_utc[0] 

        ## get the start time from the file
        self.diskfile.params['utc_start_time'] = min_max_utc[0] 
        self.diskfile.params['utc_stop_time']  = min_max_utc[1] 
        ## get the start time from the file
        self.diskfile.params['met_start_time'] = min_max_met[0] 
        self.diskfile.params['met_stop_time']  = min_max_met[1] 

        self.diskfile.params['data_level'] = 0

        try:
            self.diskfile.params['version'] = Version.Version(1, self.basename[-9:-7], 0) # 2 digit version
        except:
            return None

        return "Does your dog bite?" # anything that is non None is good enough

