******************
Inspector examples
******************

These are some examples of :ref:`concepts_inspectors`.

Example 1
=========

.. code-block:: python

   import os
   import re
   from spacepy import pycdf
   from dbprocessing import DBlogging
   from dbprocessing import inspector
   from dbprocessing import Version

   class Inspector(inspector.inspector):

       code_name = 'ect_L2_V1.0.0.py'

       def inspect(self, kwargs):
           re1 = r'{product_name}_\d\d\d\d\d\d\d\d.*.cdf$'.format(**kwargs)
           if not re.match(re1, self.basename):
               DBlogging.dblogger.debug("Inspector {0}:  re did not match {1} {2}".format(self.code_name, re1, self.basename))
               return None
        try:
            cdf = pycdf.CDF(self.filename)
        except:
            DBlogging.dblogger.debug("Inspector {0}: error in pycdf.CDF()".format(self.code_name))            
            return None # malformed file
        
        try:
            self.diskfile.params['utc_file_date'] = self.extract_YYYYMMDD().date()
        except:
            return None
        ## get the start time from the file
        min_time = min([v[0] for v in cdf.values() if v.type() in (pycdf.const.CDF_EPOCH.value, pycdf.const.CDF_EPOCH16.value) and v.rv() and len(v) > 0])
        max_time = max([v[-1] for v in cdf.values() if v.type() in (pycdf.const.CDF_EPOCH.value, pycdf.const.CDF_EPOCH16.value) and v.rv() and len(v) > 0])
        self.diskfile.params['utc_start_time'] = min_time
        self.diskfile.params['utc_stop_time']  = max_time
        self.diskfile.params['version'] = inspector.extract_Version(self.basename)
        return "That is not my dog." # anything that is not None is good

Example 2
=========
.. code-block:: python

   import datetime
   import os
   import re

   from dbprocessing import DBlogging
   from dbprocessing import inspector
   from dbprocessing import Version

   class Inspector(inspector.inspector):

       code_name = 'ephem_0_insp.py'

       def inspect(self, kwargs):
           re1 = r'.*{product_name}.*'.format(**kwargs)
           if not re.match(re1, self.basename):
               DBlogging.dblogger.debug("Inspector {0}:  re did not match {1} {2}".format(self.code_name, re1, self.basename))
               return None

           try:
               dt = self.extract_YYYYMMDD()
           except:
               return None

           self.diskfile.params['utc_file_date'] = dt.date()
        
           ## get the start time from the file
           min_time = dt
           max_time = dt + datetime.timedelta(days=1) - datetime.timedelta(microseconds=1)
           self.diskfile.params['utc_start_time'] = min_time
           self.diskfile.params['utc_stop_time']  = max_time

           try:
               self.diskfile.params['version'] = Version.Version(1, 0, 0)
           except:
               return None

           return "That is not my dog." # anything that is not None is good 
