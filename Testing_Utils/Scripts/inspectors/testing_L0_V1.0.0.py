from __future__ import print_function

import datetime
import os

from dbprocessing import inspector


class Inspector(inspector.inspector):
    code_name = os.path.basename(__file__)

    def inspect(self, kwargs):
        try:
            d = inspector.extract_YYYYMMDD(self.filename).date()
            t = datetime.time()
            dt1 = datetime.datetime.combine(d, t)
            dt2 = dt1 + datetime.timedelta(days=1)
        except:
            print("Failed extract dates")
            return False
        self.diskfile.params['utc_file_date'] = d
        self.diskfile.params['utc_start_time'] = dt1
        self.diskfile.params['utc_stop_time'] = dt2
        try:
            self.diskfile.params['version'] = inspector.extract_Version(self.basename)
        except:
            print("failed version")
            return False
        return True  # worked
