#!/usr/bin/env python

"""Inspector for "inputless" simple files"""
import datetime
import re

from dbprocessing import inspector
from dbprocessing import Version

class Inspector(inspector.inspector):
    code_name = "inputless_inspector.py"

    def inspect(self, kwargs):
        #Check for reasonable inputs
        m = re.match(r'^.*(\d{8})_v((?:\d+\.){3})txt$',
                     self.basename)
        if m is None:
            return None
        datestr, ver = m.group(1, 2)
        ver = ver[:-1]
        utc_dt = datetime.datetime.strptime(datestr, "%Y%m%d")
        self.diskfile.params['utc_file_date'] = utc_dt.date()
        self.diskfile.params['version'] = Version.Version.fromString(ver)
        self.diskfile.params['utc_start_time'] = utc_dt
        self.diskfile.params['utc_stop_time'] = utc_dt.replace(
            hour=23, minute=59, second=59, microsecond=59)
        return True
