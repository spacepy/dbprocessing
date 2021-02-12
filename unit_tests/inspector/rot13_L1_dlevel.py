from dbprocessing import inspector
from dbprocessing import Version

import re
import datetime

class Inspector(inspector.inspector):
    code_name = "rot13_L1_dlevel.py"

    def inspect(self, kwargs):
        m = re.match(r'testDB_(\d{3}).*\.raw$', self.basename)
        if not m:
            return None

        self.diskfile.params['utc_start_time'] = datetime.datetime(2016, 1, 1) + datetime.timedelta(days=int(m.group(1)))
        self.diskfile.params['utc_stop_time'] = self.diskfile.params['utc_start_time'] + datetime.timedelta(days=1)
        self.diskfile.params['utc_file_date'] = self.diskfile.params['utc_start_time'].date()
        self.diskfile.params['version'] = Version.Version(1, 0, 0)
        self.diskfile.params['process_keywords'] = 'nnn=' + m.group(1)
        # Setting data_level isn't allowed
        self.diskfile.params['data_level'] = 2.0
        return True
