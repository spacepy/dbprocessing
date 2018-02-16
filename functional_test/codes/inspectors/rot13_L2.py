from dbprocessing import inspector
from dbprocessing import Version

import re
import datetime

class Inspector(inspector.inspector):
	code_name = "rot13_L2.py"

	def inspect(self, kwargs):
		m = re.match(r'testDB_((19|20)\d\d-\d\d-\d\d)\.rot$', self.basename)
		if not m:
			return None

		self.diskfile.params['utc_start_time'] = datetime.datetime.strptime(m.group(1), '%Y-%m-%d')
		self.diskfile.params['utc_stop_time'] = self.diskfile.params['utc_start_time'] + datetime.timedelta(days=1)
		self.diskfile.params['utc_file_date'] = self.diskfile.params['utc_start_time'].date()
		self.diskfile.params['version'] = Version.Version(1, 0, 0)
		return True
