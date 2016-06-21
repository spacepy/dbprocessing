from dbprocessing import inspector
from dbprocessing import Version

import re
import datetime

class Inspector(inspector.inspector):
	code_name = "rot13_L1_minimal.py"

	def inspect(self, kwargs):
		m = re.match(r'testDB_(\d{3}).*\.raw$', self.basename)
		if not m:
			return None

		return True
