#!/usr/bin/env python2.6

"""Combined unit test suite for all dbprocessing classes"""

from DBfile_test import *
from DBqueue_test import *
from DBUtils2_test import *
from Diskfile_test import *
from ProcessQueue_test import *
from Version_test import *
from testDBStrings import *


__version__ = '2.0.3'


if __name__ == "__main__":
    unittest.main()
