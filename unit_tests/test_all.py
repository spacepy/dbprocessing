#!/usr/bin/env python

"""Combined unit test suite for all dbprocessing classes"""

import os
import os.path

#The log is opened on import, so need to quarantine the log directory
#right away
os.environ['DBPROCESSING_LOG_DIR'] = os.path.join(os.path.dirname(__file__),
                                                  'unittestlogs')
from test_addFromConfig import *
from test_CreateDB import *
from test_dbprocessing import *
from test_DBfile import *
from test_DBqueue import *
from test_DBRunner import *
from test_DButils import *
from test_Diskfile import *
from test_Version import *
from test_DBstrings import *
from test_Utils import *
from test_Inspector import *


if __name__ == "__main__":
    unittest.main()
