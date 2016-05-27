#!/usr/bin/env python

"""Combined unit test suite for all dbprocessing classes"""

from test_DBfile import *
from test_DBqueue import *
from test_DButils import *
from test_Diskfile import *
from test_Version import *
from test_DBstrings import *
from test_Utils import *
from test_Inspector import *


if __name__ == "__main__":
    unittest.main()
