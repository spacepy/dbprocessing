#!/usr/bin/env python

"""Create a file based on the date implicit in its filename."""

import os.path
import re
import sys

assert len(sys.argv) == 2
outfile = sys.argv[1]
datestr, ver = re.match(r'^.*(\d{8})_v((?:\d+\.){3})txt$',
                        os.path.basename(outfile)).group(1, 2)
ver = ver[:-1] # Cut trailing .
with open(outfile, 'w') as f:
    f.write('Date: {} Version: {}\n'.format(datestr, ver))
