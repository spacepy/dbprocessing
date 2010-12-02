#!/usr/bin/env python2.6
"""Python script to create L0 files

Call:
$ ./mk_all_l0.py datestart datestop version

dateformat %Y%M%d
YYYYMMDD
version x.y.z where x >=1

output files of the format Test-one_R0_evinst_20100204_v1.0.0.cdf 


"""

import datetime
import sys

flns = sys.argv  #get command line arguments
if len(flns) != 4:
    raise(Exception("Usage: ./mk_all_l0.py <startdate> <stopdate> <version>"))

d1 = datetime.datetime(int(flns[1][0:4]), int(flns[1][4:6]), int(flns[1][6:8]) )
#print(d1)
d2 = datetime.datetime(int(flns[2][0:4]),int(flns[2][4:6]), int(flns[2][6:8]) )
#print(d2)

version = flns[3]

for val in range((d2-d1).days + 1):
    t1 = (d1 + datetime.timedelta(val)).strftime('%Y%m%d')
    #print(t1)
    with open('Test-Test_R0_evinst_' + t1 + '_v' + version + '.cdf', 'w') as f:
        f.write('# I am a level 0 file.\tTest-Test_R0_evinst_' + t1 + '_v' + version + '.cdf\n')










