#!/usr/bin/env python
"""Python script for read, update, write of textfile

Call:
$ ./Test_one_C0_l0tol1_evinst_v0_0_0.py infile outfile"""

import sys
flns = sys.argv  #get command line arguments

# first argument on command line is two form back of flns list
#second argument on command line is one form back of flns list

#reads full contents of rext file into a list
fh = open(flns[-2])
in = fh.readlines()
fh.close()

outlist = ['level 1 file from: ']
fh = open(flns[-1], 'w')
fh.writelines(outlist.extend(in))
fh.close() 
