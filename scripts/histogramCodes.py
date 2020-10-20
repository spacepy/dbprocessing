#!/usr/bin/env python

import itertools
import glob
import os
from optparse import OptionParser
import re
import sys
import traceback
import warnings

from spacepy import toolbox as tb
import matplotlib.pyplot as plt

   

if __name__ == '__main__':
    usage = "usage: %prog logfile [[logfile]...]"
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("incorrect number of arguments")


    """
    read in the specified logfiles and extract all the code run times and
    make histograms by code
    """

    files = args
    if not files:
        parser.error("No files matching {0} found".format(args[0]))

    lines = []
    # read in each file and add to lines the lines that fit the format:
    #    2013-09-04 21:31:00,063 - runMe:105 - INFO - Command: run_hope_L0toL05_v1.4.1.py took 160.020385027 seconds
    for f in files:
        try:
            with open(f, 'r') as fp:
                dat = fp.readlines()
        except IOError:
            continue
        tmp = [v.strip() for v in dat if ("INFO - Command:" in v and 'seconds' in v)]
        lines.extend(tmp)

    # get a list of all the codes:
    codes = set([re.search(r'INFO\ \-\ Command\:\ (.*)\ took.*seconds', v).groups()[0] for v in lines])

    ans = {}
    for code in codes:
        ans[code] = []
        # get all the times for those codes
        for v in lines:
            match = re.search(r'INFO\ \-\ Command\:\ {0}\ took(.*)\ seconds'.format(code), v)
            if match:
                ans[code].append(float(match.groups()[0]))
        print('Collected {0}'.format(code))

    # make all the plots
    for k in ans:
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.hist(ans[k], tb.binHisto(ans[k])[1])
        ax.set_title(k)
        ax.set_ylabel('Occurrences')
        ax.set_xlabel('Execution Time [s]')
        fig.savefig(k + '.png')
        print('Plotted {0}'.format(k + '.png'))
