#!/usr/bin/env python

import datetime
import sys
from optparse import OptionParser

from dbprocessing import DButils
import dateutil.parser as dup

if __name__ == "__main__":
    usage = '%prog [yyyymmdd]'
    parser = OptionParser()
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    parser.add_option("-s", "--sc", dest="sc",
                      help="which spacecraft", default=None)

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    if options.mission is None:
        parser.error("No mission specified")

    if options.sc is None:
        parser.error("No spacecract specified [rbspa, rbspb, a, b]")

    if 'a' in options.sc.lower():
        prod = [37, 43, 47, 49, 50]
    elif 'b' in options.sc.lower():
        prod = [40, 46, 48, 51, 52]
    else:
        parser.error("Invalid spacecract specified [rbspa, rbspb, a, b]")

    dt = dup.parse(args[0])


    a = DButils.DButils(options.mission)

    file_ids = []
    for p in prod:
        tmp = a.getFiles_product_utc_file_date(p, dt)
        try:
            file_ids.extend(zip(*tmp)[0])
        except:
            continue

    for f in file_ids:
        print(a.getEntry('File', f).filename)
