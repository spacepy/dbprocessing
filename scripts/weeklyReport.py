#!/usr/bin/env python2.6


"""
product html pages siutable for a weekly report as generated from the
dbprocessing_log.log file
"""



"""
things included are:
    - number and list of files ingested
    - number and list of files requested for injestion
    - number and list of other products created
    - any errors or anomolies reported
"""
import bisect
import glob
import os
import sys

import dateutil.parser as dup
import numpy as np

from dbprocessing import reports

def _getFiles(path):
    path = os.path.expanduser(path)
    return glob.glob(os.path.join(path, 'dbprocessing_log.log*'))

def _getData(files, startT, stopT):
    """
    read in the file and combine all the lists together
    """
    ingested = []
    errorIngesting = []
    commandsRun = []
    dummyStart = dummy(startT)
    dummyStop = dummy(stopT)

    for f in files:
        lf = reports.logfile(f)
        ingested.extend(lf.ingested)
        errorIngesting.extend(lf.errorIngesting)
        commandsRun.extend(lf.commandsRun)
    ingested = np.sort(ingested)
    i1 = bisect.bisect_left(ingested, dummyStart)
    i2 = bisect.bisect_right(ingested, dummyStop)
    ingested = ingested[i1:i2]
    errorIngesting = np.sort(errorIngesting)
    i1 = bisect.bisect_left(errorIngesting, dummyStart)
    i2 = bisect.bisect_right(errorIngesting, dummyStop)
    errorIngesting = errorIngesting[i1:i2]
    commandsRun = np.sort(commandsRun) # no need to sort as it is a sublist
    i1 = bisect.bisect_left(commandsRun, dummyStart)
    i2 = bisect.bisect_right(commandsRun, dummyStop)
    commandsRun = commandsRun[i1:i2]

    return ingested.tolist(), errorIngesting.tolist(), commandsRun.tolist()


def makeHTML(mission, outfile, ingested, errorIngesting, commandsRun, startT, stopT):
    header = """
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
    <html><head>
      <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type"><title>DBprocessing</title>
        <style type="text/css">
            table, td, th
            {
            border:1px solid green;
            padding:3px 7px 2px 7px;
            }
            th
            {
            background-color:green;
            color:white;
            }
            tr.alt td
            {
            color:#000000;
            background-color:#EAF2D3;
            }
        </style>

    </head>


    <body>

    """
    footer = \
    """
    <br>
    <br>
    </body></html>
    """
    output = open(outfile, 'w')
    output.writelines(header)

    _writeHTML(ingested, output, mission, startT, stopT)
    _writeHTML(errorIngesting, output, mission, startT, stopT)
    _writeHTML(commandsRun, output, mission, startT, stopT)

    output.writelines(footer)
    output.close()

class dummy(object):
    def __init__(self, dt):
        self.dt = dt

def _writeHTML(inlist, output, mission, startT, stopT):
    """
    make the html for this
    """
    output.write('<h1>{0}</h1>\n'.format(mission))
    output.write('<h2>{0}--{1}</h2>\n'.format(startT.isoformat(), stopT.isoformat()))
    output.write('<h2>{0}</h2>\n'.format('Ingested Files'))
    output.write('<table style="border: medium none ; border-collapse: collapse;" border="0" cellpadding="0" cellspacing="0">\n')

    # write out the header
    output.write(inlist[0].htmlheader())

    # and write all the data
    for i, idx in enumerate(inlist):
        if i % 2 == 0:
            output.write(idx.html(alt=False))
        else:
            output.write(idx.html(alt=True))
    output.write('</table>\n')

def usage():
    """
    print the usage message out
    """
    print "Usage: {0} <input directory> <startTime> <stopTime> <filename>".format(sys.argv[0])
    print "   -> directory with the dbprocessing_log.log files"
    print "   -> start date e.g. 2000-03-12"
    print "   -> stop date e.g. 2000-03-17"
    print "   -> filename to write out the report"
    return

if __name__ == "__main__":
    if len(sys.argv) != 5:
        usage()
        sys.exit(2)
    files = _getFiles(sys.argv[1])
    if len(files) == 0:
        print "No log files in directory: {0}".format(sys.argv[1])
        sys.exit(2)
    startT = sys.argv[2]
    stopT = sys.argv[3]