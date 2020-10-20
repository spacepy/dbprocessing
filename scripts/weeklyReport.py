#!/usr/bin/env python


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
from optparse import OptionParser
import os
import re

import dateutil.parser as dup
import numpy as np

from dbprocessing import reports

def _getFiles(path, startDT, stopDT):
    path = os.path.expanduser(path)
    file_all = glob.glob(os.path.join(path, 'dbprocessing_log.log.*')) # get the rest
    files = []
    for f in file_all:
        tmp = re.findall(r'^.*dbprocessing_log\.log\.(\d\d\d\d\-\d\d\-\d\d)$', f)[0]
        if tmp <= stopDT and tmp >= startDT: # can do this on the strings
            files.append(f)
    return files

def _getData(files, startT, stopT):
    """
    read in the file and combine all the lists together
    """
    ingested = []
    commandsRun = []
    movedToError = []
    errors = []
    dummyStart = dummy(startT) # make a class so that the comparisons work
    dummyStop = dummy(stopT)
    dummyStop.dt = dummyStop.dt.replace(hour=23, minute=59, second=59)

    for f in files:
        lf = reports.logfile(f)
        print('read: {0}'.format(f))
        ingested.extend(lf.ingested)
        commandsRun.extend(lf.commandsRun)
        movedToError.extend(lf.movedToError)
        errors.extend(lf.errors)
    ingested = np.sort(ingested)
    i1 = bisect.bisect_left(ingested, dummyStart)
    i2 = bisect.bisect_right(ingested, dummyStop)
    print('\tFound {0} ingested files'.format(len(lf.ingested))),
    ingested = ingested[i1:i2]
    print('kept {0}'.format(len(lf.ingested)))
    commandsRun = np.sort(commandsRun) # no need to sort as it is a sublist
    i1 = bisect.bisect_left(commandsRun, dummyStart)
    i2 = bisect.bisect_right(commandsRun, dummyStop)
    print('\tFound {0} commands run'.format(len(lf.commandsRun))),
    commandsRun = commandsRun[i1:i2]
    print('kept {0}'.format(len(lf.commandsRun)))
    movedToError = np.sort(movedToError) # no need to sort as it is a sublist
    i1 = bisect.bisect_left(movedToError, dummyStart)
    i2 = bisect.bisect_right(movedToError, dummyStop)
    print('\tFound {0} files moved to error files'.format(len(lf.movedToError))),
    movedToError = movedToError[i1:i2]
    print('kept {0}'.format(len(lf.movedToError)))
    errors = np.sort(errors) # no need to sort as it is a sublist
    i1 = bisect.bisect_left(errors, dummyStart)
    i2 = bisect.bisect_right(errors, dummyStop)
    print('\tFound {0} errors'.format(len(lf.errors))),
    errors = errors[i1:i2]
    print('kept {0}'.format(len(lf.errors)))

    return ingested.tolist(), commandsRun.tolist(), movedToError.tolist(), errors.tolist()


def makeHTML(mission, outfile, ingested, commandsRun, movedtoerror, errors, startT, stopT):
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
    output.write('<h1>{0}</h1>\n'.format('Van Allen Probes - ECT'))
    output.write('<h2>{0}--{1}</h2>\n'.format(startT.isoformat(), stopT.replace(hour=23, minute=59, second=59).isoformat()))

    _writeHTML(ingested, output, mission, startT, stopT, 'Ingested Files')
    _writeHTML(commandsRun, output, mission, startT, stopT, 'Commands Run')
    _writeHTML(movedtoerror, output, mission, startT, stopT, 'Moved to Error')
    _writeHTML(errors, output, mission, startT, stopT, 'Errors')

    output.writelines(footer)
    output.close()

class dummy(object):
    def __init__(self, dt):
        self.dt = dt

def _writeHTML(inlist, output, mission, startT, stopT, headerStr):
    """
    make the html for this
    """
    output.write('<h2>{0}</h2>\n'.format(headerStr))
    output.write('<table style="border: medium none ; border-collapse: collapse;" border="0" cellpadding="0" cellspacing="0">\n')

    if inlist:
        # write out the header
        output.write(inlist[0].htmlheader())

        # and write all the data
        for i, idx in enumerate(inlist):
            if i % 2 == 0:
                output.write(idx.html(alt=False))
            else:
                output.write(idx.html(alt=True))
            output.write('\n')
    output.write('</table>\n')


if __name__ == "__main__":
    usage = \
    """
    Usage: {0} <input directory> <startTime> <stopTime> <filename>".format(sys.argv[0])
       -> directory with the dbprocessing_log.log files
       -> start date e.g. 2000-03-12
       -> stop date e.g. 2000-03-17
       -> filename to write out the report
    """
    parser = OptionParser(usage=usage)
    parser.add_option("-i", "", dest="i", action="store_true",
                      help="ingest mode", default=False)
    parser.add_option("-p", "", dest="p", action="store_true",
                      help="process mode", default=False)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission", default=None)
    (options, args) = parser.parse_args()
    if len(args) != 4:
        parser.error("incorrect number of arguments")

    if not re.match(r'^\d\d\d\d\-\d\d\-\d\d$', args[1]):
        parser.error("bad date format, start date")
    if not re.match(r'^\d\d\d\d\-\d\d\-\d\d$', args[2]):
        parser.error("bad date format, stop date")

    files = _getFiles(args[0], args[1], args[2])
    if len(files) == 0:
        parser.error("No log files in directory: {0}".format(args[0]))
    print('Making report from: \n\t{0}'.format('\n\t'.join(files)))

    startT = dup.parse(args[1])
    stopT = dup.parse(args[2])
    ingested, cmdrun, movedtoerror, errors = _getData(files, startT, stopT)
    print('TOTAL {0} ingested files'.format(len(ingested)))
    print('TOTAL {0} commands run'.format(len(cmdrun)))
    print('TOTAL {0} files moved to error'.format(len(movedtoerror)))
    print('TOTAL {0} errors'.format(len(errors)))

    #resort the ingested files by level and product_name and date
    ## fails as they are different types, I hear that new numpy will work
    #ind = np.lexsort(([v.level for v in ingested], [v.product_name for v in ingested], [v.dt for v in ingested]))
    #makeHTML('rbsp', args[3], ingested[ind].tolist(), cmdrun, movedtoerror, errors, startT, stopT)
    makeHTML('rbsp', args[3], ingested, cmdrun, movedtoerror, errors, startT, stopT)

