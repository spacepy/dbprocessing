#!/usr/bin/env python2.6


"""
classes as backup to making reports
"""



"""
things included are:
    - number and list of files ingested
    - number and list of files requested for injestion
    - number and list of other products created
    - any errors or anomolies reported
"""

from functools import total_ordering
import os
import re

import dateutil.parser as dup

from dbprocessing import DBUtils2

dbu = DBUtils2.DBUtils2('rbsp')
dbu._openDB()
dbu._createTableObjects()

class logfile(object):
    """
    class to hold a datafile
    """
    def __init__(self, filename, timerange=None):
        """
        read in the file and collect what we need
        """
        if not os.path.isfile(filename):
            raise(ValueError('filename does not exist'))
        self.filename = filename
        self._logData = open(self.filename, 'r').readlines()
        self.filerange = self._firstLastDate()
        if timerange is not None:
            self.setTimerange(timerange)
        self.ingested = self._ingested()
        self.errorIngesting = self._errorIngesting()

    def setTimerange(self, timerange):
        if len(timerange) != 2:
            raise(ValueError('timerange must be a list/tuple of 2 datetime objects'))
        self.timerange = timerange

    def _firstLastDate(self):
        first = dup.parse(self._logData[0].split(',')[0])
        last = dup.parse(self._logData[-1].split(',')[0])
        return first, last

    def _ingested(self):
        """
        return list of files ingested
        file_id	filename	 product
        """
        lines = []
        for line in self._logData:
            m = re.search( r'\s-\sINFO\s-\sFile\s.*\sentered.*f\_id=\d*$' , line)
            if m:
                lines.append(ingested(line))
        return lines

    def _errorIngesting(self):
        """
        return a list of files that had an error ingesting
        """
        lines = []
        for line in self._logData:
            m = re.search('WARNING - \*\*ERROR\*\*', line)
            if m:
                lines.append(errorIngesting(line))
        return lines

@total_ordering
class ingested(object):
    def __init__(self, inStr):
        """
        pass in the line and parse it grabbing what we need
        """
        self.dt = dup.parse(inStr.split(',')[0])
        m = re.search( r'\s-\sINFO\s-\sFile\s(.*)\sentered' , inStr)
        self.filename = m.group(1)
        m = re.search( r'f\_id=(\d*)' , inStr)
        self.file_id = m.group(1)
        global dbu
        try:
            tb = dbu.getFileTraceback(self.file_id)
            self.product_name = tb['product'].product_name
        except:
            self.product_name = 'unknown; file not in db'

    def __eq__(self, other):
        return self.dt == other.dt

    def __gt__(self, other):
        return self.dt > other.dt


@total_ordering
class errorIngesting(object):
    def __init__(self, inStr):
        """
        pass in the lig line and parse it savinf what we want
        """
        self.dt = dup.parse(inStr.split(',')[0])
        self.filename = inStr.split()[-4] # this is hopefully always constant
        self.movedTo = inStr.split()[-1]

    def html(self, alt=False):
        """
        return a html string for this
        """
        if alt:
            outStr = '<tr class="alt">'
        else:
            outStr = '<tr>'
        for v in ['dt', 'filename', 'movedTo']:
            if v == 'dt':
                val = self.dt.isoformat()
            else:
                val = self.__getattribute__(v)
            outStr += '<td>{0}</td>'.format(val)
        outStr += '</tr>'
        return outStr

    def __eq__(self, other):
        return self.dt == other.dt

    def __gt__(self, other):
        return self.dt > other.dt

