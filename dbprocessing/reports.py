#!/usr/bin/env python
from __future__ import print_function


"""
classes as backup to making reports
"""



"""
things included are:
    - number and list of files ingested
    - number and list of files requested for ingestion that failed
    - number and list of other products created
    - list of commands run

TODO include later
    - any errors or anomalies reported (TODO not done)

"""

import os
import re

import dateutil.parser as dup
import numpy as np

from dbprocessing import DButils


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
        #setup the instance vars so they always exist
        self._logData = []
        self.error = []
        self.info = []
        self.debug = []
        self.ingested = []
        self.movedToError = []
        self.commandsRun = []
        self.errors = []

        self.filename = filename
        self._logData = open(self.filename, 'r').readlines()
        self.filerange = self._firstLastDate()
        if timerange is not None:
            self.setTimerange(timerange)
        self.error = self._error()
        self.info = self._info()
        self.debug = self._debug()

        self.ingested = self._ingested()
        self.movedToError = self._movedToError()
        self.commandsRun = self._commandsRun()
        self.errors = self._errors()

    def setTimerange(self, timerange):
        if len(timerange) != 2:
            raise(ValueError('timerange must be a list/tuple of 2 datetime objects'))
        self.timerange = timerange

    def _firstLastDate(self):
        first = dup.parse(self._logData[0].split(',')[0])
        last = dup.parse(self._logData[-1].split(',')[0])
        return first, last

    def _error(self):
        """
        a list of all the error entries
        """
        lines = []
        for line in self._logData:
            m = re.search( r'\s-\sERROR\s-\s' , line)
            if m:
                lines.append(line)
        return lines

    def _errors(self):
        """
        build up the class objects
        """
        lines = []
        for line in self.error:
            lines.append(errors(line))
        return lines

    def _info(self):
        """
        a list of all the INFO entries
        """
        lines = []
        for line in self._logData:
            m = re.search( r'\s-\sINFO\s-\s' , line)
            if m:
                lines.append(line)
        return lines

    def _debug(self):
        """
        a list of all the DEBUG entries
        """
        lines = []
        for line in self._logData:
            m = re.search( r'\s-\sDEBUG\s-\s' , line)
            if m:
                lines.append(line)
        return lines

    def _ingested(self):
        """
        return list of files ingested
        file_id	filename	 product
        """
        lines = []
        for line in self.info:
            m = re.search( r'\s-\sINFO\s-\sFile\s.*\sentered\sin\sDB.*f\_id=\d*$' , line)
            if m:
                lines.append(ingested(line))
        return lines

    def _movedToError(self):
        """
        all the files that were moved to error
        """
        lines = []
        for line in self.info:
            m = re.search(r'INFO\s-\smoveToError', line)
            if m:
                lines.append(movedToError(line))
        return lines

    def _commandsRun(self):
        """
        return a list of the unique commands run
        """
        lines = []
        for line in self.info:
            m = re.match( r'^.*\sINFO\s\-\srunning command\:\s.*$' , line)
            if m:
                lines.append(commandsRun(line))

        names = [v.filename for v in lines]
        uniq, ind = np.unique(names, return_index=True)
        return [lines[v] for v in ind]

class HTMLbase(object):
    def __eq__(self, other):
        try:
            return self.dt == other.dt
        except TypeError:
            return self.dt.strftime('%Y-%m-%d') == other.dt

    def __ne__(self, other):
        try:
            return self.dt != other.dt
        except TypeError:
            return self.dt.strftime('%Y-%m-%d') != other.dt

    def __gt__(self, other):
        try:
            return self.dt > other.dt
        except TypeError:
            return self.dt.strftime('%Y-%m-%d') > other.dt

    def __ge__(self, other):
        try:
            return self.dt >= other.dt
        except TypeError:
            return self.dt.strftime('%Y-%m-%d') >= other.dt

    def __lt__(self, other):
        try:
            return self.dt < other.dt
        except TypeError:
            return self.dt.strftime('%Y-%m-%d') < other.dt

    def __le__(self, other):
        try:
            return self.dt <= other.dt
        except TypeError:
            return self.dt.strftime('%Y-%m-%d') <= other.dt

class commandsRun(HTMLbase):
    def __init__(self, inStr):
        """
        pass in the line and parse it grabbing what we need
        """
        global dbu
        self.dt = dup.parse(inStr.split(',')[0])
        m = re.search( r'^.*\sINFO\s\-\srunning command\:\s(.*)$' , inStr.strip() )
        self.filename = m.group(1).split()[0]
        # get the process name
#        dbu.session.query(dbu.Code).filter_by(filename = os.path.basename(self.filename))

    def htmlheader(self):
        """
        return a string html header
        """
        outStr = '<tr>'
        for attr in ['filename', ]:
            outStr += '<th>{0}</th>'.format(attr)
        outStr += '</tr>\n'
        return outStr

    def html(self, alt=False):
        """
        return a html string for this
        """
        if alt:
            outStr = '<tr class="alt">'
        else:
            outStr = '<tr>'
        for v in ['filename', ]:
            if v == 'dt':
                val = self.dt.isoformat()
            else:
                val = self.__getattribute__(v)
            outStr += '<td>{0}</td>'.format(val)
        outStr += '</tr>'
        return outStr



class ingested(HTMLbase):
    def __init__(self, inStr):
        """
        pass in the line and parse it grabbing what we need
        """
        global dbu
        self.dt = dup.parse(inStr.split(',')[0])
        m = re.search( r'\s-\sINFO\s-\sFile\s(.*)\sentered' , inStr.strip())
        self.filename = m.group(1)
        m = re.search( r'f\_id=(\d*)' , inStr)
        self.file_id = m.group(1)
        try:
            tb = dbu.getTraceback('File', self.file_id)
            self.product_name = tb['product'].product_name
            self.level = tb['file'].data_level
        except:
            self.product_name = 'unknown; file not in db'
            self.level = None

    def htmlheader(self):
        """
        return a string html header
        """
        outStr = '<tr>'
        for attr in ['dt', 'file_id', 'filename', 'product_name', 'level']:
            outStr += '<th>{0}</th>'.format(attr)
        outStr += '</tr>\n'
        return outStr

    def html(self, alt=False):
        """
        return a html string for this
        """
        if alt:
            outStr = '<tr class="alt">'
        else:
            outStr = '<tr>'
        for v in ['dt', 'file_id', 'filename', 'product_name', 'level']:
            if v == 'dt':
                val = self.dt.isoformat()
            else:
                val = self.__getattribute__(v)
            outStr += '<td>{0}</td>'.format(val)
        outStr += '</tr>'
        return outStr


class movedToError(HTMLbase):
    def __init__(self, inStr):
        """
        pass in the lig line and parse it saving what we want
        """
        self.dt = dup.parse(inStr.split(',')[0])
        self.filename = os.path.basename(inStr.split()[-4]) # this is hopefully always constant

    def htmlheader(self):
        """
        return a string html header
        """
        outStr = '<tr>'
        for attr in ['dt', 'filename', 'movedTo']:
            outStr += '<th>{0}</th>'.format(attr)
        outStr += '</tr>\n'
        return outStr

    def html(self, alt=False):
        """
        return a html string for this
        """
        if alt:
            outStr = '<tr class="alt">'
        else:
            outStr = '<tr>'
        for v in ['dt', 'filename']:
            if v == 'dt':
                val = self.dt.isoformat()
            else:
                val = self.__getattribute__(v)
            outStr += '<td>{0}</td>'.format(val)
        outStr += '</tr>'
        return outStr


class errors(HTMLbase):
    def __init__(self, inStr):
        """
        parse the error and collect what we want
        """
        self.dt = dup.parse(inStr.split(',')[0])
        m = re.findall( r'^.*,\d\d\d\s\-\s(.*)\s\-\sERROR\s\-\s(.*)$' , inStr.strip())
        self.codename = m[0][0]
        self.errormsg = m[0][1]

    def htmlheader(self):
        """
        return a string html header
        """
        outStr = '<tr>'
        for attr in ['dt', 'Codename', 'Error message']:
            outStr += '<th>{0}</th>'.format(attr)
        outStr += '</tr>\n'
        return outStr

    def html(self, alt=False):
        """
        return a html string for this
        """
        if alt:
            outStr = '<tr class="alt">'
        else:
            outStr = '<tr>'
        for v in ['dt', 'codename', 'errormsg']:
            if v == 'dt':
                val = self.dt.isoformat()
            else:
                val = self.__getattribute__(v)
            outStr += '<td>{0}</td>'.format(val)
        outStr += '</tr>'
        return outStr
