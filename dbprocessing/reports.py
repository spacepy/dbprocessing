#!/usr/bin/env python
"""
Support for making reports from dbprocessing logs.

things included are:
    - number and list of files ingested
    - number and list of files requested for ingestion that failed
    - number and list of other products created
    - list of commands run

TODO include later
    - any errors or anomalies reported (TODO not done)

"""
from __future__ import print_function



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

        Parameters
        ----------
        filename : :class:`str`
           Log file to read
        timerange : :class:`~collections.abc.Sequence`, optional
            Start and end time of log timestamps to process, default all.
            (:class:`~datetime.datetime`)
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
        """All lines in the log with errors (:class:`list` of :class:`str`)"""

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
        """Sets the time range for this report

        Parameters
        ----------
        timerange : :class:`~collections.abc.Sequence`, optional
            Start and end time of log timestamps to process, default all.
            (:class:`~datetime.datetime`)
        """
        if len(timerange) != 2:
            raise(ValueError('timerange must be a list/tuple of 2 datetime objects'))
        self.timerange = timerange

    def _firstLastDate(self):
        """Get first and last date within the log file

        Returns
        -------
        :class:`tuple` of :class:`str`
            YYYYMMDD of first and last date
        """
        first = dup.parse(self._logData[0].split(',')[0])
        last = dup.parse(self._logData[-1].split(',')[0])
        return first, last

    def _error(self):
        """
        a list of all the error entries

        Returns
        -------
        :class:`list`
            All lines from the log that are for ERROR entries.
        """
        lines = []
        for line in self._logData:
            m = re.search( r'\s-\sERROR\s-\s' , line)
            if m:
                lines.append(line)
        return lines

    def _errors(self):
        """
        Convert all error lines to HTML report fragments

        Returns
        -------
        :class:`list`
            HTML report for each error (:class:`errors`).
        """
        lines = []
        for line in self.error:
            lines.append(errors(line))
        return lines

    def _info(self):
        """
        a list of all the INFO entries

        Returns
        -------
        :class:`list`
            All lines from the log that are for INFO entries.
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

        Returns
        -------
        :class:`list`
            All lines from the log that are for DEBUG entries.
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

        Returns
        -------
        :class:`list`
            HTML report fragments for all file ingestions reported in the
            log. (:class:`ingested`)
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

        Returns
        -------
        :class:`list`
            HTML report fragments for all file moves to error reported in the
            log. (:class:`movedToError`)
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

        Returns
        -------
        :class:`list`
            HTML report fragments for unique command executions reported in the
            log. (:class:`commandsRun`)
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
    """Support comparisons based on time stored in this object"""
    
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
    """Report on commands that have been run by the chain"""

    def __init__(self, inStr):
        """
        pass in the line and parse it grabbing what we need

        Parameters
        ----------
        inStr : :class:`str`
            Line from log file
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

        Returns
        -------
        :class:`str`
            HTML table row header
        """
        outStr = '<tr>'
        for attr in ['filename', ]:
            outStr += '<th>{0}</th>'.format(attr)
        outStr += '</tr>\n'
        return outStr

    def html(self, alt=False):
        """
        return a html string for this

        Parameters
        ----------
        alt : :class:`bool`, default False
            Alternate line (used to style every other table row differently).

        Returns
        -------
        :class:`str`
            HTML table row for this entry.
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
    """Report files that have been ingested in to the chain"""

    def __init__(self, inStr):
        """
        pass in the line and parse it grabbing what we need

        Parameters
        ----------
        inStr : :class:`str`
            Line from log file
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

        Returns
        -------
        :class:`str`
            HTML table row header
        """
        outStr = '<tr>'
        for attr in ['dt', 'file_id', 'filename', 'product_name', 'level']:
            outStr += '<th>{0}</th>'.format(attr)
        outStr += '</tr>\n'
        return outStr

    def html(self, alt=False):
        """
        return a html string for this

        Parameters
        ----------
        alt : :class:`bool`, default False
            Alternate line (used to style every other table row differently).

        Returns
        -------
        :class:`str`
            HTML table row for this entry.
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
    """Report files that have moved to the dbprocessing error directory"""

    def __init__(self, inStr):
        """
        pass in the log line and parse it saving what we want

        Parameters
        ----------
        inStr : :class:`str`
            Line from log file
        """
        self.dt = dup.parse(inStr.split(',')[0])
        self.filename = os.path.basename(inStr.split()[-4]) # this is hopefully always constant

    def htmlheader(self):
        """
        return a string html header

        Returns
        -------
        :class:`str`
            HTML table row header
        """
        outStr = '<tr>'
        for attr in ['dt', 'filename', 'movedTo']:
            outStr += '<th>{0}</th>'.format(attr)
        outStr += '</tr>\n'
        return outStr

    def html(self, alt=False):
        """
        return a html string for this

        Parameters
        ----------
        alt : :class:`bool`, default False
            Alternate line (used to style every other table row differently).

        Returns
        -------
        :class:`str`
            HTML table row for this entry.
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
    """Report all ERRORs logged by dbprocessing"""
    def __init__(self, inStr):
        """
        parse the error and collect what we want

        Parameters
        ----------
        inStr : :class:`str`
            Line from log file
        """
        self.dt = dup.parse(inStr.split(',')[0])
        m = re.findall( r'^.*,\d\d\d\s\-\s(.*)\s\-\sERROR\s\-\s(.*)$' , inStr.strip())
        self.codename = m[0][0]
        self.errormsg = m[0][1]

    def htmlheader(self):
        """
        return a string html header

        Returns
        -------
        :class:`str`
            HTML table row header
        """
        outStr = '<tr>'
        for attr in ['dt', 'Codename', 'Error message']:
            outStr += '<th>{0}</th>'.format(attr)
        outStr += '</tr>\n'
        return outStr

    def html(self, alt=False):
        """
        return a html string for this

        Parameters
        ----------
        alt : :class:`bool`, default False
            Alternate line (used to style every other table row differently).

        Returns
        -------
        :class:`str`
            HTML table row for this entry.
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
