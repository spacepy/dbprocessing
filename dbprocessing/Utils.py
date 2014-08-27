# -*- coding: utf-8 -*-
"""
Class to hold random utilities of use throughout this code
"""

import collections
import datetime
import re
import os
import sys

import dateutil.rrule # do this long so where it is from is remembered

import Version

# from spacepy
def progressbar(count, blocksize, totalsize, text='Download Progress'):
    """
    print a progress bar with urllib.urlretrieve reporthook functionality

    Examples
    ========
    >>> import spacepy.toolbox as tb
    >>> import urllib
    >>> urllib.urlretrieve(config['psddata_url'], PSDdata_fname, reporthook=tb.progressbar)
    """
    percent = int(count*blocksize*100/totalsize)
    sys.stdout.write("\r" + text + " " + "...%d%%" % percent)
    if percent == 100: print('\n')
    sys.stdout.flush()


# from http://stackoverflow.com/questions/434287/what-is-the-most-pythonic-way-to-iterate-over-a-list-in-chunks
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def unique(seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if x not in seen and not seen_add(x)]

def expandDates(start_time, stop_time):
    return dateutil.rrule.rrule(dateutil.rrule.DAILY, dtstart=start_time, until=stop_time)

def daterange_to_dates(daterange):
    """
    given a daterange return the dat objects for all days in the range
    """
    return [daterange[0] + datetime.timedelta(days=val) for val in
            xrange((daterange[1]-daterange[0]).days+1)]

def parseDate(inval):
    """
    given a date of the for yyyy-mm-dd parse to a datetime
    If the format is wrong ValueError is raised
    """
    return datetime.datetime.strptime(inval, '%Y-%m-%d')

def parseVersion(inval):
    """
    given a format of the form x.y.z parse to a Version
    """
    return Version.Version.fromString(inval)

def flatten(l):
    """
    flatten an irregularly nested list of lists
    thanks SO: http://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists-in-python
    """
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el

def toBool(value):
    if value in ['True', 'true', True, 1, 'Yes', 'yes']:
        return True
    else:
        return False

def toNone(value):
    if value in ['', 'None', 'none', 'NONE']:
        return None
    else:
        return value

def strargs_to_args(strargs):
    """
    read in the arguments string from the db and change to a dict
    """
    if strargs is None:
        return None
    kwargs = {}
    if isinstance(strargs, (list, tuple)): # we have multiple to deal with
        # TODO why was this needed?
        if len(strargs) == 1:
            kwargs = strargs_to_args(strargs[0])
            return kwargs
        for val in strargs:
            tmp = strargs_to_args(val)
            for key in tmp:
                kwargs[key] = tmp[key]
        return kwargs
    try:
        for val in strargs.split():
            tmp = val.split('=')
            kwargs[tmp[0]] = tmp[1]
    except (AttributeError, KeyError): # it was None
        pass
    return kwargs

def dirSubs(path, filename, utc_file_date, utc_start_time, version):
        """
        do any substitutions that are needed to put ting in the right place
        # Honored database substitutions used as {Y}{MILLI}{PRODUCT}
        #	Y: 4 digit year
        #	m: 2 digit month
        #	b: 3 character month (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)
        #	d: 2 digit day
        #	y: 2 digit year
        #	j: 3 digit day of year
        #	H: 2 digit hour (24-hour time)
        #	M: 2 digit minute
        #	S: 2 digit second
        #	VERSION: version string, interface.quality.revision
        #	DATE: the UTC date from a file, same as Ymd
        #	MISSION: the mission name from the db
        #	SPACECRAFT: the spacecraft name from the db
        #	PRODUCT: the product name from the db
        """
        if '{INSTRUMENT}' in path or '{SATELLITE}' in path or '{SPACECRAFT}' in path or '{MISSION}' in path or '{PRODUCT}' in path:
            ftb = self.dbu.getTraceback('File', filename)
            if '{INSTRUMENT}' in path : # need to replace with the instrument name
                path = path.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
            if '{SATELLITE}' in path : # need to replace with the instrument name
                path = path.replace('{SATELLITE}', ftb['satellite'].satellite_name)
            if '{SPACECRAFT}' in path : # need to replace with the instrument name
                path = path.replace('{SPACECRAFT}', ftb['satellite'].satellite_name)
            if '{MISSION}' in path:
                path = path.replace('{MISSION}', ftb['mission'].mission_name)
            if '{PRODUCT}' in path:
                path = path.replace('{PRODUCT}', ftb['product'].product_name)

        if '{Y}' in path:
            path = path.replace('{Y}', utc_file_date.strftime('%Y'))
        if '{m}' in path:
            path = path.replace('{m}', utc_file_date.strftime('%m'))
        if '{d}' in path:
            path = path.replace('{d}', utc_file_date.strftime('%d'))
        if '{b}' in path:
            months = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}
            path = path.replace('{b}', months[utc_file_date.month])
        if '{y}' in path:
            path = path.replace('{y}', utc_file_date.strftime('%y'))
        if '{j}' in path:
            path = path.replace('{j}', utc_file_date.strftime('%j'))
        if '{H}' in path:
            path = path.replace('{H}', utc_start_time.strftime('%H'))
        if '{M}' in path:
            path = path.replace('{M}', utc_start_time.strftime('%M'))
        if '{S}' in path:
            path = path.replace('{S}', utc_start_time.strftime('%S'))
        if '{VERSION}' in path:
            if isinstance(version, (unicode, str)):
                version = Version.Version.fromString(version)
            path = path.replace('{VERSION}', '{0}'.format(str(version)))
        if '{DATE}' in path:
             path = path.replace('{DATE}', utc_file_date.strftime('%Y%m%d'))
        return path

def split_code_args(args):
    """
    split a string with a bunch of commadn line arguments into a list
    as needed by Popen

    This is different thatn just split() since we have to keep options
    together with the flags
    e.g. "code -n hello outfile" -> [code, -n hello, outfile]
    """
    # just do the spit
    ans = args.split()
    # loop through and see if an index has just a -x (any letter)
    for ii, v in enumerate(ans):
        if re.match(r'\-\S', v): # found a single letter option
            pass

def processRunning(pid):
    """
    given a PID see if it is currently running

    @param pid: a pid
    @type pid: long

    @return: True if pid is running, False otherwise
    @rtype: bool

    @author: Brandon Craig Rhodes
    @organization: Stackoverflow
    http://stackoverflow.com/questions/568271/check-if-pid-is-not-in-use-in-python

    @version: V1: 02-Dec-2010 (BAL)
    """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True
