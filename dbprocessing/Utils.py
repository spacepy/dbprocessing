# -*- coding: utf-8 -*-
"""
Various utilities of use throughout this code.
"""
from __future__ import print_function


from ast import literal_eval as make_tuple
try:
    import configparser
except ImportError: # Py2
    import ConfigParser as configparser
import collections
try:
    import collections.abc
except ImportError:  # Python 2
    collections.abc = collections
import datetime
import errno
import os
import re
import subprocess
import sys

import dateutil.rrule  # do this long so where it is from is remembered

from . import Version

try:
    str_classes = (str, bytes, unicode)
except NameError:
    str_classes = (str, bytes)


def datetimeToDate(dt):
    """
    Given an input datetime.datetime or datetime.date return a datetime.date
    
    Parameters
    ----------
    dt : :class:`~datetime.datetime` or :class:`~datetime.date`
        input to convert

    Returns
    -------
    :class:`~datetime.date`
        Same date as in ``dt``, but as a :class:`~datetime.date` object.
    """
    if hasattr(dt, 'minute'):
        return dt.date()
    else:
        return dt


def toDatetime(dt, end=False):
    """
    Convert a date, date string, or datetime to a datetime

    If a time is provided, passed through; otherwise set to start/end
    of day.

    Parameters
    ----------
    dt : :class:`~datetime.datetime`, :class:`~datetime.date`, or :class:`str`
        input to convert
    end : :class:`bool`, default False
        If input has no time, set to end of day (default to start of day)

    Returns
    -------
    :class:`~datetime.datetime`
        Input converted to :class:`~datetime.datetime`
    """
    if hasattr(dt, 'hour'): # Already datetime
        return dt # Already datetime
    dt = datetime.datetime.strptime(dt, '%Y-%m-%d') \
         if isinstance(dt, str_classes) \
         else datetime.datetime(*dt.timetuple()[:3])
    if end: # Last representable time in Python
        dt += datetime.timedelta(seconds=86399, microseconds=999999)
    return dt


def dateForPrinting(dt=None, microseconds=False, brackets='[]'):
    """
    Return a string of the date format for printing on the screen

    If dt is ``None`` return :meth:`~datetime.datetime.now`.

    Parameters
    ----------
    dt : :class:`~datetime.datetime`, default :meth:`~datetime.datetime.now`
        object to format
    microseconds : :class:`bool`, default False
        Include microseconds
    brackets : :class:`str`, optional
        Which brackets to encase the time in, default ``('[', ']')``

    Returns
    -------
    str
        Iso formatted string

    Examples
    --------
    >>> from dbprocessing.Utils import dateForPrinting
    >>> print("{0} Something occurred".format(dateForPrinting()))
    [2016-03-22T10:51:45]  Something occurred
    """
    if dt is None:
        dt = datetime.datetime.now()
    if not microseconds:
        dt = dt.replace(microsecond=0)
    return brackets[0] + dt.isoformat() + brackets[1]


def progressbar(count, blocksize, totalsize, text='Download Progress'):
    """
    Print a progress bar with urllib.urlretrieve reporthook functionality

    Taken from spacepy

    Parameters
    ----------
    count : :class:`float`
        The current count of the progressbar
    blocksize : :class:`float`
        The size of each block (mostly useful for file downloads)
    totalsize : :class:`float`
        The total size of the job, progress is ``count*blocksize*100/totalsize``
    text : :class:`str`, optional
        The text to print in the progressbar

    Examples
    --------
    >>> import spacepy.toolbox as tb
    >>> import urllib
    >>> urllib.urlretrieve(config['psddata_url'], PSDdata_fname, reporthook=tb.progressbar)
    """

    percent = int(count * blocksize * 100 / totalsize)
    sys.stdout.write("\r" + text + " " + "...%d%%" % percent)
    if percent == 100: print('\n')
    sys.stdout.flush()


def chunker(seq, size):
    """
    Return a long iterable in a tuple of shorter lists.

    Taken from https://stackoverflow.com/questions/434287/what-is-the-most-pythonic-way-to-iterate-over-a-list-in-chunks

    Parameters
    ----------
    seq : :class:`~collections.abc.Iterable`
        Iterable to split up
    size : :class:`int`
        Size of each split in the output, last one has the remaining elements
        of ``seq``

    Returns
    -------
    :class:`tuple`
        tuple of lists of the iterable ``seq`` split into ``len(seq)/size``
        segments
    """

    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def unique(seq):
    """
    Take a list and return only the unique elements in the same order

    Parameters
    ----------
    seq : :class:`list`
        List to return the unique elements of

    Returns
    -------
    :class:`list`
        List with only the unique elements
    """

    seen = set()
    seen_add = seen.add
    return [x for x in seq if x not in seen and not seen_add(x)]


def expandDates(start_time, stop_time):
    """
    Given a start and a stop date make all the dates in between

    Inclusive on the ends

    Parameters
    ----------
    start_time : :class:`~datetime.datetime`
        Date to start the list
    stop_time : :class:`~datetime.datetime`
        Date to end the list, inclusive

    Returns
    -------
    :class:`list` of :class:`~datetime.datetime`
        All the dates between start_time and stop_time
    """

    return dateutil.rrule.rrule(dateutil.rrule.DAILY, dtstart=start_time, until=stop_time)


def daterange_to_dates(daterange):
    """
    Given a daterange return the date objects for all days in the range

    Parameters
    ----------
    daterange : :class:`~collections.abc.Sequence` of :class:`~datetime.datetime`
        Start and stop dates

    Returns
    -------
    :class:`list`
        All the dates between ``daterange[0]`` and ``daterange[1]``
    """

    return [daterange[0] + datetime.timedelta(days=val) for val in
            range((daterange[1] - daterange[0]).days + 1)]


def parseDate(inval):
    """
    Given a date of the for yyyy-mm-dd parse to a datetime.

    This is just a wrapper around :meth:`~datetime.datetime.strptime`
    If the format is wrong ValueError is raised. 

    Parameters
    ----------
    inval : :class:`str`
        String date representation of the form YYYY-MM-DD

    Returns
    -------
    :class:`~datetime.datetime`
        datetime object parsed from the string
    """
    return datetime.datetime.strptime(inval, '%Y-%m-%d')


def parseVersion(inval):
    """
    Given a format of the form x.y.z parse to a Version

    This is a wrapper around :meth:`~dbprocessing.Version.Version.fromString`.

    Parameters
    ----------
    inval : :class:`str`
        String Version representation of the form xx.yy.zz

    Returns
    -------
    :class:`~dbprocessing.Version.Version`
        Version object parsed from the string
    """
    return Version.Version.fromString(inval)


def flatten(l):
    """
    Flatten an irregularly nested list of lists

    Taken from https://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists

    Parameters
    ----------
    l : :class:`list`
        Nested list of lists to flatten

    Returns
    -------
    :class:`list`
        Flattened list
    """
    for el in l:
        if isinstance(el, collections.abc.Iterable)\
           and not isinstance(el, str_classes):
            for sub in flatten(el):
                yield sub
        else:
            yield el


def toBool(value):
    """
    Returns true if passed 'True', 'true', True, 1, 'Yes', 'yes', 'Y', or 'y'

    Parameters
    ----------
    value
        Value to evaluate if true

    Returns
    -------
    :class:`bool`
    """
    return value in ['True', 'true', True, 1, 'Yes', 'yes', 'Y', 'y']


def toNone(value):
    """
    Returns None if passed '', 'None', 'none', or 'NONE'

    Parameters
    ----------
    value
        Value to evaluate if none
    
    Returns
    -------
    any
        :data:`None` or same input value
    """
    if value in [None, '', 'None', 'none', 'NONE']:
        return None
    else:
        return value


def strargs_to_args(strargs):
    """
    Read in the arguments string from the db and change to a dict

    Parameters
    ----------
    strargs : :class:`str`
        A string of arguments("foo=bar baz=qux")

    Returns
    -------
    :class:`dict`
        A dictionary of the arguments are their values
    """

    if strargs is None:
        return None
    kwargs = { }
    if isinstance(strargs, (list, tuple)):  # we have multiple to deal with
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
    except (AttributeError, KeyError, IndexError):  # it was None
        pass
    return kwargs


def dirSubs(path, filename, utc_file_date, utc_start_time, version, dbu=None):
    """
    Do any substitutions that are needed to put thing in the right place

    .. todo:: This may be useless/could be made more useful

    Honored substitutions used as {Y}{PRODUCT}{DATE}

    Parameters
    ----------
    path : str
        Path to the file
    filename : str
        Name of the time
    utc_file_date : :class:`~datetime.datetime`
        File's date
    utc_start_time : :class:`~datetime.datetime`
        File's start time
    version : :class:`~.Version`
        Version to substitute
    dbu : :class:`~.DButils.DButils`, optional
        Current database connection. If not specified, creates a new connection.

    Returns
    -------
    :class:`str`

    Notes
    -----
        Valid subsitutions are:
            * Y: 4 digit year   
            * m: 2 digit month  
            * b: 3 character month (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)  
            * d: 2 digit day
            * y: 2 digit year
            * j: 3 digit day of year
            * H: 2 digit hour (24-hour time)
            * M: 2 digit minute
            * S: 2 digit second
            * VERSION: version string, interface.quality.revision
            * DATE: the UTC date from a file, same as Ymd
            * MISSION: the mission name from the db
            * SPACECRAFT: the spacecraft name from the db
            * PRODUCT: the product name from the db
    
    """
    if '{INSTRUMENT}' in path or '{SATELLITE}' in path or '{SPACECRAFT}' in path or '{MISSION}' in path or '{PRODUCT}' in path:
        ftb = dbu.getTraceback('File', filename)
        if '{INSTRUMENT}' in path:  # need to replace with the instrument name
            path = path.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
        if '{SATELLITE}' in path:  # need to replace with the instrument name
            path = path.replace('{SATELLITE}', ftb['satellite'].satellite_name)
        if '{SPACECRAFT}' in path:  # need to replace with the instrument name
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
        months = { 1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct',
                   11: 'Nov', 12: 'Dec' }
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
        if isinstance(version, str_classes):
            version = Version.Version.fromString(version)
        path = path.replace('{VERSION}', '{0}'.format(str(version)))
    if '{DATE}' in path:
        path = path.replace('{DATE}', utc_file_date.strftime('%Y%m%d'))
    return path


def split_code_args(args):
    """
    Split a string with a bunch of command line arguments into a list

    As needed by Popen

    This is different than just split() since we have to keep options
    together with the flags.
    
    Parameters
    ----------
    args : :class:`str`

    Returns
    -------
    :class:`list` of :class:`str`

    See Also
    --------
    :mod:`shlex`

    Examples
    --------
    >>> split_code_args("code -n hello outfile")
    [code, -n hello, outfile]
    """
    # just do the spit
    ans = args.split()
    # loop through and see if an index has just a -x (any letter)
    for ii, v in enumerate(ans):
        if re.match(r'\-\S', v):  # found a single letter option
            ans[ii] = ans[ii] + " " + ans[ii + 1]
            del ans[ii + 1]

    return ans


def processRunning(pid):
    """
    Given a PID see if it is currently running.

    Taken from from https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python

    Parameters
    ----------
    pid : :class:`int`
        Process ID

    Returns
    -------
    :class:`bool`
        True if ``pid`` is running, False otherwise
    """
    if sys.platform == 'win32':
        out = subprocess.check_output([
            'tasklist', '/fi', 'PID eq {}'.format(pid)])
        return not out.startswith(b'INFO: No tasks are running')
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        elif err.errno == errno.EPERM:
            return True
        else:
            raise
    else:
        return True


def readconfig(config_filepath):
    """
    Read a database definition config file.

    Parameters
    ----------
    config_filepath : :class:`str`
        full path to the config file

    Returns
    -------
    :class:`dict`
        Dictionary of key value pairs from config files

    See Also
    --------
    :ref:`configurationfiles_addFromConfig`
    :ref:`scripts_addFromConfig_py`
    """
    # "Safe" deprecated in 3.2, but still present, so version is only way
    #  to avoid stepping on the deprecation. "Safe" preferred before 3.2
    cfg = (configparser.SafeConfigParser if sys.version_info[:2] < (3, 2)
           else configparser.ConfigParser)()
    cfg.read(config_filepath)
    sections = cfg.sections()
    # Read each parameter in turn
    ans = { }
    for section in sections:
        ans[section] = dict(cfg.items(section))
        for item in ans[section]:
            if 'input' in item:
                if '(' in ans[section][item]:
                    ans[section][item] = make_tuple(ans[section][item])
                else:
                    ans[section][item] = (ans[section][item], 0, 0)
    return ans
