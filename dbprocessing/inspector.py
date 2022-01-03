# -*- coding: utf-8 -*-

"""
Support for inspectors, which determine product type for a given file.

To write an inspector, create a Python module (i.e. a .py file).

   * This file will need to import :mod:`~dbprocessing.inspector` and
     :mod:`~dbprocessing.Version` (since it must create a
     :class:`~dbprocessing.Version.Version`).
   * This file may import :mod:`~dbprocessing.DBlogging` to permit logging
     of its work.
   * This file must contain a single class
   * The class must be named ``Inspector`` and inherit from
     :class:`inspector`.
   * The class must have a class member ``code_name`` with the name of the
     inspector file
   * The class must implement the :meth:`~inspector.inspect` method (see that
     documentation for details).

A :sql:table:`inspector` record must also then be created referencing the
inspector file and with any necessary keywords.

See Also
--------
:doc:`../inspector_examples`
"""
from __future__ import absolute_import
from __future__ import print_function

from abc import ABCMeta, abstractmethod
import datetime
import os
import re
import warnings

from . import DBlogging
from . import Diskfile
from . import Version
from . import DBstrings


class DefaultFields(dict):
    """Dict-like with defaults for the special fields used by DBformatter

    Any key not present returns a value that maintains the key reference
    and provides a match-all regex.
    """
    def __missing__(self, key):
        """Key not found, so return format-able key and match-all regex."""
        return ('{{{0}}}'.format(key), '.*')

    def __contains__(self, key):
        """Pretends key is always there, so always gets value, or default."""
        # The parser returns a spurious None for field name at end
        # of format string, so don't pretend we have a key for that.
        return key is not None


class DefaultFormatter(DBstrings.DBformatter):
    """Formatter that passes through any missing fields

    Basically does a match-all for constructing a regex. expand_format
    will also pass through the field name, but this will still fail
    on the final format() call.
    """
    # Data, not callable, can't use super.
    SPECIAL_FIELDS = DefaultFields(DBstrings.DBformatter.SPECIAL_FIELDS)


class inspector(object):
    """ ABC for inspectors to be sure the user has implemented what is required

    Provides utility routines common to many inspectors
    """
    code_name = None
    """Override this in child class, with the name of the inspector file
       (:class:`str`)"""

    def __init__(self, filename, dbu, product, **kwargs):
        """
        Parameters
        ----------
        filename : :class:`str`
            Path to file to inspect.
        dbu : :class:`.DButils`
            Open database connection.
        product : :class:`int`
            Product ID; verify if passed-in file is instance of this product.
        kwargs : :class:`dict`
            Keyword arguments passed through to :meth:`inspect` (as a dict).
        """
        DBlogging.dblogger.debug("Entered inspector {0} with kwargs: {1}".format(self.code_name, kwargs))
        self.dbu = dbu # give us access to DButils
        """Open database. (:class:`~dbprocessing.DButils.DButils`)"""
        self.filename = filename
        """Full path to the file being inspected. (:class:`str`)"""
        self.basename = os.path.basename(self.filename)
        """Filename (only, no directory) of the file being inspected.
        (:class:`str`)"""
        self.dirname = os.path.dirname(self.filename)
        """Full path to the directory containing the file. (:class:`str`)"""
        self.product = product
        """Product ID for which this inspector is being called.
           (:class:`int`)"""
        self.filenameformat = self.dbu.getEntry('Product', self.product).format
        """Format to match the filename. (:class:`str`)"""
        DBformatter = DefaultFormatter() #must instantiate class
        self.filenameregex = DBformatter.re(self.filenameformat)
        """Regular expression that will match a valid filename, derived from
           :data:`filenameformat`. (:class:`str`)"""
        self.diskfile = Diskfile.Diskfile(self.filename, self.dbu)
        """File metadata to populate. (:class:`.Diskfile.Diskfile`)"""
        insp = self.inspect(kwargs)
        if insp is None:
            self.diskfile = None
        else:
            self._populate()

    @abstractmethod
    def inspect(self, filename, kwargs):
        """Override this method to implement an inspector

        If the input file is not a match for this product, nothing else
        should be updated.

        If the input file is a match for the product, must update
        :data:`~dbprocessing.Diskfile.Diskfile.params` dict of
        :data:`diskfile` with the following keys, which map directly
        to columns of the :sql:table:`file` table in the database:

           utc_file_date (:class:`~datetime.date`)
              Characteristic date of the file.
              (:sql:column:`~file.utc_file_date`)

           utc_start time (:class:`~datetime.datetime`)
              Timestamp of first record in the file.
              (:sql:column:`~file.utc_start_time`)

           utc_stop time (:class:`~datetime.datetime`)
              Timestamp of last record in the file.
              (:sql:column:`~file.utc_stop_time`)

           version (:class:`~dbprocessing.Version.Version`)
              Version of the file.
              (:sql:column:`~file.interface_version`,
              :sql:column:`~file.quality_version`,
              :sql:column:`~file.revision_version`)

        The following keys may be updated, but are optional:

           check_date (:class:`~datetime.datetime`)
              :sql:column:`~file.check_date`

           verbose_provenance (:class:`str`)
              :sql:column:`~file.verbose_provenance`

           quality_comment (:class:`str`)
              :sql:column:`~file.quality_comment`

           caveats (:class:`str`)
              :sql:column:`~file.caveats`

           met_start_time (:class:`int`)
              :sql:column:`~file.met_start_time`

           met_stop_time (:class:`int`)
              :sql:column:`~file.met_stop_time`

           quality_checked (:class:`bool`)
              :sql:column:`~file.quality_checked`

           process_keywords (:class:`str`)
              :sql:column:`~file.process_keywords`

        The inspector may use the non-private instance attributes of
        :class:`this class <dbprocessing.inspector.inspector>`, including
        access to the database (although this should be done with care,
        as the inspector is called during active phases of processing).
        It is also expected that the inspector will open the file for reading;
        writing to the data file is highly discouraged at this point.

        Parameters
        ----------
        filename : str
            Not used; use :data:`filename` attribute instead.

        kwargs : dict
            Keyword arguments provided in :sql:column:`inspector.arguments`.

        Returns
        -------
        any
            ``None`` if the file passed in is not a match for the product,
            and anything else if it is.
        """
        return None

    def _populate(self):
        """
        populate the rest of the information to the diskfile
        """
        ptb = self.dbu.getTraceback('Product', self.product)
        self.diskfile.mission = ptb['mission'].mission_name
        self.diskfile.params['file_create_date'] = datetime.datetime.fromtimestamp(os.path.getmtime(self.diskfile.infile))
        self.diskfile.params['exists_on_disk'] = True  # we are parsing it so it exists_on_disk
        self.diskfile.params['shasum'] = Diskfile.calcDigest(self.diskfile.infile)
        self.diskfile.params['product_id'] = self.product
        if self.diskfile.params['data_level'] is not None:
            DBlogging.dblogger.info("Inspector {0}:  set level to {1}, this is ignored and set by the product definition".format(self.code_name, self.diskfile.params['data_level']))
            warnings.warn("Inspector {0}:  set level to {1}, this is ignored and set by the product definition".format(self.code_name, self.diskfile.params['data_level']))
        self.diskfile.params['data_level'] = self.dbu.getEntry('Product', self.product).level


    def __call__(self):
        """
        Do the check if the file is of the given product

        Returns
        -------
        :class:`.Diskfile`
            :data:`diskfile` if the file matches the product, or
            :data:`None` if it doesn't.
        """
        match = self.diskfile
        if match is not None:
            DBlogging.dblogger.debug("Checking the inspector has filled all the values: {0}".format(self.code_name))
        else:
            return None

        if self.diskfile.mission is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.mission is None".format(self.code_name))
        elif self.diskfile.params['filename'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['filename'] is None".format(self.code_name))
        elif self.diskfile.params['utc_file_date'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['utc_file_date'] is None".format(self.code_name))
        elif self.diskfile.params['utc_start_time'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['utc_start_time'] is None".format(self.code_name))
        elif self.diskfile.params['utc_stop_time'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['utc_stop_time'] is None".format(self.code_name))
        elif self.diskfile.params['data_level'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['data_level'] is None".format(self.code_name))
        elif self.diskfile.params['file_create_date'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['file_create_date'] is None".format(self.code_name))
        elif self.diskfile.params['exists_on_disk'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['exists_on_disk'] is None".format(self.code_name))
        elif self.diskfile.params['product_id'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['product_id'] is None".format(self.code_name))
        elif self.diskfile.params['version'] is None:
            match = None
            DBlogging.dblogger.debug("Inspector {0}:  self.diskfile.params['version'] is None".format(self.code_name))

        if match is None:
            DBlogging.dblogger.debug("No match found for inspector {0}: {1}".format(self.code_name, self.diskfile.filename))
        else:
            DBlogging.dblogger.info("Match found for inspector {0}: {1}".format(self.code_name, self.diskfile.filename))
        return match

    #==============================================================================
    # Helper routines
    #==============================================================================
    def extract_YYYYMMDD(self):
        """
        go through the filename, extract first valid YYYYMMDD as a datetime

        Returns
        -------
        :class:`~datetime.datetime`
            First date found in :data:`filename`, or :data:`None`.
        """
        return extract_YYYYMMDD(self.filename)


def extract_YYYYMMDD(filename):
    """
    Go through the filename and extract the first valid YYYYMMDD as a datetime

    Parameters
    ----------
    filename : :class:`str`
        Filename to parse for a YYYYMMDD format

    Returns
    -------
    :class:`~datetime.datetime`
        First date found in ``filename``, or :data:`None`.
    """
    # cmp = re.compile("[12][90]\d2[01]\d[0-3]\d")
    # return a datetime if there is one from YYYYMMDD
    try:
        dt = datetime.datetime.strptime(re.search("[12][90]\d\d[01]\d[0-3]\d", filename).group(), "%Y%m%d")
    except (ValueError, AttributeError): # there is not one
        return None
    if dt < datetime.datetime(1957, 10, 4, 19, 28, 34): # Sputnik 1 launch datetime
        dt = None
    # better not still be using this... present to help with random numbers combinations
    elif dt > datetime.datetime(2050, 1, 1):
        dt = None
    return dt

def extract_YYYYMM(filename):
    """
    Go through the filename and extract the first valid YYYYMM as a datetime

    Parameters
    ----------
    filename : :class:`str`
        Filename to parse for a YYYYMMDD format

    Returns
    -------
    :class:`~datetime.datetime`
        First day of first month found in ``filename``, or :data:`None`.
    """
    # cmp = re.compile("[12][90]\d2[01]\d[0-3]\d")
    # return a datetime if there is one from YYYYMMDD
    try:
        dt = datetime.datetime.strptime(re.search("[12][90]\d\d[01]\d", filename).group(), "%Y%m")
    except (ValueError, AttributeError): # there is not one
        return None
    if dt < datetime.datetime(1957, 10, 4, 19, 28, 34): # Sputnik 1 launch datetime
        return None
    # better not still be using this... present to help with random numbers combinations
    elif dt > datetime.datetime(2050, 1, 1):
        return None
    return dt.date()

def valid_YYYYMMDD(inval):
    """
    Checks if input is valid YYYYMMDD

    Parameters
    ----------
    inval : :class:`str`
        String to evaluate as possible YYYYMMDD

    Returns
    -------
    :class:`bool`
        ``True`` if valid YYYYMMDD.
    """
    try:
        ans = datetime.datetime.strptime(inval, "%Y%m%d")
    except ValueError:
        return False
    if isinstance(ans, datetime.datetime):
        return True

def extract_Version(filename, basename=False):
    """
    Go through the filename and pull out the first valid vX.Y.Z

    Parameters
    ----------
    filename : :class:`str`
        Filename to check for version

    Returns
    -------
    :class:`.Version`
        First valid version found in filename. If ``basename``,
        :class:`tuple` of version (:class:`.Version`) and
        basename (:class:`str`).

    Other Parameters
    ----------------
    basename : :class:`bool`, default False
        Include the basename after the version as well.
    """
    res = re.search("[vV]\d+\.\d+\.\d+\.", filename)
    ver = None
    base = None
    if res:
        verstring = res.group()
        tmp = verstring.split('.')
        ver = Version.Version(tmp[0][1:], tmp[1], tmp[2])
        if basename:
            base = filename.split(verstring)[0]
            return ver, base

    return ver





