# -*- coding: utf-8 -*-
from __future__ import print_function

"""
Inspector requirements:
    - One product per inspector file
    - Must implement a class called Inspector(inspector.inspector)
    - Must implement the abstract method inspect(kwargs)
    - Must implement code_name = 'codename.py' at the self level
    - inspect() must return anything that is not None for a valid match
        - It also must populate the following items:
            * self.diskfile.params['utc_file_date'] : date object
            * self.diskfile.params['utc_start_time'] : datetime object
            * self.diskfile.params['utc_stop_time'] : datetime object
            * self.diskfile.params['version'] : Version object
            * self.diskfile.params['check_date'] : None (optional)
            * self.diskfile.params['verbose_provenance'] : string (optional)
            * self.diskfile.params['quality_comment'] : string (optional)
            * self.diskfile.params['caveats'] : string (optional)
            * self.diskfile.params['release_number'] : int (optional)
            * self.diskfile.params['met_start_time'] : long (optional)
            * self.diskfile.params['met_stop_time'] : long (optional)
            * self.diskfile.params['quality_checked'] : bool (optional)
            * self.diskfile.params['process_keywords'] : str (optional)
"""
from __future__ import print_function

from abc import ABCMeta, abstractmethod
import datetime
import os
import re
import warnings

import DBlogging
import Diskfile
import Version

def EphemeralCallable(basetype=type):
    def _new_caller(cls, *args, **kwargs):
        return cls.__ephemeral_encapsulated__(*args, **kwargs)()
    class _EphemeralMetaclass(basetype):
        def __new__(cls, name, bases, dct):
            encbases = tuple([b.__ephemeral_encapsulated__
                              if hasattr(b, '__ephemeral_encapsulated__')
                              else b for b in bases])
            encaps = super(_EphemeralMetaclass, cls).__new__(
                cls, '_' + name + '_ephemeral_encapsulated', encbases, dct)
            return super(_EphemeralMetaclass, cls).__new__(
                cls, name, bases,
                {'__new__': _new_caller,
                 '__ephemeral_encapsulated__': encaps})
    return _EphemeralMetaclass

class inspector(object):
    """
    ABC for inspectors to be sure the user has implemented what is required
    and to provide for utility routes common to many inspectors
    """
    __metaclass__ = EphemeralCallable(ABCMeta)

    def __init__(self, filename, dbu, product, **kwargs):
        """"""
        DBlogging.dblogger.debug("Entered inspector {0} with kwargs: {1}".format(self.code_name, kwargs))
        self.dbu = dbu # give us access to DButils
        self.filename = filename
        self.basename = os.path.basename(self.filename)
        self.dirname = os.path.dirname(self.filename)
        self.product = product
        self.diskfile = Diskfile.Diskfile(self.filename, self.dbu)
        insp = self.inspect(kwargs)
        if insp is None:
            self.diskfile = None
        else:
            self._populate()

    @abstractmethod
    def inspect(self, filename, kwargs):
        """
        required method to populate the DiskFile object
        can take in some keyword arguments specified in the db
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
        do the check if the file is of the given type
        return None if not or the Diskfile object if so
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
        go through the filename and extract the first valid YYYYMMDD as a datetime
        """
        return extract_YYYYMMDD(self.filename)


def extract_YYYYMMDD(filename):
    """
    Go through the filename and extract the first valid YYYYMMDD as a datetime

    :param filename: Filename to parse for a YYYYMMDD format
    :type filename: str

    :return: The datetime found in the filename or None
    :rtype: datetime.datetime or None
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

    :param filename: Filename to parse for a YYYYMM format
    :type filename: str

    :return: The date found in the filename or None
    :rtype: datetime.date or None
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

    :param inval: Input to check if valid
    :type inval: str
    :return: Return True if valid, False otherwise
    :rtype: bool
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

    :param filename: Filename to check for version
    :type filename: str
    :keyword basename: True if to return version and the basename, false if just version
    :type basename: bool

    :return: The first valid version string as an object
    :rtype: :class:`.Version` or tuple(:class:`.Version`, base)
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





