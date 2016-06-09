# -*- coding: utf-8 -*-

"""
inspector requirements:
    - one product per inspector file
    - must implement a class called Inspector(inspector.inspector)
    - must implement the abstract method inspect(kwargs) # yes takes in a dict
    - must implement code_name = 'codename.py' at the self level
    - inspect() must return a complete DiskFile instance or False (no other or exceptions allowed)
        - this means populating the following items: (* user parameters)
            mission : string name of the mission (filled by base class)
            self.diskfile.params['filename'] : full path and filename (filled by base class)
            * self.diskfile.params['utc_file_date'] : date object (user)
            * self.diskfile.params['utc_start_time'] : datetime object (user)
            * self.diskfile.params['utc_stop_time'] : datetime object (user)
            self.diskfile.params['check_date'] : None (optional)
            * self.diskfile.params['verbose_provenance'] : string (user) (optional)
            * self.diskfile.params['quality_comment'] : string (user) (optional)
            * self.diskfile.params['caveats'] : string (user) (optional)
            * self.diskfile.params['release_number'] : int (user) (optional)
            self.diskfile.params['file_create_date'] : datetime object (filled by base class)
            * self.diskfile.params['met_start_time'] : long (user) (optional)
            * self.diskfile.params['met_stop_time'] : long (user) (optional)
            self.diskfile.params['exists_on_disk'] : bool (filled by base class)
            self.diskfile.params['quality_checked'] : bool (filled by base class) (optional)
            self.diskfile.params['product_id'] : long (filled by base class)
            self.diskfile.params['shasum'] : str (filled by base class) (optional)
            * self.diskfile.params['version'] : Version object (user)
            self.diskfile.params['filefilelink'] : long (filled by db)
            self.diskfile.params['filecodelink'] : long (filled by db)
            self.diskfile.params['newest_version'] : bool (filled by db)
            * self.diskfile.params['data_level'] : float (filled by db)

inspector suggestions:
"""
from __future__ import print_function

from abc import ABCMeta, abstractmethod
import datetime
import os
import re

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
            print("Inspector {0}:  set level to {1}, this is ignored and set by the product definition".format(self.code_name, self.diskfile.params['data_level']))
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
    go through the filename and extract the first valid YYYYMMDD as a datetime

    Parameters
    ==========
    filename : str
        filename to parse for a YYYYMMDD format

    Returns
    =======
    out : (None, datetime.datetime)
        the datetime found in the filename or None
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
    go through the filename and extract the first valid YYYYMM as a datetime.date

    Parameters
    ==========
    filename : str
        filename to parse for a YYYYMMDD format

    Returns
    =======
    out : (None, datetime.datetime)
        the datetime found in the filename or None
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
    if inval is valid YYYYMMDD return True, False otherwise
    """
    try:
        ans = datetime.datetime.strptime(inval, "%Y%m%d")
    except ValueError:
        return False
    if isinstance(ans, datetime.datetime):
        return True

def extract_Version(filename, basename=False):
    """
    go through the filename and pull out the fist valid vX.Y.Z and return as a
    Version object
    """
    res = re.search("[vV]\d+\.\d+\.\d+\.", filename)
    ver = None
    base = None
    if res:
        verstring = res.group()
        tmp = verstring.split('.')
        ver = Version.Version(tmp[0][1:], tmp[1], tmp[2])

    if not basename:
        return ver
    else:
        base = filename.split(verstring)[0]
        return ver, base





