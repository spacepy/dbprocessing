#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Information regarding a file as stored on disk."""

from __future__ import absolute_import
from __future__ import print_function

import glob
import hashlib
import os

from . import DBlogging

# .. todo look at the logging  of these and fix it, broke the messages printed out, probably because Exception __init__isn't called
class ReadError(Exception):
    """
    Exception that a file is not readable by the script, probably doesn't exist

    """
    def __init__(self, *params):
        super(ReadError, self).__init__(*params)
        DBlogging.dblogger.error("ReadError raised")


class FilenameError(Exception):
    """
    Exception especially for created filenames showing that they are wrong

    """
    def __init__(self, *params):
        super(FilenameError, self).__init__(*params)
        DBlogging.dblogger.error("FilenameError raised")


class WriteError(Exception):
    """
    Exception that a file is not write able by the script

    probably doesn't exist or in a ro directory
    """
    def __init__(self, *params):
        super(WriteError, self).__init__(*params)
        DBlogging.dblogger.error("WriteError raised")

        
class InputError(Exception):
    """
    Exception that input is bad to the DiskFile class
    """
    def __init__(self, *params):
        super(InputError, self).__init__(*params)
        DBlogging.dblogger.error("InputError raised")


class DigestError(Exception):
    """
    Exception that is thrown by calcDigest.

    Notes
    -----
    Maybe just combine this with ReadError for the current purpose
    """
    def __init__(self, *params):
        super(DigestError, self).__init__(*params)
        DBlogging.dblogger.error("DigestError raised")


class Diskfile(object):
    """
    Diskfile class contains methods for dealing with files on disk
    
    All parsing for what mission files belong to is continued in here
    to add a new mission code must be added here.
    """

    def __init__(self, infile, dbu):
        """
        setup a Diskfile class

        takes in a filename and creates a params dict to hold information
        about the file then tests to see what mission the file is from
        
        Parameters
        ----------
        infile : :class:`str`
            Full path to file to create a Diskfile around
        dbu : :class:`.DButils`
            Pass in the current session so that a new connection is not made
        """
        self.infile = infile
        """Path to the input file. Not validated to be either relative
           or absolute. (:class:`str`)"""
        self.checkAccess()

        self.path = os.path.dirname(self.infile)
        self.filename = os.path.basename(self.infile)

        self.params = {}
        """Parameters of this file, i.e. metadata. (:class:`dict`)"""
        self.params['filename'] = self.filename
        self.params['utc_file_date'] = None
        self.params['utc_start_time'] = None
        self.params['utc_stop_time'] = None
        self.params['data_level'] = None
        self.params['check_date'] = None
        self.params['verbose_provenance'] = None
        self.params['quality_comment'] = None
        self.params['caveats'] = None
        self.params['file_create_date'] = None
        self.params['met_start_time'] = None
        self.params['met_stop_time'] = None
        self.params['exists_on_disk'] = None
        self.params['quality_checked'] = None
        self.params['product_id'] = None
        self.params['shasum'] = None
        self.params['version'] = None
        self.params['process_keywords'] = None

        self.dbu = dbu
        self.mission = self.dbu.mission  # keeps track if we found a parsematch


    def __repr__(self):
        return "<Diskfile.Diskfile object: {0}>".format(self.infile)

    def __str__(self):
        out = ""
        for key, value in self.params.items():
            out += "params['{0}'] = {1}\n".format(key, value)
        return out

    def checkAccess(self):
        """
        Tests of the input file to be sure the script has the correct access.

        Takes no inputs; returns no values. Uses :data:`file` and raises
        exceptions if checks fail.

        Raises
        ------
        ReadError
            If file isn't readable.
        WriteError
            If file isn't writeable.
        """
        # need both read and write access
        self.READ_ACCESS = os.access(self.infile, os.R_OK)
        if not self.READ_ACCESS:
            DBlogging.dblogger.debug("{0} read access denied!".format(self.infile))
            raise ReadError("file is not readable, does it exist? {0}".format(self.infile))
        self.WRITE_ACCESS = os.access(self.infile, os.W_OK) | os.path.islink(self.infile)
        if not self.WRITE_ACCESS:
            DBlogging.dblogger.debug("{0} write access denied!".format(self.infile))
            raise WriteError("file is not writeable, won't be able to move it to proper location: {0}".format(self.infile))
#        DBlogging.dblogger.debug("{0} Access Checked out OK".format(self.infile))


def calcDigest(infile):
    """Calculate the SHA1 digest from a file.

    Parameters
    ----------
    infile : :class:`str`
        Path to the file.

    Returns
    -------
    hash : :class:`str`
        Hex digits of the file's SHA1 hash (40 bytes).
    """
    m = hashlib.sha1()
    try:
        with open(infile, 'rb') as f:
            for d in iter(lambda: f.read(1048576), b''):
                m.update(d)
    except IOError:
        raise DigestError("File not found: {0}".format(infile))

    res = m.hexdigest()
    DBlogging.dblogger.debug("digest calculated: {0}, file: {1} ".format(
        res, infile))

    return res
