#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
    Exception that a file is not write able by the script, probably doesn't exist or in a ro directory

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

    .. note: maybe just combine this with ReadError for the current purpose

    """
    def __init__(self, *params):
        super(DigestError, self).__init__(*params)
        DBlogging.dblogger.error("DigestError raised")


class Diskfile(object):
    """
    Diskfile class contains methods for dealing with files on disk,
    all parsing for what mission files belong to is continued in here
    to add a new mission code must be added here.
    """

    def __init__(self, infile, dbu):
        """
        setup a Diskfile class, takes in a filename and creates a params dict to hold information about the file
        then tests to see what mission the file is from
        
        :param infile: a file to create a Diskfile around
        :type infile: str
        :param dbu: pass in the current session so that a new connection is not made
        :type dbu: :class:`.DButils`
        """
        self.infile = infile
        self.checkAccess()

        self.path = os.path.dirname(self.infile)
        self.filename = os.path.basename(self.infile)

        self.params = {}
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
        A few simple tests of the input file to be sure the script has the correct access
        """
        # need both read and write access
        self.READ_ACCESS = os.access(self.infile, os.R_OK)
        if not self.READ_ACCESS:
            DBlogging.dblogger.debug("{0} read access denied!".format(self.infile))
            raise(ReadError("file is not readable, does it exist? {0}".format(self.infile)))
        self.WRITE_ACCESS = os.access(self.infile, os.W_OK) | os.path.islink(self.infile)
        if not self.WRITE_ACCESS:
            DBlogging.dblogger.debug("{0} write access denied!".format(self.infile))
            raise(WriteError("file is not writeable, won't be able to move it to proper location: {0}".format(self.infile)))
#        DBlogging.dblogger.debug("{0} Access Checked out OK".format(self.infile))


def calcDigest(infile):
    """Calculate the SHA1 digest from a file.

    :param infile: Path to the file
    :type infile: str

    :return: Hex digits of the file, SHA1 (40 bytes)
    :rtype: str

    """
    m = hashlib.sha1()
    try:
        with open(infile, 'rb') as f:
            m.update(f.read())
    except IOError:
        raise(DigestError("File not found: {0}".format(infile)))
        
    DBlogging.dblogger.debug("digest calculated: {0}, file: {1} ".format(m.hexdigest(), infile))

    return m.hexdigest()
