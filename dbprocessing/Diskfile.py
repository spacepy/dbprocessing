#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import glob
import hashlib
import os
import os.path

import DBlogging

__version__ = '2.0.3'


# TODO look at the logging  of these and fix it, broke the messages printed out, probably because Exception __init__isn't called
class ReadError(Exception):
    """
    Exception that a file is not readable by the script, probably doesn't exist

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
        super(ReadError, self).__init__(*params)
        DBlogging.dblogger.error("ReadError raised")


class FilenameError(Exception):
    """
    Exception especially for created filenames showing that they are wrong

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
        super(FilenameError, self).__init__(*params)
        DBlogging.dblogger.error("FilenameError raised")


class WriteError(Exception):
    """
    Exception that a file is not write able by the script, probably doesn't exist or in a ro directory

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
        super(WriteError, self).__init__(*params)
        DBlogging.dblogger.error("WriteError raised")

        
class InputError(Exception):
    """
    Exception that input is bad to the DiskFile class

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
        super(InputError, self).__init__(*params)
        DBlogging.dblogger.error("InputError raised")


class DigestError(Exception):
    """
    Exception that is thrown by calcDigest.

    TODO
    ====
    maybe just combine this with ReadError for the current purpose


    `Author:` Brian Larsen, LANL

    """
    def __init__(self, *params):
        super(DigestError, self).__init__(*params)
        DBlogging.dblogger.error("DigestError raised")


class Diskfile(object):
    """
    Diskfile class contains methods for dealing with files on disk,
    all parsing for what mission files belong to is continued in here
    to add a new mission code must be added here.

    `Author:` Brian Larsen, LANL


    Parameters
    ==========
    infile : str
        a file to create a diskfile around
    dbu :  DBUtils
        pass in the current DBUtils session so that a new connection is not made

    Attributes
    ==========
    params : ``dict`` dictionary to hold all the parameters of the file

    """

    def __init__(self,
                 infile,
                 dbu):
        """
        setup a Diskfile class, takes in a filename and creates a params dict to hold information about the file
        then tests to see what mission the file is from

        Author: Brian Larsen, LANL
        """
        self.infile = infile
        try:
            self.checkAccess()
        except WriteError:
            print("No write access on {0}".format(self.infile))

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
        self.params['release_number'] = None
        self.params['file_create_date'] = None
        self.params['met_start_time'] = None
        self.params['met_stop_time'] = None
        self.params['exists_on_disk'] = None
        self.params['quality_checked'] = None
        self.params['product_id'] = None
        self.params['shasum'] = None
        self.params['version'] = None
        self.params['filefilelink'] = None
        self.params['filecodelink'] = None
        self.params['newest_version'] = None
        self.params['process_keywords'] = None

        self.dbu = dbu
        self.mission = self.dbu.mission  # keeps track if we found a parsematch


    def __repr__(self):
        return "<Diskfile.Diskfile object: {0}>".format(self.infile)

    __str__ = __repr__

    def checkAccess(self):
        """
        A few simple tests of the input file to be sure the script has the correct access
        """
        # need both read and write access
        self.READ_ACCESS = os.access(self.infile, os.R_OK)
        if not self.READ_ACCESS:
#            # TODO this is a pngwalk hack
#            glb = glob.glob(self.infile + '*.png')
#            if len(glb) == 1:
#                self.infile = glb[0]
#            else:
            raise(ReadError("file is not readable, does it exist? {0}".format(self.infile)))
        self.WRITE_ACCESS = os.access(self.infile, os.W_OK)
        if not self.WRITE_ACCESS:
            DBlogging.dblogger.debug("{0} Access denied!".format(self.infile))
            raise(WriteError("file is not writeable, won't be able to move it to proper location: {0}".format(self.infile)))
#        DBlogging.dblogger.debug("{0} Access Checked out OK".format(self.infile))


def calcDigest( infile):
    """Calculate the SHA1 digest from a file.

    `Author:` Jon Niehof, LANL

    .. _file:

    Parameters
    ==========
    file : str
        path to the file

    Returns
    =======
    out : str
        hex digits of the file, SHA1 (40 bytes)

    """
    m = hashlib.sha1()
    try:
        with open(infile, 'rb') as f:
            m.update(f.read())
    except IOError:
        raise(DigestError("File not found: {0}".format(infile)))
        
    DBlogging.dblogger.debug("digest calculated: {0}, file: {1} ".format(m.hexdigest(), infile))

    return m.hexdigest()
