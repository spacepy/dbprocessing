#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import datetime
import hashlib
import imp
import os
import os.path
import re

from spacepy import pycdf

import DBlogging
import DBUtils2
import Version

__version__ = '2.0.3'


# TODO look at the logging  of these and fix it, broke the messages printed out, probably because Exception __init__isnt called
class ReadError(Exception):
    """
    Exception that a file is not readable by the script, probably doesnt exist

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("ReadError raised")


class FilenameError(Exception):
    """
    Exception especially for created filenames showing that they are wrong

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("FilenameError raised")


class WriteError(Exception):
    """
    Exception that a file is not writeable by the script, probably doesnt exist or in a ro directory

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("WriteError raised")

class InputError(Exception):
    """
    Exception that input is bad to the DiskFile class

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
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
        DBlogging.dblogger.error("DigestError raised")

class NoParseMatch(Exception):
    """Exception that is thrown when a file in incoming does not parse to any mission

    `Author:` Brian Larsen, LANL
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("NoParseMatch raised")


class Diskfile(object):
    """
    Diskfile class contains methods for dealing with files on disk,
    all parsing for what mission files belog to is contined in here
    to add a new mission code must be added here.

    `Author:` Brian Larsen, LANL


    Parameters
    ==========
    infile : str
        a file to create a diskfile around
    dbu :  DBUtils2
        pass in the current DBUtils2 session so that a new connection is not made

    Attributes
    ==========
    params : ``dict`` dictionary to hold all the parameters of the file

    """

    def __init__(self,
                 infile,
                 dbu,
                 parse=False):
        """
        setup a Diskfile class, takes in a filename and creates a params dict to hold information about the file
        then tests to see what mission the file is from



        `Author:` Brian Larsen, LANL
        """

        DBlogging.dblogger.info("Entered Diskfile")


        self.infile = infile
        self.checkAccess()

        self.path = os.path.split(self.infile)[0]
        self.filename = os.path.split(self.infile)[1]

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
        self.params['md5sum'] = None
        self.params['version'] = None
        self.params['filefilelink'] = None
        self.params['filecodelink'] = None
        self.params['newest_version'] = None

        self.mission = None  # keeps track if we found a parsematch

        # this keeps opening connecitons
        #dbu = DBUtils2.DBUtils2('Test')
        #dbu._openDB()
        #dbu._createTableObjects()
        self.dbu = dbu

        if parse:
            self.parseAll()


    def checkAccess(self):
        """
        A few simple tests of the input file to be sure the script has the correct access

        `Author:` Brian Larsen, LANL
        """
        # need both read and write access
        self.READ_ACCESS = os.access(self.infile, os.R_OK)
        self.WRITE_ACCESS = os.access(self.infile, os.W_OK)
        if not self.READ_ACCESS:
            raise(ReadError("file is not readable, does it exist?"))
        if not self.WRITE_ACCESS:
            raise(WriteError("file is not writeable, wont be able to move it to proper location"))
        DBlogging.dblogger.debug("Access Checked out OK")



    def makeProductFilename(self, productID, date, version, qacode = None):
        """
        go through the DB and make a filename from the product format string
        """
        if not isinstance(version, Version.Version):
            raise(InputError("Version must be an instance of a Version object"))
        if not isinstance(date, (datetime.datetime, datetime.date)):
            raise(InputError("date must be an instance of a date or datetime  object"))
        if not qacode in ['ok', 'ignore', 'problem', None]:
            raise(InputError("qacode invalid, can be ok, ignore, problem, or None "))


        filename = self.dbu._getProductFormats(productID)[0] # just for the format
        mission, satellite, instrument, product, product_id = self.dbu._getProductNames(productID)

        if qacode == None:
            qacode = 'ok'
        filename = self.dbu.format(filename,
                                   MISSION=mission,
                                   SPACECRAFT=satellite,
                                   PRODUCT=product,
                                   VERSION=str(version),
                                   INSTRUMENT=instrument,
                                   QACODE=qacode,
                                   datetime=date)
#        if self.params['product_id'] != productID:
#            raise(FilenameError("Created filename did not match convention"))

        DBlogging.dblogger.debug("Filename: %s created" % (filename))

        return filename


    def figureProduct(self):
        """
        This funtion imports the inspectors and figures out whcih inspectors claim the file
        """
        act_insp = self.dbu.getActiveInspectors()
        claimed = []
        for code, arg in act_insp:
            try:
                inspect = imp.load_source('inspect', code)
            except IOError, msg:
                DBlogging.dblogger.error("Inspector: {0} not found: {1}".format(code, msg))
                continue
            if arg != None:
                ver = inspect.inspect(self.filename, **arg)
            else:
                ver = inspect.inspect(self.filename)           
            if ver:
                self.params['version'] = ver
                claimed.append(self.dbu.inspectorToProduct(code))
                DBlogging.dblogger.debug("Match found: {0}: {1}: {2}".format(self.filename, code, claimed[-1]))
                
        if len(claimed) == 0: # no match
            DBlogging.dblogger.info("File {0} found no inspector match".format(self.filename))
            return None        
        if len(claimed) > 1:
            DBlogging.dblogger.error("File {0} matched more than one product, there is a DB error".format(self.filename))
            raise(DBUtils2.DBError("File {0} matched more than one product, there is a DB error".format(self.filename)))

        self.params['product_id'] = claimed[0] 
        return claimed[0]  # return the product number

    def populateParameters(self):
        """
        go through the file and populate the parameters needs for the db
        """
        self.mission = self.dbu._getProductNames(productID=self.params['product_id'])[0] # mission name is 0

        ## assume it is a istp cdf
        try:
            cdf = pycdf.CDF(self.infile)
        except pycdf.CDFError:
            DBlogging.dblogger.info("File {0} is not a cdf".format(self.filename))
            print("Only CDF file are currently supported")
            return None

        ## get the start cdf time from the file
        try:
            self.params['utc_start_time'] = cdf['Epoch'][0]
            self.params['utc_stop_time']  = cdf['Epoch'][-1]
        except KeyError: # file does not have Epoch, better have MET
            DBlogging.dblogger.debug("File {0} does not have Epoch, reverting to MET".format(self.filename))
            try:
                self.params['met_start_time'] = cdf['MET'][0]
                self.params['met_stop_time']  = cdf['MET'][0]
            except KeyError:
                DBlogging.dblogger.error("File {0} does not have Epoch or MET".format(self.filename))
                return None # this will move it to error in ProcessQueue
                
        # TODO is this what we want here?
        self.params['utc_file_date'] = cdf['Epoch'][0]
        DBlogging.dblogger.debug("File {0}, utc_file_date set sub-optimally".format(self.filename))

        self.params['data_level'] = self.dbu.getProductLevel(self.params['product_id'])
        self.params['check_date'] = None
        self.params['verbose_provenance'] = None
        self.params['quality_comment'] = None
        self.params['caveats'] = None
        self.params['release_number'] = None
        self.params['file_create_date'] = datetime.datetime.fromtimestamp(os.path.getmtime(self.infile))

        self.params['exists_on_disk'] = True  # we are parsing it so it exists_on_disk
        self.params['quality_checked'] = None
        
        self.params['md5sum'] = calcDigest(self.infile)
        
        DBlogging.dblogger.debug("DiskFile object fully populated for {0}".format(self.filename))
        return True

def calcDigest( infile):
    """Calculate the MD5 digest from a file.

    `Author:` Jon Niehof, LANL

    .. _file:
    
    Parameters
    ==========
    file : str
        path to the file

    Returns
    =======
    out : str
        hex digits of the file_ md5

    """
    m = hashlib.md5()
    try:
        with open(infile, 'rb') as f:
            m.update(f.read())
    except IOError:
        raise(DigestError("File not found"))

    DBlogging.dblogger.debug("digest calculated: %s, file: %s " % (m.hexdigest(), infile))

    return m.hexdigest()
