#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import os
import os.path
import DBUtils2
import datetime
import hashlib
import Version
import re

import DBlogging


# TODO look at the logging  of these and fix it, broke the messages printed out, probably because Exception __init__isnt called
class ReadError(Exception):
    """Exception that a file is not readable by the script, probably doesnt exist

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("ReadError raised")


class FilenameError(Exception):
    """
    Exception especially for created filenames showing that they are wrong

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 26-Oct-2010 (BAL)
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("FilenameError raised")


class WriteError(Exception):
    """Exception that a file is not writeable by the script, probably doesnt exist or in a ro directory

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("WriteError raised")

class InputError(Exception):
    """Exception that input is bad to the DiskFile class

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("InputError raised")


class DigestError(Exception):
    """Exception that is thrown by calcDigest.
    #TODO maybe just combine this with ReadError for the current purpose

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("DigestError raised")

class NoParseMatch(Exception):
    """Exception that is thrown when a file in incoming does not parse to any mission

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """
    def __init__(self, *params):
        DBlogging.dblogger.error("NoParseMatch raised")


class Diskfile(object):
    """
    Diskfile class contains methods for dealing with files on disk,
    all parsing for what mission files belog to is contined in here
    to add a new mission code must be added here.

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """

    def __init__(self,
                 infile,
                 parse=False):
        """
        setup a Diskfile class, takes in a filename and creates a params dict ro hold information about the file
        then tests to see what mission the file is from

        @param infile: a file to create a diskfile around
        @type infile: str

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 05-Oct-2010 (BAL)
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

        dbu = DBUtils2.DBUtils2('Test')
        dbu._openDB()
        dbu._createTableObjects()
        self.dbu = dbu

        if parse:
            self.parseAll()


    def checkAccess(self):
        """
        A few simple tests of the input file to be sure the script has the correct access


        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 05-Oct-2010 (BAL)
        """
        # need both read and write access
        self.READ_ACCESS = os.access(self.infile, os.R_OK)
        self.WRITE_ACCESS = os.access(self.infile, os.W_OK)
        if not self.READ_ACCESS:
            raise(ReadError("file is not readable, does it exist?"))
        if not self.WRITE_ACCESS:
            raise(WriteError("file is not writeable, wont be able to move it to proper location"))
        DBlogging.dblogger.debug("Access Checked out OK")



    formats = ['%MISSION', '%SPACECRAFT', '%PRODUCT', '%Y',
               '%y' , '%m' , '%d' , '%VERSION', '%b', '%j', '%H' , '%M', '%s' ,
               '%MILLI', '%MICRO', '%IGNORE', '%QACODE']



    def _isValidMission(self, m_in):
        return self.dbu._getMissions()


    def makeProductFilename(self, productID, date, version, qacode = None):
        """
        go through the DB and make a filename from the product format string
        """
        if not isinstance(version, Version.Version):
            rasie(InputError("Version must be an instance of a Version object"))
        if not isinstance(date, (datetime.datetime, datetime.date)):
            rasie(InputError("date must be an instance of a date or datetime  object"))
        if not qacode in ['ok', 'ignore', 'problem', None]:
            rasie(InputError("qacode invalid, can be ok, ignore, problem, or None "))


        filename = self.dbu._getProductFormats(productID)[0] # just for the format
        mission, satellite, instrument, product, product_id = self.dbu._getProductNames(productID)

        filename=filename.replace('%MISSION',  mission)
        filename=filename.replace('%SPACECRAFT',   satellite)
        filename=filename.replace('%PRODUCT', product )
        filename=filename.replace('%VERSION',str(version))
        filename=filename.replace('%Y', date.strftime('%Y'))
        filename=filename.replace('%m',date.strftime('%m'))
        filename=filename.replace('%d',date.strftime('%d'))
        filename=filename.replace('%INSTRUMENT', instrument)
        filename = filename.replace('%y', date.strftime('%y'))
        # TODO can be made more restrictive
        filename=filename.replace('%j',date.strftime('%j'))
        filename=filename.replace('%H', date.strftime('%H'))
        filename=filename.replace('%M',date.strftime('%M'))
        filename=filename.replace('%MILLI', date.strftime('%f')[0:3])
        filename=filename.replace('%MICRO',date.strftime('%f')[3:6])
        if qacode != None:
            filename=filename.replace('%QACODE', qacode)

        if self.figureProduct(filename) != productID:
            raise(FilenameError("Created filename did not match convention"))

        DBlogging.dblogger.debug("Filename: %s created" % (filename))

        return filename


    def figureProduct(self, filename = None):
        """
        go through the db and figure out what product we have
        codes are defined in Document XXX
        %MISSION – the mission name in its entirety
        %SPACECRAFT – the spacecraft name
        %PRODUCT – the product name from the database
        %INSTRUMENT – the instrument name from the database
        %Y – 4-digit year (1900-2200)
        %y – 2-digit year (58-57)
        %m – 2-digit month of year (1-12)
        %d – 2 digit day of month (1-31)
        %VERSION – version string (e.g. v1.1.1)
        %b – month name
        %j – 3-diiut day of year (0-366)
        %H – 2-digit hour
        %M – 2-digit minute
        %s – 2-digut second
        %MILLI – 3-digit millisecond
        %MICRO – 3-digit microsecond
        %IGNORE – ignore this section can be anything
        %QACODE – Q/A code form pngwalk (OK, PROBLEM, IGNORE)
        """
        ## if filename != None:
        ##     # get just the filename not the path
        ##     filename = os.path.basename(filename)

        #TODO is this the best way to do this?
        # step through each product format looking for that can match the input filename
        p_formats = self.dbu._getProductFormats()
        prods = self.dbu._getProductNames()
        # these two are not required to be in the same order, so we need to do something about that.
        
        # this checks for a DB error where the INstrument Product Link was not filled in
        if len(p_formats) != len(prods):
            raise(DBUtils2.DBError('self.dbu._getProductFormats() and self.dbu._getProductNames(), differnet length, check instrumentproductlink'))

        missions = zip(*prods)[0]
        satellites = zip(*prods)[1]
        instruments = zip(*prods)[2]
        products = zip(*prods)[3]
        p_ids = zip(*p_formats)[1]
        formats = zip(*p_formats)[0]

        matches = []
        for i in range(len(products)):
            # this format has which fields in it?
            expression = r'^' + formats[i] + '$'

            # TODO this can be cleaned up...
            expression=expression.replace('%MISSION',  prods[i][0])
            expression=expression.replace('%SPACECRAFT',   prods[i][1])
            expression=expression.replace('%PRODUCT', prods[i][3] )
            expression=expression.replace('%VERSION','\d.\d.\d')
            expression=expression.replace('%Y','(19|2\d)\d\d')
            expression=expression.replace('%m','(0\d|1[0-2])')
            expression=expression.replace('%d','[0-3]\d')
            expression=expression.replace('%INSTRUMENT', prods[i][2])
            expression = expression.replace('%y', '\d\d')
            # TODO can be made more restrictive
            expression=expression.replace('%j','[0-3]\d\d')
            expression=expression.replace('%H','[0-2]\d')
            expression=expression.replace('%M','[0-6]\d')
            expression=expression.replace('%MILLI','\d{3}')
            expression=expression.replace('%MICRO','\d{3}')
            expression=expression.replace('%QACODE','(ok|ignore|problem)')
            #TODO what to do with the IGNORE code?  Maybe it doesnt need to exist
            if filename == None:
                DBlogging.dblogger.debug("Matching %d:%s against %s" % (prods[i][4], expression,  os.path.basename(self.filename)))
                if re.match(expression, os.path.basename(self.filename)):
                    matches.append(prods[i][4])
                    DBlogging.dblogger.debug("Match found for product: %d" % (matches[-1]))
            else:
                DBlogging.dblogger.debug("Matching %s against %s" % (expression,  os.path.basename(filename)))
                if re.match(expression, os.path.basename(filename)):
                    matches.append(prods[i][4])
                    DBlogging.dblogger.debug("Match found for product: %d" % (matches[-1]))

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise(DBUtils2.DBError("File %s matched more than one product, there is a DB error"))
        elif len(matches) == 1:
            self.mission = self.dbu._getProductNames(matches[0])[0]

            # go through and fill in the params dict with info
            # TODO much of this should come from the file, figure out how to handle that

            # build a date
            # need year month day from the format or year DOY
            format = self.dbu._getProductFormats(matches[0])[0]  # get the format again for the correct product
            if not ('%Y' in format ) or ('%y' in format):
                raise(DBUtils2.DBError("Format %s has no year field"% (format)))
            DBlogging.dblogger.debug("Found a year in %s" % (format))
            if not (('%m' in format or '%b' in format) and ('%d' in format)) or ('%j' in format):
                raise(DBUtils2.DBError("Format %s has no month/day or DOY  field"% (format)))
            DBlogging.dblogger.debug("Found month/day or DOY in  %s" % (format))
            # now that we know we have a date fill in those fields
            #TODO utc_end_time and utc_start_time should come from file or something
            if '%y' in format:
                pass # TODO there is a 2 digit year, do somehting
            else:
                splitter = format[format.find('%Y')-1]
                try:
                    year = filename.split(splitter)[-2][0:4]
                except AttributeError:
                    year = self.filename.split(splitter)[-2][0:4]
            if '%j' in format:
                pass # TODO do something with DOY info
            if '%m' in format or '%b' in format:
                if '%b' in format:
                    pass # TODO do something with a 3 char month name
                pass

            # do a larger less general case to see if we can: '%Y%m%d'
            if '%Y%m%d' in format:
                splitter = format[format.find('%Y%m%d')-1]
                try:
                    date = filename.split(splitter)[-2]
                except AttributeError:
                    date = self.filename.split(splitter)[-2]
                year = int(date[0:4])
                month = int(date[4:6])
                day = int(date[6:8])

            date = datetime.date(year, month, day)
            self.params['utc_file_date'] = date
            self.params['utc_start_time'] = datetime.datetime.combine(date, datetime.time(0,0,0))
            self.params['utc_stop_time'] = datetime.datetime.combine(date, datetime.time(23,59,59,999999))



            self.params['data_level'] = float(self.filename.split('_')[1][1])
            self.params['check_date'] = None
            self.params['verbose_provenance'] = None
            self.params['quality_comment'] = None
            self.params['caveats'] = None
            self.params['release_number'] = None
            try:
                mtime = os.path.getmtime(self.infile)
                self.params['file_create_date'] = datetime.datetime.fromtimestamp(mtime)
            except OSError:
                pass
            self.params['met_start_time'] = None
            self.params['met_stop_time'] = None
            self.params['exists_on_disk'] = True  # we are parsing it so it exists_on_disk
            self.params['quality_checked'] = None
            product_name = self.filename.split('-')[1].split('_')[1] + '_' + self.filename.split('-')[1].split('_')[2]
            # TODO where does the product_id get set?
            self.params['product_id'] = matches[0]
            # self.params['md5sum'] = calcDigest(self.infile)
            self.params['version'] = Version.Version(int(self.filename.split('_')[-1].split('.')[0][1:]),
                                                                                                    int(self.filename.split('.')[1]),
                                                                                                    int(self.filename.split('.')[2]))
            DBlogging.dblogger.debug("Returning product %d:%s" % (matches[0], self.dbu._getProductNames(matches[0])[3]))
            return matches[0]
        else:
            return None  # can't get here

def calcDigest( infile):
    """Calculate the MD5 digest from a file.

    @param file: path to the file
    @type file: string
    @return: hex digits of L{file}'s md5
    @rtype: string

    @author: Jon Niehof
    @organization: Los Alamos National Lab
    @contact: jniehof@lanl.gov

    @version: V1: 20-Sep-2010 (JN) - stolen from command parser (BAL)
    """
    m = hashlib.md5()
    try:
        with open(infile, 'rb') as f:
            m.update(f.read())
    except IOError:
        raise(DigestError("File not found"))

    DBlogging.dblogger.debug("digest calculated: %s, file: %s " % (m.hexdigest(), infile))

    return m.hexdigest()





