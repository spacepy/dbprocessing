# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 10:12:11 2012

@author: balarsen
"""
import imp
import glob
import os
import os.path
import shutil
import subprocess
import tempfile

import DBfile
import DBlogging
import DBStrings
import DBqueue
import DBUtils
import Utils
import Version

from dbprocessing import _strargs_to_args

class runMe(object):
    """
    class holds all teh info it takes to run a process
    TODO find a better name
    """
    def __init__(self, dbu, utc_file_date, process_id, input_files,
                 incCode=True, incFiles=True):
        self.ableToRun = False
        self.incCode = incCode
        self.incFiles = incFiles
        self.utc_file_date = utc_file_date
        self.process_id = process_id
        self.input_files = input_files
        # since we have a process do we have a code that does it?
        code_id = self.dbu.getCodeFromProcess(process_id)

        self.code_id = code_id
        self.codepath = self.dbu.getCodePath(self, code_id)
        if self.codepath is None: # there is no code to actually run we are done
            return
        DBlogging.dblogger.debug("Going to run code: {0}:{1}".format(self.code_id, self.codepath))

        self.out_prod = self.dbu.getEntry('Process', self.process_id).output_product
        # grab the format
        format_str = self.dbu.getEntry('Product', self.out_prod).format
        # get the process_keywords from the file if there are any
        try:
            process_keywords = _strargs_to_args([self.dbu.getEntry('File', fid).process_keywords for fid in input_files])
            for key in process_keywords:
                format_str = format_str.replace('{'+key+'}', process_keywords[key])
        except TypeError:
            pass

        ptb = self.dbu.getProductTraceback(self.out_prod)

        ## need to build a version string for the output file
        ## this sets the interface version
        code_version = self.dbu.getCodeVersion(code_id)

        fmtr = DBStrings.DBFormatter()
        # set the default version for the output file
        self.output_version = Version.Version(code_version.interface, 0, 0)

        ## we have a filename, now we need to incement versions as needed/appropiate to
        ## come up with a unique one

        # in this loop see if the file can be created i.e. ges not already exist in the db
        while True:
            # make the filename in the loop as output_version is manipulated below
            self.filename = fmtr.expand_format(format_str, {'SATELLITE':ptb['satellite'].satellite_name,
                                                         'PRODUCT':ptb['product'].product_name,
                                                         'VERSION':str(self.output_version),
                                                         'datetime':utc_file_date,
                                                         'INSTRUMENT':ptb['instrument'].instrument_name})
            DBlogging.dblogger.debug("Filename: %s created" % (self.filename))
            f_id_db = self._fileInDB()
            if not f_id_db: # if the file is not in the db lets make it
                break # lets call this the only way out of here that creates the runner
            codechange = self._codeVerChange(f_id_db)
            if codechange: # if the code did change maybe we have a unique
                continue
            parentchange =self._parentsChanged(f_id_db)
            if parentchange:
                continue
            return # if we get here then we are not going to run anything

        ## getting here means that we are going to be returning a full
        ##   class ready to run the process
        self.ableToRun = True

    def _fileInDB(self):
        """
        check the filenae we created and see if it is in the, if it is we will
        not process wih that name
        """
        try:
            f_id_db = self.dbu.getFileID(self.filename)
            DBlogging.dblogger.debug("Filename: {0} is in the DB, have to make different version".format(self.filename))
            return f_id_db
        except (DBUtils.DBError, DBUtils.DBNoData):
            DBlogging.dblogger.debug("Filename: {0} is not in the DB, can process".format(self.filename))
            return False

    def _codeVerChange(self, f_id_db):
        """
        since the file was in the db, is the code that made that the same as what
        we want to use now?
        """
        db_code_id = self.dbu.getFilecodelink_byfile(f_id_db)
        DBlogging.dblogger.debug("f_id_db: {0}   db_code_id: {1}".format(f_id_db, db_code_id))
        if db_code_id is None:
            # I think things will also crash here
            DBlogging.dblogger.error("Database inconsistency found!! A generated file {0} does not have a filecodelink".format(self.filename))

        # Go through an look to see if the code version changed
        if db_code_id != self.code_id: # did the code change
            DBlogging.dblogger.debug("code_id: {0}   db_code_id: {1}".format(self.code_id, db_code_id))
            ver_diff = (self.dbu.getCodeVersion(self.code_id) - self.dbu.getCodeVersion(db_code_id))
            self._incVersion(ver_diff)
            return True
        else:
            return False

    def _incVersion(self, ver_diff):
        """
        given a list of the difference in versions (comes form version-version)
        increment self.output_version
        """
        if ver_diff[2] > 0:
            self.output_version.incRevision()
            DBlogging.dblogger.debug("Filename: {0} incRevision()".format(self.filename))
        ## did the quality change?
        if ver_diff[1] > 0:
            self.output_version.incQuality()
            DBlogging.dblogger.debug("Filename: {0} incQuality()".format(self.filename))
        ## did the interface change?
        if ver_diff[0] > 0:
            self.output_version.incInterface()
            DBlogging.dblogger.debug("Filename: {0} incInterface()".format(self.filename))
        if any(ver_diff):
            return True
        else:
            return False

    def _parentsChanged(self, f_id_db):
        """
        go through a files parents and see if any of the parents have new versions
        not used in this processing, if so increment the correct version number
        ** this is decided by the parents only have revision then revisin inc
            if a parent has a quality inc then inc quality
        """
        parents = self.dbu.getFileParents(f_id_db)

        DBlogging.dblogger.debug("db_file: {0} has parents: {1}".format(f_id_db, parents))
        quality_diff = False
        revision_diff = False
        for parent in parents:
            if not parent.newest_version: # if a parent is no longer newest we need to inc
                fls = self.dbu.getFiles_product_utc_file_date(parent.product, parent.utc_file_date)
                ind = zip(*fls)[0].index(parent.file_id) # get the index of the file id in the output
                vers = zip(*fls)[1]
                mx_v = max(vers)
                df = mx_v - vers[ind]
                if df[1]:
                    quality_diff = True
                elif df[2]:
                    revision_diff = True
        if quality_diff:
            self._incVersion([0,1,0])
        elif revision_diff:
            self._incVersion([0,0,1])
        if quality_diff or revision_diff:
            return True
        else:
            return False
