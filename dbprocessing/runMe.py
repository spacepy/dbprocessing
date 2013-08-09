# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 10:12:11 2012

@author: balarsen
"""
import glob
import os
import shutil
import subprocess
import tempfile
import traceback

import DBlogging
import DBStrings
import DBUtils
import dbprocessing
import Version

from Utils import strargs_to_args


class ProcessException(Exception):
    """Class for errors in running processes"""
    pass



def mk_tempdir(self, suffix='_dbprocessing'):
    """
    create a secure temp directory
    """
    tempdir = tempfile.mkdtemp(suffix)
    return tempdir

def rm_tempdir(tempdir):
    """
    remove the temp directory
    """
    name = tempdir
    shutil.rmtree(tempdir)
    tempdir = None
    DBlogging.dblogger.debug("Temp dir deleted: {0}".format(name))

def runner(runme):
    """
    decide what code and then run it
    """
    DBlogging.dblogger.debug("Testing if {0} can run: {1}".format(runme, runme.ableToRun))
    if not runme.ableToRun:
        return

    # if runme.filename is in the DB then we cannot run this.  #TODO figure out why this happens
    try:
        runme.dbu.getFileID(runme.filename)
    except:
        pass
    else:
        DBlogging.dblogger.debug("Not going to run the outfile is already in the db: {0}".format(runme.filename))
        return # if the exception did not happen return

    # make a directory to run the code
    tempdir = mk_tempdir('_dbprocessingRunMe')

    DBlogging.dblogger.debug("Created temp directory: {0}".format(tempdir))

    ## build the command line we are to run
    cmdline = [runme.codepath]

    ## get extra_params from the process
    if runme.extra_params:
        cmdline.extend(runme.extra_params)

    ## figure out how to put the arguments together
    if runme.args:
        cmdline.extend(runme.args)

    for i_fid in runme.input_files:
        cmdline.append(runme.dbu.getFileFullPath(i_fid))

    cmdline.append(os.path.join(tempdir, runme.filename))
    cmdline = [os.path.expandvars(v) for v in cmdline]

    DBlogging.dblogger.info("running command: {0}".format(' '.join(cmdline)))
    # TODO, think here on how to grab the output
    # TODO For a future revision think on adding a timeout ability to the subprocess
    #    see: http://stackoverflow.com/questions/1191374/subprocess-with-timeout
    #    for some code here
    try:
        print('cmdline', cmdline)
        subprocess.check_call(' '.join(cmdline), shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        # TODO figure out how to print what the return code was
        DBlogging.dblogger.error("Command returned a non-zero return code: {0}\n\t{1}".format(' '.join(cmdline), traceback.format_exc()))
        # assume the file is bad and move it to error
        runme.moveToError(runme.filename)
        rm_tempdir(tempdir) # clean up
        return None
    DBlogging.dblogger.debug("command finished")

    try:
        runme.moveToIncoming(os.path.join(tempdir, runme.filename))
    except IOError:
        glb = glob.glob(os.path.join(tempdir, runme.filename) + '*.png')
        if len(glb) == 1:
            runme.moveToIncoming(glb[0])

    runme._add_links(cmdline)

    rm_tempdir(tempdir) # clean up


class runMe(object):
    """
    class holds all the info it takes to run a process
    """
    def __init__(self, dbu, utc_file_date, process_id, input_files,):
        DBlogging.dblogger.debug("Entered runMe {0}, {1}, {2}, {3}".format(dbu, utc_file_date, process_id, input_files))

        self.filename = '' # initialize it empty
        self.ableToRun = False
        self.extra_params = []
        self.args = []
        self.dbu = dbu
        self.utc_file_date = utc_file_date
        self.process_id = process_id
        self.input_files = input_files
        # since we have a process do we have a code that does it?
        self.code_id = self.dbu.getCodeFromProcess(process_id, utc_file_date)
        if self.code_id is None: # there is no code to actually run we are done
            return
        self.codepath = self.dbu.getCodePath(self.code_id)
        if self.codepath is None: # there is no code to actually run we are done
            return
        DBlogging.dblogger.debug("Going to run code: {0}:{1}".format(self.code_id, self.codepath))

        self.out_prod = self.dbu.getEntry('Process', self.process_id).output_product
        self.data_level = self.dbu.getEntry('Product', self.out_prod).level # This is the level of the output product, sorts on this and date
        # grab the format
        format_str = self.dbu.getEntry('Product', self.out_prod).format
        # get the process_keywords from the file if there are any
        try:
            process_keywords = strargs_to_args([self.dbu.getEntry('File', fid).process_keywords for fid in input_files])
            for key in process_keywords:
                format_str = format_str.replace('{'+key+'}', process_keywords[key])
        except TypeError:
            pass

        ptb = self.dbu.getTraceback('Product', self.out_prod)

        ## need to build a version string for the output file
        ## this sets the interface version
        code_entry = self.dbu.getEntry('Code', self.code_id)
        code_version = code_entry.code_id
        output_interface_version = code_entry.output_interface_version

        fmtr = DBStrings.DBFormatter()
        # set the default version for the output file
        self.output_version = Version.Version(output_interface_version, 0, 0)

        ## we have a filename, now we need to increment versions as needed/appropriate to
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
                DBlogging.dblogger.debug("Code did change for file: {0}".format(self.filename))
                continue
            parentchange = self._parentsChanged(f_id_db)
            if parentchange is None: # this is an inconsitency mark it and move on
                DBlogging.dblogger.info("Parent was None for file: {0}".format(self.filename))
                break
            if parentchange:
                DBlogging.dblogger.debug("Parent did change for file: {0}".format(self.filename))
                continue
            return # if we get here then we are not going to run anything

        ## get extra_params from the process # they are split by 2 spaces
        args = self.dbu.getEntry('Process', self.process_id).extra_params
        if args is not None:
            args = args.replace('{DATE}', utc_file_date.strftime('%Y%m%d'))
            args = args.split('|')
            self.extra_params = args

        ## get arguments from the code
        args = self.dbu.getEntry('Code', self.code_id).arguments
        if args is not None:
            args = args.replace('{DATE}', utc_file_date.strftime('%Y%m%d'))
            args = args.split()
            for arg in args:
                # if 'input' not in arg and 'output' not in arg:
                self.args.append(arg)

        ## getting here means that we are going to be returning a full
        ##   class ready to run the process
        self.ableToRun = True

    def __str__(self):
        return "RunMe({0}, {1})".format(self.utc_file_date, self.process_id)

    __repr__ = __str__


    def _fileInDB(self):
        """
        check the filename we created and see if it is in the, if it is we will
        not process with that name
        """
        try:
            DBlogging.dblogger.debug("Filename: {0} check in db".format(self.filename))
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
            #attempt to figure it out and add one
            tb = self.dbu.getTraceback('File', self.filename)
            proc_id = self.dbu.getProcessFromOutputProduct(tb['product'].product_id)
            self.dbu.addFilecodelink(tb['file'].file_id, proc_id)
            db_code_id = self.dbu.getFilecodelink_byfile(f_id_db)
            DBlogging.dblogger.debug("f_id_db: {0}   db_code_id: {1}".format(f_id_db, db_code_id))

        # Go through an look to see if the code version changed
        if db_code_id != self.code_id: # did the code change
            DBlogging.dblogger.debug("code_id: {0}   db_code_id: {1}".format(self.code_id, db_code_id))
            ver_diff = (self.dbu.getCodeVersion(self.code_id) - self.dbu.getCodeVersion(db_code_id))
            if ver_diff == [0,0,0]:
                DBlogging.dblogger.error("two different codes with the same version ode_id: {0}   db_code_id: {1}".format(self.code_id, db_code_id))
                raise(DBUtils.DBError("two different codes with the same version ode_id: {0}   db_code_id: {1}".format(self.code_id, db_code_id)))
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
        ** this is decided by the parents only have revision then revision inc
            if a parent has a quality inc then inc quality
        ** if there are extra parents then we want to rerun with a new quality
        """
        parents = self.dbu.getFileParents(f_id_db)
        if not parents:
            DBlogging.dblogger.info("db_file: {0} did not have any parents".format(f_id_db,))
            return None

        DBlogging.dblogger.debug("db_file: {0} has parents: {1}".format(f_id_db,
               [p.file_id for p in parents]))

        # if there are more input files now then we need to reprocess
        if len(self.input_files) != len([p.file_id for p in parents]):
            # inc quality and go back
            self._incVersion([0,1,0])
            return True

        quality_diff = False
        revision_diff = False
        for parent in parents:
            if not parent.newest_version: # if a parent is no longer newest we need to inc
                # this might need to go over all the dates in the time range
                fls = self.dbu.getFiles_product_utc_file_date(parent.product_id, parent.utc_file_date)
                ind = zip(*fls)[0].index(parent.file_id) # get the index of the file id in the output
                vers = zip(*fls)[1]
                mx_v = max(vers)
                df = mx_v - vers[ind]
                if df[1]:
                    quality_diff = True
                    DBlogging.dblogger.debug("parent: {0} had a quality difference, will reprocess child".format(parent.file_id))
                elif df[2]:
                    revision_diff = True
                    DBlogging.dblogger.debug("parent: {0} had a revision difference, will reprocess child".format(parent.file_id))
        if quality_diff:
            self._incVersion([0,1,0])
        elif revision_diff:
            self._incVersion([0,0,1])
        if quality_diff or revision_diff:
            return True
        else:
            return False

    def moveToIncoming(self, fname):
        """
        Moves a file from location to incoming

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 18-Apr-2012 (BAL)
        """
        inc_path = self.dbu.getIncomingPath()
        if os.path.isfile(os.path.join(inc_path, os.path.basename(fname))):
        #TODO do I really want to remove old version:?
            os.remove( os.path.join(inc_path, os.path.basename(fname)) )
        shutil.move(fname, inc_path + os.sep)
        DBlogging.dblogger.info("moveToIncoming: {0} {1}".format(fname, inc_path))

    def moveToError(self, fname):
        """
        Moves a file from incoming to error

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entered moveToError: {0}".format(fname))

        path = self.dbu.getErrorPath()
        if os.path.isfile(os.path.join(path, os.path.basename(fname) ) ):
        #TODO do I really want to remove old version:?
            os.remove( os.path.join(path, os.path.basename(fname) ) )
            DBlogging.dblogger.warning("removed {0}, as it was under a copy".format(os.path.join(path, os.path.basename(fname) )))
        if path[-1] != os.sep:
            path = path+os.sep
        try:
            shutil.move(fname, path)
        except IOError:
            DBlogging.dblogger.error("file {0} was not successfully moved to error".format(os.path.join(path, os.path.basename(fname) )))
        else:
            DBlogging.dblogger.info("moveToError {0} moved to {1}".format(fname, path))

    def _add_links(self, cmdline):
        """
        add the filefilelink and filecodelink and verbose provenance
        """
        # need to add the current file to the DB so that we have the filefilelink and filecodelink info
        pq = dbprocessing.ProcessQueue(self.dbu.mission)
        current_file = os.path.join(self.dbu.getIncomingPath(), self.filename)
        df = pq.figureProduct(current_file) # uses all the inspectors to see what product a file is
        if df is None:
            DBlogging.dblogger.error("{0} did not have a product".format(current_file))
            self.moveToError(current_file)
            return
        df.params['verbose_provenance'] = ' '.join(cmdline)
        f_id = pq.diskfileToDB(df)
        ## here the file is in the DB so we can add the filefilelink an filecodelinks
        if f_id is not None: # None comes back if the file goes to error
            self.dbu.addFilecodelink(f_id, self.code_id)
            for val in self.input_files: # add a link for each input file
                self.dbu.addFilefilelink(f_id, val)




