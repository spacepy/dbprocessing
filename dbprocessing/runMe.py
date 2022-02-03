"""Support for running codes to make data files, as defined in database."""

from __future__ import print_function
from __future__ import absolute_import

import datetime
import glob
from operator import itemgetter, attrgetter
import os
import pdb
import re
import shutil
import subprocess
import tempfile
import time
import traceback

from . import DBlogging
from . import DBstrings
from . import DButils
from .inspector import extract_Version
from . import Utils
from .Utils import dateForPrinting as DFP
from . import Version


class ProcessException(Exception):
    """Class for errors in running processes"""
    pass


def mk_tempdir(suffix='_dbprocessing'):
    """
    Create a secure temp directory

    Returns
    -------
    :class:`str`
        Path to resulting directory.

    Other Parameters
    ----------------
    suffix : :class:`str`, default ``_dbprocessing``
        Suffix to include on the directory name
    """
    tempdir = tempfile.mkdtemp(suffix=suffix)
    DBlogging.dblogger.debug("Created temp directory: {0}".format(tempdir))
    return tempdir

def rm_tempdir(tempdir):
    """
    Remove the temp directory

    Parameters
    ----------
    :class:`str`
        Path to directory to remove.
    """
    name = tempdir
    shutil.rmtree(tempdir)
    tempdir = None
    DBlogging.dblogger.debug("Temp dir deleted: {0}".format(name))

def _extract_files(cmdline):
    """
    Given a command line extract out the files that are input to the process

    Parameters
    ----------
    cmdline: :class:`str` or :class:`~collections.abc.Sequence`
        Command line to parse

    Returns
    -------
    :class:`list` of :class:`str`
        All files named in the command line.
    """
    # is the input a list, if so make it a string
    if hasattr(cmdline, '__iter__'):
        cmdline = ' '.join(cmdline)
    files = []
    # the first argument of of course the code grab it and put it in
    splits = cmdline.split()
    files.append(os.path.abspath(splits[0]))
    splits = splits[1:] # drop the one we used
    # the last argument is the output file, it will not exist, drop it
    splits = splits[:-1]
    # loop over what is left and see if it looks like a file or an option
    for s in splits:
        # a long option will need another split
        if s.startswith('--'):
            tmp = s.split('=')
            if os.path.sep in tmp[-1]: # this looks like a file
                if ',' in tmp[-1]:
                    files.extend(tmp[-1].split(','))
                else:
                    files.append(tmp[-1])
        else:
            if os.path.sep in s: # this looks like a file
                if ',' in s:
                    files.extend(s.split(','))
                else:
                    files.append(s)
    return files

def _pokeFile(filename):
    """
    Given a filename open it non-blocking and see if it works

    Parameters
    ----------
    filename : :class:`str`
        Path to file

    Returns
    -------
    :class:`str`
        Status of the check: ``NOFILE``, ``OTHER``, ``FILE``, ``ERROR``.
    """
    try:
        fp = os.open(filename, os.O_NONBLOCK, os.O_RDONLY)
    except OSError:
        return 'NOFILE'
    except Exception:
        return 'OTHER'
    if fp>0: # this means it opened
        os.close(fp)
        return 'FILE'
    else: # was never opened so doesn't need to be closed
        #os.close(fp)
        return 'ERROR'

def _start_a_run(runme):
    """
    Given a runme that we want to start poke the all the files.

    This poke makes sure the automunter has them all up

    Intermediate steps:
        1. need to extract all the files that will be used for the process
           and poke them all with a :func:`os.open` with non-blocking. This
           will make sure the automounter has seen attempts on them all.
        2. Then if the open works close it and move all.  If it fails, note
           that and move on.
        3. Start the process after all opened and closed.

    Parameters
    ----------
    runme : :class:`runMe`
        Runner object to check.
    """
    # processes[subprocess.Popen(runme.cmdline, stdout=fp, stderr=fp)] = (runme, time.time(), fp )
    files2poke = _extract_files(runme.cmdline)
    for f in files2poke:
        ans = _pokeFile(f)
        if ans == 'NOFILE':
            DBlogging.dblogger.error("Command line referenced a file that did not exist {0}.  {1}"
                                     .format(f, runme.cmdline))
        elif ans == 'OTHER':
            DBlogging.dblogger.error("Command line referenced a file that did 'other' {0}.  {1}"
                                     .format(f, runme.cmdline))
        elif ans == 'ERROR':
            DBlogging.dblogger.error("Command line referenced a file that did not open {0}.  {1}"
                                     .format(f, runme.cmdline))
        elif ans == 'FILE':
            DBlogging.dblogger.debug("Command line referenced a file opened fine {0}.  {1}"
                                     .format(f, runme.cmdline))
        else:
            print("Could not have gotten here")
            raise RuntimeError("Should not have gotten here")


def runner(runme_list, dbu, MAX_PROC=2, rundir=None):
    """
    Go through a list of runMe objects and run them

    .. todo:: This function can be made a smart as one wants, for now it is
        not made to be smart, but flexible

    Parameters
    ----------
    runme_list : :class:`list` of :class:`runMe`
        List of runMe objects that need to be run.
    dbu : :class:`.DButils`
        Open database connection.
    MAX_PROC : :class:`int`, default 2
        Maximum number of processes to run at once.
    rundir : :class:`str`, optional
        Directory to run in, default use a freshly-created temp directory.

    Returns
    -------
    :class:`tuple` of :class:`int`
        number of processes that successfully completed, number of processes
        that failed.
    """
    ############################################################
    # 1) build up the command line and store in a commands list
    # 2) loop over the commands
    #  a) start up to MAX_PROC processes with subprocess.Popen
    #  b) poll that they are done or not and if they finish successfully
    #     i) True: add data to db
    #     ii) False: add errror messages
    ############################################################

    ## 11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
    # in make_command_line a tempdir gets created (self.tempdir) it will need to be cleaned
    for runme in runme_list:
        force = rundir is not None
        runme.make_command_line(force = force, rundir=rundir)

    # sort the runme_list on level and filename (which is like date and product and s/c together)
    runme_list.sort(key = lambda x: (x.data_level, x.filename))

    #########################################
    # 20140825 try another way of doing this
    # 1) grab a collection of all the output files
    # 2) make sure all are unique
    # 3) drop all but one of the non-unique ones (maintain order)
    # 4) grab a collection of all the input files (basename only)
    # 5) make the output file collection basenames only
    # 6) if there are any inputs in the outputs drop those processes
    #########################################
    print("{0} len(runme_list)={1}".format(DFP(), len(runme_list)))
    # 1
    outfiles = [os.path.basename(v.filename) for v in runme_list]
    # 2
    #Delete IF not blank AND it's already in seen (if not seen, add it to seen,
    #but don't delete)
    seen = set()
    delvals = [i for i in range(len(outfiles)) if outfiles[i] != '' and
               (outfiles[i] in seen or seen.add(outfiles[i]))]
    for i in delvals[::-1]:
        del runme_list[i]
    print("{0} len(runme_list)={1}".format(DFP(), len(runme_list)))


    # TODO For a future revision think on adding a timeout ability to the subprocess
    #    see: http://stackoverflow.com/questions/1191374/subprocess-with-timeout
    #    for some code here
    processes = {} # dict with the key as the Popen object containing a list of command line and start time

    n_good = 0 # number of processes successfully completed
    n_bad = 0 # number of processes failed

    #    while runme_list or processes:
    while runme_list or processes:
        while (len(processes) < MAX_PROC) and runme_list:
            runme = runme_list.pop(0) # pop from the list, it is sorted!!
            if runme.data_level == 5000: #RUN timebase
                runme.cmdline.pop(-1) #Chop the fake "output" file

            DBlogging.dblogger.info("Command: {0} starting".format(os.path.basename(' '.join(runme.cmdline))))

            """
            when we go to run a process capture all the stdout and stderr into a file in the running temp directory
            if the process is successful then it just gets removed with the directory, otherwise move it to the error
            directory
            """

            print("{0} Process starting ({2}): {1}".format(DFP(), ' '.join(runme.cmdline), len(runme_list)))
            if rundir is None:
                prob_name = os.path.join(runme.tempdir, runme.filename + '.prob')
            else:
                prob_name = os.path.join(rundir, runme.filename + '.prob')
            try:
                fp = open(prob_name, 'w')
                fp.write(' '.join(runme.cmdline))
                fp.write('\n\n')
                fp.write('-'*80)
                fp.write('\n\n')
                fp.flush()
            except IOError:
                DBlogging.dblogger.error("Could not create the prob file, so skipped {0}"
                                            .format(os.path.basename(' '.join(runme.cmdline))))
                #raise IOError("Could not create the prob file, so died {0}".format(os.path.basename(' '.join(runme.cmdline))))
                try:
                    rm_tempdir(runme.tempdir) # delete the tempdir
                except OSError:
                    pass
                continue # move to next process

            _start_a_run(runme)
            processes[subprocess.Popen(runme.cmdline, stdout=fp, stderr=fp)] = (runme, time.time(), fp )
            time.sleep(0.5)

        for p in list(processes.keys()):
            if p.poll() is None: # still running
                continue
            # OK process done, get the info from the dict
            rm, t, fp = processes[p] # unpack the tuple

            fp.close()
            if p.returncode != 0: # non zero return code FAILED
                DBlogging.dblogger.error("Command returned a non-zero return code ({1}): {0}"
                                         .format(' '.join(rm.cmdline), p.returncode))
                print("{0} Command returned a non-zero return code: {1}\n\t{2}".format(DFP(), ' '.join(rm.cmdline), p.returncode))

                if rundir is None:
                    rm.moveToError(fp.name)
                    # assume the file is bad and move it to error
                    rm.moveToError(os.path.join(rm.tempdir, rm.filename))
                    rm_tempdir(rm.tempdir) # delete the temp directory

                n_bad += 1

            elif p.returncode == 0: # p.returncode == 0  SUCCESS
                # this is not a perfect time since all the adding occurs before the next poll
                DBlogging.dblogger.info("Command: {0} took {1} seconds".format(os.path.basename(rm.cmdline[0]), time.time()-t))
                print("{0} Command: {1} took {2} seconds".format(DFP(), os.path.basename(rm.cmdline[0]), time.time()-t))

                if rundir is None: # if rundir then this is a test
                    if rm.data_level != 5000: # RUN timebases are allowed to not have files
                        rm.moveToIncoming(os.path.join(rm.tempdir, rm.filename))
                        rm._add_links(rm.cmdline)
                    rm_tempdir(rm.tempdir) # delete the temp directory

                print("{0} Process {1} FINISHED".format(DFP(), ' '.join(rm.cmdline)))
                n_good += 1
            else:
                raise ValueError("Should not have gotten here")

            # execution gets here if the process finished
            del processes[p]
        time.sleep(0.5)

    return n_good, n_bad


class runMe(object):
    """
    class holds all the info it takes to run a process

    Warnings
    --------
    As this object holds a reference to
    :class:`~dbprocessing.DButils.DButils`, both directly and within
    a reference to :class:`~dbprocessing.dbprocessing.ProcessQueue`,
    that database should be closed before the program terminates.
    Deleting this object will ordinarily suffice.
    """
    def __init__(self, dbu, utc_file_date, process_id, input_files, pq,
                 version_bump = None, force=False):
        """
        Parameters
        ----------
        dbu : :class:`.DButils`
            Open database connection.
        utc_file_date : :class:`~datetime.date`
            Characteristic date of the file being created.
        process_id : :class:`int`
            :sql:column:`~process.process_id` of process to run.
        input_files : :class:`list` of :class:`int`
            :sql:column:`~file.file_id` of all input files.
        pq : :class:`.ProcessQueue`
            ProcessQueue instance
        version_bump : :class:`int`, optional
            Which element of the output version to bump. Forces output if
            specified. Default: run only on changed inputs, and bump version
            according to the normal rules.
        force : :class:`bool`, default False
            Force processing regardless of version bumping or out-of-date.
        """
        DBlogging.dblogger.debug("Entered runMe {0}, {1}, {2}, {3}".format(dbu, utc_file_date, process_id, input_files))
        if isinstance(utc_file_date, datetime.datetime):
            utc_file_date = utc_file_date.date()

        self.filename = '' # initialize it empty
        self.ableToRun = False
        self.extra_params = []
        self.args = []
        self.dbu = dbu
        self.pq = pq # the ProcessQueue instance
        self.utc_file_date = utc_file_date
        self.process_id = process_id
        self.input_files = input_files
        self.version_bump = version_bump
        # since we have a process do we have a code that does it?
        self.code_id = self.dbu.getCodeFromProcess(process_id, utc_file_date)
        if self.code_id is None: # there is no code to actually run we are done
            DBlogging.dblogger.debug("Code_id is None: can't run")
            return
        self.codepath = self.dbu.getCodePath(self.code_id)
        if self.codepath is None: # there is no code to actually run we are done
            DBlogging.dblogger.debug("Codepath is None: can't run")
            return
        # get code version string
        version = self.dbu.getCodeVersion(self.code_id)
        version_st = '{}.{}.{}'.format(version.interface, version.quality,\
                                       version.revision)
        DBlogging.dblogger.debug("Going to run code: {0}:{1}".format(self.code_id, self.codepath))
        self.codepath = self.codepath.replace('{CODEVERSION}',version_st)
        self.codedir = os.path.dirname(self.codepath)

        process_entry = self.dbu.getEntry('Process', self.process_id)
        code_entry = self.dbu.getEntry('Code', self.code_id)
        output_interface_version = code_entry.output_interface_version

        # set the default version for the output file
        self.output_version = Version.Version(output_interface_version, 0, 0)

        if process_entry.output_timebase == "RUN":
            self.data_level = 5000
            self.filename = 'RUN_{0}_{1}'.format(process_entry.process_name, self.input_files[0])

            self.out_prod = -1 # Not sure if this is sane. There is no out_prod, but it's needed for runme.__eq__
        else:
            self.out_prod = process_entry.output_product
            ptb = self.dbu.getTraceback('Product', self.out_prod)
            self.data_level = ptb['product'].level # This is the level of the output product, sorts on this and date
            # grab the format
            format_str = ptb['product'].format
            # get the process_keywords from the file if there are any
            try:
                process_keywords = Utils.strargs_to_args([self.dbu.getEntry('File', fid).\
                                                          process_keywords for fid in input_files])
                for key in process_keywords:
                    format_str = format_str.replace('{'+key+'}', process_keywords[key])
            except TypeError:
                pass

            ## we have a filename, now we need to increment versions as needed/appropriate to
            ## come up with a unique one

            fmtr = DBstrings.DBformatter()
            # in this loop see if the file can be created i.e. ges not already exist in the db
            while True:
                # make the filename in the loop as output_version is manipulated below
                self.filename = fmtr.format(
                    format_str,
                    SATELLITE=ptb['satellite'].satellite_name,
                    PRODUCT=ptb['product'].product_name,
                    VERSION=str(self.output_version),
                    datetime=utc_file_date,
                    INSTRUMENT=ptb['instrument'].instrument_name)
                DBlogging.dblogger.debug("Filename: %s created" % (self.filename))
                if not force:
                    f_id_db = self._fileInDB()
                else:
                    f_id_db = False
                if not f_id_db: # if the file is not in the db lets make it
                    break # lets call this the only way out of here that creates the runner
                
                if self.version_bump == 0:
                    self.output_version.incInterface()
                    continue
                if self.version_bump == 1:
                    self.output_version.incQuality()
                    continue
                if self.version_bump == 2:
                    self.output_version.incRevision()
                    continue

                codechange = self._codeVerChange(f_id_db)
                if codechange: # if the code did change maybe we have a unique
                    DBlogging.dblogger.debug("Code did change for file: {0}".format(self.filename))
                    continue

                if self.input_files: # Parent check only if process takes input
                    parentchange = self._parentsChanged(f_id_db)
                    if parentchange is None: # this is an inconsistency mark it and move on
                        DBlogging.dblogger.info("Parent was None for file: {0}".format(self.filename))
                        break
                    if parentchange:
                        DBlogging.dblogger.debug("Parent did change for file: {0}".format(self.filename))
                        continue
                
                DBlogging.dblogger.debug("Jumping out of runme, not going to run anything".format())

                return # if we get here then we are not going to run anything

        ## get extra_params from the process
        args = process_entry.extra_params
        if args is not None:
            args = args.replace('{DATE}', utc_file_date.strftime('%Y%m%d'))
            args = args.replace('{ROOTDIR}', self.dbu.MissionDirectory)
            args = args.replace('{CODEDIR}','{}'.format(self.codedir))
            args = args.replace('{CODEVERSION}','{}'.format(version_st))
            args = args.split('|')
            self.extra_params = args
        ## get arguments from the code
        args = code_entry.arguments
        if args is not None:
            args = args.replace('{DATE}', utc_file_date.strftime('%Y%m%d'))
            args = args.replace('{ROOTDIR}', self.dbu.MissionDirectory)
            args = args.replace('{CODEDIR}','{}'.format(self.codedir))
            args = args.replace('{CODEVERSION}','{}'.format(version_st))
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

    def __eq__(self, other):
        """
        define what equals means for 2 runme objects
        """
        attrs = ['ableToRun', 'code_id', 'extra_params', 'input_files', 'out_prod', 'pq',
                 'args', 'codepath', 'filename', 'output_version', 'process_id', 'utc_file_date']
        if not isinstance(other, runMe):
            raise TypeError("Cannot compare runMe with {0}".format(type(other)))
        for a in attrs:
            if getattr(self, a) != getattr(other, a):
                return False
        return True # made it though them all

##     def __hash__(self):
##         """
##         implement a custom hash so that in will work and ignore the temp directory that is always different
##         """
##         attrs = ['ableToRun', 'code_id', 'data_level', 'extra_params', 'input_files', 'out_prod', 'pq',
##                  'args', 'codepath', 'filename', 'output_version', 'process_id', 'utc_file_date']
##         return hash(tuple([getattr(self, a) for a in attrs]))

    def _fileInDB(self):
        """
        check the filename we created and see if it is in the, if it is we will
        not process with that name

        Returns
        -------
        :class:`int` or :class:`bool`
            :sql:column:`~file.file_id` of newly created file, or False.
        """
        try:
            DBlogging.dblogger.debug("Filename: {0} check in db".format(self.filename))
            f_id_db = self.dbu.getFileID(self.filename)
            DBlogging.dblogger.info("Filename: {0} is in the DB, have to make different version".format(self.filename))
            return f_id_db
        except (DButils.DBError, DButils.DBNoData):
            #DBlogging.dblogger.info("Filename: {0} is not in the DB, can process".format(self.filename))
            return False

    def _codeVerChange(self, f_id_db):
        """
        since the file was in the db, is the code that made that the same as
        what we want to use now?

        Parameters
        ----------
        f_id_db : :class:`int`
            :sql:column:`~file.file_id` of file to check
        """
        db_code_id = self.dbu.getFilecodelink_byfile(f_id_db)
        DBlogging.dblogger.debug("f_id_db: {0}   db_code_id: {1}".format(f_id_db, db_code_id))
        if db_code_id is None:
            # I think things will also crash here
            DBlogging.dblogger.error("Database inconsistency found!! A generated file {0} does not have a filecodelink".\
                                     format(self.filename))

            #attempt to figure it out and add one
            tb = self.dbu.getTraceback('File', self.filename)
            proc_id = self.dbu.getProcessFromOutputProduct(tb['product'].product_id)

            code_id = self.dbu.getCodeFromProcess(proc_id, tb['file'].utc_file_date)
            #print("self.dbu.addFilecodelink(tb['file'].file_id, code_id)", tb['file'].file_id, code_id)
            self.dbu.addFilecodelink(tb['file'].file_id, code_id)
            db_code_id = self.dbu.getFilecodelink_byfile(f_id_db)
            DBlogging.dblogger.info("added a file code link!!  f_id_db: {0}   db_code_id: {1}".\
                                    format(f_id_db, db_code_id))

        # Go through an look to see if the code version changed
        if db_code_id != self.code_id: # did the code change
            DBlogging.dblogger.debug("code_id: {0}   db_code_id: {1}".format(self.code_id, db_code_id))
            ver_diff = (self.dbu.getCodeVersion(self.code_id) - self.dbu.getCodeVersion(db_code_id))
            if ver_diff == [0,0,0]:
                DBlogging.dblogger.error("two different codes with the same version ode_id: {0}   db_code_id: {1}".\
                                         format(self.code_id, db_code_id))
                raise DButils.DBError("two different codes with the same version ode_id: {0}   db_code_id: {1}".\
                                      format(self.code_id, db_code_id))
            # Increment output quality if code interface increments, to
            # maintain output_interface_version; else increment what code did.
            self._incVersion([0, 1, 0] if ver_diff[0] else ver_diff)
            return True
        else:
            return False

    def _incVersion(self, ver_diff):
        """
        given a list of the difference in versions (comes form version-version)
        increment self.output_version

        Parameters
        ----------
        ver_diff : :class:`list`
            Three-element list, corresponding to each component of the version
            to increment. Element greater than zero means increment that.

        Returns
        ------
        :class:`bool`
            if any element of version was incremented.
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

        return any(ver_diff)

    def _parentsChanged(self, f_id_db):
        """
        go through a files parents and see if any parents have new versions

        If parents have new versions not used in this processing, increment.
        the correct version number
        ** this is decided by the parents only have revision then revision inc
            if a parent has a quality inc then inc quality
        ** if there are extra parents then we want to rerun with a new quality

        Parameters
        ----------
        f_id_db : :class:`int`
            :sql:column:`~file.file_id` of file to check

        Returns
        -------
        :class:`bool`
            if any parent (input) files have changed since this file was made.
        """
        parents = self.dbu.getFileParents(f_id_db)
        if not parents:
            DBlogging.dblogger.info("db_file: {0} did not have any parents".format(f_id_db,))
            return None

        DBlogging.dblogger.debug("db_file: {0} has parents: {1}".format(f_id_db,
               list(map(attrgetter('file_id'), parents))))

        # if there are more input files now then we need to reprocess
        if len(self.input_files) != len(parents):
            # inc quality and go back
            self._incVersion([0,1,0])
            return True

        quality_diff = False
        revision_diff = False
        for parent in parents:
            # get all the files for the same date and product as the parent to make sure
            #  this is the newest
            parents_all = self.dbu.getFilesByProductDate(parent.product_id, [parent.utc_file_date]*2)
            parent_max = max(parents_all, key=lambda x: self.dbu.getFileVersion(x))

            DBlogging.dblogger.debug("parent: {0} version: {1} parent_max {2} version {3}".format(
                parent.file_id, self.dbu.getFileVersion(parent), parent_max.file_id,
                self.dbu.getFileVersion(parent_max)))


            # if a parent is no longer newest we need to inc
            if self.dbu.getFileVersion(parent) != self.dbu.getFileVersion(parent_max):
                # we have a parent file for a certain date,
                #   get all the files for that date and see if the parent is the newest
                #   if it is then that parent has not changed, do not run
                #   if there is a newer parent then we do need to run
                df = self.dbu.getFileVersion(parent_max) - self.dbu.getFileVersion(parent)
                DBlogging.dblogger.debug("Found a difference between files {0} and {1} -- {2}".format(
                    parent.file_id, parent_max.file_id, df))

                if df[0]:
                    # Interface change on input is quality change on output,
                    # to maintain a code's consistent output_interface_version
                    quality_diff = True
                    DBlogging.dblogger.debug("parent: {0} had an interface difference, will reprocess child".\
                                             format(parent.file_id))
                elif df[1]:
                    quality_diff = True
                    DBlogging.dblogger.debug("parent: {0} had a quality difference, will reprocess child".\
                                             format(parent.file_id))
                elif df[2]:
                    revision_diff = True
                    DBlogging.dblogger.debug("parent: {0} had a revision difference, will reprocess child".\
                                             format(parent.file_id))
        if quality_diff:
            self._incVersion([0,1,0])
        elif revision_diff:
            self._incVersion([0,0,1])

        return quality_diff or revision_diff

    def moveToIncoming(self, fname):
        """
        Moves a file from location to incoming

        Parameters
        ----------
        fname : :class:`str`
            Full path to file to move into incoming.
        """
        inc_path = self.dbu.getIncomingPath()
        if os.path.isfile(os.path.join(inc_path, os.path.basename(fname))):
        #TODO do I really want to remove old version:?
            os.remove( os.path.join(inc_path, os.path.basename(fname)) )
        try:
            shutil.move(fname, inc_path + os.sep)
        except IOError:
             DBlogging.dblogger.error("FAILED moveToIncoming: {0} {1}".format(fname, inc_path))
        DBlogging.dblogger.info("moveToIncoming: {0} {1}".format(fname, inc_path))

    def moveToError(self, fname):
        """
        Moves a file from incoming to error


        Parameters
        ----------
        fname : :class:`str`
            Full path to file to move into error directory.
        """
        DBlogging.dblogger.debug("Entered moveToError: {0}".format(fname))

        path = self.dbu.getErrorPath()
        if os.path.isfile(os.path.join(path, os.path.basename(fname) ) ):
        #TODO do I really want to remove old version:?
            os.remove( os.path.join(path, os.path.basename(fname) ) )
            DBlogging.dblogger.warning("removed {0}, as it was under a copy".\
                                       format(os.path.join(path, os.path.basename(fname) )))
                                                                                                 
        if path[-1] != os.sep:
            path = path+os.sep
        try:
            shutil.move(fname, path)
        except IOError:
            DBlogging.dblogger.error("file {0} was not successfully moved to error".\
                                     format(os.path.join(path, os.path.basename(fname) )))
        else:
            DBlogging.dblogger.info("moveToError {0} moved to {1}".format(fname, path))

    def _add_links(self, cmdline):
        """
        add the filefilelink and filecodelink and verbose provenance
        """
        # need to add the current file to the DB so that we have the filefilelink and filecodelink info
        current_file = os.path.join(self.dbu.getIncomingPath(), self.filename)
        df = self.pq.figureProduct(current_file) # uses all the inspectors to see what product a file is
        if df is None:
            DBlogging.dblogger.error("{0} did not have a product".format(current_file))
            self.moveToError(current_file)
            return
        df.params['verbose_provenance'] = ' '.join(cmdline)
        f_id = self.pq.diskfileToDB(df)
        ## here the file is in the DB so we can add the filefilelink an filecodelinks
        if f_id is not None: # None comes back if the file goes to error
            self.dbu.addFilecodelink(f_id, self.code_id)
            for val in self.input_files: # add a link for each input file
                self.dbu.addFilefilelink(f_id, val)

    def make_command_line(self, force=False, rundir=None):
        """
        make a command line for actually doing this running

        Parameters
        ----------
        rundir : :class:`str`, optional
            Directory to run in, default use a freshly-created temp directory.

        Other Parameters
        ----------------
        force : :class:`bool`, default False
            Not used.

        Notes
        -----
        Creates a temp directory that needs to be cleaned!!
        """
        # build the command line we are to run
        cmdline = [self.codepath]
        # get extra_params from the process
        if self.extra_params:
            cmdline.extend(self.extra_params)
        # figure out how to put the arguments together
        cmdline.extend(self.args)
        # put all the input files on the command line (order is not set)
        for i_fid in self.input_files:
            cmdline.append(self.dbu.getFileFullPath(i_fid))
        # the putname goes last
        if rundir is None:
            self.tempdir = mk_tempdir(suffix='_{0}_runMe'.format(self.filename))
            cmdline.append(os.path.join(self.tempdir, self.filename))
        else:
            cmdline.append(os.path.join(rundir, self.filename))
        # and make sure to expand any path variables
        cmdline = [os.path.expanduser(os.path.expandvars(v)) for v in cmdline]
        DBlogging.dblogger.debug("built command: {0}".format(' '.join(cmdline)))

        self.cmdline = cmdline
