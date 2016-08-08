from __future__ import print_function
from collections import namedtuple
import datetime
import glob
from operator import itemgetter, attrgetter
import os
import shutil
import subprocess
import tempfile
import time
import traceback

import DBlogging
import DBstrings
import DButils
from inspector import extract_Version
import Utils
from Utils import dateForPrinting as DFP
import Version


runObj = namedtuple('runObj', 'runme time probfile')
"""
used in _start_a_run() and runner() to hold the runme object that is running,
the time it started, and the probfile pointer
"""

class ProcessException(Exception):
    """Class for errors in running processes"""
    pass


def mk_tempdir(suffix='_dbprocessing'):
    """
    Create a secure temp directory
    """
    tempdir = tempfile.mkdtemp(suffix=suffix)
    DBlogging.dblogger.debug("Created temp directory: {0}".format(tempdir))
    return tempdir

def rm_tempdir(tempdir):
    """
    Remove the temp directory
    """
    name = tempdir
    shutil.rmtree(tempdir)
    tempdir = None
    DBlogging.dblogger.debug("Temp dir deleted: {0}".format(name))

def _extract_files(cmdline):
    """
    Given a command line extract out the files that are input to the process
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
                files.append(tmp[-1])
        else:
            if os.path.sep in s: # this looks like a file
                files.append(s)
    return files

def _pokeFile(filename):
    """
    Given a filename open it non-blocking and see if it works
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
    Given a runme that we want to start poke the all the files to be sure the automunter has them all up

    intermediate steps:
    1) need to extract all the files that will be used for the process and poke them all
       with a os.open with non-blocking.  This will make sure the automounter has seen attempts
       on them all
    2) Then if the open works close it and move all.  If it fails, note that and move on.
    3) Start the process after all opened and closed
    """
    # processes[subprocess.Popen(runme.cmdline, stdout=fp, stderr=fp)] = (runme, time.time(), fp )
    files2poke = _extract_files(runme.cmdline)
    for f in files2poke:
        ans = _pokeFile(f)
        if ans is 'NOFILE':
            DBlogging.dblogger.error("Command line referenced a file that did not exist {0}.  {1}"
                                     .format(f, runme.cmdline))
        elif ans is 'OTHER':
            DBlogging.dblogger.error("Command line referenced a file that did 'other' {0}.  {1}"
                                     .format(f, runme.cmdline))
        elif ans is 'ERROR':
            DBlogging.dblogger.error("Command line referenced a file that did not open {0}.  {1}"
                                     .format(f, runme.cmdline))
        elif ans is 'FILE':
            DBlogging.dblogger.debug("Command line referenced a file opened fine {0}.  {1}"
                                     .format(f, runme.cmdline))
        else:
            print("Could not have gotten here")
            raise(RuntimeError("Should not have gotten here"))


def runner(runme_list, dbu, MAX_PROC=2, rundir=None):
    """
    Go through a list of runMe objects and run them

    .. todo:: This function can be made a smart as one wants, for now it is not made to be smart, but flexible

    :param runme_list: List of runMe objects that need to be run
    :type runme_list: list

    :param rundir: Directory to run in, if None then use a temp directory
    :type rundir: str

    :return: number of processes that successfully completed, number of processes that failed
    :rtype: tuple(int, int)
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
    # get rid of all the runme objects that are not runnable
    runme_list2 = set([v for v in runme_list if v.ableToRun])
    # get the ones we are not running and delete their tempdir
    left_overs = set(runme_list).difference(runme_list2)
    for lo in left_overs: # remove the tempdir
        try:
            rm_tempdir(lo.tempdir)
        except (OSError, AttributeError):
            pass

    # sort the runme_list on level and filename (which is like date and product and s/c together)
    runme_list = sorted(list(runme_list2), key = lambda x: (x.data_level, x.filename))

    # found some cases where the same command line was in the list more than once based on
    #   more than one dependency in the process queue, go through and clean these out
    # TODO add this to the DB so that we can have a defined version string
    #basenames = Utils.unique([v.filename.split('_v')[0] for v in runme_list])
    #runme_list_uniq = []
    #for name in basenames:
    #    # loop over all the runme's with this output and see which has the most arguments
    #    tmp_rme = []
    #    for rme in runme_list:
    #        if name in rme.filename:
    #            tmp_rme.append(rme)
    #        if tmp_rme:
    #            runme_list_uniq.append(max(tmp_rme, key=lambda x: len(x.cmdline)))

    #print runme_list_uniq, runme_list
    #runme_list = runme_list_uniq

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
    def remove_dups(seq, oval):
        seen = set()
        return [oval[i] for i in xrange(len(seq)) if seq[i] ==  '' or not (seq[i] in seen or seen.add(seq[i]))]
    # 2
    runme_list = remove_dups(outfiles, runme_list)
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
            # belt and suspenders
            if not runme.ableToRun:
                continue

            DBlogging.dblogger.info("Command: {0} starting".format(os.path.basename(' '.join(runme.cmdline))))

            """
            when we go to run a process capture all the stdout and stderr into a file in the running temp directory
            if the process is successful then it just gets removed with the directory, otherwise move it to the error
            directory
            """

            # make sure the file is not in the DB before you try this
            try:
                if force:
                    raise(DButils.DBNoData)

                tmp = dbu.getEntry('File', os.path.basename(runme.cmdline[-1])) # output is last
                if tmp is not None: # we are not going to run
                    DBlogging.dblogger.info("Did Not run: {0} output was in db"
                                            .format(os.path.basename(' '.join(runme.cmdline))))
                    try:
                        rm_tempdir(runme.tempdir) # delete the tempdir
                    except AttributeError:
                        pass
            except DButils.DBNoData:
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
                    #raise(IOError("Could not create the prob file, so died {0}".format(os.path.basename(' '.join(runme.cmdline)))))
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
                raise(ValueError("Should not have gotten here"))

            # execution gets here if the process finished
            del processes[p]
        time.sleep(0.5)

    return n_good, n_bad


class runMe(object):
    """
    class holds all the info it takes to run a process

    dbu - DButils instance
    utc_file_date - datetime.date
    process_id - process to run (int)
    input_files - the files that exist to run with (list of int)
    pq - processqueue instance
    """
    def __init__(self, dbu, utc_file_date, process_id, input_files, pq, force=False):
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
        # since we have a process do we have a code that does it?
        self.code_id = self.dbu.getCodeFromProcess(process_id, utc_file_date)
        if self.code_id is None: # there is no code to actually run we are done
            DBlogging.dblogger.debug("Code_id is None: can't run")
            return
        self.codepath = self.dbu.getCodePath(self.code_id)
        if self.codepath is None: # there is no code to actually run we are done
            DBlogging.dblogger.debug("Codepath is None: can't run")
            return
        DBlogging.dblogger.debug("Going to run code: {0}:{1}".format(self.code_id, self.codepath))

        process_entry = self.dbu.getEntry('Process', self.process_id)
        if process_entry.output_timebase == "RUN":
            self.data_level = 5000
            self.filename = 'RUN_{0}_{1}'.format(process_entry.process_name, self.process_id)
        else:
            self.out_prod = process_entry.output_product
            ptb = self.dbu.getTraceback('Product', self.out_prod)
            self.data_level = ptb['product'].level # This is the level of the output product, sorts on this and date
            # grab the format
            format_str = ptb['product'].format
            # get the process_keywords from the file if there are any
            try:
                process_keywords = Utils.strargs_to_args([self.dbu.getEntry('File', fid).process_keywords for fid in input_files])
                for key in process_keywords:
                    format_str = format_str.replace('{'+key+'}', process_keywords[key])
            except TypeError:
                pass

            ## need to build a version string for the output file
            ## this sets the interface version
            code_entry = self.dbu.getEntry('Code', self.code_id)
            output_interface_version = code_entry.output_interface_version

            fmtr = DBstrings.DBformatter()
            # set the default version for the output file
            self.output_version = Version.Version(output_interface_version, 0, 0)

            ## we have a filename, now we need to increment versions as needed/appropriate to
            ## come up with a unique one

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
                codechange = self._codeVerChange(f_id_db)
                if codechange: # if the code did change maybe we have a unique
                    DBlogging.dblogger.debug("Code did change for file: {0}".format(self.filename))
                    continue
                parentchange = self._parentsChanged(f_id_db)
                if parentchange is None: # this is an inconsistency mark it and move on
                    DBlogging.dblogger.info("Parent was None for file: {0}".format(self.filename))
                    break
                if parentchange:
                    DBlogging.dblogger.debug("Parent did change for file: {0}".format(self.filename))
                    continue
                # Need to check for version_bump in the processqueue
                DBlogging.dblogger.debug("Jumping out of runme, not going to run anything".format())

                return # if we get here then we are not going to run anything

            ## get extra_params from the process
            args = process_entry.extra_params
            if args is not None:
                args = args.replace('{DATE}', utc_file_date.strftime('%Y%m%d'))
                args = args.split('|')
                self.extra_params = args

            ## get arguments from the code
            args = code_entry.arguments
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

    def __eq__(self, other):
        """
        define what equals means for 2 runme objects
        """
        return self.__dict__ == other.__dict__

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

            code_id = self.dbu.getCodeFromProcess(proc_id, tb['file'].utc_file_date)
            #print("self.dbu.addFilecodelink(tb['file'].file_id, code_id)", tb['file'].file_id, code_id)
            self.dbu.addFilecodelink(tb['file'].file_id, code_id)
            db_code_id = self.dbu.getFilecodelink_byfile(f_id_db)
            DBlogging.dblogger.info("added a file code link!!  f_id_db: {0}   db_code_id: {1}".format(f_id_db, db_code_id))

        # Go through an look to see if the code version changed
        if db_code_id != self.code_id: # did the code change
            DBlogging.dblogger.debug("code_id: {0}   db_code_id: {1}".format(self.code_id, db_code_id))
            ver_diff = (self.dbu.getCodeVersion(self.code_id) - self.dbu.getCodeVersion(db_code_id))
            if ver_diff == [0,0,0]:
                DBlogging.dblogger.error("two different codes with the same version ode_id: {0}   db_code_id: {1}".format(self.code_id, db_code_id))
                raise(DButils.DBError("two different codes with the same version ode_id: {0}   db_code_id: {1}".format(self.code_id, db_code_id)))
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
               map(attrgetter('file_id'), parents)))

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
            parent_max = max(parents_all, key=lambda x: self.dbu.getVersion(x))

            DBlogging.dblogger.debug("parent: {0} version: {1} parent_max {2} version {3}".format(
                parent.file_id, self.dbu.getVersion(parent), parent_max.file_id, self.dbu.getVersion(parent_max)))


            # if a parent is no longer newest we need to inc
            if self.dbu.getVersion(parent) != self.dbu.getVersion(parent_max):
                # we have a parent file for a certain date,
                #   get all the files for that date and see if the parent is the newest
                #   if it is then that parent has not changed, do not run
                #   if there is a newer parent then we do need to run
                df = self.dbu.getVersion(parent_max) - self.dbu.getVersion(parent)
                DBlogging.dblogger.debug("Found a difference between files {0} snd {1} -- {2}".format(
                    parent.file_id, parent_max.file_id, df))

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

        NOTE: creates a temp directory that needs to be cleaned!!
        """

        ## 1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a
        # if runme.filename is in the DB then we cannot run this.  (happens if 2 identical runMe are there
        if not force:
            try:
                file_entry = self.dbu.getEntry('File', self.filename)
                DBlogging.dblogger.debug("Not going to run the outfile is already in the db: {0}".format(self.filename))
                self.ableToRun = False
                return
            except DButils.DBNoData:
                pass # we can process this

        # build the command line we are to run
        cmdline = [self.codepath]
        # get extra_params from the process
        if self.extra_params:
            cmdline.extend(self.extra_params)
        # figure out how to put the arguments together
        if self.args:
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