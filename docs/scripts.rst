*******
Scripts
*******

.. contents::
   :depth: 1
   :local:

.. _scripts_specifying_database:

Specifying a database
=====================
Most scripts require a mission database to be specified. If this is an
existing file, this is usually interpreted as an sqlite database. Otherwise
it is assumed to be the name of a Postgresql database, and the following
environment variables are used. Unless otherwise specified, these are
optional and the Postgresql default is used if not specified (i.e.,
there is no special dbprocessing-based handling.)

.. note::

   This can result in unusual behavior when a filename that doesn't exist
   is specified as a mission database, as the "fall through" assumes
   Postgresql and might raise unexpected errors if these environment
   variables are not defined.

.. envvar:: PGUSER

   Username to use to connect to the database. This is required when using
   Postgresql databases.

.. envvar:: PGHOST

   Hostname of the database. If not specified, will use ``''``, which is
   usually treated as a domain socket connection on ``localhost``.

.. envvar:: PGPORT

   Port to connect to.

.. envvar:: PGPASSWORD

   User's database password. If not specified, no password provided.

Postgresql support is not as heavily tested and argument handling is not
yet normalized across all scripts.

Maintained scripts
==================
These scripts are of general use in dbprocessing and either are fully
tested and verified to work, or are moving to that status. They are
maintained as part of dbprocessing. They are in the ``scripts``
directory.

All scripts will take an option ``-h`` to provide brief usage help.

The most commonly used scripts are:

===================================== =========================================
:ref:`scripts_CreateDB_py`            Create dbprocessing tables in a database
:ref:`scripts_addFromConfig_py`       Add project-specific relationships to db
:ref:`scripts_ProcessQueue_py`        Ingest input files; process to new files
:ref:`scripts_clearProcessingFlag_py` Reset the lock if processing crashes
===================================== =========================================

.. _scripts_addFromConfig_py:

addFromConfig.py
----------------
.. program:: addFromConfig.py

Adds data to a database from a config file. This is the second step in
setting up a new processing chain.

See the :ref:`configuration file documentation
<configurationfiles_addFromConfig>` for a full description of the
config file format and capability.

This can be run multiple times against a database to populate information
from several config files; this is a means of, for instance, having
multiple satellites or instruments in a single database. Existing
entries in the database are left as-is; entries which do not exist are
added.

.. option:: config_file

   The name of the config file to ingest

.. option:: -m <dbname>, --mission <dbname>

   The database to apply the config file to

.. option:: -v, --verify

   Verify the config file then stop (do not apply to database)

Example usage:

.. code-block:: sh

   addFromConfig.py –m mychain.sqlite setup.config

addProductProcessLink.py
------------------------
.. program:: addProductProcessLink.py

Add a new entry to the product-process link table, so that an existing
product is added as a new input to an existing process.

.. option:: -n <dbname>, --name <dbname>

   The mission database to update.

.. option:: -c <process>, --proc <process>

   Name or ID of the process to update.

.. option:: -d <product>, --prod <product>

   Name or ID of the product to now make an input to :option:`--proc`.

.. option:: -o, --opt

   Make :option:`--prod` a mandatory input to :option:`--proc`. Default:
   new input is optional.

.. option:: -y, --yday

   Also provide previous day of :option:`--prod` when making a particular
   day using :option:`--proc`.

.. option:: -t, --tmrw

   Also provide next day of :option:`--prod` when making a particular
   day using :option:`--proc`.

changeProductDir.py
-------------------
.. program:: changeProductDir.py

Change the directory storing a product, and move all files of that
product to the new directory.

.. option:: -m <dbname>, --mission <dbname>

   The mission database to update.

.. option:: product

   Name or ID of the product to change.

.. option:: newdir

   New directory to move the file to.

.. _scripts_clearProcessingFlag_py:

clearProcessingFlag.py
----------------------
.. program:: clearProcessingFlag.py

.. index::
   single: processing flag

Clear a processing flag (lock) on a database that has crashed.

The :meth:`DButils.startLogging()
<dbprocessing.DButils.DButils.startLogging>` method locks the database
to avoid conflicts from simultaneous processing. This is only
currently used by :ref:`scripts_ProcessQueue_py`; if it crashes before
completion, the lock will still be set and needs to be cleared before
running `scripts_ProcessQueue_py` again.

.. option:: database

   Filename of the database to unlock

.. option:: message

   Log message to insert into the database, noting reason for the unlock.

Example usage:

.. code-block:: sh

   clearProcessingFlag.py mychain.sqlite "crash fix"

compareDB.py
------------
.. program:: compareDB.py

Compares two databases for having the same products, processes, codes,
and files; matching is done by name not ID, as ID may differ. The input
files for each file, and the codes used to make each file, are also
compared by filename. Output is printed to the screen.

.. option:: -m <dbname>, --mission <dbname>

   Mission database. Specify twice, for the two missions to compare.

.. _scripts_configFromDB_py:

configFromDB.py
---------------
.. program:: configFromDB.py

Build a config file from an existing database.

.. warning:: This is untested and not fully useful yet.

.. option:: filename

   The filename to save the config

.. option:: -m <dbname>, --mission <dbname>

   The database to connect to

.. option:: -f, --force

   Force the creation of the config file, allows overwrite

.. option:: -s <satellite>, --satellite <satellite>

   The name of the satellite for the config file

.. option:: -i <instrument>, --instrument <instrument>

   The name of the instrument for the config file

.. option:: -c, --nocomments

   Make the config file without a comment header block on top

.. _scripts_coveragePlot_py:

coveragePlot.py
---------------
.. program:: coveragePlot.py

Creates a coverage plot based on config file input. This script is
useful for determining which files may be missing from a processing
chain. Either this or :ref:`scripts_htmlCoverage_py` works (probably this).

.. option:: configfile

   The config file to read. See the :ref:`configuration file
   documentation <configurationfiles_coveragePlot>`.

.. warning:: Has some bugs, possibly not catching most recent files reliably.

.. _scripts_CreateDB_py:

CreateDB.py
-----------
.. program:: CreateDB.py

Create an empty database with all dbprocessing tables.

This is the first step in the setup of a new processing chain.

.. option:: -d <dialect>, --dialect <dialect>

   sqlalchemy dialect to use, ``sqlite`` (default) or ``postgresql``.
   If ``postgresql``, database must exist, this script will set up
   the tables.

.. option:: dbname

   The name of the database to create (filename if using sqlite).

Example usage:

.. code-block:: sh

   CreateDB.py mychain.sqlite

dbOnlyFiles.py
--------------
.. program:: dbOnlyFiles.py

Show file ID of files which are recorded in the database as being on
disk, but where the file is not present on disk. Optionally mark these
missing files in the database as not being on disk.

.. option:: -s <date>, --startDate <date>

   First date to check (e.g. 2012-10-02)

.. option:: -e <date>, --endDate <date>

   Last date to check, inclusive (e.g. 2012-10-25)

.. option:: -f, --fix

   Update database ``exists_on_disk`` to ``False`` for files which
   are not present.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: --echo

   echo sql queries for debugging

.. option:: -n, --newest

   Only check the newest files

.. option:: --startID <file_id>

   The File id to start on

.. option:: -v, --verbose

   Print out each file as it is checked

.. _scripts_DBRunner:

DBRunner.py
-----------
.. program:: DBRunner.py

Directly execute codes in the database. Although primarily used in
testing, this can also be used to reprocess files as needed, or to
execute codes with no input products.

As is typical, processes for which there are no input files for a date will
not be run. However, if a process has no input *products*, dates specified
will be run, depending on the values of :option:`--force` and
:option:`--update`. This is unlike :ref:`scripts_ProcessQueue_py`, which
has no way of triggering such processing.

.. option:: process_id

   Process ID or process name of process to run.

.. option:: -d, --dryrun

   Only print what would be done (not currently working).

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: --echo

   Start sqlalchemy with echo in place for debugging

.. option:: -s <date>, --startDate <date>

   First date to run code for (e.g. 2012-10-02 or 20121002)

.. option:: -e <date>, --endDate <date>

   Last date to run code, inclusive (e.g. 2012-10-25 or 20121025)

.. option:: --nooptional

   Do not include optional inputs

.. option:: -n <count>, --num-proc <count>

   Number of processes to run in parallel

.. option:: -i, --ingest

   Ingest created files into the database. This will also add them to
   the process queue, to be built into further products by
   :option:`ProcessQueue.py -p`.  (Default: create in current
   directory and do not add to database.)

.. option:: -u, --update

   Only run files that have not yet been created or with updated codes.
   Mutually exclusive with :option:`--force`, :option:`-v`. (Default: run all.)

.. option:: --force {0,1,2}

   Run all files in given date range and always increment version
   (0: interface; 1: quality; 2: revision). Mutually exclusive with
   :option:`-u`, :option:`-v`.
   (Default: run all but do not increment version.)

deleteAllDBFiles.py
-------------------
.. program:: deleteAllDBFiles.py

Deletes all file entries in the database. Removes all references in
other tables; does not remove file from disk.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

fast_data.py
------------
.. program:: fast_data.py

Delete old versions of files, by date. Used for files that may be
rapidly reprocessed, and thus old versions may not be of interest. The
assumption is that files *before* a certain cutoff date have
potentially been referenced and should be retained, and only files
after that cutoff date are subject to removal.

Removes all Level0 files, and all of their children, that are not the
newest version and are newer than the cut off date. It will still keep
the records of the files in the dbprocessing database, but sets
exists_on_disk to false.

The newest version of a file is never deleted. Files which are in the
release table are also not deleted.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: --cutoff <date>

   Specify the cutoff date; only delete files newer than this date. This
   is specified by the *file* date, i.e. the data of data in the file,
   not the timestamp of the file on the disk. Required, in form YYYY-MM-DD.

.. option:: -a <directory>, --archive <directory>

   If specified, move files to this archive directory rather than deleting.

.. option:: --reap-files

   Remove all matching files from disk (or archives if using :option:`-a`).
   Files remain in the database but are marked as not existing on disk.

.. option:: --reap-records

   Remove matching files from the database *if* they are marked as not existing
   on disk. Will also remove all references to the file from other tables.

.. option:: --verbose

   Print the name of files as they are deleted (from disk or database).

flushProcessQueue.py
--------------------
.. program:: flushProcessQueue.py

Clears the ProcessQueue of a database.

.. option:: database

   The name of the database to wipe the ProcessQueue of.

histogramCodes.py
-----------------
Reads log files to find how long codes took to run; creates a histogram
(PNG output) for each code, showing the number of runs for each runtime.

.. option:: logfile

   Log file to read, specify multiple times to read many log files.

.. _scripts_htmlCoverage_py:

htmlCoverage.py
---------------
Create HTML file with table showing the versions of products present
in the database by date.

.. note::

   Either this or :ref:`scripts_coveragePlot_py` works, not both.

.. option:: -m <dbname>, --mission <dbname>

   Desired mission database

.. option:: -d <deltadays>, --deltadays <deltadays>

   Provide output this many days past the last file in the database.
   (Default: 3)

.. option:: outbase

   String to use at the beginning of each html output file.

.. _scripts_linkUningested:

linkUningested.py
-----------------
.. program:: linkUningested.py

Find all files that are in a directory associated with a product and match
the product's file format, but are not in the database. Make a symbolic
link to the incoming directory for each file (so they will be ingested
on next run).

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database.

.. option:: -p <product>, --product <product>

   Product name or product ID to check. Optional (default will check all
   products), but highly recommended, since in particular ingestion of files
   that are normally created rather than ingested as first-order inputs might
   lead to odd results. Multiple products can be specified by specifying
   more than once.

makeLatestSymlinks.py
---------------------
.. program:: makeLatestSymlinks

Create symbolic links to the latest version of files.

For a directory containing files ("source"), creates symlinks in a
different directory ("destination"). For each file in source, only the
latest version will be linked in the destination.  Useful for having
one directory with all version of files and a different directory with
just the latest versions for each product and date.

.. note:: Operates strictly on the basis of filenames; does not access the
	  database.

.. option:: config

   The config file. See the :ref:`configuration file documentation
   <configurationfiles_makeLatestSymlinks>` for details.

.. option:: --verbose

   Print out verbose information

.. option:: -l, --list

   Instead of syncing list the sections of the conf file

.. option:: -f <filter_list>, --filter <filter_list>

   Comma separated list of strings that must be in the sync conf name
   (e.g. ``-f hope,rbspa``)

.. _scripts_MigrateDB_py:

MigrateDB.py
------------
.. program:: MigrateDB.py

Migrate a database to the latest structure.

Right now this only adds a Unix time table that stores the UTC start/end
time as seconds since Unix epoch, but planned to extend to support all
other database changes to date.

Will display all possible changes and prompt for confirmation.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: -y, --yes

   Process possible changes without asking for confirmation.

.. _scripts_missingFilesByProduct_py:

missingFilesByProduct.py
------------------------
.. program:: missingFilesByProduct.py

Find files which appear to be missing (based on gaps in the sequence)
and, optionally, attempt to reprocess them.

.. note:: 90% solution, not used much, but did work

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: product_id

   ID of product to check for gaps.

.. option:: -s <date>, --startDate <date>

   First date to check (e.g. 2012-10-02). Default 2021-08-30.

.. option:: -e <date>, --endDate <date>

   Last date to check, inclusive (e.g. 2012-10-25). Default today.

.. option:: -p, --process

   Add missing dates to the queue for processing. Files added are
   from the parent product of the missing product, so :option:`--parent`
   is required.

.. option:: --parent <parent_id>

   Product ID of the parent product, i.e. the product which is used as input
   to :option:`product_id`.

.. option:: --echo

   echo sql queries for debugging

.. option:: -f <filter>, --filter <filter>

   Unused. Intended to be space-separated globs to filter filenames.


missingFiles.py
---------------
.. program:: missingFiles.py

Reprocesses all missing files, based on noncontiguous date
ranges. Implemented as multiple calls to
:ref:`scripts_missingFilesByProduct_py`.

.. warning:: Maybe works, maybe not

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: -s <date>, --startDate <date>

   First date to check (e.g. 2012-10-02). Default 2021-08-30.

.. option:: -e <date>, --endDate <date>

   Last date to check, inclusive (e.g. 2012-10-25). Default today.

.. _scripts_possibleProblemDates_py:

possibleProblemDates.py
-----------------------
.. program:: possibleProblemDates.py

Check for various possible database inconsistencies. See also `scrubber.py`_.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: --fix

   Fix the issues. No backups are made, and not all problems are fixable.

.. option:: --echo

   Echo sql queries for debugging

.. warning:: Worth looking into and cleaning up a bit; may have sharp edges.

.. _scripts_printInfo_py:
	     
printInfo.py
------------
.. program:: printInfo.py

Print summary information about entries in the database.

.. option:: database

   The name of the database to print table of

.. option:: field

   Table for which to print information: ``Code``, ``File``, ``Mission``,
   ``Process``, or ``Product``.

.. option:: -s <date>, --startDate <date>

   First date to check (e.g. 2012-10-02). Only used for :option:`field`
   of ``File``.

.. option:: -e <date>, --endDate <date>

   Last date to check, inclusive (e.g. 2012-10-25). Only used for
   :option:`field` of ``File``.

.. option:: -p <product>, --product <product>

   Product ID or name to print files for, if :option:`field` is ``File``.
   Otherwise unused.

.. _scripts_printProcessQueue:

printProcessQueue.py
--------------------
.. program:: printProcessQueue.py

Prints the process queue, i.e., the list of files to consider as
potential inputs for processing.

.. option:: database

   The name of the database to print the queue of

.. option:: -c, --count

   Set the return code to the number of files in the queue. If there
   are more than 255 files, set to 255. With this option, it is impossible
   to differentiate between an error and a single-element process queue based
   on return code. Mutually exclusive with :option:`-e`, :option:`--exist`.

.. option:: -e, --exist

   Set the return code based on whether there are any files in the process
   queue: 0 (shell True) if there are, 1 (shell False) if there are no files.
   With this option, it is impossible to differentiate between an error and
   an empty process queue based on return code. Mutually exclusive with
   :option:`-c`, :option:`--count`.

.. option:: --html

   Provide output in HTML (default text).

.. option:: -o <filename>, --output <filename>

   The name of the file to output to (if not specified, output to stdout).

.. option:: -p <product> [<product> ...], --product <product> [<product> ...]

   Product IDs or name to include in output. May specify multiple products;
   all other products will be ignored (not included in output or :option:`-c`
   and :option:`-e` counts). Because this may be used to specify multiple
   (space-separated) options, use ``--`` to end the list of products before
   specifying the database (or use ``-p`` as the last option). Adds a table of
   included products to the output, before the queue output itself.

.. option:: -q, --quiet

   Quiet mode: produce no output. Mutually exclusive with :option:`--html`,
   :option:`-o`, :option:`--output`, :option:`-s`, :option:`--sort`.

.. option:: -s, --sort

   Sort the output. Primary sort by UTC file date, secondary by product name.
   Default is to output by the order in the process queue, i.e., the order
   in which files are considered for processing.

.. _scripts_printRequired_py:

printRequired.py
----------------
.. program:: printRequired.py

Print all required input products for one or more processes. For each
process, will print the product ID and product name of all required
input files; ends with a summary of all unique product IDs on one
line. Handy for use with :ref:`scripts_reprocessByProduct_py`.

.. option:: -m <dbname>, --mission <dbname>

   The database to read.

.. option:: process

   Process names or IDs for which to print inputs.


.. _scripts_ProcessQueue_py:

ProcessQueue.py
---------------
.. program:: ProcessQueue.py

The main script of dbprocessing. Operates in one of two modes. If
:option:`-i` is specified, attempts to ingest new files from the
incoming directory into the database. As files are ingested, they are
added to the process queue. If :option:`-p` is specified, processes
the process queue. For each file on the queue, consider all possible
files that can be made from it. If those files are not up-to-date
(i.e., are not newer than the codes that make those files and all its
input files), run the relevant codes to make those new files. These
new files are ingested, added to the process queue, and similarly
evaluated; the script does not return until the process queue is
empty.

.. seealso::
   :ref:`concepts_ingest`, :ref:`concepts_processing`

The normal use of dbprocessing is regular calls to
:option:`ProcessQueue.py -i` followed by :option:`ProcessQueue.py -p`.

.. option:: -i, --ingest

   Ingest files: evaluate all files in the incoming directory, attempt
   to add them to the database, move them to the appropriate directory
   for their identified product, and add them to the process queue.

.. option:: -p, --process

   Process files: make all possible out-of-date outputs of all of
   the inputs on the process queue, and add these new files to the
   process queue. Repeat until the queue is empty.

Common options
^^^^^^^^^^^^^^
These options are used with :option:`ProcessQueue.py -i` and
:option:`ProcessQueue.py -p`.

.. option:: -m <dbname>, --mission <dbname>

   The mission database to connect to

.. option:: -l <loglevel>, --log-level <loglevel>

   Set the logging level; messages of at least this priority are written
   to the log. Default ``debug``. See :meth:`~logging.Logger.setLevel` for
   valid levels.

.. option:: --echo

   echo sql queries for debugging

.. option:: -d, --dryrun

   Only perform a dry run, do not perform ingest/process.

   .. warning::

      This is implemented via the ``dryrun`` kwarg to
      :class:`~dbprocessing.dbprocessing.ProcessQueue` and has not
      been fully tested (there may be side effects).

.. option:: -r, --report

   Make an HTML report

   .. note::

      Not implemented.

Ingest mode options
^^^^^^^^^^^^^^^^^^^
These options are only used with :option:`ProcessQueue.py -i`.

.. option:: --glb <glob>

   Only import files from the incoming directory if their name matches
   this pattern. See :mod:`glob` for details. Default ``*``, which will
   match all files but ignore files that start with ``.``.

Process mode options
^^^^^^^^^^^^^^^^^^^^
These options are only used with :option:`ProcessQueue.py -p`.

.. option:: -n <numproc>, --num-proc <numproc>

   Number of processes to run at once. This is the number of processing
   codes to launch at a given time to create new files; each may itself
   use multiple processors. Default 2.

.. option:: -o <process>, --only <process>

   Comma-separated list of processes (IDs or names) to run. Other
   processes will not be run, as if they did not exist. This does
   not affect the removal of files from the process queue: a file
   is removed from the queue and evaluated for possible processing,
   and processing only proceeds if potential processes are on the
   provided list. The file is not returned to the queue if any other
   processes are skipped.

.. option:: -s

   Skip processes with a RUN timebase. Because these processes do not
   create an output file, they are never "up to date" and it may be useful
   to skip them to avoid extra processing time.


purgeFileFromDB.py
------------------
.. program:: purgeFileFromDB.py

Deletes individual files from the database. Also removes all references
to each deleted feile from the database. Does not remove from disk.

.. option:: filename

   Name of the file to remove; specify multiple files to remove them all.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: -r, --recursive

   Recursive removal: remove not only this file, but all files for
   which it is an input.

.. option:: -v, --verbose

   Verbose: print all files removed.

replaceArgsWithRootdir.py
-------------------------
.. program:: replaceArgsWithRootdir.py

Replace all references to the root directory of a mission in code
arguments with ``{ROOTDIR}``, so that future changes to the mission's
root directory will propagate to the arguments. I.e. replace explicit
hardcoded references to a reference that will always expand to the
current value.

.. note:: Currently only works on sqlite databases.

.. option:: mission

   Mission database to update

reprocessByCode.py
------------------
.. program:: reprocessByCode.py

Add all files made by a given code to the process queue, so they will
be evaluated as inputs on the next run of :option:`ProcessQueue.py
-p`.

.. warning:: Should work, probably doesn't

.. option:: code

   Name or ID of code to reprocess. Files *made by this code* will be
   added to the process queue to be considered as inputs; this is
   *not* the code which will be run when those files are reprocessed.

.. option:: -s <date>, --startDate <date>

   Date to start reprocessing (e.g. 2012-10-02)

.. option:: -e <date>, --endDate <date>

   Date to end reprocessing (e.g. 2012-10-25)

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: --force {0,1,2}

   Force the reprocessing. Specify which version number to increment (0,1,2)

reprocessByDate.py
------------------
.. program:: reprocessByDate.py

Goes through the database and adds all the files that are in a date
range to the process queue so that the next :option:`ProcessQueue.py
-p` will run them.

This code works and is likely the one that should be used most of the
time for reprocessing files. (Used as the default for do everything on
a date range.)

.. option:: -s <date>, --startDate <date>

   Date to start reprocessing (e.g. 2012-10-02)

.. option:: -e <date>, --endDate <date>

   Date to end reprocessing (e.g. 2012-10-25)

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: --echo

   Echo sql queries for debugging

.. option:: --force {0,1,2}

   Force the reprocessing. Specify which version number to increment (0,1,2)

.. option:: --level <level>

   Only reprocess files of this level.

reprocessByInstrument.py
------------------------
.. program:: reprocessByInstrument.py

Adds all database files of a particular instrument to the process
queue so that the next :option:`ProcessQueue.py -p` will run them.

.. option:: instrument

   The instrument to reprocess; only products of this instrument
   are added to the process queue. Name or ID.

.. option:: -s <date>, --startDate <date>

   Date to start reprocessing (e.g. 2012-10-02)

.. option:: -e <date>, --endDate <date>

   Date to end reprocessing (e.g. 2012-10-25)

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: -l <level>, --level <level>

   The level to reprocess for the given instrument

.. option:: --echo

   Echo sql queries for debugging

.. option:: --force {0,1,2}

   Force the reprocessing. Specify which version number to increment (0,1,2)

.. _scripts_reprocessByProduct_py:

reprocessByProduct.py
---------------------
.. program:: reprocessByProduct.py

Adds all database files of a particular product to the process
queue so that the next :option:`ProcessQueue.py -p` will run them.

This reprocessing script works and is used all the time; it's been
tested much more heavily than the others and is used all the time for
individual processing.

.. option:: product

   Add files of this product, ID or name.

.. option:: -s <date>, --startDate <date>

   Date to start reprocessing (e.g. 2012-10-02)

.. option:: -e <date>, --endDate <date>

   Date to end reprocessing (e.g. 2012-10-25)

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: --echo

   Echo sql queries for debugging

.. option:: --force {0,1,2}

   Force the reprocessing. Specify which version number to increment (0,1,2)

.. _scripts_testInspector_py:

testInspector.py
----------------
.. program:: testInspector.py

Run an :ref:`inspector <concepts_inspectors>` against a specific
product in a database and file. Prints contents of :class:`.Diskfile`
if it is a match.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database.

.. option:: -f <file>, --file <file>

   Path to data file to test inspector on.

.. option:: -i <inspector>, --inspector <inspector>

   Path to inspector source file.

.. option:: -p <product>, --product <product>

   Product ID of the product the file belongs to, i.e. test if inspector
   considers the file to be a match to this product.

.. option:: -a <args>, --args <args>

   Keyword arguments to pass to inspector (optional), space-separated list
   of ``key=value`` pairs, as in :sql:column:`inspector.arguments`.

scrubber.py
-----------
.. program:: scrubber.py

Checks a database for possible inconsistencies or problems. See also
:ref:`scripts_possibleProblemDates_py`.

.. option:: -m <dbname>, --mission <dbname>

   Mission database to check

updateSHAsum.py
---------------
.. program:: updateSHAsum.py

Update the stored shasum for a file; useful if the file were changed after
ingestion.

.. option:: infile

   File to update the shasum of

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

updateUnixTime.py
-----------------
.. program:: updateUnixTime.py

Rewrites all Unix timestamps in a file, recalculating them from the UTC
start/stop time. This is not needed if adding a Unix timestamp table
to an existing database (see :ref:`scripts_MigrateDB_py`); it is only required
if the algorithm for populating the Unix timestamps changes and a database
has been created with the older algorithm.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database


Examples
========
These scripts are meant as reference for specific tasks that might be
required for a particular mission. They may not be fully tested or may
be mission-specific. They are not generally maintained; some are
candidates for eventually transferring to maintained scripts. They are
in the directory ``examples/scripts``.

addVerboseProvenance.py
-----------------------
.. program:: addVerboseProvenance.py

Go into the database and get the verbose provenance for a file
then add that to the global attrs for the CDF file.
Either put out to the same file or a different file

.. warning:: This code has not been fully tested or used; never worked.

.. option:: infile

   Input CDF file

.. option:: outfile

   Output CDF file; input is copied to this file with the provenance added.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: -i, --inplace

   Edit the existing CDF file in place instead of making a new output file.

CreateDBsabrs.py
----------------
.. program:: CreateDBsabrs.py

Variant of :ref:`scripts_CreateDB_py` that was used for a project with
PostgresSQL before that functionality was integrated, but also used
slightly different table definitions.

dataToIncoming.py
-----------------
Concept, never actually used. Intended as a single script which would be
used (in conjunction with a configuration file) to handle all incoming
data for RBSP-ECT, to ingest all new files to the database. In practice,
used separate scripts for each sensor on the suite.

hopeCoverageHTML.py
-------------------
Produce a table with days that had coverage of HOPE data. See
:ref:`scripts_coveragePlot_py` and :ref:`scripts_htmlCoverage_py` for more
generic implementation.

hope_query.py
-------------
Print information on HOPE files for particular days, and particular
spacecraft. See :ref:`scripts_printInfo_py` for similar generic output.

link_missing_ql_mag_l2_mag.py
-----------------------------
RBSP-ECT had some inputs available initially in a quicklook format and
then later in a definitive level 2 format. The database treated QL as
"required,", L2 "optional". dbprocessing doesn't support "either or
but prefer this one", so this links them together and the wrapper
handles the actual selection of the file according to priority.

magephem-pre-CoverageHTML.py
----------------------------
Produce a table with days that had coverage of predictive magnetic
ephemeris data. See :ref:`scripts_coveragePlot_py` and
:ref:`scripts_htmlCoverage_py` for more generic implementation.

newestVersionProblemFinder.py
-----------------------------
Untested script to check for cases where the newest version of a file
is not consistent with version numbering and creation dates.

updateCode.py
-------------
Helper to help deploy a new version of a code. Designed to copy an
existing code entry and increment its version.

Ideally would also add all files that are inputs to the code to the
process queue, but this was not implemented.

updateProducts.py
-----------------
Intended to update products based on an updated configuration
file. Probably broken.

weeklyReport.py
---------------
Reads dbprocessing log files to produce an HTML report of activity
over a given period of time. Unused and probably broken.

writeDBhtml.py
--------------
Produces an HTML summary of a mission products and processes. Unused
and probably broken.

writeProcessConf.py
-------------------
Write the configuration file fragment for a particular process. Not
used. See :ref:`scripts_configFromDB_py`.

writeProductsConf.py
--------------------
Write the configuration file fragment for a particular product. Not
used. See :ref:`scripts_configFromDB_py`.
