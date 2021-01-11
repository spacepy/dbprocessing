dbprocessing Scripts
====================

addFromConfig.py
----------------
.. program:: addFromConfig

Adds data to a database from a config file. This is the second step in
setting up a new processing chain.

See :doc:`ConfigurationFiles` for a full description of the config file
format and capability.

.. option:: config_file The name of the config file to ingest
.. option:: -m <dbname>, --mission <dbname> The database to apply the config file to
.. option:: -v, --verify  Verify the config file then stop


addVerboseProvenance.py
-----------------------
.. program:: addVerboseProvenance

Go into the database and get the verbose provenance for a file
then add that to the global attrs for the file.
Either putout to the same file or a different file

.. warning:: This code has not been fully tested or used.

clearProcessingFlag.py
----------------------
.. program:: clearProcessingFlag

Clear a processing flag (lock) on a database that has crashed.

.. option:: database The name of the database to unlock
.. option:: message Log message to insert into the database

configFromDB.py
---------------
.. program:: configFromDB

Build a config file from an existing database.

.. option:: filename The filename to save the config
.. option:: -m <dbname>, --mission <dbname> The database to to connect to
.. option:: -f, --force Force the creation of the config file, allows overwrite
.. option:: -s, --satellite The name of the satellite for the config file
.. option:: -i, --instrument The name of the instrument for the config file
.. option:: -c, --nocomments Make the config file without a comment header block on top

.. warning:: This is untested and not fully useful yet.

coveragePlot.py
---------------
.. program:: coveragePlot

Creates a coverage plot based on config file input. This script is useful for
determining which files may be missing from a processing chain.

.. option:: configfile The config file to read.

.. warning:: Has some bugs. Doesn't catch most recent files reliably or something.

See :doc:`ConfigurationFiles` for a full description of the config file
format and capability.


CreateDB.py
-----------
.. program:: CreateDB

Create an empty sqlite database for use in dbprocessing.
(currently creates a RBSP database, this should be updated as an option).

This is the first step in the setup of a new processing chain.

.. option:: -d <dialect>, --dialect <dialect>

   sqlalchemy dialect to use, ``sqlite`` (default) or ``postgresql``

.. option:: dbname

   The name of the database to create

dataToIncoming.py
-----------------
Concept, never actually used. supposed to be one script + config file, but we wound up using separate scripts for everything

dbOnlyFiles.py:
---------------
.. program:: dbOnlyFiles.py

Show files in database but not on disk. Additionally, this can remove files from the db that are only in the db.

.. option:: -s <date>, --startDate <date> Date to start reprocessing (e.g. 2012-10-02)
.. option:: -e <date>, --endDate <date> Date to end reprocessing (e.g. 2012-10-25)
.. option:: -f, --fix Fix the database exists_on_disk field
.. option:: -m <dbname>, --mission <dbname> elected mission database
.. option:: --echo echo sql queries for debugging
.. option:: -n, --newest Only check the newest files
.. option:: --startID The File id to start on
.. option:: -v, --verbose Print out each file as it is checked

.. _scripts_DBRunner:

DBRunner.py
-----------
.. program:: DBRunner.py

Used to demo run codes for certain dates out of the database. This primarily used in testing can also be used to reprocess files as needed

As is typical, processes for which there are no input files for a date will
not be run. However, if a process has no input *products*, dates specified
will be run, depending on the values of :option:`--force` and
:option:`--update`. This is unlike `ProcessQueue.py`_, which has no way of
triggering such processing.

.. option:: process_id

   Process ID or process name of process to run.

.. option:: -d, --dryrun

   Only print what would be done (not currently working).

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

.. option:: --echo

   Start sqlalchemy with echo in place for debugging

.. option:: -s <date>, --startDate <date>

   Date to start search (e.g. 2012-10-02 or 20121002)

.. option:: -e <date>, --endDate <date>

   Date to end search (e.g. 2012-10-25 or 20121025)

.. option:: --nooptional

   Do not include optional inputs

.. option:: -n, --num-proc

   Number of processes to run in parallel

.. option:: -i, --ingest

   Ingest created files into the database. This will also add them to the
   process queue, to be built into further products by ProcessQueue -p.
   (Default: create in current directory and do not add to database.)

.. option:: -u, --update

   Only run files that have not yet been created or with updated codes.
   Mutually exclusive with --force, -v. (Default: run all.)

.. option:: --force {0,1,2}

   Run all files in given date range and always increment version
   (0: interface; 1: quality; 2: revision). Mutually exclusive with -u, -v.
   (Default: run all but do not increment version.)

deleteAllDBFiles.py:
--------------------
.. program:: deleteAllDBFiles

Deletes all file entries in the database.

.. option:: -m <dbname>, --mission <dbname> Selected mission database

deleteAllDBProducts.py:
-----------------------
.. program:: deleteAllDBProducts

Doesn't work, maybe should?

deleteFromDBifNotOnDisk.py:
---------------------------
.. program:: deleteFromDBifNotOnDisk

Finds all files that are in the DB but not found on the DB

.. option:: -m <dbname>, --mission <dbname> Selected mission database
.. option:: --fix Remove the files from the DB (make a backup first)
.. option:: --echo Echo sql queries for debugging

flushProcessQueue.py:
---------------------
.. program:: flushProcessQueue

Clears the ProcessQueue of a database.

.. option:: Database The name of the database to wipe the ProcesQueue of.

histogramCodes.py:
------------------
may or may not still work, read logs to find out what codes take a long time to run

hopeCoverageHTML.py:
--------------------
delete

hope_query.py:
--------------
delete

htmlCoverage.py:
----------------
either this or coveragePlot works, not both.

link_missing_ql_mag_l2_mag.py:
------------------------------
QL "required,", L2 "optional". We don't support "either or but prefer this one", so this links them together and the wrapper handles the actual priority

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

magephem_dataToIncoming.py:
---------------------------
What it says on tin. Delete?

magephem_def_dataToIncoming.py:
-------------------------------
What it says on tin. Delete?

magephem-pre-CoverageHTML.py:
-----------------------------
Probably works. Delete?

makeLatestSymlinks.py:
----------------------
.. program:: makeLatestSymlinks

In a given directory, make symlinks to all the newest versions of files into another directory

.. option:: config The config file
.. option:: --verbose Print out verbose information
.. option:: -l, --list Instead of syncing list the sections of the conf file
.. option:: -f, --filter Comma separated list of strings that must be in the sync conf name (e.g. -f hope,rbspa)

.. warning:: There's no documentation on the config file

.. _MigrateDB:

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

missingFilesByProduct.py:
-------------------------
Attempt to reprocess files that are missing, 90% solution, not used much, but did work

missingFiles.py:
----------------
.. program:: missingFiles

Prints out what's missing, based on noncontiguous date ranges

.. warning:: Maybe works, maybe not

possibleProblemDates.py:
------------------------
.. program:: possibleProblemDates

A database scrub/validation routine.

.. option:: -m <dbname>, --mission <dbname> Selected mission database
.. option:: --fix Fix the issues (make a backup first)
.. option:: --echo Echo sql queries for debugging

.. warning:: Worth looking into and cleaning up a bit

printInfo.py:
-------------
.. program:: printInfo

Prints a table of info about files or products or processes.

.. option:: Database The name of the database to print table of
.. option:: Field Either Product or Mission (more to come)

printProcessQueue.py:
---------------------
.. program:: printProcessQueue

Prints the process queue.

.. option:: Database The name of the database to print the queue of
.. option:: -o, --output The name of the file to output to(if blank, print to stdout)
.. option:: --html Output in HTML

ProcessQueue.py
---------------
.. program:: ProcessQueue

The main thing

purgeFileFromDB.py:
-------------------
.. program:: purgeFileFromDB

Deletes individual files from the database.

.. option:: -m <dbname>, --mission <dbname> Selected mission database
.. option:: -r, --recursive Recursive removal

reprocessByAll.py:
------------------
.. program:: reprocessByAll

Goes through the database and adds all the files that are a certain level to the processqueue so that the next ProcessQueue -p will run them

.. option:: -s <date>, --startDate <date> Date to start reprocessing (e.g. 2012-10-02)
.. option:: -e <date>, --endDate <date> Date to end reprocessing (e.g. 2012-10-25)
.. option:: -l <level>, --level <level> The level to reprocess for
.. option:: -m <dbname>, --mission <dbname> Selected mission database

.. warning:: Should work, probably doesn't

reprocessByCode.py:
-------------------
.. program:: reprocessByCode

Goes through the database and adds all the files that went into the code to the processqueue so that the next ProcessQueue -p will run them

.. option:: codeID code to reprocess for
.. option:: -s <date>, --startDate <date> Date to start reprocessing (e.g. 2012-10-02)
.. option:: -e <date>, --endDate <date> Date to end reprocessing (e.g. 2012-10-25)
.. option:: -m <dbname>, --mission <dbname> Selected mission database
.. option:: --force Force the reprocessing. Speicify which version number to increment (1,2,3)

.. warning:: Should work, probably doesn't

reprocessByDate.py:
-------------------
.. program:: reprocessByDate

Goes through the database and adds all the files that are in a date range to the processqueue so that the next ProcessQueue -p will run them

.. option:: -s <date>, --startDate <date> Date to start reprocessing (e.g. 2012-10-02)
.. option:: -e <date>, --endDate <date> Date to end reprocessing (e.g. 2012-10-25)
.. option:: -m <dbname>, --mission <dbname> Selected mission database
.. option:: --echo Echo sql queries for debugging
.. option:: --force Force the reprocessing. Speicify which version number to increment (1,2,3)

reprocessByInstrument.py:
-------------------------
.. program:: reprocessByInstrument

Goes through the database and adds all the files that are a certain instrument and level to the processqueue so that the next ProcessQueue -p will run them

.. option:: -s <date>, --startDate <date> Date to start reprocessing (e.g. 2012-10-02)
.. option:: -e <date>, --endDate <date> Date to end reprocessing (e.g. 2012-10-25)
.. option:: -m <dbname>, --mission <dbname> Selected mission database
.. option:: -l <level>, --level <level> The level to reprocess for the given instrument
.. option:: --echo Echo sql queries for debugging
.. option:: --force Force the reprocessing. Specify which version number to increment (1,2,3)

reprocessByProduct.py:
----------------------
.. program:: reprocessByProduct.

Goes through the database and adds all the files that are a certain product and put then to the processqueue so that the next ProcessQueue -p will run them

.. option:: -s <date>, --startDate <date> Date to start reprocessing (e.g. 2012-10-02)
.. option:: -e <date>, --endDate <date> Date to end reprocessing (e.g. 2012-10-25)
.. option:: -m <dbname>, --mission <dbname> Selected mission database
.. option:: --echo Echo sql queries for debugging
.. option:: --force Force the reprocessing. Specify which version number to increment (1,2,3)

updateCode.py:
--------------
New version of code, rerun based on that, better done through config files (although can't be done that way) and then run reprocessByCode

updateProducts.py:
------------------
probably broken

updateSHAsum.py:
----------------
.. program:: updateSHAsum

Goes into the database and update the shasum entry for a file that is changed after ingestion.

.. option:: infile File to update the shasum of
.. option:: -m <dbname>, --mission <dbname> Selected mission database

updateUnixTime.py
-----------------
.. program:: updateUnixTime.py

Rewrites all Unix timestamps in a file, recalculating them from the UTC
start/stop time. This is not needed if adding a Unix timestamp table
to an existing database (see :ref:`MigrateDB`); it is only required
if the algorithm for populating the Unix timestamps changes and a database
has been created with the older algorithm.

.. option:: -m <dbname>, --mission <dbname>

   Selected mission database

weeklyReport.py:
----------------
unused, probably broken, delete

writeDBhtml.py:
---------------
unused, probably broken, delete

writeProcessConf.py:
--------------------
probably not used

writeProductsConf.py:
---------------------
probably not used
