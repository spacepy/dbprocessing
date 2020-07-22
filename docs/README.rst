dbprocessing README
===================
``dbprocessing`` is a Python-based, database-driven process controller which
automates the production of derived data products upon the arrival of new
input data. Although originally written for Heliophysics data, it is
intended to be flexible enough to manage most forms of digital time-series
data.

Current Status
--------------
``dbprocessing`` has been used in production for about eight years in several
different projects; however, this has always been with the direct support
of the developers.

As a full project, ``dbprocessing`` should be considered to be in an alpha
state. It is not currently suitable for use by non-developers. The developers
are working daily to improve the maturity of the code, documentation, and
the infrastructure supporting development.

Relationship to SpacePy
-----------------------
Several ``dbprocessing`` developers are also SpacePy developers. The SpacePy
organization is hosting ``dbprocessing`` and providing community support
as ``dbprocessing`` is prepared for the public and grows its own community.
The SpacePy developers are not, as a whole, responsible for ``dbprocessing``.

``dbprocessing`` is not a component of SpacePy, nor does it require SpacePy.
SpacePy is generally useful in processing Heliophysics data, e.g. in the
codes that ``dbprocessing`` manages.

Funding and development
-----------------------
Development of ``dbprocessing`` is primarily supported by the projects
which make use of it to deliver data. Development is performed in the public
github repository at <https://github.com/spacepy/dbprocessing/>.

Out of date information
-----------------------
Information below is out of date and is in the process of being integrated
into the full documentation and updated; it is kept here for reference
in the meantime.


What dbprocessing does
----------------------
``dbprocessing`` takes L0 and QA files placed into incoming (how they arrive there is not this code's issue) and processes

all available children adding everything to the db as it goes.

When a new version of a code or L0 is added all dependent files will be recreated and version numbers bumped.


How to run
----------
python ProcessQueue.py -i -m *database*

python ProcessQueue.py -p -m *database*

This runs one instance of the ingest and process loop; it does not stay resident. Running from cron or similar recommended.

Before running, ``tail -f dbprocessing_log.log`` can help see what's going on.

Flow
----
The flow of processing is::

    Check "Currently Processing" flag (in logging)
    if set:
	check PID on system:
	    If running - Quit with message about another still running
	    if not running - There was a crash, log message, things are potentially inconsistent, set crash flag (not implemented)
    Set "processing" in logging
    perform consistency check (not implemented)
	Fail:
	    if crash is not set - quit with an error (not implemented)
	    if crash: try to repair and log  (not implemented)
    perform up-to-date check: (not implemented)
	fail:
	    if no crash: quit with error (not implemented)
	    if crash: log - add parents with expired children to "file children" and run inner loop until clear (not implemented)
    Perform main loop
    Perform inner loop
    clear processing in logging

    (Main Loop)
    Build "import me" from incoming (queue)
    foreach file in "import me":
	figure product
	add to DB
	move to final resting place
	append to "find children" (queue)
    (inner loop)
    foreach in "find children": (queue)
	figure possible children:
	    is that child build-able?
		no - move to next child
		yes:
		    make the child on disk (in /tmp)
		    add child to db, including filefile and filecode links
		    move child to final resting
		    append child of child to "find children" (queue)


Important Files
---------------
DButils.py - file contains most of the routines that directly interface to the db (not all are still useful or functional)

DBfile.py - class that represents a file in the db and what can be done with a file (delete not yet supported)

DBlogging.py - class that logs message from program execution, use ``tail -f dbprocessing_log.log``

dbprocessing_log.log - the log file for processing that is appended to as the program runs, is has a max size of 2000000 bytes and is rolled over and saved for 5 backups

DBqueue.py - class that implements a queue that the db uses to process from

Diskfile.py - class that represents a file on disk, includes figure product and make output name

RunMe.py - class that performs execution of codes with various inputs as defined in the db

ProcessQueue.py - this is the main class, processes incoming and performs the above flow

Version.py - class that represents a version code for a file, has gt, lt, eq, etc in it

To see how many connections there are if using PostgreSQL as the database (useful for runaway mistakes), run from postgres command line:
``SELECT * FROM pg_stat_activity;``

Database structure and relations are laid out in DB_Structure_4Oct2010.pdf

Create the DB with ``CreateDB.py`` script.


Helpful DB commands:
--------------------
Remove all file dependencies: ``DELETE FROM filefilelink ;``

Remove all code dependencies: ``DELETE FROM filecodelink ;``
Remove all file entries (no need to remove files on disk):  ``DELETE FROM file;``


Other things:
-------------
If the ProcessQueue dies then you are locked out, use ``clearProcessingFlag.py``


Calling conventions:
--------------------
to executor from process table.

If extra_params_in == None then input filename else the contents of extra_params_in


Known Shortcomings:
--------------------
- There is no consistency checking
- There is no mechanism for adding new versions of processing codes and having files reprocessed
