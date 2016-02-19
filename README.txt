README file for dbprocessing

Revisions:
8-Dec-2010 Brian Larsen Initial revision


How this file works:
--------------------
This is a bit stream of consciousness and a bit documentation, comments to the reader are in angle brackets <<comment>>




What the Chain Does:
--------------------
The processing chain takes L0 and QA files placed into incoming (how they arrive there is not this code's issue) and processes
all available children adding everything to the db as it goes.
When a new version of a code or L0 is added all dependent files will be recreated and version numbers bumped.


How to run the chain:
---------------------
in your cvs checkout directory
python2.6 ProcessQueue.py
<<this runs one instance of the loop, does not stay resident, will be run form cron>>
before I run i always tail -f dbprocessing_log.log  so I can see whats going on


Flow:
-----
Check "Currently Processing" flag (in logging)
if set:
    check PID on system:
        If running - Quit with message about another still running
        if not running - There was a crash, log message, things are potentially inconsistent, set crash flag <<not implemented>>
Set "processing" in logging
perform consistency check <<not implemented>>
    Fail:
        if crash is not set - quit with an error <<not implemented>>
        if crash: try to repair and log  <<not implemented>>
perform up-to-date check: <<not implemented>>
    fail:
        if no crash: quit with error <<not implemented>>
        if crash: log - add parents with expired children to "file children" and run inner loop until clear <<not implemented>>
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


Important Files:
-----------------
DButils.py - file contains most of the routines that directly interface to the db <<not all are still useful or functional>>
DBfile.py - class that represents a file in the db and what can be done with a file <<delete not yet supported>>
DBlogging.py - class that logs message from program execution, I use tail -f dbprocessing_log.log
dbprocessing_log.log - the log file for processing that is appended to as the program runs, is has a max size of 2000000 bytes
    and is rolled over and saved for 5 backups
DBqueue.py - class that implements a queue that the db uses to process from
Diskfile.py - class that represents a file on disk, includes figure product and make output name
RunMe.py - class that performs execution of codes with various inputs as defined in the db
ProcessQueue.py - this is the main class, processes incoming and performs the above flow
Version.py - class that represents a version code for a file, has gt, lt, eq, etc in it




---------------------------------------------
OLD OUT OF DATE STUFF



Important Directories: (Test mission)
----------------------
cvs checkout directory: code should run anyway but /n/projects must be mapped, sqlalchemy required
/n/projects/cda/Test/incoming: incoming directory where input files are placed
/n/projects/cda/Test/Create_L0: location of mk_all_l0.py
/n/projects/cda/Test/errors: directory where files with problems are moved, bad filename, duplicate, etc
/n/projects/cda/Test: in the data and proc directories are all the data and processes for the mission Test


Database info:
---------------
to connect to the db as ops:  (Don't do this not setup yet)
psql -a -d rbsp -h edgar -p 5432 -U rbsp_ops

to connect to the db as rbsp_owner (RECCOMENDED)
psql -a -d rbsp -h edgar -p 5432 -U rbsp_owner
<<See Brian for passwords>>

To see how many connections there are (useful for runaway mistakes)
SELECT * FROM pg_stat_activity;  <<query to db, run from postgres command line>>

Database structure and relations are laid out in:
DB_Structure_4Oct2010.pdf

Commands to create the DB are in:
DatabaseCreationCommands.sql  <<as of 8-Dec-2010 this is slightly outdated on some constraints>>


Helpful DB commands:
---------------------
DELETE FROM filefilelink ;   <<removes all file dependencies>>
DELETE FROM filecodelink ;   <<removes all code dependeciues>>
DELETE FROM file;            <<removes all file entries>>
<<no need to remove files on disk>>


Other things:
-------------
If the PorcessQueue dies then you are locked out:
- run ProcessQueue  <<dies will not run>>
- pq.dbu.resetProcessingFlag('Some comment')



Calling conventions:
--------------------
to executor from process table.

If extra_params_in == None then input filename else the contenst of extra_params_in


Known Shortcomings:
--------------------
- There is no consistency checking
- Version is ignored when processing <<so don't try new versions of files and expect it to work as you want>>
- Unittests are incomplete <<I want to merge these all to one file>>
- There is no mechanism for adding new versions of processing codes and having files reprocessed
- No good way to initialize a fresh DB


Big Tracker:
------------
Using the sourceforge tracker at:
https://sf4.lanl.gov/sf/tracker/do/listTrackers/projects.rbsp_ect_soc/tracker
