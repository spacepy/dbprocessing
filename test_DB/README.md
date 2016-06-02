test_DB
=======

What this is:
-------------
The goal here is to have the simplest test of dbprocessing so testing is quick
and complete without having to use real data. Level 0 files are just simple
words stored in testDB_{set}_(first|sec).raw format. Level 1 files are the
concatenated versions of level 0, stored in testDB_{set}.cat format. Level 2
files are rot13’d versions of the level 1 files, and are stored in
testDB_{set}.rot format.


How to run this:
----------------
Eventually, this will be a full unittest suite and will simply be run that way,
but for now scripts/makeDB.sh will make a testDB.sqlite.bak file with the needed
things added to the database, and scripts/runThisThing.sh will excute the needed
scripts to run the entire dbprocessing chain. makeDB.sh will not need to be run
more than once, as runThisThing.sh will make a copy of the database to work on,
and will not change the orignal db every time it is run.


Revisions:
----------
1-June-2016 Myles Johnson: Inital revision