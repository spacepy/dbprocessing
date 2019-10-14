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
but for now scripts/functionalTest.sh will execute the needed scripts to run the
entire dbprocessing chain.

Relation to unit tests:
-----------------------
The unit tests run assuming the state of the database AFTER the functional
tests are run. This is the state that's checked in to the repository. The
first step of the functional test is to remove the output files and db, and
create new ones.

Ultimately the functional test should be updated to not affect the state of
the working directory and the unit tests updated to match appropriately.


Revisions:
----------
1-June-2016 Myles Johnson: Initial revision
14-October-2019 Jon Niehof: Notes on relationship to unit tests.
