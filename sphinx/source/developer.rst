#####################
Developer Information
#####################
.. contents::
   :depth: 1
   :local:

*************
Build/install
*************
To build the module::

    python setup.py build

This will copy all the python files into the ``build`` directory (and may do other things).

To install::

    python setup.py install --user

This will install into ``.local`` in your home directory.

*****
Tests
*****
There are two types of testing done with this project. Unit tests, which test if the individual pieces work, and functional tests, which test if the pieces work together

.. _unit:


Unit Tests
==========
Unit Tests are located in the tests/ directory.

To check test coverage (i.e., how many lines of code are actually hit by the tests)::

    python-coverage run test_all.py
    python-coverage html
    firefox htmlcov/index.html

Of course, instead of ``test_all.py``, you can specify the name of the module you want to test.

.. _functional:

Functional Tests
================
Located in test_DB/ is a functional test. The goal was to have the simplest test of dbprocessing so testing is quick and complete without having to use real data.

Level 0 files are just simple words stored in testDB_{set}_(first|sec).raw format.

Level 1 files are the concatenated versions of level 0, stored in testDB_{set}.cat format.

Level 2 files are rot13’d versions of the level 1 files, and are stored in testDB_{set}.rot format.

``testDB/scripts/runThisThing.sh`` will execute the needed scripts to run the entire dbprocessing chain.

*************
Documentation
*************
`Sphinx <http://www.sphinx-doc.org/>`_ documentation is stored in the ``sphinx`` directory.

``Makefile`` is used to build the documentation.

Run ``make html`` in the ``sphinx`` directory to build the html documentation, output is in ``build/html``.

``build`` contains the built documentation (and intermediate files); it can safely be deleted.

``source`` contains the ReStructuredText source files; note that a large quantity of the documentation is not built from here but from the Python source files. ``source/autosummary`` contains the docs extracted from those source files; it can safely be deleted. If anything's weird about the docs generated from the Python source, try deleting ``autosummary`` first and then rebuilding. The ``autosummary`` docs are extracted from the version of the module in top-level ``build``, i.e., run ``python setup.py build`` before generating the documentation.

***********************
Repository organization
***********************
top-level
=========
There should be(but there totally are) no committed files at the top level besides ``setup.py``, and ``.gitignore`` (not normally visible).

build
=====
This is used by ``setup.py`` in the build/install process; it is ignored by git. Don't hand-edit anything under here; can be safely deleted.

dbprocessing
============
Source tree for the main ``dbprocessing`` module. If it's in this directory, it's meant to be installed.

Documents
=========
?

gui
===
?

OneOffs
=======
?

scripts
=======
Scripts meant to be called from the command line that should be installed with the module. They should be added to the ``scripts`` list in ``setup.py`` and documented in :doc:`scripts` (source file ``scripts.rst``).

sphinx
======
See `Documentation`_.

test_codes
==========
?

testDB
======
See :ref:`functional`.

Testing_Utils
=============
?

tests
=====
See :ref:`unit`.

tests_scripts
=============
?

******************
Database Structure
******************
.. image:: out.png
	:scale: 50 %

*****
Notes
*****

Ingest and Process steps
========================

Ingest(-i)
----------
1. checkIncoming - Gets all files in incoming directory, and adds them to 'queue'(removes duplicate files)
2. importFromIncoming - Pops files off 'queue', checks that they don't exist in the db already runs figureProduct() on them, then calls diskfileToDB().
3. figureProduct - runs every inspector on the files, stops and returns the diskfile that is created by the inspector when one matches.
4. diskfileToDB - Enters file into DB, moves the file to the its correct home, sets files in the db of the same product and same utc_file_date to not be newest version, adds to processqueue for later processing, and returns file_id

Process(-p)
-----------
1. _processqueueClean - Go through the process queue and clear out lower versions of the same files. Then, sort on dates, then sort on level. (some half baked newest_version stuff in here)
2. buildChilden - Called on every item in the processqueue. Calculates all possible children products
3. runMe.runner - Created for every item on the runme_list. Handles all the magic of running codes.

	A. Build up the command line and store in a commands list
	B. Loop over the commands

		a. Start up to MAX_PROC processes with subprocess.Popen
		b. Poll if they are done or not, and if they finished successfully
			i. Success: Move output file to incoming dir, run all inspectors on it to see what product a file is(why?), diskfileToDB is run(see -i section)
			ii. Failure: move stdout and stderr to errors

****
Todo
****

FastData
========

Multiday file handling
======================
The project needs a way to pass more than just "today" and "yesterday" to the codes.

Adding "previous" and "next" columns to the product process link may be a way of handling this("previous=2" would mean "to make a product of date 2018-01-15, hand in 2018-01-13 and 2018-01-14 of the input product as well at 2018-01-15" and "next=1" would put in 2018-01-16.)

This would require establishing a previous/next(chronologically) relationship. The currently proposed idea is to add a new table to the database similar to the existing filefilelink table, which has a file's id and it's previous file id, adding to this table in the same place to filefilelink table is added to.
