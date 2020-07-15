#####
Tests
#####

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

