Developer Information
=====================
.. contents::
   :depth: 1
   :local:

Build/install
-------------
To build the module::

    python setup.py build

This will copy all the python files into the ``build`` directory (and may do other things).

To install::

    python setup.py install --user

This will install into ``.local`` in your home directory.


Tests
-----
There are two types of testing done with this project. Unit tests, which test if the individual pieces work, and functional tests, which test if the peices work together

Unit Tests:
~~~~~~~~~~~
Unit Tests are located in the tests/ directory.

To check test coverage (i.e., how many lines of code are actually hit by the tests)::

    python-coverage run test_all.py
    python-coverage html
    firefox htmlcov/index.html

Of course, instead of ``test_all.py``, you can specify the name of the module you want to test.

Functional Tests:
~~~~~~~~~~~~~~~~~
Todo

Documentation
-------------
`Sphinx <http://www.sphinx-doc.org/>`_ documentation is stored in the ``sphinx`` directory.

``Makefile`` is used to build the documentation.

Run ``make html`` in the ``sphinx`` directory to build the html documentation, output is in ``build/html``.

``build`` contains the built documentation (and intermediate files); it can safely be deleted.

``source`` contains the ReStructuredText source files; note that a large quantity of the documentation is not built from here but from the Python source files. ``source/autosummary`` contains the docs extracted from those source files; it can safely be deleted. If anything's weird about the docs generated from the Python source, try deleting ``autosummary`` first and then rebuilding. The ``autosummary`` docs are extracted from the version of the module in top-level ``build``, i.e., run ``python setup.py build`` before generating the documentation.

Repository organization
-----------------------
Essentially none