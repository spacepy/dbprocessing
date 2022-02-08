***************
Getting Started
***************

This is a brief guide to setting up dbprocessing to support a new project.

.. contents::
   :depth: 2
   :local:

Dependencies
============
Currently dbprocessing runs on Linux systems (Mac and Windows are in testing.)

Python is required, either 2.7 or 3.2+.

Other dependencies are automatically installed if you install
``dbprocessing`` using ``pip``; these include SQLAlchemy and dateutil.

If you wish to use a PostgreSQL database, PostgreSQL is required, with
appropriate permissions set up (but you can use an sqlite database
with no database manager setup.) ``psycopg2`` is also required for
PostgreSQL and will not be installed automatically; *one* of the
following lines will likely be appropriate, depending on your
environment:

.. code-block:: sh

   sudo apt-get install python-psycopg2
   sudo apt-get install python3-psycopg2
   conda install psycopg2
   pip install psycopg2

It is recommended to use the same method (system package, conda, or
pip) for psycopg2 as for SQLAlchemy.

Manual dependency installation
------------------------------

`SQLAlchemy <https://www.sqlalchemy.org/>`_ is required. This is available
in most distributions; in Ubuntu, you can usually install it with:

.. code-block:: sh

   sudo apt-get install python-sqlalchemy

or

.. code-block:: sh

   sudo apt-get install python3-sqlalchemy

It is also usually available via pip:

.. code-block:: sh

   pip install sqlalchemy

Finally, `dateutil <https://dateutil.readthedocs.io/en/stable/>`_ is required. In Ubuntu this can be installed with:

.. code-block:: sh

   sudo apt-get install python-dateutil

or

.. code-block:: sh

   sudo apt-get install python3-dateutil

or via pip:

.. code-block:: sh

   pip install python-dateutil

Installation
============
dbprocessing itself is a Python package and must be installed.

This can usually be done with:

.. code-block:: sh

   pip install dbprocessing

which will also install necessary dependencies.

But it can also be installed by downloading the distribution and running:

.. code-block:: sh

   python setup.py install --user

``--user`` is recommended to install for a particular user.

Scripts needed to run dbprocessing are installed into a default
location which is usually on the path. Specify a different location
(e.g. a directory devoted just to dbprocessing scripts) with
``--install-scripts=DIRECTORY``.


Directory layout
================
There are several directories that should be reserved, usually one as
a temporary location for incoming data files, one for data files once
they have been brought into the database, and one for processing codes.

.. seealso::
   :ref:`concepts_missions`

Processing Codes
================
A processing code or script is specific to your project and takes
less processed data into a more processed form. dbprocessing calls
these codes, but they do not need to be aware of dbprocessing or
interact with it. This is one of the interfaces between the generic
dbprocessing and your specific project.

.. seealso::
   :ref:`concepts_codes`

Inspectors
==========
An inspector is a small piece of Python code which can identify certain
metadata about your data files and provide it to dbprocessing. This is
the second interface between dbprocessing and your project.

Examples are forthcoming.

.. seealso::
   :ref:`concepts_inspectors`

Configuration file
==================
The dbprocessing configuration file is a human-readable description of
your project's data files, processing codes, and the interactions
between them. This human-readable description is parsed into the database
structure. In principle these relationships can be defined directly in
the database; in practice it is much easier to describe with this file.

This is the third and final interface between dbprocessing and your project.

.. seealso::
   :ref:`configurationfiles_addFromConfig`

Database creation
=================
If using PostgreSQL, the database itself must first be created without
any tables. This step is skipped for an sqlite database.

Then the tables and relations are created with :ref:`scripts_CreateDB_py`.
This creates all dbprocessing structures, with no information specific
to a project.

Finally, :ref:`scripts_addFromConfig_py` adds project-specific information
from the configuration file.

Initial ingest
==============
The first set of files to bring into dbprocessing should be placed in
the incoming directory, and :option:`ProcessQueue.py -i` used to ingest
them into the database.

.. seealso::

   :ref:`concepts_ingest`

Processing
==========
Run :option:`ProcessQueue.py -p` to produce all possible output files from
the initial set of inputs.

.. seealso::

   :ref:`concepts_processing`

Automation
==========
Although dbprocessing can be run "by hand" as above, normally it is
recommended to perform the following sequence on an automated basis
(e.g. in cron or from a daemon that calls them regularly.

   1. Place new files in the incoming directory (or link them).
   2. Call :option:`ProcessQueue.py -i`.
   3. Call :option:`ProcessQueue.py -p`.

Examples are pending.

A few considerations relating to automation:

   1. :ref:`ProcessQueue.py <scripts_ProcessQueue_py>` should not be run
      with partially-copied files in the incoming directory; it doesn't
      check if they are being written to. There are two ways to address
      this need:

      a. Ensure that the code which populates incoming never runs at the
	 same time as ``ProcessQueue.py``.
      b. Copy files to incoming with a name starting with ``.``, so they
	 will be ignored on ingest. Then perform a rename once the
	 copy is done. This rename is atomic.

   2. Two instances of ``ProcessQueue.py`` cannot run on the same database
      at the same time. This means ingest must complete before processing,
      but it also means if, for instance, a processing run takes 90 minutes
      to complete, the process should not be run hourly. This suggests using
      a script that waits a predefined time between the end and the start
      of processing, rather than always starting processing at a fixed
      interval. A lock on the database ensures no data corruption if two
      instances are run at once; ``ProcessQueue.py`` will simply return
      with an error. Handling this error gracefully and trying later is also
      a reasonable approach.
