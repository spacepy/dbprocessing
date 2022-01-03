***********************
Repository organization
***********************

top-level
=========
There should be (but currently are) no committed files at the top level
besides ``setup.py``, and ``.gitignore`` (not normally visible).

build
=====
This is used by ``setup.py`` in the build/install process; it is ignored by git. Don't hand-edit anything under here; can be safely deleted.

dbprocessing
============
Source tree for the main ``dbprocessing`` module. If it's in this directory, it's meant to be installed.

developer
=========
Miscellaneous bits for sharing between developers. Included in the source
tarball but not in binary distributions.

docs
====
Source for :doc:`documentation`.

examples
========
Examples of how to set up dbprocessing for a project, including
configuration and scripts that are specific to missions.

functional_test
===============
Full dbprocessing setup for end-to-end functional test (:ref:`functional`).

gui
===
Initial work on a Qt-based GUI; not complete.

scripts
=======
Scripts meant to be called from the command line that should be installed with the module. They should be added to the ``scripts`` list in ``setup.py`` and documented in :doc:`../scripts` (source file ``scripts.rst``).

sphinx
======
Built :doc:`documentation`.


unit_tests
==========
See :ref:`unit`.
