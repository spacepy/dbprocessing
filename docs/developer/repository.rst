#######################
Repository organization
#######################

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
Scripts meant to be called from the command line that should be installed with the module. They should be added to the ``scripts`` list in ``setup.py`` and documented in :doc:`../scripts` (source file ``scripts.rst``).

sphinx
======
See :doc:`documentation`.

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

