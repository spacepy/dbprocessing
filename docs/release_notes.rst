=============
Release Notes
=============

.. contents::
   :depth: 2
   :local:

0.1 Series
==========

0.1 (xxxx-xx-xx)
----------------
This is the first public packaged release of dbprocessing. For the convenience
of those working directly from git checkouts, these release notes summarize
changes made since the creation of the public repository on 2020-07-15.

New features
^^^^^^^^^^^^
Support for processes that take no input was added, as part of many
enhancements to :ref:`scripts_DBRunner`  (`26 <https://github.com/spacepy/
dbprocessing/pull/26>`_).

Deprecations and removals
^^^^^^^^^^^^^^^^^^^^^^^^^

Dependency requirements
^^^^^^^^^^^^^^^^^^^^^^^

Major bugfixes
^^^^^^^^^^^^^^

Fixed :class:`~dbprocessing.dbprocessing.ProcessQueue.buildChildren` on
older databases without ``yesterday`` and ``tomorrow`` columns in the
``Productprocesslink`` table (`20 <https://github.com/spacepy/dbprocessing/
issues/20>`_).

Other changes
^^^^^^^^^^^^^

Pull requests merged this release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Other issues closed this release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
