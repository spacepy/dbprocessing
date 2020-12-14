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

New script :ref:`scripts_linkUningested` to find files which match product
format but are not in database, and symlink them to the incoming directory
so they can be ingested (`54 <https://github.com/spacepy/dbprocessing/
pull/54>`_).

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

Fixed :class:`~dbprocessing.runMe.runMe` to always maintain the
``output_interface`` version specified by the code. Updates to a code's
interface version will increment the quality version of its output (not
the interface); updates to a file's interface version will increment the
quality version of child files (rather than assuming children were up-to-date
and failing to reprocess them). (`63 <https://github.com/spacepy/dbprocessing/
pull/63>`_)

Other changes
^^^^^^^^^^^^^

Pull requests merged this release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

..
   Normally these aren't committed to the repository until release time,
   but the script to generate this section has been run early to allow
   tweaking of the format before release.

PR `8 <https://github.com/spacepy/dbprocessing/pull/8>`_: Fix unit tests for scripts (`44690a95 <https://github.com/spacepy/dbprocessing/commit/44690a955d41544af9a10c9c316221cc4154bf14>`_)

PR `13 <https://github.com/spacepy/dbprocessing/pull/13>`_: Circleci project setup (`c99ffc43 <https://github.com/spacepy/dbprocessing/commit/c99ffc43961ff389ff3d8645ab92ead41af4d9e0>`_)

PR `18 <https://github.com/spacepy/dbprocessing/pull/18>`_: Fix handling of inspector regex for ECT (`61a2215e <https://github.com/spacepy/dbprocessing/commit/61a2215ec449ebc253dab2e98716c734dd1092e2>`_)

PR `1 <https://github.com/spacepy/dbprocessing/pull/1>`_: Basic respository organization (`362a0b72 <https://github.com/spacepy/dbprocessing/commit/362a0b72b868d5ad6019784acd6052d07b8d2a35>`_)

PR `19 <https://github.com/spacepy/dbprocessing/pull/19>`_: test_checkIncoming update to guarantee directory state (Closes #17) (`16899b2c <https://github.com/spacepy/dbprocessing/commit/16899b2c8e83236b1687bfe50d1bea304811efc2>`_)

    `17 <https://github.com/spacepy/dbprocessing/issues/17>`_: test_checkIncoming  fails when a direction on my system is not empty

PR `21 <https://github.com/spacepy/dbprocessing/pull/21>`_: Fix _getRequiredProducts/getInputProductID on old DB (Closes #20) (`07f3e3a1 <https://github.com/spacepy/dbprocessing/commit/07f3e3a1ace8f4f72e482e2dd47b951fe00d140d>`_)

    `20 <https://github.com/spacepy/dbprocessing/issues/20>`_: 'Productprocesslink' has no attribute 'yesterday', old database

PR `24 <https://github.com/spacepy/dbprocessing/pull/24>`_: Testing changes in preparation for no-input support (`24ca0b57 <https://github.com/spacepy/dbprocessing/commit/24ca0b577b0487393a333d601b59212cd086a236>`_)

PR `27 <https://github.com/spacepy/dbprocessing/pull/27>`_: Run unit tests in CircleCI (`1fe1ef46 <https://github.com/spacepy/dbprocessing/commit/1fe1ef4648768c151cd895c96f725b3d3ae112e7>`_)

    `16 <https://github.com/spacepy/dbprocessing/issues/16>`_: Document CircleCI setup and github integration

    `15 <https://github.com/spacepy/dbprocessing/issues/15>`_: Execute unit tests in CircleCI

PR `26 <https://github.com/spacepy/dbprocessing/pull/26>`_: DBRunner enhancements for ingest, update-only, force (Closes #9) (`5c05a165 <https://github.com/spacepy/dbprocessing/commit/5c05a165ed0072770a34cffa3c0d7894203af6f8>`_)

    `9 <https://github.com/spacepy/dbprocessing/issues/9>`_: Support products with no input

PR `31 <https://github.com/spacepy/dbprocessing/pull/31>`_: Speed up getFiles by start/stop time (Closes #23) (`b5f30ef3 <https://github.com/spacepy/dbprocessing/commit/b5f30ef366e63fdfb846ca688f98a61ec782436e>`_)

    `23 <https://github.com/spacepy/dbprocessing/issues/23>`_: ProcessQueue "Command Build Progress" is slow

PR `36 <https://github.com/spacepy/dbprocessing/pull/36>`_: addFile: fix utc_start_time/stop_time as dates with Unix time table (`333e47dc <https://github.com/spacepy/dbprocessing/commit/333e47dc7ef320ebd941d06e62be4414d0586e22>`_)

PR `39 <https://github.com/spacepy/dbprocessing/pull/39>`_: CircleCI: update apt cache before installing packages (fix CreateDBsabrs test) (`00922ed5 <https://github.com/spacepy/dbprocessing/commit/00922ed5404c92607849ee99334c4eddc825e2d3>`_)

PR `38 <https://github.com/spacepy/dbprocessing/pull/38>`_: Fix Unix time table calculation to maintain file day (`20afffc4 <https://github.com/spacepy/dbprocessing/commit/20afffc47c2d8d30018af953372c59fa0d82d87a>`_)

PR `41 <https://github.com/spacepy/dbprocessing/pull/41>`_: remove the #! to python2.6 in favor of python, this was making my system barf that wants to use 2.7 (`8e5d3ae4 <https://github.com/spacepy/dbprocessing/commit/8e5d3ae432fb35227c74bc5615422cf456b39578>`_)

Other issues closed this release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

..
   Normally these aren't committed to the repository until release time,
   but the script to generate this section has been run early to allow
   tweaking of the format before release.

`2 <https://github.com/spacepy/dbprocessing/issues/2>`_: Website not finding CSS

`42 <https://github.com/spacepy/dbprocessing/issues/42>`_: PR checklist calls for CHANGELOG update, yet there is no changelog
