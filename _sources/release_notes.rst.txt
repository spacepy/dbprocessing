*************
Release Notes
*************

.. contents::
   :depth: 2
   :local:

0.1 Series
==========

0.1.0 (2022-02-10)
------------------
This is the first public packaged release of dbprocessing. For the convenience
of those working directly from git checkouts, these release notes summarize
changes made since the creation of the public repository on 2020-07-15.

New features
^^^^^^^^^^^^
Support for processes that take no input was added, as part of many
enhancements to :ref:`scripts_DBRunner` (`26 <https://github.com/spacepy/
dbprocessing/pull/26>`_).

New script :ref:`scripts_linkUningested` to find files which match product
format but are not in database, and symlink them to the incoming directory
so they can be ingested (`54 <https://github.com/spacepy/dbprocessing/
pull/54>`_).

Python 3 support was added (`77 <https://github.com/spacepy/dbprocessing/
pull/77>`_).

Added options :option:`printProcessQueue.py --count`,
:option:`printProcessQueue.py --exist`, and :option:`printProcessQueue.py
--quiet` to allow flow control in shell scripts based on the process
queue state (`87
<https://github.com/spacepy/dbprocessing/issues/87>`_, `88
<https://github.com/spacepy/dbprocessing/pull/88>`_).

Added options :option:`printProcessQueue.py --product` to filter output
and :option:`printProcessQueue.py --sort` to sort output
(`93 <https://github.com/spacepy/dbprocessing/pull/93>`_).

Initial PostgreSQL support was added; see
:ref:`scripts_specifying_database` and :option:`CreateDB.py --dialect`
(`64 <https://github.com/spacepy/dbprocessing/pull/64>`_,
`78 <https://github.com/spacepy/dbprocessing/pull/78>`_
`114 <https://github.com/spacepy/dbprocessing/pull/114>`_).

Deprecations and removals
^^^^^^^^^^^^^^^^^^^^^^^^^
None

Dependency requirements
^^^^^^^^^^^^^^^^^^^^^^^
`SQLAlchemy <https://www.sqlalchemy.org/>`_ and `dateutil
<https://dateutil.readthedocs.io/en/stable/>`_ are required. No
minimum version has been established, but ``dbprocessing`` is known to
work with SQLAlchemy 1.1 and dateutil 2.6.

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

Fixed :meth:`~dbprocessing.dbprocessing.ProcessQueue.buildChildren`
(specifically, helper method ``_getRequiredProducts``) to only look
for files which are recorded in the database as existing on disk. Most
notably, this means :ref:`ProcessQueue.py <scripts_ProcessQueue_py>`
will not attempt to use nonexistent files as inputs to processing.

Other changes
^^^^^^^^^^^^^

Pull requests merged this release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

PR `49 <https://github.com/spacepy/dbprocessing/pull/49>`_: Build Sphinx docs in CI; check for warnings (`240be3df <https://github.com/spacepy/dbprocessing/commit/240be3df1a4670d839e4238446f8860f313f94b8>`_)

PR `50 <https://github.com/spacepy/dbprocessing/pull/50>`_: More documentation of pull request processing; release notes (`b470f436 <https://github.com/spacepy/dbprocessing/commit/b470f43689e97d04a47b2805a597214aaee12979>`_)

    `28 <https://github.com/spacepy/dbprocessing/issues/28>`_: Set up release notes

    `30 <https://github.com/spacepy/dbprocessing/issues/30>`_: Document use of checklists and draft PRs in developer docs

    `29 <https://github.com/spacepy/dbprocessing/issues/29>`_: Document github magic references for commit messages and PRs

PR `51 <https://github.com/spacepy/dbprocessing/pull/51>`_: add docker authentication (`af0ab929 <https://github.com/spacepy/dbprocessing/commit/af0ab929e152dabe354aa617c271ab7d000ccd89>`_)

    `32 <https://github.com/spacepy/dbprocessing/issues/32>`_: Add Docker auth to CircleCI

PR `56 <https://github.com/spacepy/dbprocessing/pull/56>`_: Fix make clean for the autosummary docs (`e4eb0c01 <https://github.com/spacepy/dbprocessing/commit/e4eb0c01ad5b8d7fd9b4ab9e52439406849fccab>`_)

PR `55 <https://github.com/spacepy/dbprocessing/pull/55>`_: Move table definitions to separate, common module. (`bfb806af <https://github.com/spacepy/dbprocessing/commit/bfb806afe2f01135d88675397b3b20e677c9c6b2>`_)

PR `54 <https://github.com/spacepy/dbprocessing/pull/54>`_: New linkUningested script: find files not in database and symlink for import (`734f37b1 <https://github.com/spacepy/dbprocessing/commit/734f37b1bfb3540f5682edd6dbb2e590eb51a3ff>`_)

PR `63 <https://github.com/spacepy/dbprocessing/pull/63>`_: Increment output quality versions when input/code interface version changes (`1c153080 <https://github.com/spacepy/dbprocessing/commit/1c15308027ad95784b4d05cfc9f77d6164a7f8aa>`_)

PR `64 <https://github.com/spacepy/dbprocessing/pull/64>`_: Minimal postgresql support in dbUtils (`11a417e8 <https://github.com/spacepy/dbprocessing/commit/11a417e89ac8dd2168982e12008504c66c13f8c0>`_)

PR `66 <https://github.com/spacepy/dbprocessing/pull/66>`_: Triage scripts and port to argparse (`80d12b59 <https://github.com/spacepy/dbprocessing/commit/80d12b59b29f9e5a0b6cc795fadc3c33daaab3cd>`_)

PR `67 <https://github.com/spacepy/dbprocessing/pull/67>`_: Fix test_DBfile when called by itself (`142a4976 <https://github.com/spacepy/dbprocessing/commit/142a4976f66d28d227e95ae6acfc73376adc660f>`_)

PR `73 <https://github.com/spacepy/dbprocessing/pull/73>`_: Update postgresql dialect name in CreateDBsabrs (`b5ba751b <https://github.com/spacepy/dbprocessing/commit/b5ba751b07dd200cff73421df24eb719b5257c6c>`_)

PR `72 <https://github.com/spacepy/dbprocessing/pull/72>`_: fast_data: add option to archive files instead of delete. (`dc3fb0be <https://github.com/spacepy/dbprocessing/commit/dc3fb0be50d0293a4669a9b76daed1843ce53b9f>`_)

PR `77 <https://github.com/spacepy/dbprocessing/pull/77>`_: Python 3 support (`29b66787 <https://github.com/spacepy/dbprocessing/commit/29b66787b28137bbf7fdb37310bbef0c9d659de4>`_)

PR `79 <https://github.com/spacepy/dbprocessing/pull/79>`_: Fix addFromConfig for processes with no output product (`3aafa1e1 <https://github.com/spacepy/dbprocessing/commit/3aafa1e12180b62e771e29094bb807b2296eeaa3>`_)

PR `78 <https://github.com/spacepy/dbprocessing/pull/78>`_: add postgresql support and unit testing (`5c0082c9 <https://github.com/spacepy/dbprocessing/commit/5c0082c9c64e0b7c0b244ee168f4e571a19109dd>`_)

PR `90 <https://github.com/spacepy/dbprocessing/pull/90>`_:  Only use products that exist on disk when figuring inputs (`0bae771b <https://github.com/spacepy/dbprocessing/commit/0bae771bb106e94a228b672596856d28a7ab524a>`_)

    `47 <https://github.com/spacepy/dbprocessing/issues/47>`_: _getRequiredProducts should require input files to exist on disc

PR `91 <https://github.com/spacepy/dbprocessing/pull/91>`_: fast_data: skip deleting files that don't exist on disk (`b6711a74 <https://github.com/spacepy/dbprocessing/commit/b6711a74a57c25e0938f082f8eb570a98800cbad>`_)

PR `92 <https://github.com/spacepy/dbprocessing/pull/92>`_: Use docutils <0.16 when building docs in CI (`d81efdfb <https://github.com/spacepy/dbprocessing/commit/d81efdfbbfe7f6cae1798e68d93bfabd4dde419f>`_)

PR `88 <https://github.com/spacepy/dbprocessing/pull/88>`_: add count/exist/quiet options to printProcessQueue (`5e6e4132 <https://github.com/spacepy/dbprocessing/commit/5e6e4132c14cac5ece34ee20aab64b76e94b661d>`_)

    `87 <https://github.com/spacepy/dbprocessing/issues/87>`_: Use exit code of printProcessQueue to indicate if the queue is empty

PR `93 <https://github.com/spacepy/dbprocessing/pull/93>`_: printProcessQueue: add ability to filter by product, sort (`6469cb26 <https://github.com/spacepy/dbprocessing/commit/6469cb26a1b98d7832a58e60b318cbf44e238857>`_)

PR `98 <https://github.com/spacepy/dbprocessing/pull/98>`_: Force empty output products to null (Closes #95) (`4a8ec7ad <https://github.com/spacepy/dbprocessing/commit/4a8ec7ad96b44158f9d72970fa3301a0d2ac62a3>`_)

    `95 <https://github.com/spacepy/dbprocessing/issues/95>`_: No output fail in postgres

PR `94 <https://github.com/spacepy/dbprocessing/pull/94>`_: Make documentation fully automatic (`79363013 <https://github.com/spacepy/dbprocessing/commit/7936301363eca49f5b5f473ca8a86805fff57909>`_)

PR `99 <https://github.com/spacepy/dbprocessing/pull/99>`_: test_DButils: clarify no-output tests (`34d3cb0b <https://github.com/spacepy/dbprocessing/commit/34d3cb0b9f3175b29f0207cc11458dcc4dd79ab0>`_)

PR `100 <https://github.com/spacepy/dbprocessing/pull/100>`_: Update documentation for scripts (`fdcc7e38 <https://github.com/spacepy/dbprocessing/commit/fdcc7e38cde75cd7695258a231f2090e1942f1f9>`_)

PR `101 <https://github.com/spacepy/dbprocessing/pull/101>`_: Close DBrunner db on exit; document the need to close (`74cec351 <https://github.com/spacepy/dbprocessing/commit/74cec351ef5cc38db1b7385ecba2b058ea42382e>`_)

PR `102 <https://github.com/spacepy/dbprocessing/pull/102>`_: Document tables/database structure (`6852abd1 <https://github.com/spacepy/dbprocessing/commit/6852abd100df4b3c8a64966154ce4a40a65e2b18>`_)

PR `112 <https://github.com/spacepy/dbprocessing/pull/112>`_: Documentation overhaul (`a48f2629 <https://github.com/spacepy/dbprocessing/commit/a48f262978caa8f524fdb0f06ec9748751d00087>`_)

PR `113 <https://github.com/spacepy/dbprocessing/pull/113>`_: Create "empty" test database in unit tests (`4300d704 <https://github.com/spacepy/dbprocessing/commit/4300d704f40e239fda7b590c87ddebb95941b2d7>`_)

PR `114 <https://github.com/spacepy/dbprocessing/pull/114>`_: Convert all unit tests to use Postgres (`fb95a08f <https://github.com/spacepy/dbprocessing/commit/fb95a08ff01bf293a4a4059f331ad4860066ddb7>`_)

    `34 <https://github.com/spacepy/dbprocessing/issues/34>`_: Create RBSP_MAGEIS.sqlite database

PR `119 <https://github.com/spacepy/dbprocessing/pull/119>`_: Get unit tests working on Windows (`2ca3554f <https://github.com/spacepy/dbprocessing/commit/2ca3554fb28a6846ff3d2a7201492f60e0efb2ba>`_)

PR `120 <https://github.com/spacepy/dbprocessing/pull/120>`_: Fix python 3.9 and sqlalchemy 1.4 deprecations (`0a0a7442 <https://github.com/spacepy/dbprocessing/commit/0a0a74424ddab2b927ae887e9b8949810a2129a6>`_)

    `83 <https://github.com/spacepy/dbprocessing/issues/83>`_: deprecations: collections.abc, Engine.table_name

PR `122 <https://github.com/spacepy/dbprocessing/pull/122>`_: Provide support documentation integrated with GitHub and other doc tweaks (`6b6ad6b8 <https://github.com/spacepy/dbprocessing/commit/6b6ad6b802febe9de383455965763e0ebbae3e7b>`_)

    `118 <https://github.com/spacepy/dbprocessing/issues/118>`_: Link github page in documentation

PR `123 <https://github.com/spacepy/dbprocessing/pull/123>`_: Updates for release 0.1.0

PR `124 <https://github.com/spacepy/dbprocessing/pull/124>`_: Updated docs for release 0.1.0

Other issues closed this release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`2 <https://github.com/spacepy/dbprocessing/issues/2>`_: Website not finding CSS

`42 <https://github.com/spacepy/dbprocessing/issues/42>`_: PR checklist calls for CHANGELOG update, yet there is no changelog

`48 <https://github.com/spacepy/dbprocessing/issues/48>`_: Clean up tags in repo
