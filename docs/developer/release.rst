******************
Building a release
******************

.. contents::
   :depth: 2
   :local:

Release notes
=============

The release notes should be updated with the date of release and checked
for completeness and accuracy. The release series (x.y) is formatted
as a section, the actual release (x.y.z) as a subsection, and the possible
subsubsections follow here.

Normally release notes should be updated as functionality is added/changed,
not only at release time.

Release note subsubsections
---------------------------
If any of these is not used for a particular release, it may be omitted.

New features
^^^^^^^^^^^^
Brief description of any features that were added in this release. Link to
appropriate documentation of the feature (e.g. user documentation such
as the script, or API documentation). Link to either the Github pull request
that introduced the feature or the enhancement request issue related to it,
whichever has the most information.

This includes added functionality in existing scripts, functions, and classes.

Deprecations and removals
^^^^^^^^^^^^^^^^^^^^^^^^^
Any functionality that has been removed or is planned to be removed
(deprecated). This includes API deprecations. For deprecated features,
link their documentation; for deprecated or removed, link the appropriate
replacement.

This also includes any changes in support for older database versions.

Dependency requirements
^^^^^^^^^^^^^^^^^^^^^^^
Any changes to dependencies (e.g. new dependencies, new minimum versions.)

Major bugfixes
^^^^^^^^^^^^^^
Fixed bugs that are likely to be of interest to a substantial fraction of
dbprocessing users. Omit very obscure bugs that are unlikely to be hit;
they will be included in the full list of closed issues.

Normally link to the relevant Github issue, although in some cases the pull
request that closed the bug may be more appropriate.

Other changes
^^^^^^^^^^^^^
Any other changes that are likely to be of interest to a substantial fraction
of users and do not fit the above categories. In particular, any changes
in behavior or expectations that are not new features or bug fixes.

Pull requests merged this release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Should be generated and pasted into release notes at release time; see below.

Other issues closed this release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Also generated at release time.

PRs and issues in release notes
===============================
The list of pull requests and issues closed in a release is created from the
script ``git_notes.py`` in ``developer/scripts``.

This script requires `GitPython <https://gitpython.readthedocs.io/en/
stable/>`_ and `PyGithub <https://pygithub.readthedocs.io/en/latest/
introduction.html>`_. On Ubuntu these are packaged as ``python-git`` and
``python-github`` (or ``python3-git`` and ``python3-github``.)

You need a `Github token <https://docs.github.com/en/authentication/
keeping-your-account-and-data-secure/creating-a-personal-access-token>`_
to avoid `throttling <https://docs.github.com/en/rest/overview/
resources-in-the-rest-api#rate-limiting>`_. No scopes are
required.

Run the script with your token and the name of the last release tag. The
relevant rST will be sent to stdout; redirect to a file, and copy/paste into
the release notes. Do not do this on a shared machine, as the token will be
visible in the process table.

.. code-block:: sh

    python ./git_notes.py -t GITHUB_TOKEN -r release-0.1

Note that the PR including the release should also be in the list; you
can open a draft PR to get the PR number, and manually add that to the
list. Similarly, the commit against ``gh-pages`` that updates the
website should be in the list.

Preparing the release commit
============================

The following changes should result in either the final release version,
or the appropriate release candidate (i.e. add "rc1" if building rc).

   * Change version in ``setup.py``, in setup_kwargs near bottom.
   * Change ``__version__`` around line 209 of ``dbprocessing/__init__.py``.

Commit these changes.

If this is the release version, make a second commit setting all the
versions to the next version number and ``rc0``. Then submit the
PR. The tagging is done in github and will be done from the last
commit before the ``rc0`` commit.

Isolated anaconda environment
=============================

You may wish to perform all the steps after this within an isolated
Anaconda environment, separate from your machine's Python setup. In
that case, download `Miniconda
<https://docs.conda.io/en/latest/miniconda.html>`_, install it, and
use that environment:

.. code-block:: sh

   bash ./Miniconda3-latest-Linux-x86_64.sh -b -p ~/miniconda
   ~/miniconda/bin/conda create -y -n dbp_build python=3
   export PYTHONNOUSERSITE=1
   export PYTHONPATH=
   source ~/miniconda/bin/activate dbp_build
   conda install sqlalchemy python-dateutil sphinx numpydoc twine numpy wheel

Note numpy is only required for the :mod:`.reports` module (and thus
its documentation).

Preparing the distributions
===========================

.. code-block:: sh

   python setup.py sdist --formats=gztar,zip
   python setup.py bdist_wheel --universal

Note this will perform a "build" and then rebuild the documentation
(in ``sphinx/build``) for the souce distribution. Tarball, zip, and
wheel are in the dist directory.

Docs
====

From the ``sphinx`` directory:

.. code-block:: sh

   make latexpdf
   cp build/latex/dbprocessing.pdf dbprocessing-x.y.z-doc.pdf
   cd build/html
   zip -r ../../dbprocessing-x.y.z-doc.zip *

Test PyPI
=========

`Test PyPI <https://packaging.python.org/guides/using-testpypi/>`_
allows the upload and test of a complete build.

A `release candidate
<https://www.python.org/dev/peps/pep-0440/#pre-releases>`_ build
should be uploaded first. ``rc0`` is used as the version number
throughout development; actual release candidates should start with
``rc1``.

Make a `.pypirc file <https://packaging.python.org/en/latest/specifications/pypirc/>`_

.. code-block:: ini

   [distutils]
   index-servers =
       pypi
       testpypi

   [pypi]
   username: <username>

   [testpypi]
   repository: https://test.pypi.org/legacy/
   username: <username>

Put all the builds (source dists) into one
directory and then do the upload:

.. code-block:: sh

   twine upload -r testpypi dbprocessing-*.zip dbprocessing-*.whl

PyPI can only take one source distribution (zip or tar.gz), so we use zip.

Test installing with:

.. code-block:: sh

   pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ dbprocessing

You can use the ``--pre`` flag to install the RC version; in that
case, probably want to use ``--no-deps`` so don't get RC version of
dependencies! (Or can specify the rc version,
e.g. ``dbprocessing==0.1.0rc1``).

Release to PyPI
===============

See `PyPI upload directions
<https://python-packaging-tutorial.readthedocs.io/en/latest/uploading_pypi.html>`_

.. code-block:: sh

   twine upload dbprocessing-*.zip dbprocessing-*.whl

Do not upload the .tar.gz since can only upload one source package per release.

There's no longer any capability to edit information on PyPI, it's
straight from the setup.py metadata.

Release to github
=================

See `GitHub directions <https://docs.github.com/en/repositories/
releasing-projects-on-github/managing-releases-in-a-repository>`_.

On the code tab, click on "n releases" (on the right column, below
"about"). Click "Draft a new release." Make the tag "release-x.y.z"
and select the appropriate commit as the target (the one where the
version number was updated).

Use just "x.y.z" as the title. The "describe" should link the release
notes on the website and also have a brief version of the release
notes included.

Click in the "upload binaries" area and upload all the files: source
distributions (zip and tar), wheel, documentation PDF
(``dbprocessing-x.y.z-doc.pdf``) and a zip
(``dbprocessing-x.y.z-doc.zip``).

Web page update
===============

Check out the ``gh-pages`` branch. Right now the root of the branch is
basically the root of the ``sphinx/build/html`` output. Copy all the
freshly-built docs there, commit, submit PR against the ``gh-pages``
branch.
