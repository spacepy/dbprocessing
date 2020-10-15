##################
Building a release
##################

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

Release subsubsections
----------------------
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

You need a `Github token <https://docs.github.com/en/free-pro-team@latest/
github/authenticating-to-github/creating-a-personal-access-token>`_ to avoid
`throttling <https://developer.github.com/v3/#rate-limiting>`_. No scopes are
required.

Run the script with your token and the name of the last release tag. The
relevant rST will be sent to stdout; redirect to a file, and copy/paste into
the release notes. Do not do this on a shared machine, as the token will be
visible in the process table.

.. code-block:: sh

    python ./git_notes.py -t GITHUB_TOKEN -r release-0.1

