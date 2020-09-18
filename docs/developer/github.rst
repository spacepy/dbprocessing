******************
Github integration
******************

There are several files in the repository that serve to manage the interaction
between GitHub and the repository, and other settings in GitHub that are
relevant. Generally administrators manage thes settings, but anybody can
submit a pull request that changes the files managing this interaction.

.. contents::
   :local:

Documentation
=============
Several documentation files are also rendered by Github. See
:ref:`documentation-magic-github`.

Issue and Pull Request Templates
================================
These files are in the ``.github`` directory of the repository, and documented
`at GitHub <https://docs.github.com/en/github/building-a-strong-community/
about-issue-and-pull-request-templates>`_.

Settings
========
See `repository permissions <https://docs.github.com/en/github/
setting-up-and-managing-organizations-and-teams/
repository-permission-levels-for-an-organization>`_.
     
Website
=======
This is stored on a separate branch; `this page <https://docs.github.com/
en/github/working-with-github-pages/
configuring-a-publishing-source-for-your-github-pages-site>`_ describes
how it is rendered.

CircleCI
========
Continuous integration is managed through `CircleCI <https://circleci.com/>`_.

Some project settings:

"Only build pull requests" is on; we will not build all branches.

CircleCI refers to a PR that is relative to a fork as a "forked PR". Because
this the preferred means of submitting a PR, "Build forked pull requests"
is on. (The PR itself must actually be submitted to the dbprocessing
repository).

We require status checks to pass before merging to master; in the GitHub
branch settings, this takes the form of a branch protection rule that requires
the circleci jobs to succeed on the PR. `This document
<https://support.circleci.com/hc/en-us/articles/
360004346254-Workflow-status-checks-never-completes-because-
of-ci-circleci-Waiting-for-status-to-be-reported>`_ is somewhat ambiguous,
but what it appears to mean is that the top-level circleci check should not
be selected (individual jobs should.). All jobs are selected.

The GitHub checks are set up as described `in the CircleCI docs
<https://circleci.com/docs/2.0/enable-checks/>`_. This involves
enabling GitHub checks on the dbprocessing repository (which
ultimately redirects to a GitHub setting) and then enabling "GitHub
Status Updates" in the CircleCI advanced project settings.

The "webhooks" repository setting on GitHub controls the triggering of
CircleCI; this is set to "Let me select individual events" and we have
just "Pull requests", "Pushes" and "Releases" selected. (This was the
default setup; normally only PRs matter, as we don't do pushes without
PR, and we don't do CircleCI processing of releases.) This is also
where a hook can be re-delivered to CircleCI (triggering processing
again) if the build never happened on CircleCI (the build can also be
restarted on CircleCI if it has already run.)

For ssh access to the CircleCI build environment, click "Project
Settings" in the dbprocessing project on CircleCI, then "SSH Keys" and
add a key there. (This needs to be the private key, so use one just
for this purpose.)
