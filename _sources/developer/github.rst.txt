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
`at GitHub <https://docs.github.com/en/communities/
using-templates-to-encourage-useful-issues-and-pull-requests/
about-issue-and-pull-request-templates>`_.

Settings
========
See `repository roles <https://docs.github.com/en/organizations/
managing-access-to-your-organizations-repositories/
repository-roles-for-an-organization>`_.
     
Website
=======
This is stored on a separate branch; `this page <https://docs.github.com/
en/pages/getting-started-with-github-pages/
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
the CircleCI ``main`` workflow to succeed on the PR. `This document
<https://web.archive.org/web/20210304061613/
https://support.circleci.com/hc/en-us/articles/
360004346254-Workflow-status-checks-never-completes-because-of-
ci-circleci-Waiting-for-status-to-be-reported>`_ appears to state
the top-level circleci check should not be selected, but it may predate
pipelines. The ``main`` workflow appears to work; if it fails, the checks
can be expanded to identify the failed job (rather than requiring all
individual jobs to succeed, which increases the chance that a status
may not be communicated back to GitHub.)

The GitHub checks are set up as described `in the CircleCI docs
<https://circleci.com/docs/2.0/enable-checks/>`_. This involves
enabling GitHub checks on the dbprocessing repository (which
ultimately redirects to a GitHub setting) and then enabling "GitHub
Status Updates" in the CircleCI advanced project settings.

The "webhooks" repository setting on GitHub controls the triggering of
CircleCI; this is set to "Let me select individual events" and we have
just "Pull requests" selected. Otherwise CircleCI will build when a
PR is opened (the "Pull requests" selection) *and* when it's merged
(since this manifests as a push to the repository.) This is also
where a hook can be re-delivered to CircleCI (triggering processing
again) if the build never happened on CircleCI (the build can also be
restarted on CircleCI if it has already run.)

For ssh access to the CircleCI build environment, click "Project
Settings" in the dbprocessing project on CircleCI, then "SSH Keys" and
add a key there. (This needs to be the private key, so use one just
for this purpose.)

Users with a CircleCI account should *not* follow their fork on
CircleCI, as `this will cause the checks to fail
<https://support.circleci.com/hc/en-us/articles/
360008097173-Why-aren-t-pull-requests-triggering-jobs-on-my-organization->`_.
(In general, triggering a CircleCI build on a branch before submitting the
PR `may cause problems <https://ideas.circleci.com/cloud-feature-requests/
p/trigger-new-build-when-a-pull-request-is-opened>`_.)

We use `Docker authentication <https://circleci.com/docs/2.0/
private-images/>`_ to avoid `rate-limiting <https://www.docker.com/blog/
scaling-docker-to-serve-millions-more-developers-network-egress/>`_ on
the pull of the image from DockerHub. This requires a `DockerHub <https://
hub.docker.com/>`_ account (which has been created); the username and password
are set up as environment variables in CircleCI (Project Settings, Environment
Variables in CircleCI). The variables are referenced in the CircleCI
``config.yaml``. These credentials should not be reused.
