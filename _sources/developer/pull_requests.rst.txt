**************************
Working With Pull Requests
**************************

Documented here is the procedure for working with pull requests (PRs)
in the ``dbprocessing`` project. It does not include details on working
with git and github in general; relevant docs are linked.

If you can't figure it out, please do your best and ask for help in the PR
(use the ``question`` tag).

.. contents::
   :local:

Creating a PR
=============

It is strongly recommended to create a pull request from a *topic
branch* (not ``master``) in a *fork* of `the spacepy/dbprocessing
repository <https://github.com/spacepy/dbprocessing/>`_. This allows a
clean separation of code that's in-progress vs. ready-to-go, and also
a distinction between different pull requests. Submitting a pull
request from master, even of your fork, makes it difficult to have two
open PRs, and make cleanup after a PR is merged very difficult.

Before working on an issue, then, `fork <https://docs.github.com/en/
get-started/quickstart/fork-a-repo>`_ the `spacepy/dbprocessing
repository <https://github.com/spacepy/dbprocessing/>`_. Make a `branch
<https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/
proposing-changes-to-your-work-with-pull-requests/
about-branches>`_. Work on the branch locally and, when complete, `push
the branch <https://docs.github.com/en/get-started/using-git/
pushing-commits-to-a-remote-repository>`_ to your fork. Then `open a pull
request <https://docs.github.com/en/pull-requests/
collaborating-with-pull-requests/
proposing-changes-to-your-work-with-pull-requests/
creating-a-pull-request>`_
against ``spacepy/dbprocessing master``.

If you use CircleCI, `unfollow your fork before submitting a PR
<https://support.circleci.com/hc/en-us/articles/
360008097173-Why-aren-t-pull-requests-triggering-jobs-on-my-organization->`_.

The preferred flow of code is summarized:

    1. Code is created on a `branch <https://docs.github.com/en/pull-requests/
       collaborating-with-pull-requests/
       proposing-changes-to-your-work-with-pull-requests/about-branches>`__ of a
       fork, not directly on `master`.
    2. Code enters the ``dbprocessing`` repository via pull requests.    
       This applies to contributors and developers alike; developers do
       not push directly. (Usually PRs are relative to the ``master`` branch
       but in some cases a topic branch may be created.)
    3. Code enters the ``master`` branch of a fork by `syncing upstream
       to the fork <https://docs.github.com/en/pull-requests/
       collaborating-with-pull-requests/working-with-forks/
       syncing-a-fork>`_ after the pull request has been merged.

When creating the PR, following the provided template as closely as
possible will facilitate its review.

Use ``closes #x`` in the description of the PR if merging the PR will
close that issue. (``Closes`` is preferred to ``fixes`` because e.g
closing an enhancement issue is not exactly a fix.) Referencing
other related issues or PRs is also encouraged, e.g. ``see #x``.
Avoid the `issue-closing magic words <https://docs.github.com/en/
issues/tracking-your-work-with-issues/
linking-a-pull-request-to-an-issue>`_ unless closing the issue,
in which case ``closes`` is preferred.

The template includes a checklist;
consider every item on the list and check it if completed. If an item
is not relevant, check it, add "(N/A)" to the start of the line, and
include an explanation below the checklist. E.g.:

.. code-block:: none

   - [X] ...
   - [X] (N/A) Major new functionality has appropriate Sphinx documentation
   - [X] ...

   This is a pure bugfix, no new functionality or documentation.

If working in a draft PR, adding more checklists to the description is
fine. A PR will be reviewed when:

   1. All checklists are checked (this shows in the pull request list
      as e.g. "8 of 8").
   2. The PR is marked ready for review, i.e. not draft.
   3. All CI checks pass.

Feel free to request help before this point (tag the PR with ``question``
to make it stand out).

Reviews and updating
====================

Developers will `review <https://docs.github.com/en/pull-requests/
collaborating-with-pull-requests/reviewing-changes-in-pull-requests/
reviewing-proposed-changes-in-a-pull-request>`_ PRs for inclusion, but
reviews and comments are welcome from all.
Our experience has been that using the github interface to suggest
line-by-line diffs doesn't work very well; line-by-line comments are fine
(and helpful!)

You can request a specific reviewer, but are not required to.

The review will mostly evaluate whether the PR checklist has been met,
all tests pass, and the contribution meets requirements in the :doc:`index`.

If changes are requested, they can usually be addressed by making additional
commits on your branch and pushing to your fork. The PR will be automatically
updated.

In some cases the master branch of the repository may have changed in a
way that's incompatible with the changes in the pull request. The solution
is to `rebase <https://docs.github.com/en/get-started/using-git/
about-git-rebase>`_
the topic branch against the new master. (The easiest way to do this is to
update the fork master from the upstream master, then rebase the branch.)
In the case of a conflict rebase, this can get messy...feel free to ask
for help. A developer may be able to perform the rebase if `maintainer
edits are enabled <https://docs.github.com/en/pull-requests/
collaborating-with-pull-requests/working-with-forks/
allowing-changes-to-a-pull-request-branch-created-from-a-fork>`_.
After the rebase, the updated branch will have to be `force-pushed
<https://stackoverflow.com/questions/5509543/
how-do-i-properly-force-a-git-push>`_ to the fork on github.

Conditions for merging
======================
In order to be merged, a pull request must:

    1. Have an approving review from a developer who is not the author
       of the PR. (Usually PR authors are barred from reviewing their own
       work.)
    2. Have no outstanding requests for changes from developers. Change
       requests can be addressed either by updating the code or
       convincing the developer through the discussion that the requested
       change is not desirable. Outstanding change requests from
       contributors who are not developers should also be seriously
       considered; it is preferred to have them resolved.
    3. Be open for at least 72 hours, so all developers have a chance to
       review. A *major* change (by discretion of developers commenting)
       should remain open for 168 hours (i.e. one week) if at all possible;
       *urgent* changes (again, by discretion of developers) may be merged
       24 hours from submission.
    4. Pass all continuous integration checks, unless there is a
       documented failure in CI and the PR is part of fixing that failure.
    5. Be merged by someone who is not the author of the PR. Developers
       may not merge their own PR, even with an approving review.
    6. Be merged by someone who has made an approving review.
    7. Be merged by someone who has *not contributed any code to the PR*.
       There are two exceptions, provided all other requirements are met:

       a. A PR may be merged by a developer whose only contribution is to
	  rebase the code to account for changes in master.
       b. A PR authored by a developer may be merged by a different
	  developer who has authored commits that make relatively minor
	  changes to the PR; again, at the discretion of the developers
	  involved.

Developers pledge to make an effort to review pull requests within one week.

Pull requests are merged via the `rebase and merge method
<https://docs.github.com/en/repositories/
configuring-branches-and-merges-in-your-repository/
configuring-pull-request-merges/
about-merge-methods-on-github>`_. This maintains a linear history and
also makes it clear both who authored the commit and who approved it
for the repository.

Once all conditions are met, a developer can `perform the merge
<https://docs.github.com/en/pull-requests/
collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/
merging-a-pull-request>`_.

Post-merge cleanup
==================
After merge, the contents of the pull request are in two separate sets
of commits: the original commits on the topic branch, and new commits on
master. To finish cleanup, the `fork should be synchronized to the
updated master <https://docs.github.com/en/pull-requests/
collaborating-with-pull-requests/working-with-forks/syncing-a-fork>`_ and the
`topic branch deleted <https://docs.github.com/en/pull-requests/
collaborating-with-pull-requests/
proposing-changes-to-your-work-with-pull-requests/
creating-and-deleting-branches-within-your-repository>`_.
