#!/usr/bin/env python

import argparse
import datetime
import operator
import os.path
import re
import sys
import time

import git
import github


class NoteCreator(object):
    """Class to grab information for release notes from github"""

    def __init__(self, repodir, rev=None, token=None):
        """Initialize variables

        Parameters
        ----------
        repodir : str
            Path to the dbprocessing repository checkout

        rev : str, optional
            Revision (tag) of the last release. Will only get information
            from this point on. Default: most recent tag.

        token : str, optional
            Github `access token <https://docs.github.com/en/
            free-pro-team@latest/github/authenticating-to-github/
            creating-a-personal-access-token>`_. This is optional but
            anonymous access might be `throttled
            <https://developer.github.com/v3/#rate-limiting>`_
        """
        self.repo = git.Repo(repodir)
        """Access to the local checked-out repo"""
        if rev is None:
            commit = sorted(
                self.repo.tags, key=lambda x: x.commit.committed_date)[-1]\
                .commit
        else:
            commit = self.repo.commit(rev=rev)
        self.last_release = datetime.datetime(
            *time.gmtime(commit.committed_date)[:6])
        """Time of the last release"""
        gh = github.Github(token)
        self.github = gh.get_repo('spacepy/dbprocessing')
        """Access to the dbprocessing github repo"""

    def main(self):
        """Perform all the operations of the script"""
        issues, prs = self.get_issues()
        closed_issues = self.print_prs(prs)
        self.print_issues(issues, closed_issues)

    def get_issues(self):
        """Get all closed issues/PRs from the last release

        Returns
        =======
        issues, prs : list
            All the issues (as :class:`~github.Issue.Issue`) and pull requests
            (:class:`~github.PullRequest.PullRequest`) closed since
            :data:`last_release`. Sorted ascending by close date.
        """
        issues = self.github.get_issues(state='closed', since=self.last_release)
        issues = [i for i in issues
                  if i.closed_at >= self.last_release]
        # Split into PRs...
        prs = [self.github.get_pull(p.number) for p in issues
               if p.pull_request is not None]
        prs = [p for p in prs if p.merged and p.base.ref == 'master']
        prs.sort(key=operator.attrgetter('merged_at'))
        # ... and non-prs
        issues = [i for i in issues if i.pull_request is None]
        issues.sort(key=operator.attrgetter('closed_at'))
        return issues, prs

    def print_prs(self, prs):
        """Print out information on the closed PRs

        Parameters
        ==========
        prs : list
            All the pull requests to print information about

        Returns
        =======
        list
            All the issue numbers that were closed by these pull requests
        """
        # Also have available body
        # There should be .labels in PRs but maybe that's just in
        # later versions...issues have "get_labels". This is also available in
        # PRs that are treated as issue objects, but not once they've been
        # cast to PR, so maybe that's an improvement for the future.
        # Can only get issues closed by PR by parsing the body
        # of the pull request for the magic words (p.body)
        # https://github.community/t/github-api-how-to-get-issues-closed-by-a-pullrequest/14114
        pat = re.compile(
            r'(?:(?:close|resolve)(?:s|d)?|fix(?:es|ed)?) \#(\d+)',
            re.IGNORECASE)
        all_closed = []
        print('\nPull requests merged this release')
        print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
        if not prs:
            print('None')
        for p in prs:
            # Remove HTML comments in case someone has magic words in comment
            body_stripped = re.sub(r'<!--.*?-->', '', p.body, flags=re.DOTALL)
            closed = [int(i) for i in re.findall(pat, body_stripped)]
            all_closed.extend(closed)
            commit_url = self.github.get_commit(p.merge_commit_sha)\
                                    .commit.html_url
            print('')
            title = 'PR `{n} <{url}>`_: {title} (`{commit} <{commit_url}>`_)'\
                    .format(commit=p.merge_commit_sha[:8],
                            commit_url=commit_url,
                            n=p.number, title=p.title, url=p.html_url)
            print(title)
            for c in closed:
                i = self.github.get_issue(c)
                print('\n    `{n} <{url}>`_: {title}'.format(
                    n=c, title=i.title, url=i.html_url))
        return list(set(all_closed))

    def print_issues(self, issues, closed_issues):
        """Print out information on closed issues

        Parameters
        ==========
        issues : list
            All the issues to print information about
        closed_issues : list
            Issue numbers that have been closed in PRs, so can be
            treated differently.
        """
        issues = [i for i in issues if i.number not in closed_issues]
        print('\nOther issues closed this release')
        print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
        if not issues:
            print('None')
        for i in issues:
            print('')
            print('`{n} <{url}>`_: {title}'.format(
                n=i.number, title=i.title, url=i.html_url))

    @staticmethod
    def parse_args(argv=None):
        """Parse command line arguments

        Parameters
        ----------
        argv : list, optional
            Command line arguments, default from :data:`sys.argv`

        Returns
        -------
        dict
            Keyword arguments suitable for passing to :meth:`__init__`.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--token',
                            help='github access token, optional')
        parser.add_argument('-r', '--rev',
                            help='git rev (usually tag name) of last release')
        kwargs = vars(parser.parse_args(argv))
        kwargs['repodir'] = os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..', '..'))
        return kwargs


if __name__ == '__main__':
    NoteCreator(**NoteCreator.parse_args()).main()
    
