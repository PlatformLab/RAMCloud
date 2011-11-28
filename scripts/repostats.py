#!/usr/bin/env python

# Copyright (c) 2011 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""Uploads a report to dumpstr with statistics about the git repo."""

from __future__ import division, print_function

from collections import defaultdict
import random
import re
import os
import subprocess
import sys

from common import captureSh, getDumpstr
from ordereddict import OrderedDict

# Utilities

def first(x): return x[0]
def second(x): return x[1]

def seq_to_freq(seq):
    """Given a list of things, count how many times each one occurs in the
    list.

    Returns these counts as a dictionary.
    """

    freq = defaultdict(int)
    for x in seq:
        freq[x] += 1
    return freq


class FileType:
    """Methods for classifying files as different types."""

    PATTERNS = [
        ('script', '^scripts/'),
        ('misc', '^bindings/'),
        ('misc', '^misc/'),
        ('script', '^hooks/'),

        ('script', '\.py$'),
        ('script', '\.sh$'),
        ('script', '\.bash$'),
        ('test', 'Test\.cc$'),
        ('source', '^src/.*\.h$'),
        ('source', '^src/.*\.cc$'),

        ('misc', '.*')
    ]

    ALL_FILETYPES = sorted(set(map(first, PATTERNS)))

    @classmethod
    def get(cls, filename):
        """Given a filename, return its type."""
        for filetype, pattern in cls.PATTERNS:
            if re.search(pattern, filename) is not None:
                return filetype

    @classmethod
    def make_filetype_to_int_map(cls):
        """Return a dict mapping from all possible file types to 0."""

        return OrderedDict([(filetype, 0) for filetype in cls.ALL_FILETYPES])

class Author:
    """Methods for dealing with author names."""

    REMAPPING = {
        'ankitak': 'Ankita Kejriwal',
    }

    @classmethod
    def get(cls, author_name):
        """Given an author name from git, return the author name to use.

        The returned value is often the same as the input, but occasionally
        people will use multiple aliases that need to be collapsed down.
        """

        try:
            return cls.REMAPPING[author_name]
        except KeyError:
            return author_name

def blame(filename):
    """Run git blame on a file and return a mapping from each author to the
    number of lines of code each author accounts for."""

    try:
        output = captureSh('git blame -p %s' % filename,
                           stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        # may be a submodule
        print('Warning: Skipping', filename, '- could not git blame')
        return {}

    authors = {}
    current_commit = None
    commit_to_author = {}
    commit_to_num_lines = defaultdict(int)
    num_lines = None
    for line in output.split('\n'):
        m = re.search('^([0-9a-f]{40}) \d+ \d+ (\d+)$', line)
        if m is not None:
            current_commit = m.group(1)
            num_lines = int(m.group(2))
            commit_to_num_lines[current_commit] += num_lines
            continue
        m = re.search('^author (.*)$', line)
        if m is not None:
            author = Author.get(m.group(1))
            commit_to_author[current_commit] = author
            continue

    author_to_num_lines = defaultdict(int)
    for commit, num_lines in commit_to_num_lines.items():
        author = commit_to_author[commit]
        author_to_num_lines[author] += num_lines
    return author_to_num_lines

def print_and_upload_report(report, trends):
    """Dump the report to the screen and upload it."""

    dumpstr = getDumpstr()
    dumpstr.print_report(report)
    s = dumpstr.upload_report('gitrepo', report, trends)
    print('You can view your report at %s' % s['url'])


def get_commits_by_author():
    """Return a map from author to the number of commits that author has
    made."""

    output = captureSh('git log --pretty=format:%an').split('\n')
    return seq_to_freq(map(Author.get, output))

if __name__ == '__main__':

    # A complete list of files in the repo.
    files = captureSh('git ls-files').split('\n')

    # Uncomment this during testing to skip about 90% of the files and make the
    # script run faster.
    #files = filter(lambda x: random.randrange(10) == 0, files)

    blame_data = dict([(filename, blame(filename))
                       for filename in files])

    commits_by_author = get_commits_by_author()
    num_commits = sum(commits_by_author.values())

    report = []
    trends = []

    summary_lines = []
    report.append({'key': 'Summary', 'lines': summary_lines})

    # Number of files, broken up by file type
    filetype_to_num_files = defaultdict(int)
    for filename in files:
        filetype = FileType.get(filename)
        filetype_to_num_files[filetype] += 1
    summary_lines.append({
        'key': 'Number of files',
        'summary': ['% 6d  total' % len(files),
                    '% 6d source' % filetype_to_num_files['source']],
        'points': sorted(filetype_to_num_files.items()),
        'unit': ''
    })
    trends.append(('repo_num_files', len(files)))
    trends.append(('repo_num_source_files', filetype_to_num_files['source']))

    # Lines of code, broken up by file type
    filetype_to_loc = defaultdict(int)
    for filename, author_to_num_lines in blame_data.items():
        filetype = FileType.get(filename)
        loc = sum(author_to_num_lines.values())
        filetype_to_loc[filetype] += loc
    summary_lines.append({
        'key': 'Lines of code',
        'summary': ['% 6d  total' % sum(filetype_to_loc.values()),
                    '% 6d source' % filetype_to_loc['source']],
        'points': sorted(filetype_to_loc.items()),
        'unit': ''
    })
    trends.append(('repo_loc', sum(filetype_to_loc.values())))
    trends.append(('repo_source_loc', filetype_to_loc['source']))

    # Number of commits
    summary_lines.append({
        'key': 'Number of commits',
        'summary': '% 6d' % num_commits,
        'points': num_commits,
        'unit': ''
    })
    trends.append(('repo_num_commits', num_commits))

    # Lines of code by author, broken up by file type
    # map from author to (map from type to lines of code)
    loc_by_author_type = defaultdict(FileType.make_filetype_to_int_map)
    for filename, author_to_num_lines in blame_data.items():
        filetype = FileType.get(filename)
        for author, num_lines in author_to_num_lines.items():
            loc_by_author_type[author][filetype] += num_lines
    loc_by_author_lines = []
    report.append({'key': 'Lines of code by author',
                   'lines': loc_by_author_lines})
    for author, loc_by_type in sorted(loc_by_author_type.items(),
                                      key=lambda x: sum(x[1].values()),
                                      reverse=True):
        loc_by_author_lines.append({
            'key': author,
            'summary': ['% 6d  total' % sum(loc_by_type.values()),
                        '% 6d source' % loc_by_type['source']],
            'points': loc_by_type.items(),
            'unit': ''
        })

    # Number of commits by author
    commits_by_author_lines = []
    report.append({'key': 'Commits by author',
                   'lines': commits_by_author_lines})
    for author, count in sorted(commits_by_author.items(),
                               key=second, reverse=True):
        commits_by_author_lines.append({'key': author,
                                        'summary': count,
                                        'points': count,
                                        'unit': ''})

    print_and_upload_report(report, trends)
