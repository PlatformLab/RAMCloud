#!/usr/bin/env python
# Copyright (c) 2010 Stanford University
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

"""
A collection of methods for managing log files for applications such as
cluster.py.
"""

import os
import glob
import time

__all__ = ['scan']

def createDir(top, log_exists=False):
    """
    Given a top-level log directory, create a subdirectory within that
    directory to use for log files for a particular run of an application,
    and make a symbolic link from "latest" to that subdirectory. Return the
    path to the subdirectory.
    """

    try:
        os.mkdir(top)
    except:
        pass

    # when a new server is started after the clusterperf test is started,
    # it uses a new cluster object but is still part of the overall
    # test. It has to use the same log directory that was used by the
    # original cluster so that the clusterperf.py is able to gather the
    # log output from the 'latest' log directory (symbolic link)
    if log_exists:
        subdir = '%s/latest' % (top)
        return subdir

    datetime = time.strftime('%Y%m%d%H%M%S')
    latest = '%s/latest' % top
    subdir = '%s/%s' % (top, datetime)
    os.mkdir(subdir)
    try:
        os.remove('%s/latest' % top)
    except:
        pass
    os.symlink(datetime, latest)
    return subdir

def scan(dir, strings, skip_strings = []):
    """
    Read all .log files in dir, searching for lines that contain any
    strings in strings (and omitting lines that contain any string in
    skip_strings).  Return all of the matching lines, along with
    info about which log file they were in.
    """
    
    result = ""
    for name in glob.iglob(dir + '/*.log'):
        matchesThisFile = False
        for line in open(name, 'r'):
            for s in strings:
                if line.find(s) >= 0:
                    skip = False
                    for skip_string in skip_strings:
                        if line.find(skip_string) >= 0:
                            skip = True
                    if skip:
                        continue
                    if not matchesThisFile:
                        result += '**** %s:\n' % os.path.basename(name)
                        matchesThisFile = True
                    result += line
                    break;
    return result
