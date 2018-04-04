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

"""
This module defines a collection of variables that specify site-specific
configuration information such as names of RAMCloud hosts and the location
of RAMCloud binaries.  This should be the only file you have to modify to
run RAMCloud scripts at your site.
"""

from common import captureSh
import os
import re
import subprocess

__all__ = ['coordinator_port', 'default_disks', 'git_branch',
           'git_ref', 'git_diff', 'hosts','obj_dir', 'obj_path',
           'local_scripts_path', 'second_backup_port', 'server_port',
           'top_path']

# git_branch is the name of the current git branch, which is used
# for purposes such as computing objDir.
try:
    git_branch = re.search('^refs/heads/(.*)$',
                           captureSh('git symbolic-ref -q HEAD 2>/dev/null'))
except subprocess.CalledProcessError:
    git_branch = None
    obj_dir = 'obj'
else:
    git_branch = git_branch.group(1)
    obj_dir = 'obj.%s' % git_branch

# git_ref is the id of the commit at the HEAD of the current branch.
try:
    git_ref = captureSh('git rev-parse HEAD 2>/dev/null')
except subprocess.CalledProcessError:
    git_ref = '{{unknown commit}}'

# git_diff is None if the working directory and index are clean, otherwise
# it is a string containing the unified diff of the uncommitted changes.
try:
    git_diff = captureSh('git diff HEAD 2>/dev/null')
    if git_diff == '':
        git_diff = None
except subprocess.CalledProcessError:
    git_diff = '{{using unknown diff against commit}}'

# obj_dir is the name of the directory containing binaries for the current
# git branch (it's just a single name such as "obj.master", not a full path)
if git_branch == None:
    obj_dir = 'obj'
else:
    obj_dir = 'obj.%s' % git_branch

# The full path name of the directory containing this script file.
local_scripts_path = os.path.dirname(os.path.abspath(__file__))

# The full pathname of the parent of scriptsPath (the top-level directory
# of a RAMCloud source tree).
top_path = os.path.abspath(local_scripts_path + '/..')

# Add /usr/local/lib to LD_LIBARY_PATH it isn't already there (this was
# needed for CentOS 5.5, but should probably be deleted now).
try:
    ld_library_path = os.environ['LD_LIBRARY_PATH'].split(':')
except KeyError:
    ld_library_path = []
if '/usr/local/lib' not in ld_library_path:
    ld_library_path.insert(0, '/usr/local/lib')
os.environ['LD_LIBRARY_PATH'] = ':'.join(ld_library_path)

# Host on which old master is run for running recoveries.
# Need not be a member of hosts
#old_master_host = ('rcmaster', '192.168.1.1', 81)
old_master_host = None

# Full path to the directory containing RAMCloud executables.
obj_path = '%s/%s' % (top_path, obj_dir)

# Ports (for TCP, etc.) to use for each kind of server.
coordinator_port = 12246
server_port = 12247
second_backup_port = 12248

# Command-line argument specifying where the server should store the segment
# replicas by default.
default_disks = '-f /dev/sda2,/dev/sdb2'

# List of machines available to use as servers or clients; see
# common.getHosts() for more information on how to set this variable.
hosts = None

class NoOpClusterHooks:
    def __init__(self):
        pass

    def cluster_enter(self, cluster):
        self.cluster = cluster

    def cluster_exit(self):
        pass
    
    def get_remote_wd(self):
        return os.getcwd()

    def get_remote_scripts_path(self):
        return os.path.join(self.get_remote_wd(), 'scripts')

    def get_remote_obj_path(self):
        return os.path.join(self.get_remote_wd(), obj_dir)

hooks = NoOpClusterHooks()

# Try to include local overrides.
try:
    from localconfig import *
except ImportError:
    pass

if __name__ == '__main__':
    import common
    print('\n'.join([s[0] for s in common.getHosts()]))
