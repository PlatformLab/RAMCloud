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
import commands
import os
import re
import subprocess
import sys

__all__ = ['coordinator_port', 'default_disk1','default_disk2', 'git_branch',
           'git_ref', 'git_diff', 'obj_dir', 'obj_path', 'scripts_path',
           'second_backup_port', 'server_port', 'top_path', 'getHosts']

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
scripts_path = os.path.dirname(os.path.abspath(__file__))

# The full pathname of the parent of scriptsPath (the top-level directory
# of a RAMCloud source tree).
top_path = os.path.abspath(scripts_path + '/..')

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
# hosts = None
old_master_host = ('rcmaster', '192.168.1.1', 81)

# Full path to the directory containing RAMCloud executables.
obj_path = '%s/%s' % (top_path, obj_dir)

# Ports (for TCP, etc.) to use for each kind of server.
coordinator_port = 12246
server_port = 12247
second_backup_port = 12248

# Command-line argument specifying where the first backup on each
# server should storage the segment replicas.
default_disk1 = '-f /dev/sda2'

# Command-line argument specifying where the second backup should
# store its segment replicas.
default_disk2 = '-f /dev/sdb2'

# Try to include local overrides.
try:
    from localconfig import *
except ImportError:
    pass


# Returns a list of the hosts available for servers or clients;
# each entry consists of a name for the host (for ssh), an IP address
# to use for creating service locators, and an id for generating
# Ethernet addresses.
#
# By default, the function will return a list generated from servers
# locked by the current user in rcres (an RAMCloud internal utility).
# If rcres is not available, a custom list can be defined in
# localconfig.py (see below and the wiki for additional instructions).
# In the event that rcres is available and a custom list is defined,
# the function will validate the custom list against rcres.
#
# Example for constructing a custom list in localconfig.py:
# hosts = []
# for i in range(1, 61):
#     hosts.append(('rc%02d' % i,
#                   '192.168.1.%d' % (100 + i),
#                   i))

def getHosts():
  # Find servers locked by user via rcres
  rcresOutput = commands.getoutput('rcres ls -l | grep "$(whoami)" | cut -c13-16 | grep "rc[0-9]" | cut -c3-4')
  rcresFailed = re.match(".*not found.*", rcresOutput)

  # If hosts overridden in localconfig.py, check that all servers are locked
  if 'hosts' in globals():
    requstedUnlockedHosts = []
    for host in hosts:
      if str("%02d" % host[2]) not in rcresOutput.split():
        requstedUnlockedHosts.append(host[0])

    if not rcresFailed and len(requstedUnlockedHosts) > 0:
      raise Exception ("Manually defined hosts list in localconfig.py includes the "
        "following servers not locked by user in rcres:\r\n\t%s" % requstedUnlockedHosts)

    return hosts

  # hosts has not been overridden, check that rcres has some servers for us
  else:
    if rcresFailed:
      raise Exception ("config.py could not invoke rcres (%s);\r\n"
        "\tplease specify a custom hosts list in scripts/localconfig.py" % rcresOutput)

    if len(rcresOutput) == 0:
      raise Exception ("config.py found 0 rcXX servers locked in rcres;\r\n"
        "\tcheck your locks or specify a custom hosts list in scripts/localconfig.py")

    # Everything checks out, build list
    serverList = []
    for hostNum in rcresOutput.split():
      i = int(hostNum)
      serverList.append(('rc%02d' % i,
                         '192.168.1.%d' % (100 + i),
                         i))
    return serverList

if __name__ == '__main__':
    print('\n'.join([s[0] for s in getHosts()]))
