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

"""Misc utilities and variables for Python scripts."""

import os
import re
import shlex
import subprocess
import sys

import remoteexec

__all__ = ['git_branch', 'obj_dir', 'sh', 'captureSh', 'rsh']

try:
    ld_library_path = os.environ['LD_LIBRARY_PATH'].split(':')
except KeyError:
    ld_library_path = []
if '/usr/local/lib' not in ld_library_path:
    ld_library_path.insert(0, '/usr/local/lib')
os.environ['LD_LIBRARY_PATH'] = ':'.join(ld_library_path)

def sh(command, bg=False, **kwargs):
    """Execute a local command."""

    kwargs['shell'] = True
    if bg:
        return subprocess.Popen(command, **kwargs)
    else:
        subprocess.check_call(command, **kwargs)

def captureSh(command, **kwargs):
    """Execute a local command and capture its output."""

    kwargs['shell'] = True
    kwargs['stdout'] = subprocess.PIPE
    p = subprocess.Popen(command, **kwargs)
    rc = p.wait()
    if rc:
        raise subprocess.CalledProcessError(rc, command)
    output = p.stdout.read()
    if output.count('\n') and output[-1] == '\n':
        return output[:-1]
    else:
        return output

remoteexec_src = os.path.abspath(sys.modules['remoteexec'].__file__)
if remoteexec_src.endswith('.pyc'):
    remoteexec_src = remoteexec_src[:-1]

def rsh(host, command, bg=False, **kwargs):
    """Execute a remote command."""
    # Wrap remote command with remoteexec.py since it won't receive SIGHUP.
    # Assume remoteexec.py is at the same path on the remote machine.
    sh_command = ['ssh', host, 'python', remoteexec_src, "'%s'" % command]
    if bg:
        return subprocess.Popen(sh_command, **kwargs)
    else:
        subprocess.check_call(sh_command, **kwargs)

try:
    git_branch = re.search('^refs/heads/(.*)$',
                           captureSh('git symbolic-ref -q HEAD 2>/dev/null'))
except subprocess.CalledProcessError:
    git_branch = None
    obj_dir = 'obj'
else:
    git_branch = git_branch.group(1)
    obj_dir = 'obj.%s' % git_branch
