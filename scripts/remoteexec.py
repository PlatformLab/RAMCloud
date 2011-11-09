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

"""A wrapper for executing commands over ssh.

Usage: remoteexec.py command wd

Changes working directory to "wd" (if it is specified), then runs
bash command "command".

Remote ssh commands from a script do not run in a terminal, so they don't get
SIGHUP when the ssh client is closed. This script will read on stdin until EOF
instead.
"""

if __name__ == '__main__':

    import os
    import sys

    if sys.version_info < (2,5):
        # Python version too old, try ~/bin/python
        try:
            if sys.executable != os.path.abspath('bin/python'):
                os.execv('bin/python', ['bin/python'] + sys.argv)
        finally:
            raise Exception("Python version is too old: %d.%d" %
                            sys.version_info[:2])

    import signal
    import subprocess

    # bash will source this before running the command
    os.environ['BASH_ENV'] = '~/.bashrc.ssh'
    if len(sys.argv) > 2:
        os.chdir(sys.argv[2])
    command = ['bash', '-c', sys.argv[1]]
    p = subprocess.Popen(command)
    signal.signal(signal.SIGCHLD, lambda signum, frame: sys.exit(p.wait()))

    r = sys.stdin.read()
    assert r == ''
    # stdin was closed, user aborted
    p.kill()
    sys.exit(1)
