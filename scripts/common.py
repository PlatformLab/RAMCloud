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

import contextlib
import os
import random
import re
import shlex
import signal
import subprocess
import sys
import time

__all__ = ['sh', 'captureSh', 'Sandbox', 'getDumpstr']

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
    output = p.communicate()[0]
    if p.returncode:
        raise subprocess.CalledProcessError(p.returncode, command)
    if output.count('\n') and output[-1] == '\n':
        return output[:-1]
    else:
        return output

class Sandbox(object):
    """A context manager for launching and cleaning up remote processes."""
    class Process(object):
        def __init__(self, host, command, kwargs, sonce, proc,
                     ignoreFailures, kill_on_exit, server_process):

            self.host = host
            self.command = command
            self.kwargs = kwargs
            self.sonce = sonce
            self.proc = proc
            self.ignoreFailures = ignoreFailures
            self.kill_on_exit = kill_on_exit
            self.server_process = server_process

        def __repr__(self):
            return repr(self.__dict__)

    def __init__(self, cleanup=True):
        # cleanup indicates whether this this Sandbox needs to clean up
        # processes that are currently running as part of this run of
        # clusterperf or not.
        self.processes = []
        self.cleanup = cleanup

    def rsh(self, host, command, locator=None, ignoreFailures=False,
            is_server=False, kill_on_exit=True, bg=False, **kwargs):

        """Execute a remote command.

        @return: If bg is True then a Process corresponding to the command
                 which was run, otherwise None.
        """
        if bg:
            sonce = ''.join([chr(random.choice(range(ord('a'), ord('z'))))
                             for c in range(8)])

            server_process = is_server

            if is_server:
                # Assumes scripts are at same path on remote machine
                sh_command = ['ssh', host,
                              '%s/serverexec' % scripts_path,
                              host, os.getcwd(), "'%s'" % locator,
                              "'%s'" % command]
            else:
                # Assumes scripts are at same path on remote machine
                sh_command = ['ssh', host,
                              '%s/regexec' % scripts_path, sonce,
                              os.getcwd(), "'%s'" % command]

            p = subprocess.Popen(sh_command, **kwargs)
            process = self.Process(host, command, kwargs, sonce,
                                   p, ignoreFailures, kill_on_exit,
                                   server_process)

            self.processes.append(process)
            return process
        else:
            sh_command = ['ssh', host,
                          '%s/remoteexec.py' % scripts_path,
                          "'%s'" % command, os.getcwd()]
            subprocess.check_call(sh_command, **kwargs)
            return None

    def kill(self, process):
        """Kill a remote process started with rsh().

        @param process: A Process corresponding to the command to kill which
                        was created with rsh().
        """
        killer = subprocess.Popen(['ssh', process.host,
                                   '%s/killpid' % scripts_path,
                                    process.sonce])
        killer.wait()
        try:
            process.proc.kill()
        except:
            pass
        process.proc.wait()
        self.processes.remove(process)

    def restart(self, process):
        self.kill(process)
        self.rsh(process.host, process.command, process.ignoreFailures, True, **process.kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        with delayedInterrupts():
            killers = []
            for p in self.processes:
                # If this sandbox does not require a cleanup of its processes
                # now, don't do it. Currently only servers are started in the
                # context of such objects. They will be reaped later by some
                # object in whose context, this object was created.
                if not self.cleanup:
                    to_kill = '0'
                    killers.append(subprocess.Popen(['ssh', p.host,
                                        '%s/killserver' % scripts_path,
                                        to_kill, os.getcwd(), p.host]))
                # invoke killpid only for processes that are not servers.
                # server processes will be killed by killserver outside this
                # loop below.
                elif not p.server_process:
                    # Assumes scripts are at same path on remote machine
                    killers.append(subprocess.Popen(['ssh', p.host,
                                                     '%s/killpid' % scripts_path,
                                                     p.sonce]))

            if self.cleanup:
                chost = getHosts()[-1] # coordinator
                killers.append(subprocess.Popen(['ssh', chost[0],
                                    '%s/killcoord' % scripts_path]))

                path = '%s/logs/shm' % os.getcwd()
                files = ""
                try:
                    files = sorted([f for f in os.listdir(path)
                        if os.path.isfile( os.path.join(path, f) )])
                except:
                    pass

                # kill all the servers that are running
                for mhost in files:
                    if mhost != 'README' and not mhost.startswith("cluster"):
                        to_kill = '1'
                        killers.append(subprocess.Popen(['ssh', mhost.split('_')[0],
                                            '%s/killserver' % scripts_path,
                                            to_kill, os.getcwd(), mhost]))
                try:
                    os.remove('%s/logs/shm/README' % os.getcwd())
                    # remove the file that represents the name of the cluster.
                    # This is used so that new backups can be told whether
                    # or not to read data from their disks

                    for fname in os.listdir(path):
                        if fname.startswith("cluster"):
                            os.remove(os.path.join(path, fname))
                except:
                    pass

            for killer in killers:
                killer.wait()
        # a half-assed attempt to clean up zombies
        for p in self.processes:
            try:
                p.proc.kill()
            except:
                pass
            p.proc.wait()

    def checkFailures(self):
        """Raise exception if any process has exited with a non-zero status."""
        for p in self.processes:
            if (p.ignoreFailures == False):
                rc = p.proc.poll()
                if rc is not None and rc != 0:
                    # raise subprocess.CalledProcessError(rc, p.command)
                    # don't raise exception because the test may involve intentional
                    # crashing of the coordinator or master/backup servers
                    pass

@contextlib.contextmanager
def delayedInterrupts():
    """Block SIGINT and SIGTERM temporarily."""
    quit = []
    def delay(sig, frame):
        if quit:
            print ('Ctrl-C: Quitting during delayed interrupts section ' +
                   'because user insisted')
            raise KeyboardInterrupt
        else:
            quit.append((sig, frame))
    sigs = [signal.SIGINT, signal.SIGTERM]
    prevHandlers = [signal.signal(sig, delay)
                    for sig in sigs]
    try:
        yield None
    finally:
        for sig, handler in zip(sigs, prevHandlers):
            signal.signal(sig, handler)
        if quit:
            raise KeyboardInterrupt(
                'Signal received while in delayed interrupts section')

# This stuff has to be here, rather than at the beginning of the file,
# because config needs some of the functions defined above.
from config import *
import config
__all__.extend(config.__all__)

def getDumpstr():
    """Returns an instance of Dumpstr for uploading reports.

    You should set dumpstr_base_url in your config file if you want to use this
    to upload reports. See the dumpstr README for more info. You might be able
    to find that README on the web at
    https://github.com/ongardie/dumpstr/blob/master/README.rst
    """

    from dumpstr import Dumpstr
    try:
        url = config.dumpstr_base_url
    except AttributeError:
        d = Dumpstr("")
        def error(*args, **kwargs):
            raise Exception(
                "You did not set your dumpstr_base_url "
                "in localconfig.py, so you can't upload reports.")
        d.upload_report = error
        return d
    else:
        return Dumpstr(url)
