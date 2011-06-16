#!/usr/bin/env python

# Copyright (c) 2010-2011 Stanford University
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

"""Runs a RAMCloud.

Starts a coordinator, some masters, and optionally some backups.
It has a pluggable interface so it can be used to run a variety of clients.
"""

from __future__ import division
from common import *
import itertools
import metrics
import os
import pprint
import re
import subprocess
import sys
import time

hosts = []
for i in range(1, 37):
    hosts.append(('rc%02d' % i,
                  '192.168.1.%d' % (100 + i)))

obj_path = '%s/%s' % (top_path, obj_dir)
coordinatorBin = '%s/coordinator' % obj_path
backupBin = '%s/backup' % obj_path
masterBin = '%s/server' % obj_path
ensureHostsBin = '%s/ensureHosts' % obj_path

locators = {
    'coordinator': {
        'tcp': 'tcp:host=%(host)s,port=12246',
        'fast+udp': 'fast+udp:host=%(host)s,port=12246',
        'unreliable+udp': 'unreliable+udp:host=%(host)s,port=12246',
        'infrc': 'infrc:host=%(host)s,port=12246',
        # Coordinator uses udp even when rest of cluster uses infud
        'fast+infud': 'fast+udp:host=%(host)s,port=12246',
        'unreliable+infud': 'fast+udp:host=%(host)s,port=12246',
        'fast+infeth': 'fast+udp:host=%(host)s,port=12246',
        'unreliable+infeth': 'fast+udp:host=%(host)s,port=12246',
    },
    'backup': {
        'tcp': 'tcp:host=%(host)s,port=%(port)d',
        'fast+udp': 'fast+udp:host=%(host)s,port=%(port)d',
        'unreliable+udp': 'unreliable+udp:host=%(host)s,port=%(port)d',
        'infrc': 'infrc:host=%(host)s,port=%(port)d',
        'fast+infud': 'fast+infud:',
        'unreliable+infud': 'unreliable+infud:',
        'fast+infeth': 'fast+infeth:mac=00:11:22:33:44:%(id)02x',
        'unreliable+infeth': 'unreliable+infeth:mac=00:11:22:33:44:%(id)02x',
    },
    'master': {
        'tcp': 'tcp:host=%(host)s,port=%(port)d',
        'fast+udp': 'fast+udp:host=%(host)s,port=%(port)d',
        'unreliable+udp': 'unreliable+udp:host=%(host)s,port=%(port)d',
        'infrc': 'infrc:host=%(host)s,port=%(port)d',
        'fast+infud': 'fast+infud:',
        'unreliable+infud': 'unreliable+infud:',
        'fast+infeth': 'fast+infeth:mac=00:11:22:33:44:%(id)02x',
        'unreliable+infeth': 'unreliable+infeth:mac=00:11:22:33:44:%(id)02x',
    },
}

def cluster(runClients,
            numBackups=1,
            numMasters=1,
            replicas=1,
            disk=1,
            timeout=60,
            coordinatorArgs='',
            backupArgs='',
            masterArgs='-m 2048',
            hostAllocationStrategy=0,
            transport='infrc'):

    ids = itertools.count().next

    coordinatorHost = hosts[0]
    coordinatorLocator = (locators['coordinator'][transport] %
                            {'host': coordinatorHost[1],
                             'id': ids()})

    backupHosts = (hosts[1:] + [hosts[0]])[:numBackups]
    backupLocators = [(locators['backup'][transport] %
                       {'host': host[1], 'port': 12243, 'id': ids()})
                      for host in backupHosts]
    if disk == 0:
        backupDisks = ['-m' for backup in backupHosts]
    elif disk == 1:
        backupDisks = ['-f /dev/sda2' for backup in backupHosts]
    elif disk == 2:
        backupDisks = ['-f /dev/sdb2' for backup in backupHosts]
    elif disk == 4:
        backupDisks = ['-f /dev/md2' for backup in backupHosts]
    elif disk == 3:
        firstHalf = (numBackups + 1) // 2
        secondHalf = numBackups // 2
        backupHosts = ((hosts[1:] + [hosts[0]])[:firstHalf] +
                       (hosts[1:] + [hosts[0]])[:secondHalf])
        backupLocators = ([(locators['backup'][transport] %
                            {'host': host[1], 'port': 12243, 'id': ids()})
                           for host in backupHosts[:firstHalf]] +
                          [(locators['backup'][transport] %
                            {'host': host[1], 'port': 12242})
                           for host in backupHosts[:secondHalf]])
        backupDisks = (['-f /dev/sda2' for i in backupHosts[:firstHalf]] +
                       ['-f /dev/sdb2' for i in backupHosts[:secondHalf]])
    else:
        raise Exception('Disk should be an integer between 0 and 4')

    if hostAllocationStrategy == 1:
        masterHosts = (list(reversed(hosts[1:])) +
                          [hosts[0]])[:numMasters]
        masterLocators = [(locators['master'][transport] %
                           {'host': host[1], 'port': 12247, 'id': ids()})
                          for host in masterHosts]
    else:
        masterHosts = (hosts[1:] + [hosts[0]])[:numMasters]
        masterLocators = [(locators['master'][transport] %
                           {'host': host[1], 'port': 12247, 'id': ids()})
                          for host in masterHosts]

    try:
        os.mkdir('run')
    except:
        pass
    datetime = time.strftime('%Y%m%d%H%M%S')
    run = 'run/%s' % datetime
    os.mkdir(run)
    try:
        os.remove('run/latest')
    except:
        pass
    os.symlink(datetime, 'run/latest')

    coordinator = None
    backups = []
    masters = []
    client = None
    with Sandbox() as sandbox:
        def ensureHosts(qty):
            sandbox.checkFailures()
            try:
                sandbox.rsh(hosts[0][0], '%s -C %s -n %d -l 1' %
                            (ensureHostsBin, coordinatorLocator, qty))
            except:
                # prefer exceptions from dead processes to timeout error
                sandbox.checkFailures()
                raise

        # start coordinator
        coordinator = sandbox.rsh(coordinatorHost[0],
                  ('%s -C %s %s' %
                   (coordinatorBin, coordinatorLocator, coordinatorArgs)),
                  bg=True, stderr=subprocess.STDOUT,
                  stdout=open(('%s/coordinator.%s.log' %
                               (run, coordinatorHost[0])), 'w'))
        ensureHosts(0)

        # start backups
        for i, (backupHost,
                backupLocator,
                backupDisk) in enumerate(zip(backupHosts,
                                             backupLocators,
                                             backupDisks)):
            if disk == 3:
                filename = ('%s/backup.%s.%s.log' %
                            (run, backupHost[0],
                             backupDisk.split('/')[-1]))
            else:
                filename = '%s/backup.%s.log' % (run, backupHost[0])
            backups.append(sandbox.rsh(backupHost[0],
                       ('%s %s -C %s -L %s %s' %
                        (backupBin,
                         backupDisk,
                         coordinatorLocator,
                         backupLocator,
                         backupArgs)),
                       bg=True, stderr=subprocess.STDOUT,
                       stdout=open(filename, 'w')))
        ensureHosts(len(backups))

        # start masters
        for i, (masterHost,
                masterLocator) in enumerate(zip(masterHosts,
                                                masterLocators)):
            masters.append(sandbox.rsh(masterHost[0],
                                  ('%s -r %d -C %s -L %s %s' %
                                   (masterBin,
                                    replicas,
                                    coordinatorLocator,
                                    masterLocator,
                                    masterArgs)),
                                  bg=True, stderr=subprocess.STDOUT,
                                  stdout=open(('%s/master.%s.log' %
                                               (run, masterHost[0])),
                                              'w')))
        ensureHosts(len(backups) + len(masters))

        # start clients
        client = runClients(sandbox, run, coordinatorLocator, ensureHosts)

        start = time.time()
        while client.returncode is None:
            sandbox.checkFailures()
            time.sleep(.1)
            if time.time() - start > timeout:
                raise Exception('timeout exceeded')

        stats = {}
        for i in range(10):
            try:
                stats['metrics'] = metrics.parseRecovery(run)
            except:
                time.sleep(0.1)
                continue
            break
        stats['run'] = run
        return stats

if __name__ == '__main__':
    def runClients(sandbox, run, coordinator, ensureHosts):
        host = hosts[0][0]
        binary = os.path.abspath(sys.argv[1])
        return sandbox.rsh(host,
                     ('%s -C %s %s' %
                      (binary,
                       coordinator,
                       ' '.join(sys.argv[2:]))),
                     bg=True, stderr=subprocess.STDOUT,
                     stdout=open('%s/client.%s.log' % (run, host), 'w'))
    print(cluster(runClients))
