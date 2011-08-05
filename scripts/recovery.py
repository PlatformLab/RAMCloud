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

"""Runs a recovery of a master."""

from __future__ import division
from common import *
import log
import metrics
import os
import pprint
import re
import subprocess
import time

hosts = []
for i in range(1, 37):
    hosts.append(('rc%02d' % i,
                  '192.168.1.%d' % (100 + i)))

obj_path = '%s/%s' % (top_path, obj_dir)
coordinatorBin = '%s/coordinator' % obj_path
serverBin = '%s/server' % obj_path
clientBin = '%s/client' % obj_path
ensureServersBin = '%s/ensureServers' % obj_path

def recover(numBackups=1,
            numPartitions=1,
            objectSize=1024,
            numObjects=626012,
            numRemovals=0,
            replicas=1,
            disk=1,
            timeout=60,
            coordinatorArgs='',
            backupArgs='',
            oldMasterArgs='-t 2048',
            newMasterArgs='-t 2048',
            clientArgs='-f',
            hostAllocationStrategy=0,
            debug=0):

    coordinatorHost = hosts[0]
    coordinatorLocator = 'infrc:host=%s,port=12246' % coordinatorHost[1]

    # use the coordinator host as server too, but only as a last resort
    serverHosts = (hosts[1:] + [hosts[0]])

    # Figure out which ranges of serverHosts will serve as backups, as
    # recovery masters, and as dual-backups (if we're using two disks
    # on each backup).
    if disk != 3:
        doubleBackupEnd = 0;
    else:
        doubleBackupEnd = numBackups
    if hostAllocationStrategy == 1:
        masterEnd = len(serverHosts) - 1
    else:
        masterEnd = numPartitions
    masterStart = masterEnd - numPartitions

    # Figure out which disk will be used by each of primary and secondary
    # backup
    if disk == 0:
        primaryDisk = '-m'
    elif disk == 1:
        primaryDisk = '-f /dev/sda2'
    elif disk == 2:
        primaryDisk = '-f /dev/sdb2'
    elif disk == 4:
        primaryDisk = '-f /dev/md2'
    elif disk == 3:
        primaryDisk = '-f /dev/sda2'
        secondaryDisk = '-f /dev/sdb2'
    else:
        raise Exception('Disk should be an integer between 0 and 4')

    oldMasterHost = hosts[0]
    oldMasterLocator = 'infrc:host=%s,port=12242' % oldMasterHost[1]

    clientHost = hosts[0]

    run = log.createDir('recovery')

    coordinator = None
    oldMaster = None
    servers = []
    extraBackups = []
    client = None
    with Sandbox() as sandbox:
        def ensureServers(qty):
            sandbox.checkFailures()
            try:
                sandbox.rsh(clientHost[0], '%s -C %s -n %d -l 1' %
                            (ensureServersBin, coordinatorLocator, qty))
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
        ensureServers(0)

        # start dying master
        oldMaster = sandbox.rsh(oldMasterHost[0],
                ('%s -C %s -L %s -r %d %s -M' %
                 (serverBin, coordinatorLocator, oldMasterLocator, replicas,
                  oldMasterArgs)),
                 bg=True, stderr=subprocess.STDOUT,
                 stdout=open(('%s/oldMaster.%s.log' %
                              (run, oldMasterHost[0])), 'w'))
        ensureServers(1)

        # start other servers
        totalServers = 1
        for i in range(len(serverHosts)):
            # first start the main server on this host, which runs either or
            # both of recovery master & backup
            host = serverHosts[i]
            command = ('%s -C %s -L infrc:host=%s,port=12243' %
                       (serverBin, coordinatorLocator, host[1]))
            isBackup = isMaster = False
            if (i >= masterStart) and (i < masterEnd):
                isMaster = True
                command += ' -r %d %s' % (replicas, newMasterArgs)
                totalServers += 1
            else:
                command += ' -B'
            if i < numBackups:
                isBackup = True
                command += ' %s %s' % (primaryDisk, backupArgs)
                totalServers += 1
            else:
                command += ' -M'
            if isMaster or isBackup:
                servers.append(sandbox.rsh(host[0], command, bg=True,
                               stderr=subprocess.STDOUT,
                               stdout=open('%s/server.%s.log' %
                                           (run, host[0]), 'w')))

            # start extra backup server on this host, if we are using
            # dual disks.
            if isBackup and disk == 3:
                command = ('%s -C %s -L infrc:host=%s,port=12244 -B %s %s' %
                           (serverBin, coordinatorLocator, host[1],
                            secondaryDisk, backupArgs))
                extraBackups.append(sandbox.rsh(host[0], command, bg=True,
                                             stderr=subprocess.STDOUT,
                                             stdout=open('%s/backup.%s.log' %
                                                         (run, host[0]), 'w')))
                totalServers += 1
        ensureServers(totalServers)

        # pause for debugging setup, if requested
        if debug:
            print "Servers started; pausing for debug setup."
            raw_input("Type <Enter> to continue: ")

        # start client
        client = sandbox.rsh(clientHost[0],
                 ('%s -d -C %s -n %d -r %d -s %d -t %d -k %d %s' %
                  (clientBin, coordinatorLocator, numObjects, numRemovals,
                   objectSize, numPartitions, numPartitions, clientArgs)),
                 bg=True, stderr=subprocess.STDOUT,
                 stdout=open('%s/client.%s.log' % (run, clientHost[0]), 'w'))

        start = time.time()
        while client.returncode is None:
            sandbox.checkFailures()
            time.sleep(.1)
            if time.time() - start > timeout:
                raise Exception('timeout exceeded')

        stats = {}
        for i in range(100):
            try:
                stats['metrics'] = metrics.parseRecovery(run)
            except:
                time.sleep(0.1)
                continue
            break
        stats['run'] = run
        stats['count'] = numObjects
        stats['size'] = objectSize
        stats['ns'] = stats['metrics'].client.recoveryNs
        return stats

def insist(*args, **kwargs):
    """Keep trying recoveries until the damn thing succeeds"""
    while True:
        try:
            return recover(*args, **kwargs)
        except subprocess.CalledProcessError, e:
            print 'Recovery failed:', e
            print 'Trying again...'
        except ValueError, e:
            print 'Recovery failed:', e
            print 'Trying again...'

if __name__ == '__main__':
    args = {}
    args['numBackups'] = 33
    args['numPartitions'] = 11
    args['objectSize'] = 1024
    args['disk'] = 3
    args['numObjects'] = 626012 * 600 // 640
    args['oldMasterArgs'] = '-t %d' % (900 * args['numPartitions'])
    args['newMasterArgs'] = '-t 16000'
    args['replicas'] = 3
    pprint.pprint(recover(**args))
