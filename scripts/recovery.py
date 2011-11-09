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

from __future__ import division, print_function
from common import *
import cluster
import config
import log
import recoverymetrics
import os
import pprint
import re
import subprocess
import sys
import time

def recover(num_servers,
            backups_per_server,
            num_partitions,
            object_size,
            num_objects,
            replicas,
            master_ram=None,
            old_master_ram=None,
            num_removals=0,
            transport='infrc',
            timeout=60):
    """Run a recovery on the cluster specified in the config module and
    return statisitics about the recovery.

    See the config module for more parameters that affect how the recovery
    will be run.  In particular config.hosts and config.old_master_host which
    select which hosts in the cluster recovery will be run on.

    @param num_servers: Number of hosts on which to run Masters.
    @type  num_servers: C{int}

    @param backups_per_server: Number of Backups to colocate on the same host
                               with each Master.  If this is 1 the Backup is
                               run in the same process as the Master.  If this
                               is 2 then an additional process is started to
                               run the second Backup.
    @type  backups_per_server: C{int}

    @param num_partitions: Number of partitions to create on the old Master
                           before it is crashed.  This also determines how many
                           Recovery Masters will participate in recovery.
    @type  num_partitions: C{int}

    @param object_size: Size of objects to fill old Master with before crash.
    @type  object_size: C{int}

    @param num_objects: Number of objects to fill old Master with before crash.
    @type  num_objects: C{int}

    @param replicas: Number of times each segment is replicated to different
                     backups in the cluster.
    @type  replicas: C{int}

    @param master_ram: Megabytes of space allocated in each of the Masters
                       (except the old Master, see old_master_ram).  If left
                       unspecified same sane default will be attempted based
                       on num_objects and object_size.
    @type  master_ram: C{int}

    @param old_master_ram: Megabytes of space allocated in the old Master that
                           will eventually be crashed.  If left unspecified same
                           sane default will be attempted based on num_objects
                           and object_size.
    @type  old_master_ram: C{int}

    @param num_removals: Number of erases to do after inserts.  Allows
                         recoveries where some of the log contains tombstones.
    @type  num_removals: C{int}

    @param transport: A transport name (e.g. infrc, fast+udp, tcp, ...)
    @type  transport: C{str}

    @param timeout: Seconds to wait before giving up and declaring the recovery
                    to have failed.
    @type  timeout: C{int}

    @return: A recoverymetrics stats struct (see recoverymetrics.parseRecovery).
    """
    server_binary = '%s/server' % obj_path
    client_binary = '%s/recovery' % obj_path
    ensure_servers_bin = '%s/ensureServers' % obj_path

    args = {}
    args['num_servers'] = num_servers
    args['backups_per_server'] = backups_per_server
    args['replicas'] = replicas
    args['transport'] = transport
    args['timeout'] = timeout
    # Just a guess of about how much capacity a master will have to have
    # to hold all the data from all the partitions
    if master_ram:
        log_space_per_partition = master_ram
    else:
        log_space_per_partition = (200 + (1.3 * num_objects / object_size))
    args['master_args'] = '-t %d' % log_space_per_partition
    args['client'] = ('%s -f -n %d -r %d -s %d '
                      '-t %d -k %d' % (client_binary,
                      num_objects, num_removals, object_size,
                      num_partitions, num_servers))
    args['old_master_host'] = config.old_master_host
    if old_master_ram:
        args['old_master_args'] = '-t %d' % old_master_ram
    else:
        args['old_master_args'] = '-t %d' % (log_space_per_partition *
                                             num_partitions)
    recovery_logs = cluster.run(**args)

    # Collect metrics information.
    stats = {}
    stats['metrics'] = recoverymetrics.parseRecovery(recovery_logs)
    report = recoverymetrics.textReport(stats['metrics'])
    f = open('%s/metrics' % (recovery_logs), 'w')
    f.write(str(report))
    f.write('\n')
    f.close()
    stats['run'] = recovery_logs
    stats['count'] = num_objects
    stats['size'] = object_size
    stats['ns'] = stats['metrics'].client.recoveryNs
    return stats

def insist(*args, **kwargs):
    """Keep trying recoveries until the damn thing succeeds"""
    while True:
        try:
            return recover(*args, **kwargs)
        except KeyboardInterrupt, e:
            raise
        except Exception, e:
            print('Recovery failed:', e)
            print('Trying again...')
        time.sleep(0.1)

if __name__ == '__main__':
    args = {}
    args['num_servers'] = 10
    args['backups_per_server'] = 2
    args['num_partitions'] = 10
    args['object_size'] = 1024
    args['num_objects'] = 592415 # 600MB
    args['replicas'] = 3
    args['transport'] = 'infrc'
    stats = recover(**args)
    print('Recovery time: %.3fs' % (stats['ns']/1e09))
