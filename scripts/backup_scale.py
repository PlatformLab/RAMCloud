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

"""Generates data for a recovery performance graph.

Varies the number of backups feeding data to one recovery master.
"""

from __future__ import division, print_function
from common import *
import config
import metrics
import recovery
import subprocess

dat = open('%s/recovery/backup_scale.data' % top_path, 'w', 1)

for numBackups in range(3, len(config.hosts)):
    print('Running recovery with %d backup(s)' % numBackups)
    args = {}
    args['num_servers'] = numBackups
    args['num_partitions'] = 1
    args['object_size'] = 1024
    args['num_objects'] = 592950 # 600MB
    args['master_ram'] = 8000
    args['replicas'] = 3
    r = recovery.insist(**args)
    print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])
    masterCpuMs = metrics.average(
        [(master.master.recoveryTicks / master.clockFrequency)
         for master in r['metrics'].masters]) * 1e3
    diskBandwidth = sum([(backup.backup.storageReadBytes +
                          backup.backup.storageWriteBytes) / 2**20
                         for backup in r['metrics'].backups]) * 1e9 / r['ns']
    diskActiveMsPoints = [backup.backup.storageReadTicks * 1e3 /
                          backup.clockFrequency
                          for backup in r['metrics'].backups]
    print(numBackups,
          r['ns'] / 1e6,
          masterCpuMs,
          diskBandwidth,
          metrics.average(diskActiveMsPoints),
          min(diskActiveMsPoints),
          max(diskActiveMsPoints),
          metrics.average([master.master.logSyncTicks * 1e3 /
                           master.clockFrequency
                           for master in r['metrics'].masters]),
          metrics.average([(master.master.replicationBytes * 8 / 2**30) /
                           (master.master.replicationTicks /
                            master.clockFrequency)
                           for master in r['metrics'].masters]),
          metrics.average([(master.master.logSyncBytes * 8 / 2**30) /
                           (master.master.logSyncTicks /
                            master.clockFrequency)
                           for master in r['metrics'].masters]),
          metrics.average([master.master.replicationTicks * 1e3 /
                           master.clockFrequency
                           for master in r['metrics'].masters]),
          metrics.average([master.transport.clientRpcsActiveTicks * 1e3 /
                           master.clockFrequency
                           for master in r['metrics'].masters]),
          file=dat)
