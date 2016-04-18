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

Keeps partition size constant and scales the number of recovery masters.
"""

from __future__ import division, print_function
from common import *
import config
import metrics
import recovery
import subprocess

dat = open('%s/recovery/partition_scale.data' % top_path, 'w', 1)

num_hosts = len(config.hosts)
print('#', num_hosts, 'backups', file=dat)
for numPartitions in range(1, num_hosts):
    args = {}
    args['num_servers'] = num_hosts
    args['num_partitions'] = numPartitions
    args['object_size'] = 1024
    args['replicas'] = 3
    args['num_objects'] = 626012 * 400 // 640
    args['timeout'] = 180
    print(num_hosts, 'backups')
    print(numPartitions, 'partitions')
    r = recovery.insist(**args)
    print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])
    diskActiveMsPoints = [backup.backup.storageReadTicks * 1e3 /
                          backup.clockFrequency
                          for backup in r['metrics'].backups]
    segmentsPerBackup = [backup.backup.storageReadCount
                         for backup in r['metrics'].backups]
    masterRecoveryMs = [master.master.recoveryTicks / master.clockFrequency * 1000
                        for master in r['metrics'].masters]
    print(numPartitions, r['ns'] / 1e6,
          metrics.average(diskActiveMsPoints),
          min(diskActiveMsPoints),
          max(diskActiveMsPoints),
          (min(segmentsPerBackup) *
           sum(diskActiveMsPoints) / sum(segmentsPerBackup)),
          (max(segmentsPerBackup) *
           sum(diskActiveMsPoints) / sum(segmentsPerBackup)),
          metrics.average(masterRecoveryMs),
          min(masterRecoveryMs),
          max(masterRecoveryMs),
          file=dat)
