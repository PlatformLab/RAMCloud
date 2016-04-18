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

"""Generates data for a recovery performance graph in the SOSP11 paper.

Proportional scaling: keeps partition size constant and scales the number
of recovery masters and backups.
"""

from __future__ import division, print_function
from common import *
import config
from ordereddict import OrderedDict
import metrics
import recovery
import subprocess

class AveragingDict(OrderedDict):
    def __setitem__(self, key, value):
        if key not in self:
            OrderedDict.__setitem__(self, key, [value])
        else:
            OrderedDict.__getitem__(self, key).append(value)
    def __getitem__(self, key):
        value = OrderedDict.__getitem__(self, key)
        return tuple(map(metrics.average, zip(*value)))

def write(data, filename):
    with open(filename, 'w') as f:
        for x, ys in data.items():
            print(x, *ys, file=f)

data = AveragingDict()
maxPartitions = len(config.hosts)
for trial in range(5):
    print('Trial', trial)
    for numPartitions in range(2, maxPartitions + 1, 2):
        print(numPartitions, ' partitions')

        args = {}
        args['num_servers'] = numPartitions
        args['num_partitions'] = numPartitions
        args['object_size'] = 1024
        args['replicas'] = 3
        args['num_objects'] = 592950
        args['timeout'] = 300
        r = recovery.insist(**args)
        print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])

        diskActiveMsPoints = [backup.backup.storageReadTicks * 1e3 /
                              backup.clockFrequency
                              for backup in r['metrics'].backups]
        segmentsPerBackup = [backup.backup.storageReadCount
                             for backup in r['metrics'].backups]
        masterRecoveryMs = [master.master.recoveryTicks / master.clockFrequency * 1000
                            for master in r['metrics'].masters]

        stats = (
              r['ns'] / 1e6,
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
              metrics.average([(master.master.replicationBytes * 8 / 2**30) /
                               (master.master.replicationTicks /
                                master.clockFrequency)
                               for master in r['metrics'].masters]),
              metrics.average([master.transport.clientRpcsActiveTicks * 1e3 /
                               master.clockFrequency
                               for master in r['metrics'].masters]),
        )
        print(stats)
        data[numPartitions] = stats
        write(data, filename='%s/recovery/prop_scale.data' % top_path)
