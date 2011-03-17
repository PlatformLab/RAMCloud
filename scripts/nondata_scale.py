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
import metrics
import recovery
import subprocess

dat = open('%s/recovery/nondata_scale.data' % top_path, 'w', 1)

print("""Don\'t forget to set your segment size to 16 * 1024!
Don\'t forget to set MAX_RPC_SIZE in InfRcTransport.h to 8 * 1024 * 1024 + 4096!
Don\'t forget to set LogDigest::SegmentId to uint16_t!""")

for numObjects in [-1, 1]:
    for numPartitions in reversed(range(1, 12)):
        args = {}
        args['numBackups'] = min(numPartitions * 6, 70)
        args['numPartitions'] = numPartitions
        args['objectSize'] = 1024
        args['disk'] = 3
        args['replicas'] = 3
        if numObjects == -1:
            numObjects = 626012 * (1.16 - .0075 * (numPartitions-1)) // 640
        args['numObjects'] = numObjects
        args['oldMasterArgs'] = '-m 1200'
        args['newMasterArgs'] = '-m 16000'
        print(numPartitions, 'partitions')
        trials = []
        for i in range(5):
            r = recovery.insist(**args)
            print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])
            print(sum([backup.backup.storageReadCount
                       for backup in r['metrics'].backups]))
            print(sum([backup.backup.storageReadCount
                       for backup in r['metrics'].backups]) / numPartitions)
            trials.append(str(r['ns'] / 1e6))
        print(numPartitions,
              ' '.join(trials),
              file=dat)
    print(file=dat)
    print(file=dat)
