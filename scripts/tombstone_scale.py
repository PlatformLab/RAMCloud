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

Keeps partition size constant and scales percentage of tombstones
from 0-50% of the total partition size.
"""

from __future__ import division, print_function
from common import *
import recovery
import subprocess

dat = open('%s/recovery/tombstone_scale.data' % top_path, 'w', 1)

tombBytes = 44                          # each tombstone is 44 bytes in the log
minObjBytes = 36                        # 0-length object is 36 bytes in the log
objectBytes = tombBytes - minObjBytes   # how many bytes for equal tomb/obj size
partitionBytes = 600 * 1024 * 1024      # use a 600MB partition

for tombPct in range(0, 51, 10):
    tombPct /= 100.0
    numObjs = ((1.0 - tombPct) * partitionBytes) / (objectBytes + minObjBytes)
    numTombs = (tombPct * partitionBytes) / tombBytes
    print('# objectBytes, numObjs, numTombs, tombPct:', objectBytes,
        numObjs, numTombs, tombPct * 100.0, file=dat)

    args = {}
    args['numBackups'] = 36
    args['numPartitions'] = 1
    args['objectSize'] = objectBytes
    args['disk'] = 1
    args['replicas'] = 3
    print('Running with objects of size %d for a %d MB partition with '
          '%d objs, %d tombstones (%.2f%% of space is tombstones)' %
          (objectBytes, partitionBytes / 1024 / 1024, numObjs, numTombs,
           tombPct * 100.0))

    r = recovery.insist(
        oldMasterArgs='-m 1600',
        newMasterArgs='-m 16000',
        numObjects=numObjs,
        numRemovals=numTombs,
        timeout=180,
        **args)
    print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])
    print(objectBytes, partitionBytes / 1024 / 1024, numObjs, numTombs,
          tombPct * 100.0, r['ns'] / 1e6, file=dat)

    print(file=dat)
    print(file=dat)
