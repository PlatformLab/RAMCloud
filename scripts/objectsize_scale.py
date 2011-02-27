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

Keeps partition size constant and scales the number of objects.
"""

from __future__ import division, print_function
from common import *
import recovery
import subprocess

dat = open('%s/recovery/objectsize_scale.data' % top_path, 'w', 1)

for objectSize in [128, 256, 1024]:
    print('# objectSize:', objectSize, file=dat)
    for partitionSize in range(1, 1050, 100):
        args = {}
        args['numBackups'] = 36
        args['numPartitions'] = 1
        args['objectSize'] = objectSize
        args['disk'] = '/dev/sda2'
        args['replicas'] = 3
        numObjectsPerMb = 2**20 / (objectSize + 40)
        print('Running with objects of size %d for a %d MB partition' %
              (objectSize, partitionSize))
        r = recovery.insist(
            oldMasterArgs='-m 1600',
            newMasterArgs='-m 1600',
            numObjects=int(numObjectsPerMb * partitionSize),
            **args)
        print(' ->' , r['ns'] / 1e6, 'ms')
        print(partitionSize, r['ns'] / 1e6, file=dat)
    print(file=dat)
    print(file=dat)
