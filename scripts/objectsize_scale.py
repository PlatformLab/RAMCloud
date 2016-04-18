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

Measures recovery time as a function of partition size for a
single recovery Master and 3 different object sizes.
"""

from __future__ import division, print_function
from common import *
import config
import recovery
import subprocess

dat = open('%s/recovery/objectsize_scale.data' % top_path, 'w', 1)

numBackups = len(config.hosts)

for objectSize in [128, 256, 1024]:
    print('# objectSize:', objectSize, file=dat)
    print('# Data sourced by %d backups' % objectSize, file=dat)
    for partitionSize in range(1, 1050, 100):
        args = {}
        args['num_servers'] = numBackups
        args['num_partitions'] = 1
        args['object_size'] = objectSize
        args['replicas'] = 3
        args['master_ram'] = 8000
        numObjectsPerMb = 2**20 / (objectSize + 38)
        args['num_objects'] = int(numObjectsPerMb * partitionSize)
        print('Running with %d backups' % numBackups)
        print('Running with objects of size %d for a %d MB partition' %
              (objectSize, partitionSize))
        r = recovery.insist(**args)
        print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])
        print(partitionSize, r['ns'] / 1e6, file=dat)
    print(file=dat)
    print(file=dat)
