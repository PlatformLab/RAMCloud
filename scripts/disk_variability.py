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

"""Generates data for a recovery performance graph."""

from __future__ import division, print_function
from common import *
import config
from glob import glob
import math
import metrics
import recovery
import re
import sys

def median(l):
    l = sorted(l)
    if len(l) % 2 == 0:
        return metrics.average(l[len(l)//2:len(l)//2+1])
    else:
        return l[len(l)//2]

if len(sys.argv) > 1:
    recovery_dir = sys.argv[1]
else:
    recovery_dir = 'logs/latest'

NUMBACKUPS = len(config.hosts)
TRIALS = 25
backups = [[] for i in range(NUMBACKUPS)]
for trial in range(TRIALS):
    args = {}
    args['num_servers'] = NUMBACKUPS
    args['num_partitions'] = 12
    args['object_size'] = 1024
    args['replicas'] = 3
    args['num_objects'] = (626012 * args['num_servers'] * 80 //
                           args['num_partitions'] // 640)
    r = recovery.insist(**args)
    print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])
    for i, backup in enumerate(r['metrics'].backups):
        backups[i].append(
              (backup.backup.storageReadBytes / 2**20) /
              (backup.backup.storageReadTicks / backup.clockFrequency))

    with open('%s/recovery/disk_variability.data' % top_path, 'w', 1) as dat:
        for outliersPass in [False, True]:
            for i, read in enumerate(sorted(backups, key=median)):
                read.sort()
                m = median(read)
                # Mendenhall and Sincich method
                q1 = read[int(math.ceil(1 * (len(read) + 1) / 4)) - 1]
                q3 = read[int(math.floor(3 * (len(read) + 1) / 4)) - 1]
                iqr = q3 - q1
                include = []
                outliers = []
                for x in read:
                    if x < q1 - 1.5 * iqr or x > q3 + 1.5 * iqr:
                        outliers.append(x)
                    else:
                        include.append(x)
                if outliersPass:
                    for outlier in outliers:
                        print(i, outlier, file=dat)
                else:
                    print(i, min(include), q1, m, q3, max(include), file=dat)
            print(file=dat)
            print(file=dat)
