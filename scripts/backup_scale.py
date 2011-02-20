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

from __future__ import division
from common import *
import metrics
import recovery
import subprocess

dat = open('%s/recovery/backup_scale.data' % top_path, 'w')

for numBackups in range(1, 7):
    print 'Running recovery with %d backup(s)' % numBackups
    args = {}
    args['numBackups'] = numBackups
    args['numPartitions'] = 1
    args['objectSize'] = 1024
    args['disk'] = '/dev/sdb1'
    args['numObjects'] = 626012 * 400 // 640
    args['oldMasterArgs'] = '-m 3000'
    args['replicas'] = 1
    r = recovery.insist(**args)
    masterCpuNs = metrics.average(
        [(master.recoveryTicks -
          master.master.segmentOpenStallTicks -
          master.master.segmentWriteStallTicks -
          master.master.segmentReadStallTicks) /
         master.clockFrequency
         for master in r['metrics'].masters]) * 1e9
    dat.write('%d\t%d\t%d\n' % (numBackups, r['ns'], masterCpuNs))
    dat.flush()
