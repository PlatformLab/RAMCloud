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
"""Generates data for a recovery time distribution graph.

Keeps partition size constant and scales the number of recovery masters.
"""

from __future__ import division, print_function
from common import *
import metrics
import recovery
import subprocess
import sys

strategies = 4

def recreateCdf():
    for strategyFilter in range(strategies):
        indat = open('%s/recovery/recovery_dist.data' % top_path, 'r', 1)
        cdfdat = open('%s/recovery/recovery_dist_cdf%d.data' % (top_path, strategyFilter), 'w', 1)
        times = []
        for line in indat.readlines():
            time, diskMin, diskMax, diskAvg, strategy = line.split()
            strategy = int(strategy)
            if strategy == strategyFilter:
                time = float(time)
                if time > 5000:
                    continue
                times.append(time)

        times.sort()

        prob = 0.0
        #print (0, 0, file=cdfdat)
        for time in times:
            prob += 1.0 / len(times)
            print(prob, time, file=cdfdat)
        indat.close()
        cdfdat.close()

def main():
    dat = open('%s/recovery/recovery_dist.data' % top_path, 'w', 1)

    for i in range(10000000):
        for backupStrategy in range(strategies):
            args = {}
            args['numBackups'] = 36
            args['numPartitions'] = 12
            args['objectSize'] = 1024
            args['disk'] = 1
            args['replicas'] = 3
            args['numObjects'] = 626012 * 400 // 640
            args['backupArgs'] = '--backupStrategy=%d' % backupStrategy
            args['oldMasterArgs'] = '-m 17000'
            args['newMasterArgs'] = '-m 600'
            args['timeout'] = 60
            print('iteration', i, 'strategy', backupStrategy)
            r = recovery.insist(**args)
            print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])
            diskReadingMsPoints = [backup.backup.readingDataTicks * 1e3 /
                                   backup.clockFrequency
                                   for backup in r['metrics'].backups]
            print(r['ns'] / 1e6,
                  min(diskReadingMsPoints),
                  max(diskReadingMsPoints),
                  sum(diskReadingMsPoints) / len(diskReadingMsPoints),
                  backupStrategy,
                  file=dat)
            recreateCdf()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'cdf':
            recreateCdf()
        elif sys.argv[1] == 'run':
            main()
    else:
        raise Exception('Give a valid operation')
