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
import time
import random

strategies = 4

numHosts = 60

def setFans(high):
    for i in range(3):
        exe = 'rcfans-max' if high else 'rcfans-normal'
        subprocess.check_call(exe, shell=True)
        time.sleep(30)
    exe = 'rcfans-assert-high' if high else 'rcfans-assert-normal'
    r = subprocess.check_call(exe, shell=True)
    if r != 0:
        raise Exception('Tried to set all fans to %s but it did not work on some hosts' % high)
    proc = subprocess.Popen('rcstatus', shell=True, stdout=subprocess.PIPE)
    out = proc.communicate()[0]
    print(out, file=sys.stderr)
    time.sleep(30)

def recreateCdf(inFileName):
    cdfdat = open('%s/recovery/recovery_dist_cdf.data' % top_path, 'w', 1)
    for tagFilter in (0, 1):
        for strategyFilter in range(strategies):
            indat = open(inFileName, 'r', 1)
            times = []
            for line in indat.readlines():
                time, diskMin, diskMax, diskAvg, strategy, tag, commentChar, run = line.split()
                time = float(time)
                diskMin = float(diskMin)
                diskMax = float(diskMax)
                diskAvg= float(diskAvg)
                tag = int(tag)
                strategy = int(strategy)
                if tag != tagFilter:
                    continue
                if strategy != strategyFilter:
                    continue
                times.append(time)

            times.sort()

            prob = 0.0
            if len(times) == 0:
                print (0, 0, file=cdfdat)
            else:
                print (0, times[0], file=cdfdat)
            for time in times:
                prob += 1.0 / len(times)
                print(prob, time, file=cdfdat)
            print(file=cdfdat)
            print(file=cdfdat)
    indat.close()
    cdfdat.close()

def main(fileName, append=False, tag=0, iterations=100000):
    mode = 'a+' if append else 'w'
    dat = open(fileName, mode, 1)

    i = 1
    # find the right strategy to resume on
    if append:
        d = open(fileName, mode, 1)
        backupStrategy = strategies - 1
        for line in d.readlines():
            time, diskMin, diskMax, diskAvg, strategy, t, commentChar, run = line.split()
            if t == tag:
                backupStrategy = int(strategy)
            i += 1
        d.close()
        backupStrategy = (backupStrategy + 1) % strategies
        print('Resuming measurements on strategy', backupStrategy, file=sys.stderr)
    else:
        backupStrategy = 0

    while iterations:
        if backupStrategy == 0:
            # skip the min strategy, we don't plot it anymore
            backupStrategy = (backupStrategy + 1) % strategies

        args = {}
        args['numBackups'] = 60
        args['numPartitions'] = 20
        args['objectSize'] = 1024
        args['disk'] = 3
        args['replicas'] = 3
        args['numObjects'] = 592415 # 600 * 2**20 / (1024 + 38)
        args['backupArgs'] = '--backupStrategy=%d' % backupStrategy
        args['oldMasterArgs'] = '-t 17000'
        args['newMasterArgs'] = '-t 8000'
        args['timeout'] = 120
        print('starting iteration', i, 'strategy', backupStrategy, file=sys.stderr)
        r = recovery.insist(**args)
        try:
            diskReadingMsPoints = [backup.backup.readingDataTicks * 1e3 /
                                   backup.clockFrequency
                                   for backup in r['metrics'].backups]
        except:
            print('No metrics, trying again', file=sys.stderr)
            continue
        print('completed iteration', i, 'strategy', backupStrategy, r['ns'] / 1e6, 'ms', '(run %s' % r['run'], 'fans %d)' % tag, file=sys.stderr)
        print(r['ns'] / 1e6,
              min(diskReadingMsPoints),
              max(diskReadingMsPoints),
              sum(diskReadingMsPoints) / len(diskReadingMsPoints),
              backupStrategy,
              tag,
              '#',
              r['run'],
              file=dat)
        recreateCdf(fileName)
        backupStrategy = (backupStrategy + 1) % strategies
        i += 1
        iterations -= 1

def runTogglingFans():
    iterations = (strategies - 1) * 20
    #iterations = (strategies - 1)
    fans = sys.argv[2] == 'high'
    while True:
        print('Setting fans to', fans, file=sys.stderr)
        setFans(fans)
        main(fileName, append=True, tag=1 if fans else 0, iterations=iterations)
        fans = not fans

if __name__ == '__main__':
    tag=0
    if sys.argv[1] in ['run', 'continue'] and len(sys.argv) > 2:
        tag = int(sys.argv[2])
        print('Tagging output files with %d' % tag, file=sys.stderr)
        if not tag in (0, 1):
            raise Exception('Tag must be 0 for normal fans, 1 for high fans')

    fileName = '%s/recovery/recovery_dist.data' % top_path
    if len(sys.argv) > 1:
        if sys.argv[1] == 'cdf':
            recreateCdf(fileName)
        elif sys.argv[1] == 'run':
            main(fileName, False, tag)
        elif sys.argv[1] == 'continue':
            main(fileName, True, tag)
        elif sys.argv[1] == 'toggle':
            runTogglingFans()
    else:
        raise Exception('Give a valid operation')
