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

"""Generates data for graphing latency of different transports.
"""

from __future__ import division, print_function
from common import *
import subprocess
from collections import defaultdict
from transportbench import *
import sys

uncached = True
if len(sys.argv) > 1 and sys.argv[1] == '-c':
    print('Running cached')
    uncached = False
numObjects = 10000
objectSizes = [128, 256, 262144, 512, 1024, 32768, 4096, 8192, 131072,
               16384, 65536, 2048, 524288, 1048576]
transports = [
              'infrc',
              'fast+infud',
              'unreliable+infud',
              'fast+infeth',
              'unreliable+infeth',
              'tcp',
              'fast+udp',
              'unreliable+udp'
             ]
fields = ('latency', 'throughput')

def writeResultsToFiles(results):
    for field in fields:
        fileName = '%s/run/transport_%s.data' % (top_path, field)
        dat = open(fileName, 'w', 1)
        for transport in transports:
            print('# %s' % transport , file=dat)
            for objectSize in sorted(objectSizes):
                r = results[field][transport][objectSize]
                if r == 0:
                    continue
                print(objectSize, r, file=dat)
            print(file=dat)
            print(file=dat)

results = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
for objectSize in objectSizes:
    for transport in transports:
        print('Running', transport, 'with',
              numObjects, objectSize, 'byte objects')
        stats = transportBench(numObjects, objectSize, transport, uncached)
        run = stats['run']

        runTime = stats['ns'] / 1e9
        readSize = stats['size']
        readCount = stats['count']

        mbPerSecond = (readSize * readCount) / (runTime * 2**20)
        usPerObject = runTime * 1000000.0 / numObjects
        print('Run', run, transport,
              '%.2f MB/s %.2f us/read' % (mbPerSecond, usPerObject))
        results['latency'][transport][objectSize] = usPerObject
        results['throughput'][transport][objectSize] = mbPerSecond

        writeResultsToFiles(results)
