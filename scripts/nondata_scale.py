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

Keeps partition size constant and scales the number of recovery masters.
"""

from __future__ import division, print_function
from common import *
import metrics
import recovery
import subprocess

print("""Don\'t forget to set your segment size to 16 * 1024!
Don\'t forget to set MAX_RPC_SIZE in InfRcTransport.h to 8 * 1024 * 1024 + 4096!
Don\'t forget to set LogDigest::SegmentId to uint16_t!""")

def averageTuples(l):
    return [sum(m) / len(l) for m in zip(*l)]

def writeFile(trials):
    dat = open('%s/recovery/nondata_scale.data' % top_path, 'w', 1)
    times = []
    outerKeys = trials.keys()
    outerKeys.sort()
    for numObjects in outerKeys:
        partitions = trials[numObjects]
        print('#numObjects:', numObjects, file=dat)
        innerKeys = partitions.keys()
        innerKeys.sort()
        for partitionCount in innerKeys:
            samples = partitions[partitionCount]
            avgs = averageTuples(samples)
            print(partitionCount, ' '.join([str(f) for f in avgs]), '#', str(samples), file=dat)
            #print(partitionCount, *avgs, file=dat)
        print(file=dat)
        print(file=dat)

def main():
    # trials has format { numObjects: { partitionCount: [(recoveryTime, )] } }
    trials = {}
    for trial in range(5):
        for numObjects in [1, -1]:
            for numPartitions in reversed(range(3, 61)):
                args = {}
                args['numBackups'] = numPartitions
                args['numPartitions'] = numPartitions
                args['objectSize'] = 1024
                args['disk'] = 4
                args['replicas'] = 3

                # Note: 96 MBbytes of memory means about 6000 segments, which
                # means > 12kbytes for log digests in the worst case.  If more
                # segments are needed in the future, it will be necessary to
                # increase the segment size to make room for larger log
                # digests.
                args['oldMasterArgs'] = '-t 96'
                args['newMasterArgs'] = '-t 96'
                if numObjects == -1:
                    # The following calculation produces about 75 segments
                    # for each partition.  It's based on the following
                    # measurements made on
                    # August 22, 2011:
                    # numPartitions == 60 and numObjects == 800 produces
                    #     75 segs/partition
                    # numPartitions == 3 and numObjects == 1150 produces
                    #     75 segs/partition
                    #
                    # The tricky the issue is that as the total number of
                    # segments increases the log digests take up more and
                    # more space, so we need to create fewer objects per
                    # partition
                    args['numObjects'] = 1150 + (800-1150)*(numPartitions-3)/(60 - 3)
                else:
                    args['numObjects'] = numObjects
                print('%d partitions, each with %d objects' %
                      (numPartitions, args['numObjects']))
                if numObjects not in trials:
                    trials[numObjects] = {}
                while True:
                    r = None
                    try:
                        r = recovery.insist(**args)
                        print('->', r['ns'] / 1e6, 'ms', '(run %s)' % r['run'])
                        metrics = r['metrics']
                        print('iteration', trial,
                              sum([backup.backup.storageReadCount
                                   for backup in metrics.backups]),
                              sum([backup.backup.storageReadCount
                                   for backup in metrics.backups]) / numPartitions)
                        if numPartitions not in trials[numObjects]:
                            trials[numObjects][numPartitions] = []
                        hosts = metrics.masters + metrics.backups
                        hosts.append(metrics.coordinator)
                        diskReadingMsPoints = [backup.backup.readingDataTicks * 1e3 /
                                               backup.clockFrequency
                                               for backup in r['metrics'].backups]
                        messageCount = sum([h.transport.transmit.messageCount for h in hosts])
                        trials[numObjects][numPartitions].append(
                            (r['ns'] / 1e6, max(diskReadingMsPoints), messageCount))
                        print(diskReadingMsPoints)
                        break
                    except KeyError, e:
                        print(e)
                        try:
                            run = r['run']
                        except:
                            run = '??'
                        print('Broken metrics, trying again (run %s)' % run)
                writeFile(trials)

if __name__ == '__main__':
    main()
