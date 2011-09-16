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

"""
This file contains the definitions for all of the RawMetrics supported by
RAMCloud.  When executed, it generates two files, RawMetrics.in.h and
RawMetrics.in.cc, which are included by other files when building RAMCloud.
"""

from __future__ import division, print_function
from glob import glob
from optparse import OptionParser
from pprint import pprint
from functools import partial
import math
import os
import random
import re
import sys

from common import *

__all__ = ['average', 'avgAndStdDev', 'parseRecovery']

### Utilities:

class Counter:
    """Used to share an incrementing value.
    """
    def __init__(self):
        self.current = 0
    def next(self):
        self.current += 1
    def value(self):
        return self.current

class Out:
    """Indents text and writes it to a file.

    Useful for generated code.
    """
    def __init__(self, stream=sys.stdout, indent=0):
        self._stream = stream
        self._indent = indent
    def __call__(self, s):
        self._stream.write('%s%s\n' % (' ' * 4 * self._indent, s))
    def indent(self):
        return Out(self._stream, self._indent + 1)

class AttrDict(dict):
    """A mapping with string keys that aliases x.y syntax to x['y'] syntax.

    The attribute syntax is easier to read and type than the item syntax.
    """
    def __getattr__(self, name):
        return self[name]
    def __setattr__(self, name, value):
        self[name] = value
    def __delattr__(self, name):
        del self[name]

class Struct(object):
    """A container for other metrics."""

    # These attributes are defined out here to simplify
    # the implementation of getFields.
    doc = ''
    typeName = ''

    def __init__(self, doc, typeName):
        self.doc = doc
        self.typeName = typeName

    def getFields(self):
        fieldNames = sorted(set(dir(self)) - set(dir(Struct)))
        return [(n, getattr(self, n)) for n in fieldNames]

    def dumpHeader(self, out, name=''):
        indent = ' ' * 4 * (out._indent + 2)
        out('/// %s' % self.doc)
        constructorBody = ''
        if self.typeName != 'RawMetrics':
            out('struct %s {' % self.typeName)
        else:
            constructorBody = 'init();'
        fields = self.getFields()
        if fields:
            out('    %s()' % self.typeName)
            out('        : %s {%s}' %
                (('\n%s, ' % (indent)).join(
                    ['%s(%s)' % (n, f.initializer()) for n, f in fields]),
                    constructorBody))
            for n, f in fields:
                f.dumpHeader(out.indent(), n)
        if self.typeName != 'RawMetrics':
            if name:
                out('} %s;' % name)
            else:
                out('};')

    def initializer(self):
        return ''

    def dumpMetricInfoCode(self, out, name, counter):
        # name is the path (e.g., 'backup.local') to get to this struct
        # counter is a Counter used to generate "case" clauses with
        # incrementing values.
        childPrefix = name + '.'
        if len(name) == 0:
            childPrefix = ''
        for n, f in self.getFields():
            f.dumpMetricInfoCode(out, '%s%s' % (childPrefix, n), counter)

class u64(object):
    """A 64-bit unsigned integer metric."""

    def __init__(self, doc):
        self.doc = doc
    def dumpHeader(self, out, name):
        out('/// %s' % self.doc)
        out('RawMetric %s;' % name)
    def initializer(self):
        return '0'
    def dumpMetricInfoCode(self, out, name, counter):
        out('        case %s:' % (counter.value()))
        out('            return {"%s",' % (name))
        out('                    &%s};' % (name))
        counter.next()

### Metrics definitions:
class Temp(Struct):
    # see for loop below
    pass
for i in range(10):
    setattr(Temp, 'ticks{0:}'.format(i), 
            u64('total amount of time for some undefined activity'))
    setattr(Temp, 'count{0:}'.format(i), 
            u64('total number of occurrences of some undefined event'))

class TransmitReceiveCommon(Struct):
    ticks = u64('total time elapsed transmitting/receiving messages')
    messageCount = u64('total number of messages transmitted/received')
    packetCount = u64('total number of packets transmitted/received')
    iovecCount = u64('total number of Buffer chunks transmitted/received')
    byteCount = u64('total number of bytes transmitted/received')

class Transmit(TransmitReceiveCommon):
    copyTicks = u64('total time elapsed copying messages')
    dmaTicks = u64('total time elapsed waiting for DMA to HCA')

class Infiniband(Struct):
    transmitActiveTicks = u64('total time with packets on the transmit queue')

class Transport(Struct):
    transmit = Transmit('transmit docs', 'Transmit')
    receive = TransmitReceiveCommon('receive docs', 'Receive')
    infiniband = Infiniband('infiniband docs', 'Infiniband')
    sessionOpenTicks = u64(
        'total amount of time opening sessions for RPCs')
    sessionOpenCount = u64(
        'total amount of sessions opened for RPCs')
    sessionOpenSquaredTicks = u64(
        'used for calculating the standard deviation of sessionOpenTicks')
    retrySessionOpenCount = u64(
        'total amount of timeouts during session open')
    clientRpcsActiveTicks = u64(
        'total amount of time with a client RPC active on the network')

class Coordinator(Struct):
    recoveryCount = u64(
        'total number of recoveries in which this coordinator participated')
    recoveryTicks = u64('total time elapsed during recovery')
    recoveryConstructorTicks = u64(
        'total amount of time in Recovery constructor')
    recoveryStartTicks = u64('total amount of time in Recovery::start')
    tabletsRecoveredTicks = u64('total amount of time in Recovery::start')
    setWillTicks = u64('total amount of time in Recovery::setWill')
    getTabletMapTicks = u64('total amount of time in Recovery::setWill')
    recoveryCompleteTicks = u64(
        'total amount of time sending recovery complete RPCs to backups')

class Master(Struct):
    recoveryCount = u64(
        'total number of recoveries in which this master participated')
    recoveryTicks = u64('total time elapsed during recovery')
    tabletsRecoveredTicks = u64(
        'total amount of time spent calling Coordinator::tabletsRecovered')
    backupManagerTicks = u64(
        'total amount of time spent in BackupManager')
    segmentAppendChecksumTicks = u64(
        'total amount of time spent checksumming in Segment::append')
    segmentAppendCopyTicks = u64(
        'total amount of time spent copying in Segment::append')
    segmentOpenStallTicks = u64(
        'total amount of time waiting for segment open responses from backups')
    segmentReadCount = u64(
        'total number of BackupClient::getRecoveryData calls issued')
    segmentReadTicks = u64(
        'total elapsed time for RPCs reading segments from backups')
    segmentReadStallTicks = u64(
        'total amount of time stalled waiting for segments from backups')
    segmentReadByteCount = u64(
        'total size in bytes of recovery segments received from backups')
    verifyChecksumTicks = u64(
        'total amount of time verifying checksums on objects from backups')
    recoverSegmentTicks = u64(
        'total amount of time spent in MasterServer::recoverSegment')
    segmentAppendTicks = u64(
        'total amount of time spent in Segment::append')
    backupInRecoverTicks = u64(
        'total amount of time spent in BackupManager::proceed '
        'called from MasterServer::recoverSegment')
    segmentCloseCount = u64(
        'total number of complete segments written to backups')
    segmentWriteStallTicks = u64(
        'total amount of time spent stalled on writing segments to backups')
    recoverySegmentEntryCount = u64(
        'total number of recovery segment entries (e.g. objects, tombstones)')
    recoverySegmentEntryBytes = u64(
        'total number of entry bytes in recovery segments (without overhead)')
    liveObjectCount = u64('total number of live objects')
    liveObjectBytes = u64('total number of bytes of live object data')
    objectAppendCount = u64('total number of objects appended to the log')
    objectDiscardCount = u64('total number of objects not appended to the log')
    tombstoneAppendCount = u64(
        'total number of tombstones kept (currently to the side)')
    tombstoneDiscardCount = u64('total number of tombstones discarded')
    logSyncTicks = u64(
        'total amount of time syncing the log at the end of recovery')
    logSyncBytes = u64(
        'total bytes sent during log sync')
    recoveryWillTicks = u64(
        'total amount of time rebuilding will at the end of recovery')
    removeTombstoneTicks = u64(
        'total amount of time deleting tombstones at the end of recovery')
    replicationTicks = u64(
        'total time with outstanding RPCs to backups')
    replicationBytes = u64(
        'total bytes sent from first gRD response through log sync')
    replicas = u64('number of backups on which to replicate each segment')
    replayCloseTicks = u64(
        'total amount of time R-th replica took to close during replay')
    replayCloseCount = u64(
        'total number of segments closed during replay')
    logSyncCloseTicks = u64(
        'total amount of time R-th replica took to close during log sync')
    logSyncCloseCount = u64(
        'total number of segments closed during log sync')
    taskIterations = u64(
        'total times recover checked for a completed task')

class Backup(Struct):
    recoveryCount = u64(
        'total number of recoveries in which this backup participated')
    recoveryTicks = u64('total time elapsed during recovery')
    serviceTicks = u64('total time spent servicing RPC requests')
    startReadingDataTicks = u64('total amount of time in sRD')
    readRequestCount = u64(
        'total number of getRecoveryData requests processed to completion')
    readCompletionCount = u64(
        'total number of getRecoveryData requests processed to completion')
    readTicks = u64(
        'total number of time servicing getRecoveryData RPC')
    readingDataTicks = u64(
        'total amount of time between startReadingData to done reading')
    storageReadCount = u64('total number of segment reads from disk')
    storageReadBytes = u64('total amount of bytes read from disk')
    storageReadTicks = u64('total amount of time reading from disk')
    writeTicks = u64('total amount of time servicing write RPC')
    writeClearTicks = u64(
        'total amount of time clearing segment memory during segment open')
    writeCopyBytes = u64(
        'total bytes written to backup segments')
    writeCopyTicks = u64(
        'total amount of time clearing segment memory during segment open')
    writeCount = u64('total number of writeSegment requests processed')
    storageWriteCount = u64('total number of segment writes to disk')
    storageWriteBytes = u64('total amount of bytes written to disk')
    storageWriteTicks = u64('total amount of time writing to disk')
    filterTicks = u64('total amount of time filtering segments')
    currentOpenSegmentCount = u64('total number of open segments')
    totalSegmentCount = u64('total number of open or closed segments')
    primaryLoadCount = u64('total number of primary segments requested')
    secondaryLoadCount = u64('total number of secondary segments requested')
    storageType = u64('1 = in-memory, 2 = on-disk')

class RawMetrics(Struct):
    serverId = u64('server id assigned by coordinator')
    pid = u64('process ID on machine')
    clockFrequency = u64('cycles per second for the cpu')
    segmentSize = u64('size in bytes of segments')
    transport = Transport('metrics related to transports', 'Transport')
    coordinator = Coordinator('metrics for coordinator', 'Coordinator')
    master = Master('metrics for masters', 'Master')
    backup = Backup('metrics for backups', 'Backup')
    temp = Temp('metrics for temporary use', 'Temp')

def writeBuildFiles(definitions):
    counter = Counter()
    cc = Out(open('%s/RawMetrics.in.cc' % obj_dir, 'w'))
    cc('// This file was automatically generated by scripts/rawmetrics.py.')
    cc('// Do not edit it.')
    cc('namespace RAMCloud {')
    cc('RawMetrics::MetricInfo RawMetrics::metricInfo(int i)\n{')
    cc('    switch (i) {')
    definitions.dumpMetricInfoCode(cc, '', counter)
    cc('    }')
    cc('    return {NULL, NULL};')
    cc('}')
    cc('} // namespace RAMCloud')

    h = Out(open('%s/RawMetrics.in.h' % obj_dir, 'w'))
    h('// This file was automatically generated by scripts/rawmetrics.py.')
    h('// Do not edit it.')
    definitions.dumpHeader(h)
    h('    static const int numMetrics = %d;' % (counter.value()))

definitions = RawMetrics('server metrics', 'RawMetrics')

if __name__ == '__main__':
    writeBuildFiles(definitions)
    sys.exit()

