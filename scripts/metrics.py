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

"""Recovery metrics."""

from __future__ import division, print_function
from glob import glob
from optparse import OptionParser
from pprint import pprint
from functools import partial
import math
import random
import re
import sys

from common import *

__all__ = ['average', 'avgAndStdDev', 'parseRecovery']

### Utilities:

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
        out('/// %s' % self.doc)
        out('struct %s {' % self.typeName)
        fields = self.getFields()
        if fields:
            out('    %s()' % self.typeName)
            out('        : %s {}' %
                ('\n%s, ' % (' ' * 4 * (out._indent + 2))).join(
                    ['%s(%s)' % (n, f.initializer()) for n, f in fields]))
            for n, f in fields:
                f.dumpHeader(out.indent(), n)
        if name:
            out('} %s;' % name)
        else:
            out('};')

    def initializer(self):
        return ''

    def dumpLoggingCode(self, out, name, fullName):
        # name is the path (e.g., 'metrics->master') to get to this struct
        #
        # fullName is the prefix of the path (e.g., 'metrics->master.') to get
        # to the members of this struct
        for n, f in self.getFields():
            f.dumpLoggingCode(out,
                              '%s%s' % (fullName, n),
                              '%s%s.' % (fullName, n))

    def assign(self, values):
        # values is a list of ('x.y.z', '3') pairs
        valuesDict = dict(values)
        ret = AttrDict()
        for var in set([var.split('.')[0] for var, _ in values]):
            filtValues = [(v.split('.', 1)[1], value)
                          for v, value in values if v.startswith('%s.' % var)]
            if var in valuesDict:
                filtValues.append(('', valuesDict[var]))
            ret[var] = getattr(self, var).assign(filtValues)
        return ret

class u64(object):
    """A 64-bit unsigned integer metric."""

    def __init__(self, doc):
        self.doc = doc
    def dumpHeader(self, out, name):
        out('/// %s' % self.doc)
        out('Metric %s;' % name)
    def initializer(self):
        return '0'
    def dumpLoggingCode(self, out, name, fullName):
        out('LOG(NOTICE,')
        out('    "%s = %%lu",' % name)
        out('    static_cast<uint64_t>(%s));' % name)
    def assign(self, values):
        assert len(values) == 1
        assert not values[0][0]
        return int(values[0][1])

def parse(f, definitions):
    """Parse a RAMCloud log file into nested AttrDicts of metrics values."""
    lines = None
    for line in f:
        line = ' '.join(line[:-1].split(' ')[6:])
        if line == 'Metrics:':
            lines = []
        elif line == 'End of Metrics':
            break
        else:
            if lines is not None:
                lines.append(line)
    if not lines:
        raise Exception, 'no metrics in %s' % f.name
    values = []
    for line in lines:
        var, value = line.split(' = ')
        var = var.split('metrics->')[1]
        values.append((var, value))
    return definitions.assign(values)

def average(points):
    """Return the average of a sequence of numbers."""
    return sum(points)/len(points)

def avgAndStdDev(points):
    """Return the (average, standard deviation) of a sequence of numbers."""
    avg = average(points)
    variance = average([p**2 for p in points]) - avg**2
    if variance < 0:
        # poor floating point arithmetic made variance negative
        assert variance > -0.1
        stddev = 0.0
    else:
        stddev = math.sqrt(variance)
    return (avg, stddev)

def seq(x):
    """Turn the argument into a sequence.

    If x is already a sequence, do nothing. If it is not, wrap it in a list.
    """
    try:
        iter(x)
    except TypeError:
        return [x]
    else:
        return x

### Report formatting:

def defaultFormat(x):
    """Return a reasonable format string for the argument."""
    if type(x) is int:
        return '{0:6d}'
    elif type(x) is float:
        return '{0:6.1f}'
    else:
        return '{0:>6s}'

class Report(object):
    """A concatenation of Sections."""
    def __init__(self):
        self.sections = []
    def add(self, section):
        self.sections.append(section)
        return section
    def __str__(self):
        return '\n\n'.join([str(section)
                            for section in self.sections if section])

class Section(object):
    """A part of a Report consisting of lines with present metrics."""

    def __init__(self, title):
        self.title = title
        self.lines = []

    def __len__(self):
        return len(self.lines)

    def __str__(self):
        if not self.lines:
            return ''
        lines = []
        lines.append('=== {0:} ==='.format(self.title))
        maxLabelLength = max([len(label) for label, columns in self.lines])
        for label, columns in self.lines:
            lines.append('{0:<{labelWidth:}} {1:}'.format(
                '{0:}:'.format(label), columns,
                labelWidth=(maxLabelLength + 1)))
        return '\n'.join(lines)

    def line(self, label, columns, note=''):
        """Add a line of text to the Section.

        It will look like this:
            label: columns[0] / ... / columns[-1] (note)
        """
        columns = ' / '.join(columns)
        if note:
            right = '{0:s}  ({1:s})'.format(columns, note)
        else:
            right = columns
        self.lines.append((label, right))

    def avgStdSum(self, label, points, pointFormat=None, note=''):
        """Add a line with the average, std dev, and sum of a set of points.

        label and note are passed onto line()

        If more than one point is given, the columns will be the average,
        standard deviation, and sum of the points. If, however, only one point
        is given, the only column will be one showing that point.

        If pointFormat is not given, a reasonable default will be determined
        with defaultFormat(). A floating point format will be used for the
        average and standard deviation.
        """
        points = seq(points)
        columns = []
        if len(points) == 1:
            point = points[0]
            if pointFormat is None:
                pointFormat = defaultFormat(point)
            columns.append(pointFormat.format(point))
        else:
            if pointFormat is None:
                avgStdFormat = '{0:6.1f}'
                sumFormat = defaultFormat(points[0])
            else:
                avgStdFormat = pointFormat
                sumFormat = pointFormat
            avg, stddev = avgAndStdDev(points)
            columns.append('{0:} avg'.format(avgStdFormat.format(avg)))
            columns.append('stddev {0:}'.format(avgStdFormat.format(stddev)))
            columns.append('{0:} total'.format(sumFormat.format(sum(points))))
        self.line(label, columns, note)

    def avgStdFrac(self, label, points, pointFormat=None,
                 total=None, fractionLabel='', note=''):
        """Add a line with the average, std dev, and avg percentage of a
        set of points.

        label and note are passed onto line()

        If more than one point is given, the columns will be the average and
        standard deviation of the points. If total is given, an additional
        column will show the percentage of total that the average of the points
        make up.

        If, however, only one point is given, the first column will be one
        showing that point. If total is given, an additional column will show
        the percentage of total that the point makes up.

        If pointFormat is not given, a reasonable default will be determined
        with defaultFormat(). A floating point format will be used for the
        average and standard deviation.

        If total and fractionLabel are given, fractionLabel will be printed
        next to the percentage of total that points make up.
        """
        points = seq(points)
        if fractionLabel:
            fractionLabel = ' {0:}'.format(fractionLabel)
        columns = []
        if len(points) == 1:
            point = points[0]
            if pointFormat is None:
                pointFormat = defaultFormat(point)
            columns.append(pointFormat.format(point))
            if total is not None:
                columns.append('{0:6.2%}{1:}'.format(point / total,
                                                     fractionLabel))
        else:
            if pointFormat is None:
                pointFormat = '{0:6.1f}'
            avg, stddev = avgAndStdDev(points)
            columns.append('{0:} avg'.format(pointFormat.format(avg)))
            columns.append('stddev {0:}'.format(pointFormat.format(stddev)))
            if total is not None:
                columns.append('{0:6.2%} avg{1:}'.format(avg / total,
                                                         fractionLabel))
        self.line(label, columns, note)

    avgStd = avgStdFrac
    """Same as avgStdFrac.

    The intent is that you don't pass total, so you won't get the Frac part.
    """

    def ms(self, label, points, **kwargs):
        """Calls avgStdFrac to print the points shown in milliseconds.

        points and total should still be provided in full seconds!
        """
        kwargs['pointFormat'] = '{0:6.1f} ms'
        if 'total' in kwargs:
            kwargs['total'] *= 1000
        self.avgStdFrac(label, [p * 1000 for p in seq(points)], **kwargs)

### Metrics definitions:
class Local(Struct):
    # see for loop below
    pass
for i in range(10):
    setattr(Local, 'ticks{0:}'.format(i), 
            u64('total amount of time for some undefined activity'))
    setattr(Local, 'count{0:}'.format(i), 
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

class Transport(Struct):
    transmit = Transmit('transmit docs', 'Transmit')
    receive = TransmitReceiveCommon('receive docs', 'Receive')
    sessionOpenTicks = u64(
        'total amount of time opening sessions for RPCs')
    sessionOpenCount = u64(
        'total amount of sessions opened for RPCs')
    sessionOpenSquaredTicks = u64(
        'used for calculating the standard deviation of sessionOpenTicks')
    retrySessionOpenCount = u64(
        'total amount of timeouts during session open')

class Coordinator(Struct):
    recoveryConstructorTicks = u64(
        'total amount of time in Recovery constructor')
    recoveryStartTicks = u64('total amount of time in Recovery::start')
    tabletsRecoveredTicks = u64('total amount of time in Recovery::start')
    setWillTicks = u64('total amount of time in Recovery::setWill')
    getTabletMapTicks = u64('total amount of time in Recovery::setWill')
    recoveryCompleteTicks = u64(
        'total amount of time sending recovery complete RPCs to backups')
    local = Local('local metrics', 'Local')

class Master(Struct):
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
    segmentReadStallTicks = u64(
        'total amount of time stalled waiting for segments from backups')
    segmentReadByteCount = u64(
        'total size in bytes of recovery segments received from backups')
    verifyChecksumTicks = u64(
        'total amount of time verifying checksums on objects from backups')
    recoverSegmentTicks = u64(
        'total amount of time spent in MasterServer::recoverSegment')
    segmentCloseCount = u64(
        'total number of complete segments written to backups')
    segmentWriteStallTicks = u64(
        'total amount of time spent stalled on writing segments to backups')
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
        'total time from first gRD response through log sync')
    replicationBytes = u64(
        'total bytes sent from first gRD response through log sync')
    local = Local('local metrics', 'Local')
    replicas = u64('number of backups on which to replicate each segment')

class Backup(Struct):
    startReadingDataTicks = u64('total amount of time in sRD')
    readCount = u64(
        'total number of getRecoveryData requests processed to completion')
    readStallTicks = u64(
        'total amount of time in gRD waiting for filtered segment')
    readingDataTicks = u64(
        'total amount of time between startReadingData to done reading')
    storageReadCount = u64('total number of segment reads from disk')
    storageReadBytes = u64('total amount of bytes read from disk')
    storageReadTicks = u64('total amount of time reading from disk')
    writeTicks = u64('total amount of time servicing write RPC')
    writeClearTicks = u64(
        'total amount of time clearing segment memory during segment open')
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
    local = Local('local metrics', 'Local')

class Recovery(Struct):
    serverId = u64('server id assigned by coordinator')
    pid = u64('process ID on machine')
    serverRole = u64('0 = coordinator, 1 = master, 2 = backup')
    clockFrequency = u64('cycles per second for the cpu')
    recoveryTicks = u64('total time elapsed during recovery')
    dispatchIdleTicks = u64('total time spinning in Dispatch::handleEvent')
    recvIdleTicks = u64(
        'total time spent spinning in TransportManager::serverRecv')
    segmentSize = u64('size in bytes of segments')
    transport = Transport('transport docs', 'Transport')
    coordinator = Coordinator('coordinator docs', 'Coordinator')
    master = Master('master docs', 'Master')
    backup = Backup('backup docs', 'Backup')

def parseRecovery(recovery_dir, definitions=None):
    if definitions is None:
        definitions = globals()['definitions']
    data = AttrDict()
    data.coordinator = parse(open(glob('%s/coordinator.*.log' %
                                       recovery_dir)[0]),
                             definitions)
    data.masters = [parse(open(f), definitions)
                    for f in sorted(glob('%s/newMaster.*.log' % recovery_dir))]
    data.backups = [parse(open(f), definitions)
                    for f in sorted(glob('%s/backup.*.log' % recovery_dir))]
    data.client = AttrDict()
    for line in open(glob('%s/client.*.log' % recovery_dir)[0]):
        m = re.search(r'\bRecovery completed in (\d+) ns\b', line)
        if m:
            data.client.recoveryNs = int(m.group(1))
    return data

def writeBuildFiles(definitions):
    h = Out(open('%s/Metrics.in.h' % obj_dir, 'w'))
    h('// This file was automatically generated by scripts/metrics.py.')
    h('// Do not edit it.')
    h('namespace RAMCloud {')
    definitions.dumpHeader(h)
    h('} // namespace RAMCloud')

    cc = Out(open('%s/Metrics.in.cc' % obj_dir, 'w'))
    cc('// This file was automatically generated by scripts/metrics.py.')
    cc('// Do not edit it.')
    definitions.dumpLoggingCode(cc, 'metrics', 'metrics->')

def rawSample(data):
    """Prints out some raw data for debugging"""

    print('Client:')
    pprint(data.client)
    print('Coordinator:')
    pprint(data.coordinator)
    print()
    print('Sample Master:')
    pprint(random.choice(data.masters))
    print()
    print('Sample Backup:')
    pprint(random.choice(data.backups))

def rawFull(data):
    """Prints out all raw data for debugging"""

    pprint(data)

def latexReport(data):
    """Generate LaTeX commands"""

    coord = data.coordinator
    masters = data.masters
    backups = data.backups

    def newcommand(prefix, name, numbers, precision=0):
        try:
            iter(numbers)
        except TypeError:
            print('\\newcommand{{\\{0:}{1:}}}{{{2:.{precision}f}}}'.format(
                  prefix, name, numbers, precision=precision))
        else:
            avg, stddev = avgAndStdDev(seq(numbers))
            print('\\newcommand{\\%s%sAvg}{%.2f}' % (prefix, name, avg))
            print('\\newcommand{\\%s%sStdDev}{%.2f}' % (prefix, name, stddev))
    pnewcommand = partial(newcommand, 'breakdown')
    def msWithPct(name, numbers, divisor):
        try:
            iter(numbers)
        except TypeError:
            pass
        else:
            numbers, stddev = avgAndStdDev(seq(numbers))
        numbers *= 1000
        divisor *= 1000
        pnewcommand(name, numbers, 1)
        pnewcommand('%sPct' % name,  100.0 * numbers / divisor, 2)
    recoveryTime = coord.recoveryTicks / coord.clockFrequency
    pnewcommand('RecoveryTime', recoveryTime, 2)
    pnewcommand('Masters', len(masters))
    pnewcommand('Backups', len(backups))
    pnewcommand('ObjectCount', sum([master.master.liveObjectCount
                                   for master in masters]))
    pnewcommand('ObjectSize', [master.master.liveObjectBytes /
                              master.master.liveObjectCount
                              for master in masters])
    msWithPct('CoordinatorIdle', (coord.idleTicks / coord.clockFrequency),
                               recoveryTime)
    msWithPct('CoordinatorStartRecoveryOnBackups',
               coord.coordinator.recoveryConstructorTicks /
               coord.clockFrequency,
               recoveryTime)
    msWithPct('CoordinatorStartRecoveryOnMasters',
               coord.coordinator.recoveryStartTicks /
               coord.clockFrequency,
               recoveryTime)
    msWithPct('MasterTotal', [master.recoveryTicks / master.clockFrequency
                              for master in masters], recoveryTime)
    msWithPct('MasterRecoverSegment',
              [master.master.recoverSegmentTicks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterBackupOpenWrite',
              [master.master.backupManagerTicks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterApproxCpu',
              [(master.master.recoverSegmentTicks -
              master.master.backupManagerTicks)
              / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterVerifyChecksum',
              [master.master.verifyChecksumTicks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterSegmentAppendCopy',
              [master.master.segmentAppendCopyTicks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterSegmentAppendChecksum',
              [master.master.segmentAppendChecksumTicks /
              master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterHtProfiler',
              [(master.master.recoverSegmentTicks -
              master.master.backupManagerTicks -
              master.master.verifyChecksumTicks -
              master.master.segmentAppendCopyTicks -
              master.master.segmentAppendChecksumTicks) /
              master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterWaitingForBackups',
              [(master.master.segmentOpenStallTicks +
              master.master.segmentWriteStallTicks +
              master.master.segmentReadStallTicks)
              / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterStalledOnSegmentOpen',
              [master.master.segmentOpenStallTicks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterStalledOnSegmentWrite',
              [master.master.segmentWriteStallTicks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterStalledOnSegmentRead',
              [master.master.segmentReadStallTicks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterRemovingTombstones',
              [master.master.removeTombstoneTicks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('MasterTransmittingInTransport',
              [master.transport.transmit.ticks / master.clockFrequency
              for master in masters],
              recoveryTime)
    msWithPct('BackupTotalMainThread',
                     [backup.recoveryTicks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupIdle',
                     [backup.idleTicks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupStartReadingData',
                     [backup.backup.startReadingDataTicks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupOpenWriteSegment',
                     [backup.backup.writeTicks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupOpenSegmentZeroMemory',
                     [backup.backup.writeClearTicks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupWriteCopy',
                     [backup.backup.writeCopyTicks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupWriteOther',
                     [(backup.backup.writeTicks -
                       backup.backup.writeClearTicks -
                       backup.backup.writeCopyTicks) / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupReadSegmentStall',
                     [backup.backup.readStallTicks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupTransmittingInTransport',
                     [backup.transport.transmit.ticks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupOther',
                     [(backup.recoveryTicks -
                       backup.idleTicks -
                       backup.backup.startReadingDataTicks -
                       backup.backup.writeTicks -
                       backup.backup.readStallTicks -
                       backup.transport.transmit.ticks) /
                      backup.clockFrequency
                      for backup in backups],
                     recoveryTime)
    msWithPct('BackupFilteringSegmentsThread',
                     [backup.backup.filterTicks / backup.clockFrequency
                      for backup in backups],
                     recoveryTime)

def textReport(data):
    """Generate ASCII report"""

    coord = data.coordinator
    masters = data.masters
    backups = data.backups

    recoveryTime = data.client.recoveryNs / 1e9
    report = Report()

    # TODO(ongaro): Size distributions of filtered segments

    summary = report.add(Section('Summary'))
    summary.avgStd('Recovery time', recoveryTime, '{0:6.3f} s')
    summary.avgStd('Masters', len(masters))
    summary.avgStd('Backups', len(backups))
    summary.avgStd('Replicas',
                   masters[0].master.replicas)
    summary.avgStd('Total objects',
                   sum([master.master.liveObjectCount
                        for master in masters]))
    summary.avgStd('Objects per master',
                   [master.master.liveObjectCount
                        for master in masters])
    summary.avgStd('Object size',
                   [master.master.liveObjectBytes /
                    master.master.liveObjectCount
                    for master in masters],
                   '{0:6.0f} bytes avg')

    if backups:
        storageTypes = set([backup.backup.storageType for backup in backups])
        if len(storageTypes) > 1:
            storageType = 'mixed'
        else:
            storageType = {1: 'memory',
                           2: 'disk'}.get(int(storageTypes.pop()),
                                          'unknown')
        summary.line('Storage type', [storageType])

    coordSection = report.add(Section('Coordinator Time'))
    coordSection.ms('Total',
        coord.recoveryTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('  Recv Idle',
        coord.recvIdleTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('  Dispatch Idle',
        coord.dispatchIdleTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('  Starting recovery on backups',
        coord.coordinator.recoveryConstructorTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('  Starting recovery on masters',
        coord.coordinator.recoveryStartTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('  Tablets recovered',
        coord.coordinator.tabletsRecoveredTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('    Completing recovery on backups',
        coord.coordinator.recoveryCompleteTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('  Set will',
        coord.coordinator.setWillTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('  Get tablet map',
        coord.coordinator.getTabletMapTicks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('  Other',
        ((coord.recoveryTicks -
          coord.recvIdleTicks -
          coord.coordinator.recoveryConstructorTicks -
          coord.coordinator.recoveryStartTicks -
          coord.coordinator.setWillTicks -
          coord.coordinator.getTabletMapTicks -
          coord.coordinator.tabletsRecoveredTicks) /
         coord.clockFrequency),
        total=recoveryTime,
        fractionLabel='of total recovery')
    coordSection.ms('Receiving in transport',
        coord.transport.receive.ticks / coord.clockFrequency,
        total=recoveryTime,
        fractionLabel='of total recovery')

    masterSection = report.add(Section('Master Time'))
    masterSection.ms('Total',
        [master.recoveryTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('  Dispatch Idle',
        [master.dispatchIdleTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('Inside recoverSegment',
        [master.master.recoverSegmentTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('  Backup opens, writes',
        [(master.master.backupManagerTicks - master.master.logSyncTicks)
         / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('  Verify checksum',
        [master.master.verifyChecksumTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('  Segment append copy',
        [master.master.segmentAppendCopyTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('  Segment append checksum',
        [master.master.segmentAppendChecksumTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('  HT, profiler, etc',
        [(master.master.recoverSegmentTicks -
          master.master.backupManagerTicks +
          master.master.logSyncTicks -
          master.master.verifyChecksumTicks -
          master.master.segmentAppendCopyTicks -
          master.master.segmentAppendChecksumTicks) /
         master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery',
        note='other')
    masterSection.ms('Waiting for backups',
        [(master.master.segmentReadStallTicks + master.master.logSyncTicks)
         / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('  Stalled on segment read',
        [master.master.segmentReadStallTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('  Log sync',
        [master.master.logSyncTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('Removing tombstones',
        [master.master.removeTombstoneTicks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('Receiving in transport',
        [master.transport.receive.ticks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    masterSection.ms('Transmitting in transport',
        [master.transport.transmit.ticks / master.clockFrequency
         for master in masters],
        total=recoveryTime,
        fractionLabel='of total recovery')
    if (any([master.transport.sessionOpenCount for master in masters])):
        masterSection.ms('Opening sessions',
            [master.transport.sessionOpenTicks / master.clockFrequency
             for master in masters],
            total=recoveryTime,
            fractionLabel='of total recovery')
        if sum([master.transport.retrySessionOpenCount for master in masters]):
            masterSection.avgStd('  Timeouts:',
                [master.transport.retrySessionOpenCount for master in masters],
                label='!!!')
        sessionOpens = []
        for master in masters:
            avg = (master.transport.sessionOpenTicks /
                   master.transport.sessionOpenCount)
            if avg**2 > 2**64 - 1:
                stddev = -1.0 # 64-bit arithmetic could have overflowed
            else:
                variance = (master.transport.sessionOpenSquaredTicks /
                            master.transport.sessionOpenCount) - avg**2
                if variance < 0:
                    # poor floating point arithmetic made variance negative
                    assert variance > -0.1
                    stddev = 0.0
                else:
                    stddev = math.sqrt(variance)
                stddev /= master.clockFrequency / 1e3
            avg /= master.clockFrequency / 1e3
            sessionOpens.append((avg, stddev))
        masterSection.avgStd('  Avg per session',
                             [x[0] for x in sessionOpens],
                             pointFormat='{0:6.1f} ms')
        masterSection.avgStd('  Std dev per session',
                             [x[1] for x in sessionOpens],
                             pointFormat='{0:6.1f} ms')

    masterSection.ms('Replicating one segment',
        [(master.master.replicationTicks / master.clockFrequency) /
         (master.master.replicationBytes / master.segmentSize /
          master.master.replicas)
         for master in masters])
    masterSection.ms('  During replay',
        [((master.master.replicationTicks - master.master.logSyncTicks) /
          master.clockFrequency) /
         ((master.master.replicationBytes - master.master.logSyncBytes) /
           master.segmentSize / master.master.replicas)
         for master in masters])
    masterSection.ms('  During log sync',
        [(master.master.logSyncTicks / master.clockFrequency) /
         (master.master.logSyncBytes /
          master.segmentSize / master.master.replicas)
         for master in masters])

    backupSection = report.add(Section('Backup Time'))
    backupSection.ms('Total in RPC thread',
        [backup.recoveryTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('  Recv Idle',
        [backup.recvIdleTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('  startReadingData',
        [backup.backup.startReadingDataTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('  Open/write segment',
        [backup.backup.writeTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('    Open segment memset',
        [backup.backup.writeClearTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('    Copy',
        [backup.backup.writeCopyTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('    Other',
        [(backup.backup.writeTicks -
          backup.backup.writeClearTicks -
          backup.backup.writeCopyTicks) / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('  Read segment stall',
        [backup.backup.readStallTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('  Transmitting in transport',
        [backup.transport.transmit.ticks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('  Other',
        [(backup.recoveryTicks -
          backup.recvIdleTicks -
          backup.backup.startReadingDataTicks -
          backup.backup.writeTicks -
          backup.backup.readStallTicks -
          backup.transport.transmit.ticks) /
         backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('Filtering segments',
        [backup.backup.filterTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('Reading segments',
        [backup.backup.readingDataTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')
    backupSection.ms('  Using disk',
        [backup.backup.storageReadTicks / backup.clockFrequency
         for backup in backups],
        total=recoveryTime,
        fractionLabel='of total recovery')

    efficiencySection = report.add(Section('Efficiency'))

    # TODO(ongaro): get stddev among segments
    efficiencySection.avgStd('recoverSegment CPU',
        (sum([master.master.recoverSegmentTicks / master.clockFrequency
              for master in masters]) * 1000 /
         sum([master.master.segmentReadCount
              for master in masters])),
        pointFormat='{0:6.2f} ms avg',
        note='per filtered segment')

    # TODO(ongaro): get stddev among segments
    try:
        efficiencySection.avgStd('Writing a segment',
            (sum([backup.backup.writeTicks / backup.clockFrequency
                  for backup in backups]) * 1000 /
        # Divide count by 2 since each segment does two writes: one to open the segment
        # and one to write the data.
            sum([backup.backup.writeCount / 2
                 for backup in backups])),
            pointFormat='{0:6.2f} ms avg',
            note='backup RPC thread')
    except:
        pass

    # TODO(ongaro): get stddev among segments
    try:
        efficiencySection.avgStd('Filtering a segment',
            sum([backup.backup.filterTicks / backup.clockFrequency * 1000
                  for backup in backups]) /
            sum([backup.backup.storageReadCount
                 for backup in backups]),
            pointFormat='{0:6.2f} ms avg')
    except:
        pass

    networkSection = report.add(Section('Network Utilization'))
    networkSection.avgStdFrac('Aggregate',
        (sum([host.transport.transmit.byteCount
              for host in [coord] + masters + backups]) *
         8 / 2**30 / recoveryTime),
        '{0:4.2f} Gb/s',
        total=(max(len(masters), len(backups)) * 32),
        fractionLabel='of network capacity',
        note='overall')
    networkSection.avgStdSum('Master in',
        [(master.transport.receive.byteCount * 8 / 2**30) /
         recoveryTime for master in masters],
        '{0:4.2f} Gb/s',
        note='overall')
    networkSection.avgStdSum('Master out',
        [(master.transport.transmit.byteCount * 8 / 2**30) /
         recoveryTime for master in masters],
        '{0:4.2f} Gb/s',
        note='overall')
    networkSection.avgStdSum('  Master out during replication',
        [(master.master.replicationBytes * 8 / 2**30) /
          (master.master.replicationTicks / master.clockFrequency)
         for master in masters],
        '{0:4.2f} Gb/s',
        note='overall')
    networkSection.avgStdSum('  Master out during log sync',
        [(master.master.logSyncBytes * 8 / 2**30) /
         (master.master.logSyncTicks / master.clockFrequency)
         for master in masters],
        '{0:4.2f} Gb/s',
        note='overall')
    networkSection.avgStdSum('Backup in',
        [(backup.transport.receive.byteCount * 8 / 2**30) /
         recoveryTime for backup in backups],
        '{0:4.2f} Gb/s',
        note='overall')
    networkSection.avgStdSum('Backup out',
        [(backup.transport.transmit.byteCount * 8 / 2**30) /
         recoveryTime for backup in backups],
        '{0:4.2f} Gb/s',
        note='overall')

    diskSection = report.add(Section('Disk Utilization'))
    diskSection.avgStdSum('Effective bandwidth',
        [(backup.backup.storageReadBytes + backup.backup.storageWriteBytes) /
         2**20 / recoveryTime
         for backup in backups],
        '{0:6.2f} MB/s')
    try:
        diskSection.avgStdSum('Active bandwidth',
            [((backup.backup.storageReadBytes + backup.backup.storageWriteBytes) /
              2**20) /
             ((backup.backup.storageReadTicks + backup.backup.storageWriteTicks) /
              backup.clockFrequency)
             for backup in backups
             if (backup.backup.storageReadTicks +
                 backup.backup.storageWriteTicks)],
            '{0:6.2f} MB/s')
    except:
        pass
    diskSection.avgStd('Disk active',
        [((backup.backup.storageReadTicks + backup.backup.storageWriteTicks) *
          100 / backup.clockFrequency) /
         recoveryTime
         for backup in backups],
        '{0:6.2f}%',
        note='of total recovery')
    diskSection.avgStd('  Reading',
        [100 * (backup.backup.storageReadTicks / backup.clockFrequency) /
         recoveryTime
         for backup in backups],
        '{0:6.2f}%',
        note='of total recovery')
    diskSection.avgStd('  Writing',
        [100 * (backup.backup.storageWriteTicks / backup.clockFrequency) /
         recoveryTime
         for backup in backups],
        '{0:6.2f}%',
        note='of total recovery')

    backupSection = report.add(Section('Backup Events'))
    backupSection.avgStd('Segments read',
        [backup.backup.storageReadCount for backup in backups])
    backupSection.avgStd('Primary segments loaded',
        [backup.backup.primaryLoadCount for backup in backups])
    backupSection.avgStd('Secondary segments loaded',
        [backup.backup.secondaryLoadCount for backup in backups])

    localSection = report.add(Section('Local Metrics'))
    for hosts, attr in [([coord], 'coordinator'),
                        (masters, 'master'),
                        (backups, 'backup')]:
        for i in range(10):
            field = 'ticks{0:}'.format(i)
            points = [host[attr].local[field] / host.clockFrequency
                      for host in hosts]
            if any(points):
                localSection.ms('{0:}.local.{1:}'.format(attr, field),
                                points,
                                total=recoveryTime,
                                fractionLabel='of total recovery')
        for i in range(10):
            field = 'count{0:}'.format(i)
            points = [host[attr].local[field] for host in hosts]
            if any(points):
                localSection.avgStd('{0:}.local.{1:}'.format(attr, field),
                                    points)

    print(report)

definitions = Recovery('recovery metrics', 'Metrics')

def main():
    ### Parse command line options
    parser = OptionParser()
    parser.add_option('-b', '--build-only',
        dest='buildOnly', action='store_true',
        help='Only generate C++ files, do not run a report')
    parser.add_option('-r', '--raw',
        dest='raw', action='store_true',
        help='Print out raw data (helpful for debugging)')
    parser.add_option('-a', '--all',
        dest='all', action='store_true',
        help='Print out all raw data not just a sample')
    parser.add_option('-l', '--latex',
        dest='latex', action='store_true',
        help='Print out a short report in LaTeX for the SOSP paper')
    options, args = parser.parse_args()
    if len(args) > 0:
        recovery_dir = args[0]
    else:
        recovery_dir = 'recovery/latest'

    if options.buildOnly:
        # Called automatically by make when this file is modified
        writeBuildFiles(definitions)
        return

    data = parseRecovery(recovery_dir)

    if options.raw:
        if options.all:
            rawFull(data)
        else:
            rawSample(data)

    if options.latex:
        latexReport(data)
        return

    textReport(data)

if __name__ == '__main__':
    sys.exit(main())
