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

class Metric:
    """A single performance metric.
    """
    def __init__(self, name, documentation):
        """ name is the variable name to use for this metric """
        self.name = name
        self.documentation = documentation
    def dump_header(self, out):
        out('/// %s' % self.documentation)
        out('RawMetric %s;' % self.name)
    def initializer(self):
        return '%s(0)' % (self.name)
    def instance_name(self):
        """ Compute the name to use for an instance of this metric. """
        return self.name
    def dump_metric_info_code(self, out, path, counter):
        """ Generate a case statement as part of a giant switch statement
            that  allows for iteration over all metrics.

            path is a hierarchical name identifying this element, such
            as 'backup.local' (it includes this object's name, if that
            is desired).
            counter is a Counter used to generate "case" clauses with
            incrementing values.
        """
        out('        case %s:' % (counter.value()))
        out('            return {"%s",' % path)
        out('                    &%s};' % path)
        counter.next()

class Group:
    """A group of related performance metrics and subgroups.  Translates
       into a nested struct inside the C++ RawMetrics object.
    """

    def __init__(self, name, documentation):
        """ name is the name of a class to use for this group (i.e.
            initial capital letter).
        """
        self.name = name
        self.documentation = documentation
        self.metrics = []
        self.groups = []

    def metric(self, name, documentation):
        self.metrics.append(Metric(name, documentation))

    def group(self, group):
        self.groups.append(group)

    def dump_header(self, out):
        indent = ' ' * 4 * (out._indent + 2)
        out('/// %s' % self.documentation)
        constructorBody = ''
        if self.name != 'RawMetrics':
            out('struct %s {' % self.name)
        else:
            constructorBody = 'init();'
        children = self.groups + self.metrics;
        out('    %s()' % self.name)
        out('        : %s {%s}' %
            (('\n%s, ' % (indent)).join(
                [child.initializer() for child in children]),
                constructorBody))
        for child in children:
            child.dump_header(out.indent())
        if self.name != 'RawMetrics':
            out('} %s;' % self.instance_name())

    def initializer(self):
        return '%s()' % self.instance_name()

    def instance_name(self):
        """ Compute the name to use for an instance of this group. """
        return self.name[0].lower() + self.name[1:]

    def dump_metric_info_code(self, out, path, counter):
        """ Generate a case statement as part of a giant switch statement
            that  allows for iteration over all metrics.

            path is a hierarchical name identifying this element, such
            as 'backup.local' (it includes this object's name, if that
            is desired).
            counter is a Counter used to generate "case" clauses with
            incrementing values.
        """
        prefix = path
        if len(path) != 0:
            prefix += '.'
        for child in self.groups + self.metrics:
            child.dump_metric_info_code(out,
                    prefix + child.instance_name(), counter)

### Metrics definitions:

coordinator = Group('Coordinator', 'metrics for coordinator')
coordinator.metric('recoveryCount',
    'number of recoveries in which this coordinator participated')
coordinator.metric('recoveryTicks', 'elapsed time during recoveries')
coordinator.metric('recoveryBuildReplicaMapTicks',
                   'time contacting backups and finding replicas for crashed '
                   'master')
coordinator.metric('recoveryStartTicks', 'time in Recovery::start')
coordinator.metric('recoveryCompleteTicks',
    'time sending recovery complete RPCs to backups')

master = Group('Master', 'metrics for masters')
master.metric('recoveryCount',
    'number of recoveries in which this master participated')
master.metric('recoveryTicks', 'the elapsed time during recoveries')
master.metric('replicaManagerTicks', 'time spent in ReplicaManager')
master.metric('segmentAppendTicks', 'time spent in Segment::append')
master.metric('segmentAppendCopyTicks',
    'time spent copying in Segment::append')
master.metric('segmentReadCount',
    'number of BackupClient::getRecoveryData calls issued')
master.metric('segmentReadTicks',
    'elapsed time for getRecoveryData calls to backups')
master.metric('segmentReadStallTicks',
    'time stalled waiting for segments from backups')
master.metric('segmentReadByteCount',
    'bytes of recovery segments received from backups')
master.metric('verifyChecksumTicks',
    'time verifying checksums on objects from backups')
master.metric('recoverSegmentTicks',
    'spent in MasterService::recoverSegment')
master.metric('backupInRecoverTicks',
    'time spent in ReplicaManager::proceed '
    'called from MasterService::recoverSegment')
master.metric('segmentCloseCount',
    'number of complete segments written to backups')
master.metric('recoverySegmentEntryCount',
    'number of recovery segment entries (e.g. objects, tombstones)')
master.metric('recoverySegmentEntryBytes',
    'number of bytes in recovery segment entries (without overhead)')
master.metric('liveObjectCount',
    'number of live objects written during recovery')
master.metric('liveObjectBytes',
    'number of bytes of live object data written during recovery')
master.metric('objectAppendCount',
    'number of objects appended to the log during recovery')
master.metric('objectDiscardCount',
    'number of objects not appended to the log during recovery')
master.metric('safeVersionRecoveryCount',
    'number of safeVersion updates during recovery')
master.metric('safeVersionNonRecoveryCount',
    'number of safeVersion discarded during recovery')
master.metric('tombstoneAppendCount',
    'number of tombstones kept during recovery')
master.metric('tombstoneDiscardCount',
    'number of tombstones discarded during recovery')
master.metric('logSyncTicks',
    'time syncing the log at the end of recovery')
master.metric('logSyncBytes',
    'bytes sent during log sync')
master.metric('recoveryWillTicks',
    'time rebuilding will at the end of recovery')
master.metric('removeTombstoneTicks',
    'time deleting tombstones at the end of recovery')
master.metric('replicationTicks',
    'time with outstanding RPCs to backups')
master.metric('replicationBytes',
    'bytes sent during recovery from first gRD response '
    'through log sync')
master.metric('replicas',
    'number of backups on which to replicate each segment')
master.metric('backupCloseTicks',
    'time closing segments in ReplicaManager')
master.metric('backupCloseCount',
    'number of segments closed in ReplicaManager')
master.metric('logSyncCloseTicks',
    'time close segments during log sync')
master.metric('logSyncCloseCount',
    'number of segments closed during log sync')
master.metric('replicaRecoveries',
    'number of replicas which have started replica recreation')
master.metric('openReplicaRecoveries',
    'of replicaRecoveries how many were for replicas which were open')
master.metric('replicationTasks',
    'max number of outstanding tasks in ReplicaManager')
master.metric('replicationTransmitCopyTicks',
    'time spent copying outgoing rpcs in transport')
master.metric('logSyncTransmitCopyTicks',
    'time spent copying outgoing rpcs in transport just during log sync')
master.metric('replayMemoryReadBytes',
    'rough estimate of memory read during log replay')
master.metric('replayMemoryWrittenBytes',
    'rough estimate of memory written during log replay')
master.metric('replicationTransmitActiveTicks',
    'time transport tx was active during replication')
master.metric('logSyncTransmitActiveTicks',
    'time transport tx was active during log sync')
master.metric('replicationPostingWriteRpcTicks',
    'time spent during recovery starting write rpcs in transport')
master.metric('recoverSegmentPostingWriteRpcTicks',
    'time spent during recoverSegment starting write rpcs in transport')
master.metric('logSyncPostingWriteRpcTicks',
    'time spent during recovery final log sync starting write rpcs in transport')

backup = Group('Backup', 'metrics for backups')
backup.metric('recoveryCount',
    'number of recoveries in which this backup participated')
backup.metric('recoveryTicks', 'elapsed time during recovery')
backup.metric('serviceTicks', 'time spent servicing RPC requests')
backup.metric('readCompletionCount',
    'number of getRecoveryData requests successfully completed')
backup.metric('readingDataTicks',
    'time from startReadingData to done reading')
backup.metric('storageReadCount', 'number of segment reads from disk')
backup.metric('storageReadBytes', 'amount of bytes read from disk')
backup.metric('storageReadTicks', 'time reading from disk')
backup.metric('writeClearTicks',
    'time clearing segment memory during segment open')
backup.metric('writeCopyBytes', 'bytes written to backup segments')
backup.metric('writeCopyTicks', 'time copying data to backup segments')
backup.metric('storageWriteCount', 'number of segment writes to disk')
backup.metric('storageWriteBytes', 'bytes written to disk')
backup.metric('storageWriteTicks', 'time writing to disk')
backup.metric('filterTicks', 'time filtering segments')
backup.metric('primaryLoadCount', 'number of primary segments requested')
backup.metric('secondaryLoadCount', 'number of secondary segments requested')
backup.metric('storageType', '1 = in-memory, 2 = on-disk')

# This class records basic statistics for RPCs (count & execution time):
rpc = Group('Rpc', 'metrics for remote procedure calls')
# The order of entries here, and for the "*Ticks" definitions below,
# must be the same as the order in the RpcOpcode definition in Rpc.h.
rpc.metric('rpc0Count', 'number of invocations of RPC 0 (undefined)')
rpc.metric('rpc1Count', 'number of invocations of RPC 1 (undefined)')
rpc.metric('rpc2Count', 'number of invocations of RPC 2 (undefined)')
rpc.metric('rpc3Count', 'number of invocations of RPC 3 (undefined)')
rpc.metric('rpc4Count', 'number of invocations of RPC 4 (undefined)')
rpc.metric('rpc5Count', 'number of invocations of RPC 5 (undefined)')
rpc.metric('rpc6Count', 'number of invocations of RPC 6 (undefined)')
rpc.metric('pingCount', 'number of invocations of PING RPC')
rpc.metric('proxyPingCount', 'number of invocations of PROXY_PING RPC')
rpc.metric('killCount', 'number of invocations of KILL RPC')
rpc.metric('createTableCount', 'number of invocations of CREATE_TABLE RPC')
rpc.metric('getTableIdCount', 'number of invocations of GET_TABLE_ID RPC')
rpc.metric('dropTableCount', 'number of invocations of DROP_TABLE RPC')
rpc.metric('readCount', 'number of invocations of READ RPC')
rpc.metric('writeCount', 'number of invocations of WRITE RPC')
rpc.metric('removeCount', 'number of invocations of REMOVE RPC')
rpc.metric('enlistServerCount', 'number of invocations of ENLIST_SERVER RPC')
rpc.metric('getServerListCount', 'number of invocations of GET_SERVER_LIST RPC')
rpc.metric('getTabletMapCount', 'number of invocations of GET_TABLET_MAP RPC')
rpc.metric('recoverCount', 'number of invocations of RECOVER RPC')
rpc.metric('hintServerDownCount', 'number of invocations of HINT_SERVER_DOWN RPC')
rpc.metric('recoveryMasterFinishedCount', 'number of invocations of RECOVERY_MASTER_FINISHED RPC')
rpc.metric('enumerateCount', 'number of invocations of ENUMERATE RPC')
rpc.metric('setMasterRecoveryInfoCount', 'number of invocations of SET_MASTER_RECOVERY_INFO RPC')
rpc.metric('fillWithTestDataCount', 'number of invocations of FILL_WITH_TEST_DATA RPC')
rpc.metric('multiReadCount', 'number of invocations of MULTI_READ RPC')
rpc.metric('getMetricsCount', 'number of invocations of GET_METRICS RPC')
rpc.metric('rpc27Count', 'number of invocations of RPC 27 (undefined)')
rpc.metric('backupFreeCount', 'number of invocations of BACKUP_FREE RPC')
rpc.metric('backupGetRecoveryDataCount', 'number of invocations of BACKUP_GETRECOVERYDATA RPC')
rpc.metric('rpc30Count', 'number of invocations of RPC 30 (undefined)')
rpc.metric('backupStartReadingDataCount', 'number of invocations of BACKUP_STARTREADINGDATA RPC')
rpc.metric('backupWriteCount', 'number of invocations of BACKUP_WRITE RPC')
rpc.metric('backupRecoveryCompleteCount', 'number of invocations of BACKUP_RECOVERYCOMPLETE RPC')
rpc.metric('backupQuiesceCount', 'number of invocations of BACKUP_QUIESCE RPC')
rpc.metric('updateServerListCount', 'number of invocations of UPDATE_SERVER_LIST RPC')
rpc.metric('backupStartPartitionCount', 'time spent executing BACKUP_STARTPARTITION RPC')
rpc.metric('rpc37Count', 'number of invocations of RPC 37 (undefined)')
rpc.metric('rpc38Count', 'number of invocations of RPC 38 (undefined)')
rpc.metric('dropTabletOwnershipCount', 'number of invocations of DROP_TABLET_OWNERSHIP RPC')
rpc.metric('takeTabletOwnershipCount', 'number of invocations of TAKE_TABLET_OWNERSHIP RPC')
rpc.metric('backupAssignGroupCount', 'number of invocations of BACKUP_ASSIGN_GROUP RPC')
rpc.metric('getHeadOfLogCount', 'number of invocations of GET_HEAD_OF_LOG RPC')
rpc.metric('incrementRpcCount', 'number of invocations of INCREMENT RPC')
rpc.metric('prepForMigrationCount', 'number of invocations of PREP_FOR_MIGRATION RPC')
rpc.metric('receiveMigrationDataCount', 'number of invocations of RECEIVE_MIGRATION_DATA RPC')
rpc.metric('reassignTabletOwnershipCount', 'number of invocations of REASSIGN_TABLET_OWNERSHIP RPC')
rpc.metric('migrateTabletCount', 'number of invocations of MIGRATE_TABLET RPC')
rpc.metric('isReplicaNeededCount', 'number of invocations of IS_REPLICA_NEEDED RPC')
rpc.metric('splitTabletCount', 'number of invocations of SPLIT_TABLET')
rpc.metric('getServerStatisticsCount', 'number of invocations of GET_SERVER_STATISTICS RPC')
rpc.metric('setRuntimeOptionCount', 'number of invocations of SET_RUNTIME_OPTION RPC')
rpc.metric('getServerConfigCount', 'number of invocations of GET_SERVER_CONFIG RPC')
rpc.metric('getLogMetricsCount', 'number of invocations of GET_LOG_METRICS RPC')
rpc.metric('multiWriteCount', 'number of invocations of MULTI_WRITE RPC')
rpc.metric('verifyMembershipCount', 'number of invocations of VERIFY_MEMBERSHIP RPC')
rpc.metric('illegalRpcCount', 'number of invocations of RPCs with illegal opcodes')

rpc.metric('rpc0Ticks', 'time spent executing RPC 0 (undefined)')
rpc.metric('rpc1Ticks', 'time spent executing RPC 1 (undefined)')
rpc.metric('rpc2Ticks', 'time spent executing RPC 2 (undefined)')
rpc.metric('rpc3Ticks', 'time spent executing RPC 3 (undefined)')
rpc.metric('rpc4Ticks', 'time spent executing RPC 4 (undefined)')
rpc.metric('rpc5Ticks', 'time spent executing RPC 5 (undefined)')
rpc.metric('rpc6Ticks', 'time spent executing RPC 6 (undefined)')
rpc.metric('pingTicks', 'time spent executing PING RPC')
rpc.metric('proxyPingTicks', 'time spent executing PROXY_PING RPC')
rpc.metric('killTicks', 'time spent executing KILL RPC')
rpc.metric('createTableTicks', 'time spent executing CREATE_TABLE RPC')
rpc.metric('getTableIdTicks', 'time spent executing GET_TABLE_ID RPC')
rpc.metric('dropTableTicks', 'time spent executing DROP_TABLE RPC')
rpc.metric('readTicks', 'time spent executing READ RPC')
rpc.metric('writeTicks', 'time spent executing WRITE RPC')
rpc.metric('removeTicks', 'time spent executing REMOVE RPC')
rpc.metric('enlistServerTicks', 'time spent executing ENLIST_SERVER RPC')
rpc.metric('getServerListTicks', 'time spent executing GET_SERVER_LIST RPC')
rpc.metric('getTabletMapTicks', 'time spent executing GET_TABLET_MAP RPC')
rpc.metric('recoverTicks', 'time spent executing RECOVER RPC')
rpc.metric('hintServerDownTicks', 'time spent executing HINT_SERVER_DOWN RPC')
rpc.metric('recoveryMasterFinishedTicks', 'time spent executing RECOVERY_MASTER_FINISHED RPC')
rpc.metric('enumerateTicks', 'time spent executing ENUMERATE RPC')
rpc.metric('setMasterRecoveryInfoTicks', 'time spent executing SET_MASTER_RECOVERY_INFO RPC')
rpc.metric('fillWithTestDataTicks', 'time spent executing FILL_WITH_TEST_DATA RPC')
rpc.metric('multiReadTicks', 'time spent executing MULTI_READ RPC')
rpc.metric('getMetricsTicks', 'time spent executing GET_METRICS RPC')
rpc.metric('rpc27Ticks', 'time spent executing RPC 27 (undefined)')
rpc.metric('backupFreeTicks', 'time spent executing BACKUP_FREE RPC')
rpc.metric('backupGetRecoveryDataTicks', 'time spent executing BACKUP_GETRECOVERYDATA RPC')
rpc.metric('rpc30Ticks', 'time spent executing RPC 30 (undefined)')
rpc.metric('backupStartReadingDataTicks', 'time spent executing BACKUP_STARTREADINGDATA RPC')
rpc.metric('backupWriteTicks', 'time spent executing BACKUP_WRITE RPC')
rpc.metric('backupRecoveryCompleteTicks', 'time spent executing BACKUP_RECOVERYCOMPLETE RPC')
rpc.metric('backupQuiesceTicks', 'time spent executing BACKUP_QUIESCE RPC')
rpc.metric('setServerListTicks', 'time spent executing SET_SERVER_LIST RPC')
rpc.metric('updateServerListTicks', 'time spent executing BACKUP_STARTPARTITION RPC')
rpc.metric('backupStartPartition', 'time spent executing RPC 4 (undefined)')
rpc.metric('rpc37Ticks', 'time spent executing RPC 37 (undefined)')
rpc.metric('rpc38Ticks', 'time spent executing RPC 38 (undefined)')
rpc.metric('dropTabletOwnershipTicks', 'number of invocations of DROP_TABLET_OWNERSHIP RPC')
rpc.metric('takeTabletOwnershipTicks', 'number of invocations of TAKE_TABLET_OWNERSHIP RPC')
rpc.metric('backupAssignGroupTicks', 'time spent executing BACKUP_ASSIGN_GROUP RPC')
rpc.metric('getHeadOfLogTicks', 'time spent executing GET_HEAD_OF_LOG RPC')
rpc.metric('incrementTicks', 'time spent executing INCREMENT RPC')
rpc.metric('prepForMigrationTicks', 'time spent executing PREP_FOR_MIGRATION RPC')
rpc.metric('receiveMigrationDataTicks', 'time spent executing RECEIVE_MIGRATION_DATA RPC')
rpc.metric('reassignTabletOwnershipTicks', 'time spent executing REASSIGN_TABLET_OWNERSHIP RPC')
rpc.metric('migrateTabletTicks', 'time spent executing MIGRATE_TABLET RPC')
rpc.metric('isReplicaNeededTicks', 'time spent executing IS_REPLICA_NEEDED_RPC')
rpc.metric('splitTabletTicks', 'time spent executing SPLIT_TABLET RPC')
rpc.metric('getServerStatisticsTicks', 'time spent executing GET_SERVER_STATISTICS RPC')
rpc.metric('setRuntimeOptionTicks', 'time spent executing SET_RUNTIME_OPTION RPC')
rpc.metric('getServerConfigTicks', 'time spent executing GET_SERVER_CONFIG RPC')
rpc.metric('getLogMetricsTicks', 'time spent executing GET_LOG_METRICS RPC')
rpc.metric('multiWriteTicks', 'time spent executing MULTI_WRITE RPC')
rpc.metric('verifyMembershipTicks', 'number of invocations of VERIFY_MEMBERSHIP')
rpc.metric('illegalRpcTicks', 'time spent executing RPCs with illegal opcodes')

transmit = Group('Transmit', 'metrics related to transmitting messages')
transmit.metric('ticks', 'elapsed time transmitting messages')
transmit.metric('messageCount', 'number of messages transmitted')
transmit.metric('packetCount', 'number of packets transmitted')
transmit.metric('iovecCount', 'number of Buffer chunks transmitted')
transmit.metric('byteCount', 'number of bytes transmitted')
transmit.metric('copyTicks', 'elapsed time copying messages')
transmit.metric('dmaTicks', 'elapsed time waiting for DMA to HCA')

receive = Group('Receive', 'metrics related to receiving messages')
receive.metric('ticks', 'elapsed time receiving messages')
receive.metric('messageCount', 'number of messages received')
receive.metric('packetCount', 'number of packets received')
receive.metric('iovecCount', 'number of Buffer chunks received')
receive.metric('byteCount', 'number of bytes received')

infiniband = Group('Infiniband', 'metrics for Infiniband networking')
infiniband.metric('transmitActiveTicks', 'time with packets on the transmit queue')

transport = Group('Transport', 'transport metrics')
transport.group(transmit)
transport.group(receive)
transport.group(infiniband)
transport.metric('sessionOpenTicks',
    'time opening sessions for RPCs')
transport.metric('sessionOpenCount',
    'number of sessions opened for RPCs')
transport.metric('sessionOpenSquaredTicks',
    'used for calculating the standard deviation of sessionOpenTicks')
transport.metric('retrySessionOpenCount',
    'member of timeouts during session open')
transport.metric('clientRpcsActiveTicks',
    'time with a client RPC active on the network')

temp = Group('Temp', 'metrics for temporary use')
for i in range(10):
    temp.metric('ticks{0:}'.format(i),'amount of time for some undefined activity')
    temp.metric('count{0:}'.format(i),'number of occurrences of some undefined event')

definitions = Group('RawMetrics', 'server metrics')
definitions.group(coordinator);
definitions.group(master);
definitions.group(backup);
definitions.group(rpc);
definitions.group(transport);
definitions.group(temp);
definitions.metric('serverId', 'server id assigned by coordinator')
definitions.metric('pid', 'process ID on machine')
definitions.metric('clockFrequency', 'cycles per second for the cpu')
definitions.metric('segmentSize','size in bytes of segments')

def writeBuildFiles(definitions):
    counter = Counter()
    cc = Out(open('%s/RawMetrics.in.cc' % obj_dir, 'w'))
    cc('// This file was automatically generated by scripts/rawmetrics.py.')
    cc('// Do not edit it.')
    cc('namespace RAMCloud {')
    cc('RawMetrics::MetricInfo RawMetrics::metricInfo(int i)\n{')
    cc('    switch (i) {')
    definitions.dump_metric_info_code(cc, '', counter)
    cc('    }')
    cc('    return {NULL, NULL};')
    cc('}')
    cc('} // namespace RAMCloud')

    h = Out(open('%s/RawMetrics.in.h' % obj_dir, 'w'))
    h('// This file was automatically generated by scripts/rawmetrics.py.')
    h('// Do not edit it.')
    definitions.dump_header(h)
    h('    static const int numMetrics = %d;' % (counter.value()))


if __name__ == '__main__':
    writeBuildFiles(definitions)
    sys.exit()

