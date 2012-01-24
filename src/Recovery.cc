/* Copyright (c) 2010-2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <unordered_map>

#include "Common.h"
#include "Dispatch.h"
#include "BackupClient.h"
#include "Buffer.h"
#include "ShortMacros.h"
#include "MasterClient.h"
#include "Tub.h"
#include "ProtoBuf.h"
#include "Recovery.h"

namespace RAMCloud {

namespace RecoveryInternal {

/// Used in #Recovery::start().
struct MasterStartTask {
    MasterStartTask(Recovery& recovery,
                    const CoordinatorServerList::Entry& masterEntry,
                    uint32_t partitionId,
                    const vector<RecoverRpc::Replica>& replicaLocations)
        : recovery(recovery)
        , masterEntry(masterEntry)
        , replicaLocations(replicaLocations)
        , partitionId(partitionId)
        , tablets()
        , masterClient()
        , rpc()
        , done(false)
    {}
    bool isReady() { return rpc && rpc->isReady(); }
    bool isDone() { return done; }
    void send() {
        auto locator = masterEntry.serviceLocator.c_str();
        LOG(NOTICE, "Initiating recovery with recovery master %s, "
            "partition %d", locator, partitionId);
        try {
            masterClient.construct(
                Context::get().transportManager->getSession(locator));
            rpc.construct(*masterClient,
                          recovery.masterId, partitionId,
                          tablets,
                          replicaLocations.data(),
                          downCast<uint32_t>(replicaLocations.size()));
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact %s, trying next master; "
                "failure was: %s", locator, e.message.c_str());
            send();
        } catch (const ClientException& e) {
            LOG(WARNING, "recover failed on %s, trying next master; "
                "failure was: %s", locator, e.toString());
            send();
        }
    }
    void wait() {
        try {
            (*rpc)();
        } catch (const TransportException& e) {
            auto locator = masterEntry.serviceLocator.c_str();
            LOG(WARNING, "Couldn't contact %s, trying next master; "
                "failure was: %s", locator, e.message.c_str());
            send();
            return;
        } catch (const ClientException& e) {
            auto locator = masterEntry.serviceLocator.c_str();
            LOG(WARNING, "recover failed on %s, trying next master; "
                "failure was: %s", locator, e.toString());
            send();
            return;
        }
        recovery.tabletsUnderRecovery += tablets.tablet_size();
        done = true;
    }

    /// The parent recovery object.
    Recovery& recovery;

    /// The master server to kick off this partition's recovery on.
    const CoordinatorServerList::Entry& masterEntry;

    /// XXX- Document me for the love of God.
    const vector<RecoverRpc::Replica>& replicaLocations;
    const uint32_t partitionId;
    ProtoBuf::Tablets tablets;
    Tub<MasterClient> masterClient;
    Tub<MasterClient::Recover> rpc;
    bool done;
    DISALLOW_COPY_AND_ASSIGN(MasterStartTask);
};

struct BackupEndTask {
    BackupEndTask(const string& serviceLocator, ServerId masterId)
        : masterId(masterId)
        , serviceLocator(serviceLocator)
        , client()
        , rpc()
        , done(false)
    {}
    bool isReady() { return rpc && rpc->isReady(); }
    bool isDone() { return done; }
    void send() {
        auto locator = serviceLocator.c_str();
        try {
            client.construct(
                Context::get().transportManager->getSession(locator));
            rpc.construct(*client, masterId);
            return;
        } catch (const TransportException& e) {
            LOG(WARNING, "Couldn't contact %s, ignoring; "
                "failure was: %s", locator, e.message.c_str());
        } catch (const ClientException& e) {
            LOG(WARNING, "recoveryComplete failed on %s, ignoring; "
                "failure was: %s", locator, e.toString());
        }
        client.destroy();
        rpc.destroy();
        done = true;
    }
    void wait() {
        if (!rpc)
            return;
        try {
            (*rpc)();
        } catch (const TransportException& e) {
            auto locator = serviceLocator.c_str();
            LOG(WARNING, "Couldn't contact %s, ignoring; "
                "failure was: %s", locator, e.message.c_str());
        } catch (const ClientException& e) {
            auto locator = serviceLocator.c_str();
            LOG(WARNING, "recoveryComplete failed on %s, ignoring; "
                "failure was: %s", locator, e.toString());
        }
        done = true;
    }
    const ServerId masterId;
    const string& serviceLocator; // NOLINT
    Tub<BackupClient> client;
    Tub<BackupClient::RecoveryComplete> rpc;
    bool done;
    DISALLOW_COPY_AND_ASSIGN(BackupEndTask);
};

} // RecoveryInternal namespace
using namespace RecoveryInternal; // NOLINT

// class Recovery::BackupStartTask

Recovery::BackupStartTask::BackupStartTask(
            const CoordinatorServerList::Entry& backupHost,
            ServerId crashedMasterId,
            const ProtoBuf::Tablets& partitions)
    : backupHost(backupHost)
    , crashedMasterId(crashedMasterId)
    , partitions(partitions)
    , client()
    , rpc()
    , result()
    , done()
{
}

void
Recovery::BackupStartTask::send()
{
    client.construct(
        Context::get().transportManager->getSession(
            backupHost.serviceLocator.c_str()));
    rpc.construct(*client, crashedMasterId, partitions);
    RAMCLOUD_LOG(DEBUG, "Starting startReadingData on %s",
                 backupHost.serviceLocator.c_str());
}

void
Recovery::BackupStartTask::wait()
{
    try {
        result = (*rpc)();
    } catch (const TransportException& e) {
        LOG(WARNING, "Couldn't contact %s, failure was: %s",
            backupHost.serviceLocator.c_str(), e.str().c_str());
        // Leave empty result as if the backup has no replicas.
    } catch (const ClientException& e) {
        LOG(WARNING, "startReadingData failed on %s, failure was: %s",
            backupHost.serviceLocator.c_str(), e.str().c_str());
        // Leave empty result as if the backup has no replicas.
    }
    rpc.destroy();
    client.destroy();
    done = true;
    LOG(DEBUG, "Backup %lu has %lu segment replicas",
        backupHost.serverId.getId(),
        result.segmentIdAndLength.size());
}

// class Recovery

/**
 * Create a Recovery to coordinate a recovery from the perspective
 * of a CoordinatorService.
 *
 * \param masterId
 *      The crashed master this Recovery will rebuild.
 * \param will
 *      A paritions set of tablets or "will" of the crashed Master.
 *      It is represented as a tablet map with a partition id in the
 *      user_data field.  Partition ids must start at 0 and be
 *      consecutive.  No partition id can have 0 entries before
 *      any other partition that has more than 0 entries.  This
 *      is because the recovery recovers partitions up but excluding the
 *      first with no entries.
 * \param serverList
 *      Reference to the Coordinator's list of all servers in RAMCloud. This
 *      is used to select recovery masters and to find all backup data for
 *      the crashed master.
 */
Recovery::Recovery(ServerId masterId,
                   const ProtoBuf::Tablets& will,
                   const CoordinatorServerList& serverList)

    : recoveryTicks(&metrics->coordinator.recoveryTicks)
    , replicaLocations()
    , serverList(serverList)
    , masterId(masterId)
    , tabletsUnderRecovery()
    , will(will)
{
    CycleCounter<RawMetric> _(&metrics->coordinator.recoveryConstructorTicks);
    metrics->coordinator.recoveryCount++;
    buildSegmentIdToBackups();
}

Recovery::~Recovery()
{
    recoveryTicks.stop();
}

bool
expectedLoadCmp(const ProtoBuf::ServerList::Entry& l,
                const ProtoBuf::ServerList::Entry& r)
{
    return l.user_data() < r.user_data();
}

/**
 * Creates a flattened ServerList of backups describing for each copy
 * of each segment on some backup the service locator where that backup is
 * that can be tranferred easily over the wire. Only used by the
 * constructor.
 */
void
Recovery::buildSegmentIdToBackups()
{
    LOG(DEBUG, "Getting segment lists from backups and preparing "
               "them for recovery");


    const uint32_t numBackups = serverList.backupCount();
    const uint32_t maxActiveBackupHosts = 10;

    /// List of asynchronous startReadingData tasks and their replies
    auto backupStartTasks = std::unique_ptr<Tub<BackupStartTask>[]>(
            new Tub<BackupStartTask>[numBackups]);
    uint32_t nextIssueIndex = 0;
    for (uint32_t i = 0; i < numBackups; ++i) {
        nextIssueIndex = serverList.nextBackupIndex(nextIssueIndex);
        backupStartTasks[i].construct(*serverList[nextIssueIndex],
                                      masterId, will);
        ++nextIssueIndex;
    }
    parallelRun(backupStartTasks.get(), numBackups, maxActiveBackupHosts);

    // Map of Segment Ids -> counts of backup copies that were found.
    // This is used to cross-check with the log digest.
    std::unordered_map<uint64_t, uint32_t> segmentMap;
    for (uint32_t i = 0; i < numBackups; ++i) {
        const auto& task = backupStartTasks[i];
        foreach (auto replica, task->result.segmentIdAndLength) {
            uint64_t segmentId = replica.first;
            ++segmentMap[segmentId];
        }
    }

    // List of serialised LogDigests from possible log heads, including
    // the corresponding Segment IDs and lengths.
    vector<SegmentAndDigestTuple> digestList;
    for (uint32_t i = 0; i < numBackups; ++i) {
        const auto& task = backupStartTasks[i];

        // If this backup returned a potential head of the log and it
        // includes a LogDigest, set it aside for verifyCompleteLog().
        if (task->result.logDigestBuffer) {
            digestList.push_back({ task->backupHost.serviceLocator.c_str(),
                                   task->result.logDigestSegmentId,
                                   task->result.logDigestSegmentLen,
                                   task->result.logDigestBuffer.get(),
                                   task->result.logDigestBytes });

            if (task->result.logDigestSegmentId == (uint32_t)-1) {
                LOG(ERROR, "segment %lu has a LogDigest, but len "
                    "== -1!! from %s\n", task->result.logDigestSegmentId,
                    task->backupHost.serviceLocator.c_str());
            }
        }
    }

    /*
     * Check to see if the Log being recovered can actually be recovered.
     * This requires us to use the Log head's LogDigest, which was returned
     * in response to startReadingData. That gives us a complete list of
     * the Segment IDs we need to do a full recovery.
     */
    uint64_t headId = ~0UL;
    uint32_t headLen = 0;
    {
        // find the newest head
        SegmentAndDigestTuple* headReplica = NULL;

        foreach (auto& digestTuple, digestList) {
            uint64_t id = digestTuple.segmentId;
            uint32_t len = digestTuple.segmentLength;

            if (id < headId || (id == headId && len >= headLen)) {
                headReplica = &digestTuple;
                headId = id;
                headLen = len;
            }
        }

        if (headReplica == NULL) {
            // we're seriously boned.
            LOG(ERROR, "No log head & digest found!! Kiss your data good-bye!");
            throw Exception(HERE, "ouch! data lost!");
        }

        LOG(NOTICE, "Segment %lu of length %u bytes from backup %s "
            "is the head of the log",
            headId, headLen, headReplica->backupServiceLocator);

        // scan the backup map to determine if all needed segments are available
        uint32_t missing = 0;
        for (int i = 0; i < headReplica->logDigest.getSegmentCount(); i++) {
            uint64_t id = headReplica->logDigest.getSegmentIds()[i];
            if (!contains(segmentMap, id)) {
                LOG(ERROR, "Segment %lu is missing!", id);
                missing++;
            }
        }

        if (missing) {
            LOG(ERROR,
                "%u segments in the digest, but not obtained from backups!",
                missing);
            // TODO(ongaro): Should we abort here?
        }
    }

    // Create the ServerList 'script' that recovery masters will replay.
    // First add all primaries to the list, then all secondaries.
    // Order primaries (and even secondaries among themselves anyway) based
    // on when they are expected to be loaded in from disk.
    // TODO(ongaro): Stop using a protobuf here.
    vector<ProtoBuf::ServerList::Entry> backupsToSort;
    for (uint32_t nextBackupIndex = 0, taskIndex = 0;
         taskIndex < numBackups;
         nextBackupIndex++, taskIndex++) {
        nextBackupIndex = serverList.nextBackupIndex(nextBackupIndex);
        assert(nextBackupIndex != (uint32_t)-1);

        const auto& task = backupStartTasks[taskIndex];
        const auto* backup = serverList[nextBackupIndex];
        const uint64_t speed = backup->backupReadMegsPerSecond;

        LOG(DEBUG, "Adding %lu segment replicas from %s "
                   "with bench speed of %lu",
            task->result.segmentIdAndLength.size(),
            backup->serviceLocator.c_str(), speed);

        for (size_t i = 0; i < task->result.segmentIdAndLength.size(); ++i) {
            uint64_t expectedLoadTimeMs;
            if (i < task->result.primarySegmentCount) {
                // for primaries just estimate when they'll load
                expectedLoadTimeMs = i * 8 * 1000 / (speed ?: 1);
            } else {
                // for secondaries estimate when they'll load
                // but add a huge bias so secondaries don't overlap
                // with primaries but are still interleaved
                expectedLoadTimeMs = (i - task->result.primarySegmentCount) *
                                     8 * 1000/ (speed ?: 1);
                expectedLoadTimeMs += 1000000;
            }
            uint64_t segmentId = task->result.segmentIdAndLength[i].first;
            uint32_t segmentLen = task->result.segmentIdAndLength[i].second;
            if (segmentId < headId ||
                (segmentId == headId && segmentLen == headLen)) {
                ProtoBuf::ServerList::Entry newEntry;
                backup->serialise(newEntry, true);
                newEntry.set_user_data(expectedLoadTimeMs);
                newEntry.set_segment_id(segmentId);
                backupsToSort.push_back(newEntry);
            } else {
                const char* why;
                if (segmentId == headId && segmentLen < headLen) {
                    why = "shorter than";
                } else if (segmentId == headId && segmentLen > headLen) {
                    why = "longer than";
                } else {
                    why = "past";
                }
                LOG(DEBUG, "Ignoring replica for "
                    "segment ID %lu, len %u from backup %s "
                    "because it's %s the head segment (%lu, %u)",
                    segmentId, segmentLen,
                    backup->serviceLocator.c_str(),
                    why, headId, headLen);
            }
        }
    }
    std::sort(backupsToSort.begin(), backupsToSort.end(), expectedLoadCmp);
    foreach(const auto& backup, backupsToSort) {
        LOG(DEBUG, "Load segment %lu from %s with expected load time of %lu",
            backup.segment_id(), backup.service_locator().c_str(),
            backup.user_data());
        RecoverRpc::Replica replica;
        replica.backupId = backup.server_id();
        replica.segmentId = backup.segment_id();
        replicaLocations.push_back(replica);
    }
}

/**
 * Begin recovery, recovering one partition on a master.
 *
 * \bug Only tries each master once regardless of failure.
 * \bug Only tries each master once regardless of whether recovery
 *      failure could be avoided by doing multiple parition recoveries
 *      on a single master.
 */
void
Recovery::start()
{
    CycleCounter<RawMetric> _(&metrics->coordinator.recoveryStartTicks);

    // Figure out the number of partitions specified in the will.
    uint32_t numPartitions = 0;
    foreach (auto& tablet, will.tablet()) {
        if (tablet.user_data() + 1 > numPartitions)
            numPartitions = downCast<uint32_t>(tablet.user_data()) + 1;
    }

    if (serverList.masterCount() < numPartitions) {
        // TODO(ongaro): this is not ok in a real system
        DIE("not enough recovery masters to complete recovery "
            "(only %d available)", serverList.masterCount());
    }

    // Set up the tasks to execute the RPCs.
    uint32_t nextMasterIndex = 0;
    Tub<MasterStartTask> recoverTasks[numPartitions];
    for (uint32_t i = 0; i < numPartitions; i++) {
        auto& task = recoverTasks[i];
        nextMasterIndex = serverList.nextMasterIndex(nextMasterIndex);
        task.construct(*this, *serverList[nextMasterIndex],
                       i, replicaLocations);
        nextMasterIndex++;

    }
    foreach (auto& tablet, will.tablet()) {
        auto& task = recoverTasks[tablet.user_data()];
        *task->tablets.add_tablet() = tablet;
    }

    // Tell the recovery masters to begin recovery.
    LOG(NOTICE, "Starting recovery for %u partitions", numPartitions);
    parallelRun(recoverTasks, numPartitions, 10);
}

bool
Recovery::tabletsRecovered(const ProtoBuf::Tablets& tablets)
{
    tabletsUnderRecovery--;
    if (tabletsUnderRecovery != 0)
        return false;

    CycleCounter<RawMetric> ticks(&metrics->coordinator.recoveryCompleteTicks);
    // broadcast to backups that recovery is done
    uint32_t numBackups = serverList.backupCount();
    Tub<BackupEndTask> tasks[numBackups];
    for (size_t i = 0, taskNum = 0; i < serverList.size(); i++) {
        if (serverList[i] == NULL || !serverList[i]->isBackup())
            continue;
        auto& backup = *serverList[i];
        tasks[taskNum++].construct(backup.serviceLocator, masterId);
    }
    parallelRun(tasks, numBackups, 10);
    return true;
}

} // namespace RAMCloud
