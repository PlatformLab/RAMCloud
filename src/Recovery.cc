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
                    const char* backups, uint32_t backupsLen)
        : recovery(recovery)
        , masterEntry(masterEntry)
        , backups(backups)
        , backupsLen(backupsLen)
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
                          backups, backupsLen);
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
    const char* backups;
    const uint32_t backupsLen;
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
    , backups()
    , serverList(serverList)
    , masterId(masterId)
    , tabletsUnderRecovery()
    , will(will)
    , tasks(new Tub<BackupStartTask>[serverList.backupCount()])
{
    CycleCounter<RawMetric> _(&metrics->coordinator.recoveryConstructorTicks);
    metrics->coordinator.recoveryCount++;
    buildSegmentIdToBackups();
}

Recovery::~Recovery()
{
    delete[] tasks;
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
    uint32_t activeBackupHosts = 0;
    uint32_t numberIssued = 0;
    uint32_t nextIssueIndex = 0;

    // Start off first round of RPCs
    while (numberIssued < numBackups &&
           activeBackupHosts < maxActiveBackupHosts) {
        nextIssueIndex = serverList.nextBackupIndex(nextIssueIndex);
        tasks[numberIssued].construct(*serverList[nextIssueIndex],
                                      masterId, will);
        ++activeBackupHosts;
        ++numberIssued;
        ++nextIssueIndex;
    }

    // As RPCs complete kick off new ones
    while (activeBackupHosts > 0) {
        for (uint32_t i = 0; i < numBackups; ++i) {
            auto& task = tasks[i];
            if (!task || !task->isReady() || task->isDone())
                continue;

            try {
                (*task)();
                LOG(DEBUG, "Backup %lu has %lu segments",
                    task->backupHost.serverId.getId(),
                    task->result.segmentIdAndLength.size());
            } catch (const TransportException& e) {
                LOG(WARNING, "Couldn't contact %s, "
                    "failure was: %s",
                    task->backupHost.serviceLocator.c_str(),
                    e.str().c_str());
                task.destroy();
            } catch (const ClientException& e) {
                LOG(WARNING, "startReadingData failed on %s, "
                    "failure was: %s",
                    task->backupHost.serviceLocator.c_str(),
                    e.str().c_str());
                task.destroy();
            }

            if (numberIssued < numBackups) {
                nextIssueIndex = serverList.nextBackupIndex(nextIssueIndex);
                tasks[numberIssued].construct(*serverList[nextIssueIndex],
                                              masterId, will);
                ++numberIssued;
                ++nextIssueIndex;
            } else {
                --activeBackupHosts;
            }
        }
    }

    // Map of Segment Ids -> counts of backup copies that were found.
    // This is used to cross-check with the log digest.
    boost::unordered_map<uint64_t, uint32_t> segmentMap;
    for (size_t j = 0; j < serverList.backupCount(); ++j) {
        const auto& task = tasks[j];
        if (!task)
            continue;
        for (size_t i = 0; i < task->result.segmentIdAndLength.size(); ++i) {
            uint64_t segmentId = task->result.segmentIdAndLength[i].first;
            ++segmentMap[segmentId];
        }
    }

    // List of serialised LogDigests from possible log heads, including
    // the corresponding Segment IDs and lengths.
    vector<SegmentAndDigestTuple> digestList;
    for (size_t i = 0; i < serverList.backupCount(); i++) {
        const auto& task = tasks[i];

        // If this backup returned a potential head of the log and it
        // includes a LogDigest, set it aside for verifyCompleteLog().
        if (task->result.logDigestBuffer != NULL) {
            digestList.push_back({ task->result.logDigestSegmentId,
                                   task->result.logDigestSegmentLen,
                                   task->result.logDigestBuffer,
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
    { // verifyCompleteLog
        // find the newest head
        LogDigest* headDigest = NULL;

        foreach (auto digestTuple, digestList) {
            uint64_t id = digestTuple.segmentId;
            uint32_t len = digestTuple.segmentLength;
            LogDigest *ld = &digestTuple.logDigest;

            if (id < headId || (id == headId && len >= headLen)) {
                headDigest = ld;
                headId = id;
                headLen = len;
            }
        }

        if (headDigest == NULL) {
            // we're seriously boned.
            LOG(ERROR, "No log head & digest found!! Kiss your data good-bye!");
            throw Exception(HERE, "ouch! data lost!");
        }

        LOG(NOTICE, "Segment %lu of length %u bytes is the head of the log",
            headId, headLen);

        // scan the backup map to determine if all needed segments are available
        uint32_t missing = 0;
        for (int i = 0; i < headDigest->getSegmentCount(); i++) {
            uint64_t id = headDigest->getSegmentIds()[i];
            if (!contains(segmentMap, id)) {
                LOG(ERROR, "Segment %lu is missing!", id);
                missing++;
            }
        }

        if (missing) {
            LOG(ERROR,
                "%u segments in the digest, but not obtained from backups!",
                missing);
        }
    }

    // Create the ServerList 'script' that recovery masters will replay.
    // First add all primaries to the list, then all secondaries.
    // Order primaries (and even secondaries among themselves anyway) based
    // on when they are expected to be loaded in from disk.
    vector<ProtoBuf::ServerList::Entry> backupsToSort;
    for (uint32_t nextBackupIndex = 0, taskIndex = 0; taskIndex < numBackups;
      nextBackupIndex++, taskIndex++) {
        nextBackupIndex = serverList.nextBackupIndex(nextBackupIndex);
        assert(nextBackupIndex != (uint32_t)-1);

        if (!tasks[taskIndex])
            continue;

        const auto& task = tasks[taskIndex];
        const auto* backup = serverList[nextBackupIndex];
        const uint64_t speed = backup->backupReadMegsPerSecond;

        LOG(DEBUG, "Adding all segments from %s with bench speed of %lu",
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
            uint64_t segmentLen = task->result.segmentIdAndLength[i].second;
            if (segmentId < headId ||
                (segmentId == headId && segmentLen == headLen)) {
                ProtoBuf::ServerList::Entry newEntry;
                backup->serialise(newEntry, true);
                newEntry.set_user_data(expectedLoadTimeMs);
                newEntry.set_segment_id(segmentId);
                backupsToSort.push_back(newEntry);
            }
        }
    }
    std::sort(backupsToSort.begin(), backupsToSort.end(), expectedLoadCmp);
    foreach(const auto& backup, backupsToSort) {
        LOG(DEBUG, "Load segment %lu from %s with expected load time of %lu",
            backup.segment_id(), backup.service_locator().c_str(),
            backup.user_data());
        auto& entry = *backups.add_server();
        entry = backup;
        // Just for debugging for now so the debug print out can include
        // whether or not each entry is a primary
        entry.set_user_data(entry.user_data() < 1000000);
    }

    LOG(DEBUG, "--- Replay script ---");
    foreach (const auto& backup, backups.server()) {
        LOG(DEBUG, "backup: %lu, locator: %s, segmentId %lu, primary %lu",
            backup.server_id(), backup.service_locator().c_str(),
            backup.segment_id(), backup.user_data());
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

    // Pre-serialize the backup schedule, since this takes a while and is the
    // same for each master.
    Buffer backupsBuffer;
    uint32_t backupsLen = serializeToRequest(backupsBuffer, backups);
    const char* backupsBuf =
        static_cast<const char*>(backupsBuffer.getRange(0, backupsLen));

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
            i, backupsBuf, backupsLen);
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
