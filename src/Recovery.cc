/* Copyright (c) 2010 Stanford University
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
#include "MasterClient.h"
#include "Tub.h"
#include "ProtoBuf.h"
#include "Recovery.h"

namespace RAMCloud {

namespace RecoveryInternal {

/// Used in #Recovery::start().
struct MasterStartTask {
    MasterStartTask(Recovery& recovery,
                    uint32_t& recoveryMasterIndex,
                    uint32_t partitionId,
                    const char* backups, uint32_t backupsLen)
        : recovery(recovery)
        , recoveryMasterIndex(recoveryMasterIndex)
        , backups(backups)
        , backupsLen(backupsLen)
        , partitionId(partitionId)
        , tablets()
        , masterHost()
        , masterClient()
        , rpc()
        , done(false)
    {}
    bool isReady() { return rpc && rpc->isReady(); }
    bool isDone() { return done; }
    void send() {
        if (recoveryMasterIndex ==
            static_cast<uint32_t>(recovery.masterHosts.server_size())) {
            // TODO(ongaro): this is not ok in a real system
            DIE("not enough recovery masters to complete recovery");
        }
        masterHost = &recovery.masterHosts.server(recoveryMasterIndex++);
        auto locator = masterHost->service_locator().c_str();
        try {
            masterClient.construct(transportManager.getSession(locator));
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
            auto locator = masterHost->service_locator().c_str();
            LOG(WARNING, "Couldn't contact %s, trying next master; "
                "failure was: %s", locator, e.message.c_str());
            send();
            return;
        } catch (const ClientException& e) {
            auto locator = masterHost->service_locator().c_str();
            LOG(WARNING, "recover failed on %s, trying next master; "
                "failure was: %s", locator, e.toString());
            send();
            return;
        }
        recovery.tabletsUnderRecovery += tablets.tablet_size();
        done = true;
    }
    Recovery& recovery;
    uint32_t& recoveryMasterIndex;
    const char* backups;
    const uint32_t backupsLen;
    const uint32_t partitionId;
    ProtoBuf::Tablets tablets;
    const ProtoBuf::ServerList::Entry* masterHost;
    Tub<MasterClient> masterClient;
    Tub<MasterClient::Recover> rpc;
    bool done;
    DISALLOW_COPY_AND_ASSIGN(MasterStartTask);
};

struct BackupEndTask {
    BackupEndTask(const string& serviceLocator, uint64_t masterId)
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
            client.construct(transportManager.getSession(locator));
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
    const uint64_t masterId;
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
 * of a CoordinatorServer.
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
 * \param masterHosts
 *      A list of all master hosts in the RAMCloud.  This is used to select
 *      recovery masters. The user_data field is ignored.
 * \param backupHosts
 *      A list of all backup hosts in the RAMCloud.  This is used to find all
 *      the backup data for the crashed master. The user_data field is ignored.
 */
Recovery::Recovery(uint64_t masterId,
                   const ProtoBuf::Tablets& will,
                   const ProtoBuf::ServerList& masterHosts,
                   const ProtoBuf::ServerList& backupHosts)

    : recoveryTicks(&metrics->recoveryTicks)
    , backups()
    , masterHosts(masterHosts)
    , backupHosts(backupHosts)
    , masterId(masterId)
    , tabletsUnderRecovery()
    , will(will)
    , tasks(new Tub<BackupStartTask>[backupHosts.server_size()])
    , digestList()
    , segmentMap()
{
    CycleCounter<Metric> _(&metrics->coordinator.recoveryConstructorTicks);
    reset(metrics, 0, 0);
    buildSegmentIdToBackups();
    verifyCompleteLog();
}

Recovery::~Recovery()
{
    delete[] tasks;
    recoveryTicks.stop();
    dump(metrics);
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

    const uint32_t numBackups = backupHosts.server_size();
    const uint32_t maxActiveBackupHosts = 10;
    uint32_t activeBackupHosts = 0;
    uint32_t firstNotIssued = 0;

    // Start off first round of RPCs
    while (firstNotIssued < numBackups &&
           activeBackupHosts < maxActiveBackupHosts) {
        tasks[firstNotIssued].construct(backupHosts.server(firstNotIssued),
                                        masterId, will);
        ++firstNotIssued;
        ++activeBackupHosts;
    }

    // As RPCs complete kick off new ones
    while (activeBackupHosts > 0) {
        while (Dispatch::poll()) {
            /* pass */;
        }
        for (uint32_t i = 0; i < numBackups; ++i) {
            auto& task = tasks[i];
            if (!task || !task->isReady() || task->isDone())
                continue;

            try {
                (*task)();
                LOG(DEBUG, "Backup %lu has %lu segments",
                    task->backupHost.server_id(),
                    task->result.segmentIdAndLength.size());
            } catch (const TransportException& e) {
                LOG(WARNING, "Couldn't contact %s, "
                    "failure was: %s",
                    task->backupHost.service_locator().c_str(),
                    e.str().c_str());
                task.destroy();
            } catch (const ClientException& e) {
                LOG(WARNING, "startReadingData failed on %s, "
                    "failure was: %s",
                    task->backupHost.service_locator().c_str(),
                    e.str().c_str());
                task.destroy();
            }

            if (firstNotIssued < numBackups) {
                tasks[firstNotIssued].construct(
                        backupHosts.server(firstNotIssued),
                        masterId, will);
                ++firstNotIssued;
            } else {
                --activeBackupHosts;
            }
        }
    }

    // Create the ServerList 'script' that recovery masters will replay
    // First add all primaries to the list, then all secondaries
    for (bool primary = true;; primary = !primary) {
        for (uint32_t slot = 0;; ++slot) {
            // Keep working if some segment list had a value in offset 'slot'.
            bool stillWorking = false;
            // TODO(stutsman) we'll want to add some modular arithmetic here
            // to generate unique replay scripts for each recovery master
            for (int i = 0; i < backupHosts.server_size(); ++i) {
                if (!tasks[i])
                    continue;
                const auto& task = tasks[i];

                // Amount per result's segment array to compensate to
                // skip over primaries in the list, only used when adding
                // secondaries to the script, no effect on primary pass
                uint32_t firstSecondary = primary ?
                                            0 :
                                            task->result.primarySegmentCount;
                uint32_t adjustedSlot = slot + firstSecondary;
                if (adjustedSlot >= task->result.segmentIdAndLength.size() ||
                    (primary &&
                     adjustedSlot >= task->result.primarySegmentCount))
                    continue;
                stillWorking = true;
                ProtoBuf::ServerList::Entry& backupHost = *backups.add_server();
                // Copy backup host into the new server list.
                backupHost = task->backupHost;
                // Augment it with the segment id.
                uint64_t segmentId =
                    task->result.segmentIdAndLength[adjustedSlot].first;
                backupHost.set_segment_id(segmentId);
                // Just for debugging for now so the debug print out can include
                // whether or not each entry is a primary
                backupHost.set_user_data(adjustedSlot <
                                         task->result.primarySegmentCount);

                // Keep count of each segmentId seen so we can cross check with
                // the LogDigest.
                ++segmentMap[segmentId];
            }
            if (!stillWorking)
                break;
        }
        if (!primary)
            break;
    }

    LOG(DEBUG, "=== Replay script ===");
    foreach (const auto& backup, backups.server()) {
        LOG(DEBUG, "backup: %lu, locator: %s, segmentId %lu, primary %lu",
            backup.server_id(), backup.service_locator().c_str(),
            backup.segment_id(), backup.user_data());
    }

    for (int i = 0; i < backupHosts.server_size(); i++) {
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
                    task->backupHost.service_locator().c_str());
            }
        }
    }
}

/*
 * Check to see if the Log being recovered can actually be recovered.
 * This requires us to use the Log head's LogDigest, which was returned
 * in response to startReadingData. That gives us a complete list of
 * the Segment IDs we need to do a full recovery. 
 */
void
Recovery::verifyCompleteLog()
{
    // find the newest head
    LogDigest* headDigest = NULL;

    uint64_t headId = 0;
    uint32_t headLen = 0;
    foreach (auto digestTuple, digestList) {
        uint64_t id = digestTuple.segmentId;
        uint32_t len = digestTuple.segmentLength;
        LogDigest *ld = &digestTuple.logDigest;

        if (id > headId || (id == headId && len >= headLen)) {
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
        if (segmentMap.find(id) == segmentMap.end()) {
            LOG(ERROR, "Segment %lu is missing!", id);
            missing++;
        }
    }

    if (missing) {
        LOG(ERROR, "%u segments in the digest, but not obtained from backups!",
            missing);
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
    CycleCounter<Metric> _(&metrics->coordinator.recoveryStartTicks);

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

    // Set up the tasks to execute the RPCs.
    uint32_t recoveryMasterIndex = 0;
    Tub<MasterStartTask> recoverTasks[numPartitions];
    for (uint32_t i = 0; i < numPartitions; i++) {
        auto& task = recoverTasks[i];
        task.construct(*this, recoveryMasterIndex, i, backupsBuf, backupsLen);
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

    // broadcast to backups that recovery is done
    uint32_t numBackups = static_cast<uint32_t>(backupHosts.server_size());
    Tub<BackupEndTask> tasks[numBackups];
    for (uint32_t i = 0; i < numBackups; ++i) {
        auto& backup = backupHosts.server(i);
        tasks[i].construct(backup.service_locator(), masterId);
    }
    parallelRun(tasks, numBackups, 10);
    return true;
}

} // namespace RAMCloud
