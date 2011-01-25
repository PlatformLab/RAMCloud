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
#include "BackupClient.h"
#include "Buffer.h"
#include "MasterClient.h"
#include "ObjectTub.h"
#include "ProtoBuf.h"
#include "Recovery.h"

namespace RAMCloud {

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
    : backups()
    , masterHosts(masterHosts)
    , backupHosts(backupHosts)
    , masterId(masterId)
    , tabletsUnderRecovery()
    , will(will)
{
    buildSegmentIdToBackups();
    verifyCompleteLog();
}

Recovery::~Recovery()
{
}

namespace {
// Only used in Recovery::buildSegmentIdToBackups().
struct Task {
    Task(const ProtoBuf::ServerList::Entry& backupHost,
         uint64_t crashedMasterId,
         const ProtoBuf::Tablets& partitions)
        : backupHost(backupHost)
        , response()
        , client()
        , rpc()
        , result()
        , done()
    {
        response.construct();
        client.construct(
            transportManager.getSession(backupHost.service_locator().c_str()));
        rpc.construct(*client, crashedMasterId, partitions);
        LOG(DEBUG, "Starting startReadingData on %s",
            backupHost.service_locator().c_str());
    }

    bool isDone() const { return done; }
    bool isReady() { return rpc && rpc->isReady(); }

    void
    operator()()
    {
        (*rpc)(&result);
        rpc.destroy();
        client.destroy();
        response.destroy();
        done = true;
    }

    const ProtoBuf::ServerList::Entry& backupHost;
    ObjectTub<Buffer> response;
    ObjectTub<BackupClient> client;
    ObjectTub<BackupClient::StartReadingData> rpc;
    BackupClient::StartReadingData::Result result;
    bool done;
    DISALLOW_COPY_AND_ASSIGN(Task);
};
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

    // Adjust this array size to hone the number of concurrent requests.
    ObjectTub<Task> tasks[backupHosts.server_size()];
    auto backupHostsIt = backupHosts.server().begin();
    auto backupHostsEnd = backupHosts.server().end();
    const uint32_t maxActiveBackupHosts = 10;
    uint32_t activeBackupHosts = 0;

    // Start off first round of RPCs
    for (int i = 0; i < backupHosts.server_size(); ++i) {
        auto& task = tasks[i];
        if (backupHostsIt == backupHostsEnd ||
            activeBackupHosts == maxActiveBackupHosts)
            break;
        task.construct(*backupHostsIt++, masterId, will);
        ++activeBackupHosts;
    }

    // As RPCs complete kick off new ones
    while (activeBackupHosts > 0) {
        for (int i = 0; i < backupHosts.server_size(); ++i) {
            auto& task = tasks[i];
            if (!task || !task->isReady() || task->isDone())
                continue;

            try {
                (*task)();
                LOG(DEBUG, "%s returned %lu segment id/lengths",
                    task->backupHost.service_locator().c_str(),
                    task->result.segmentIdAndLength.size());
            } catch (const TransportException& e) {
                LOG(DEBUG, "Couldn't contact %s, "
                    "failure was: %s",
                    task->backupHost.service_locator().c_str(),
                    e.str().c_str());
            } catch (const ClientException& e) {
                LOG(DEBUG, "startReadingData failed on %s, "
                    "failure was: %s",
                    task->backupHost.service_locator().c_str(),
                    e.str().c_str());
            }

            if (backupHostsIt == backupHostsEnd) {
                --activeBackupHosts;
                continue;
            }
        }
    }

    // Create the ServerList 'script' that recovery masters will replay
    for (uint32_t slot = 0;; ++slot) {
        // Keep working if some segment list had a value in offset 'slot'.
        bool stillWorking = false;
        // TODO(stutsman) we'll want to add some modular arithmetic here
        // to generate unique replay scripts for each recovery master
        for (int i = 0; i < backupHosts.server_size(); ++i) {
            assert(tasks[i]);
            const auto& task = tasks[i];
            if (slot >= task->result.segmentIdAndLength.size())
                continue;
            stillWorking = true;
            ProtoBuf::ServerList::Entry& backupHost = *backups.add_server();
            // Copy backup host into the new server list.
            backupHost = task->backupHost;
            // Augment it with the segment id.
            uint64_t segmentId = task->result.segmentIdAndLength[slot].first;
            backupHost.set_segment_id(segmentId);
        }
        if (!stillWorking)
            break;
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
#if 0
    // determine which LogDigest is the HOL.

    // scan the backup map
    uint32_t missing = 0;
    for (segmentId in hol.LogDigest()) {
        if (segmentIdToBackups.find(segmentId) == segmentIdToBackups.end()) {
            LOG(WARNING, "Segment %lu is missing!", segmentId);
            missing++;
        }
    }

    if (missing) {
        // what to do?!
    }
#endif
}

/**
 * Begin recovery, recovering one partition on a master.
 *
 * \bug Only tries each master once regardless of failure.
 * \bug Only tries each master once regardless of whether recovery
 *      failure could be avoided by doing multiple parition recoveries
 *      on a single master.
 * \bug Completely serial.  Needs to work asynchronously on all the
 *      masters at once.
 */
void
Recovery::start()
{
    uint64_t partitionId = 0;
    int hostIndexToRecoverOnNext = 0;
    for (;;) {
      continueToNextPartition:
        // Split off a sub tablet from the will.
        bool partitionExists = false;
        ProtoBuf::Tablets tablets;
        for (int willIndex = 0; willIndex < will.tablet_size(); willIndex++) {
            const ProtoBuf::Tablets::Tablet& tablet(will.tablet(willIndex));
            if (tablet.user_data() == partitionId) {
                *tablets.add_tablet() = tablet;
                partitionExists = true;
            }
        }
        // If no partitions with the next higher id, then we are all done.
        if (!partitionExists)
            break;
        // We now have a partitioned piece of the will, choose a master.
        for (; hostIndexToRecoverOnNext < masterHosts.server_size();
             hostIndexToRecoverOnNext++)
        {
            const ProtoBuf::ServerList::Entry&
                host(masterHosts.server(hostIndexToRecoverOnNext));
            // Make sure not to recover to a backup or to the crashed master
            if (host.server_id() != masterId) {
                const string& locator = host.service_locator();
                LOG(DEBUG, "Trying partition recovery on %s with "
                    "%u tablets and %u hosts", locator.c_str(),
                    tablets.tablet_size(), backups.server_size());
                try {
                    MasterClient master(
                            transportManager.getSession(locator.c_str()));
                    master.recover(masterId, partitionId, tablets, backups);
                } catch (const TransportException& e) {
                    LOG(WARNING, "Couldn't contact %s, trying next master; "
                        "failure was: %s", locator.c_str(), e.message.c_str());
                    continue;
                } catch (const ClientException& e) {
                    LOG(WARNING, "recover failed on %s, trying next master; "
                        "failure was: %s", locator.c_str(), e.toString());
                    continue;
                }
                tabletsUnderRecovery += tablets.tablet_size();
                // Success, next try next partiton with next host.
                hostIndexToRecoverOnNext++;
                partitionId++;
                goto continueToNextPartition;
            }
        }
        // If we fell out of the inner for loop then we ran out of hosts.
        LOG(ERROR, "Failed to recover all partitions for a crashed master, "
            "your RAMCloud is now busted.");
        return;
    }
}

bool
Recovery::tabletsRecovered(const ProtoBuf::Tablets& tablets)
{
    tabletsUnderRecovery--;
    return tabletsUnderRecovery == 0;
}

} // namespace RAMCloud
