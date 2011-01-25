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
    , segmentIdToBackups()
    , tabletsUnderRecovery()
    , will(will)
{
    buildSegmentIdToBackups();

    // Check that the Log to be recovered can actually be recovered, i.e.
    // the head was found and copies of all needed Segments are apparently
    // available.
    verifyCompleteLog();

    // Wait to create this list after all the inserts because multimap sorts
    // it by segment id for us, flatten it and augment it with segment ids.
    createBackupList(backups);
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
        , client(
            transportManager.getSession(backupHost.service_locator().c_str()))
        , rpc(client, crashedMasterId, partitions)
    {
        LOG(DEBUG, "Starting startReadingData on %s",
            backupHost.service_locator().c_str());
    }
    const ProtoBuf::ServerList::Entry& backupHost;
    Buffer response;
    BackupClient client;
    BackupClient::StartReadingData rpc;
    DISALLOW_COPY_AND_ASSIGN(Task);
};
}

/**
 * Builds the segmentIdToBackups by contacting all backups in
 * the RAMCloud describing where each copy of each segment can be found
 * by backup locator.  Only used by the constructor.
 */
void
Recovery::buildSegmentIdToBackups()
{
    LOG(DEBUG, "Getting segment lists from backups and preparing "
               "them for recovery");

    // Adjust this array size to hone the number of concurrent requests.
    ObjectTub<Task> tasks[10];
    auto backupHostsIt = backupHosts.server().begin();
    auto backupHostsEnd = backupHosts.server().end();
    uint32_t activeBackupHosts = 0;

    // Start off first round of RPCs
    foreach (auto& task, tasks) {
        if (backupHostsIt == backupHostsEnd)
            break;
        task.construct(*backupHostsIt++, masterId, will);
        ++activeBackupHosts;
    }

    // As RPCs complete, process them and start more
    while (activeBackupHosts > 0) {
        foreach (auto& task, tasks) {
            vector<pair<uint64_t, uint32_t>> idAndLengths;
            if (!task || !task->rpc.isReady())
                continue;
            const auto& locator = task->backupHost.service_locator();
            try {
                idAndLengths = (task->rpc)();
            } catch (const TransportException& e) {
                LOG(DEBUG, "Couldn't contact %s, "
                    "failure was: %s", locator.c_str(), e.str().c_str());
            } catch (const ClientException& e) {
                LOG(DEBUG, "startReadingData failed on %s, "
                    "failure was: %s", locator.c_str(), e.str().c_str());
            }

            LOG(DEBUG, "%s returned %lu segment id/lengths",
                locator.c_str(), idAndLengths.size());
            foreach (const auto& idAndLength, idAndLengths) {
                auto segmentId = idAndLength.first;
                auto segmentWrittenLength = idAndLength.second;
                LOG(DEBUG, "%s has %lu with length %u",
                    locator.c_str(),
                    segmentId, segmentWrittenLength);
                // A copy of the entry is made, augmented with a segment id.
                segmentIdToBackups.insert(
                    BackupMap::value_type(segmentId, task->backupHost));
            }

            task.destroy();
            if (backupHostsIt == backupHostsEnd) {
                --activeBackupHosts;
                continue;
            }
            task.construct(*backupHostsIt++, masterId, will);
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
 * Creates a flattened ServerList of backups describing for each copy
 * of each segment on some backup the service locator where that backup is
 * that can be tranferred easily over the wire.
 *
 * \param backups
 *      The ProtoBuf::ServerList to append the flattened list to.
 */
void
Recovery::createBackupList(ProtoBuf::ServerList& backups) const
{
    foreach (const BackupMap::value_type& value, segmentIdToBackups) {
        ProtoBuf::ServerList_Entry& server(*backups.add_server());
        server = value.second;
        server.set_segment_id(value.first);
    }
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
