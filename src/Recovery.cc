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
 * \param hosts
 *      A list of all hosts in the RAMCloud.  This is used to find all
 *      the backup data for the crashed master and to select recovery
 *      masters.
 */
Recovery::Recovery(uint64_t masterId,
                   const ProtoBuf::Tablets& will,
                   const ProtoBuf::ServerList& hosts)
    : backups()
    , hosts(hosts)
    , masterId(masterId)
    , segmentIdToBackups()
    , will(will)
{
    buildSegmentIdToBackups();

    // Wait to create this list after all the inserts because multimap sorts
    // it by segment id for us, flatten it and augment it with segment ids.
    createBackupList(backups);
}

Recovery::~Recovery()
{
}

/**
 * Builds the segmentIdToBackups by contacting all backups in
 * the RAMCloud describing where each copy of each segment can be found
 * by backup locator.  Only used by the constructor.
 *
 * \bug Completely serial.  Should overlap each of the startReadingData
 *      requests.
 */
void
Recovery::buildSegmentIdToBackups()
{
    // Find all the segments for the crashed master from each of
    // the backups in the entire RAMCloud.
    for (int i = 0; i < hosts.server_size(); i++) {
        const ProtoBuf::ServerList::Entry& host(hosts.server(i));
        if (host.server_type() == ProtoBuf::BACKUP) {
            BackupClient backup(
                transportManager.getSession(host.service_locator().c_str()));
            vector<uint64_t> ids(backup.startReadingData(masterId));
            foreach (uint64_t id, ids) {
                // A copy of the entry is made, we augment it with a segment id.
                segmentIdToBackups.insert(BackupMap::value_type(id, host));
            }
        }
    }
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
        LOG(DEBUG, "%s has segment %lu",
            server.service_locator().c_str(), server.segment_id());
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
        for (; hostIndexToRecoverOnNext < hosts.server_size();
             hostIndexToRecoverOnNext++)
        {
            const ProtoBuf::ServerList::Entry&
                host(hosts.server(hostIndexToRecoverOnNext));
            // Make sure not to recover to a backup or to the crashed master
            if (host.server_type() == ProtoBuf::MASTER &&
                host.server_id() != masterId) {
                const string& locator = host.service_locator();
                LOG(DEBUG, "Trying partition recovery on %s", locator.c_str());
                try {
                    MasterClient master(
                            transportManager.getSession(locator.c_str()));
                    master.recover(masterId, tablets, backups);
                } catch (const TransportException& e) {
                    LOG(WARNING, "Couldn't contact %s, trying next master; "
                        "failure was: %s", locator.c_str(), e.message.c_str());
                    continue;
                } catch (const ClientException& e) {
                    LOG(WARNING, "recover failed on %s, trying next master; "
                        "failure was: %s", locator.c_str(), e.toString());
                    continue;
                }
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

} // namespace RAMCloud
