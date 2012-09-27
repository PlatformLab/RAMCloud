/* Copyright (c) 2011-2012 Stanford University
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

#include "BackupSelector.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "Segment.h"

namespace RAMCloud {

// --- BackupStats ---

/**
 * Return the expected number of milliseconds the backup would take to read
 * from its disk all of the primary segments this master has stored on it
 * plus an additional segment.
 */
uint32_t
BackupStats::getExpectedReadMs() {
    // Careful here - this math will overflow 32-bit arithmetic.
    // TODO(stutsman): Shouldn't use SEGMENT_SIZE constant here, but
    //                 it doesn't affect correctness.
    return downCast<uint32_t>(
           uint64_t(primaryReplicaCount + 1) * 1000 *
           Segment::DEFAULT_SEGMENT_SIZE /
           1024 / 1024 / expectedReadMBytesPerSec);
}

// --- BackupSelector ---

/**
 * Constructor.
 * \param context
 *      Overall information about this RAMCloud server; used to register
 *      #tracker with this server's ServerList.
 * \param serverId
 *      The ServerId of the backup. Used for selecting appropriate primary
 *      and secondary replicas.
 */
BackupSelector::BackupSelector(Context* context, const ServerId serverId)
    : tracker(context)
    , serverId(serverId)
{
}

/**
 * From a set of 5 backups that does not conflict with an existing set of
 * backups choose the one that will minimize expected time to read replicas
 * from disk in the case that this master should crash. The ServerId returned
 * is !isValid() if there are no machines to selectSecondary() from.
 * \param numBackups
 *      The number of entries in the \a backupIds array.
 * \param backupIds
 *      An array of numBackups backup ids, none of which may conflict with the
 *      returned backup. All existing replica locations as well as the
 *      server id of the master should be listed.
 */
ServerId
BackupSelector::selectPrimary(uint32_t numBackups,
                              const ServerId backupIds[])
{
    ServerId primary = selectSecondary(numBackups, backupIds);
    if (!primary.isValid())
        return primary;

    for (uint32_t i = 0; i < 5 - 1; ++i) {
        ServerId candidate = selectSecondary(numBackups, backupIds);
        if (!candidate.isValid())
            break;

        if (tracker[primary]->getExpectedReadMs() >
            tracker[candidate]->getExpectedReadMs()) {
            primary = candidate;
        }
    }
    BackupStats* stats = tracker[primary];
    LOG(DEBUG, "Chose server %s with %u primary replicas and %u MB/s disk "
               "bandwidth (expected time to read on recovery is %u ms)",
               primary.toString().c_str(), stats->primaryReplicaCount,
               stats->expectedReadMBytesPerSec, stats->getExpectedReadMs());
    ++stats->primaryReplicaCount;

    return primary;
}

/**
 * Choose a random backup that does not conflict with an existing set of
 * backups. The ServerId will be invalid if there are no more machines to
 * choose from.
 * \param numBackups
 *      The number of entries in the \a backupIds array.
 * \param backupIds
 *      An array of numBackups backup ids, none of which may conflict with the
 *      returned backup. All existing replica locations as well as the
 *      server id of the master should be listed.
 */
ServerId
BackupSelector::selectSecondary(uint32_t numBackups,
                                const ServerId backupIds[])
{
    uint64_t startTicks = Cycles::rdtsc();
    while (true) {
        applyTrackerChanges();
        ServerId id = tracker.getRandomServerIdWithService(
            WireFormat::BACKUP_SERVICE);
        if (id.isValid() &&
            !conflictWithAny(id, numBackups, backupIds)) {
            return id;
        }
        auto waited = Cycles::toNanoseconds(Cycles::rdtsc() - startTicks);
        if (waited > 20000000lu) {
            LOG(WARNING, "BackupSelector could not find a suitable server in "
                "the last 20ms; seems to be stuck; waiting for the coordinator "
                "to notify this master of newly enlisted backups");
            return ServerId(/* Invalid */);
        }
    }
}

// - private -

/**
 * Apply all updates to #tracker from the Server's ServerList since the last
 * call to applyTrackerChanges().  selectSecondary() uses this to ensure that
 * it is working with a fresh list of backups.
 * This call allocates and deallocates tracker entries including their
 * associated BackupStats structs.
 */
void
BackupSelector::applyTrackerChanges()
{
    ServerDetails server;
    ServerChangeEvent event;
    while (tracker.getChange(server, event)) {
        // If we receive SERVER_ADDED and the server already exists on the
        // tracker, it means the coordinator is assigning it a new replication
        // group. In this case we don't need to create a new instance of
        // BackupStats.
        if (event == SERVER_ADDED) {
            if (!tracker[server.serverId]) {
                tracker[server.serverId] = new BackupStats;
            }
            tracker[server.serverId]->expectedReadMBytesPerSec =
                server.expectedReadMBytesPerSec;
        } else if (event == SERVER_REMOVED) {
            BackupStats* stats = tracker[server.serverId];
            delete stats;
            tracker[server.serverId] = NULL;
        }
    }
}

/**
 * Return whether it is unwise to place a replica on \a backup given
 * that a replica exists on backup \a otherBackupId.
 * For example, it is unwise to place two replicas on the same backup or
 * on backups that share a common power source.
 */
bool
BackupSelector::conflict(const ServerId backupId,
                         const ServerId otherBackupId) const
{
    if (backupId == otherBackupId)
        return true;
    // TODO(ongaro): Add other notions of conflicts, such as same rack.
    return false;
}

/**
 * Return whether it is unwise to place a replica on backup 'backup' given
 * that replica exists on 'backups'. See conflict().
 */
bool
BackupSelector::conflictWithAny(const ServerId backupId,
                                uint32_t numBackups,
                                const ServerId backupIds[]) const
{
    for (uint32_t i = 0; i < numBackups; ++i) {
        if (conflict(backupId, backupIds[i]))
            return true;
    }
    if (!backupId.isValid()) {
        return false;
    }
    // Finally, check if backup conflicts with the server's own Id.
    return conflict(backupId, serverId);
}

} // namespace RAMCloud
