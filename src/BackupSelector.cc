/* Copyright (c) 2011-2015 Stanford University
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
    // Shouldn't use SEGMENT_SIZE constant here, but
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
 * \param numReplicas
 *      The replication factor of each segment.
 * \param allowLocalBackup
 *      Specifies whether to allow replication to the local backup.
 */
BackupSelector::BackupSelector(Context* context, const ServerId* serverId,
                               uint32_t numReplicas, bool allowLocalBackup)
    : tracker(context)
    , serverId(serverId)
    , numReplicas(numReplicas)
    , allowLocalBackup(allowLocalBackup)
    , replicationIdMap()
    , okToLogNextProblem(true)
    , maxAttempts(100)
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
 *      returned backup. All existing replica locations should be listed (the
 *      server Id of the master itself should not be listed).
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
    uint32_t attempts = 0;
    for (attempts = 0; attempts < maxAttempts; attempts++) {
        applyTrackerChanges();
        ServerId id = tracker.getRandomServerIdWithService(
            WireFormat::BACKUP_SERVICE);
        if (id.isValid() &&
            !conflictWithAny(id, numBackups, backupIds)) {
            okToLogNextProblem = true;
            return id;
        }
    }
    if (okToLogNextProblem) {
        RAMCLOUD_CLOG(WARNING, "BackupSelector could not find a suitable "
            "server in %d attempts; may need to wait for additional "
            "servers to enlist",
            attempts);
        okToLogNextProblem = false;
    }
    return ServerId(/* Invalid */);
}

/**
 * Inform the BackupSelector that a primary replica has been freed from
 * backupId. This allows the BackupSelector to maintain proper stats.  This
 * should only be called in in the event a primary replica is freed.
 * \param backupId
 *      The ServerId of the backup whose primary replica the BackupSelector
 *      should now recognize as freed.
 */
void
BackupSelector::signalFreedPrimary(const ServerId backupId)
{
    BackupStats* stats = tracker[backupId];
    --stats->primaryReplicaCount;
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
            } else {
                eraseReplicationId(tracker[server.serverId]->replicationId,
                                    server.serverId);
            }
            tracker[server.serverId]->expectedReadMBytesPerSec =
                server.expectedReadMBytesPerSec;
            tracker[server.serverId]->replicationId = server.replicationId;
            replicationIdMap.insert(std::make_pair(
                server.replicationId, server.serverId));
        } else if (event == SERVER_REMOVED) {
            BackupStats* stats = tracker[server.serverId];
            // Stats could be null if we joined the cluster at a point where
            // the server was in crashed state (so we didn't see the
            // SERVER_ADDED event).
            if (stats != NULL) {
                eraseReplicationId(stats->replicationId, server.serverId);
                delete stats;
                tracker[server.serverId] = NULL;
            }
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
    if (!allowLocalBackup) {
        // Check if backup conflicts with the server's own Id.
        return conflict(backupId, *serverId);
    }
    return false;
}

/**
 * Remove an entry from the \a replicationIdMap.
 * \param replicationId
 *     The replication group Id of the entry in the map.
 * \param backupId
 *     The ServerId of the entry in the map.
 */
void
BackupSelector::eraseReplicationId(uint64_t replicationId,
                                   const ServerId backupId)
{
    auto range = replicationIdMap.equal_range(replicationId);
    replicationIter it;
    for (it = range.first; it != range.second; ++it) {
        if (it->second == backupId) {
            replicationIdMap.erase(it);
            return;
        }
    }
}

} // namespace RAMCloud
