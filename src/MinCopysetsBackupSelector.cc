/* Copyright (c) 2011-2014 Stanford University
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

#include "Cycles.h"
#include "MinCopysetsBackupSelector.h"
#include "ShortMacros.h"

namespace RAMCloud {

// --- MinCopysetsBackupSelector ---

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
MinCopysetsBackupSelector::MinCopysetsBackupSelector(Context* context,
    const ServerId* serverId, uint32_t numReplicas, bool allowLocalBackup)
    : BackupSelector(context, serverId, numReplicas, allowLocalBackup)
{
}


/**
 * Select a node that is a member of the same replication group (i.e., a node
 * that has the same replication Id) of the first entry in the \a backupIds
 * array.
 * \param numBackups
 *      The number of entries in the \a backupIds array.
 * \param backupIds
 *      An array of numBackups backup Ids, none of which may conflict with
 *      the returned backup or the server itself. All of the replica locations
 *      should be listed (the server Id of the master itself should not
 *      be listed).
 */
ServerId
MinCopysetsBackupSelector::selectSecondary(uint32_t numBackups,
                                           const ServerId backupIds[])
{
    uint64_t startTicks = Cycles::rdtsc();
    while (true) {
        applyTrackerChanges();
        if (backupIds == NULL || !backupIds[0].isValid()) {
            return ServerId(/* Invalid */);
        }
        uint64_t replicationId = tracker[backupIds[0]]->replicationId;
        // A replication Id of 0 represents a node without a replication group.
        if (replicationId == 0u) {
            return ServerId(/* Invalid */);
        }

        // If this is the first call to selectSecondary, we check whether there
        // enough nodes with the same replication Id to form a replication
        // group.
        if (numBackups == 1) {
            if (replicationIdMap.count(replicationId) != numReplicas) {
                return ServerId(/* Invalid */);
            }
        }

        ServerId id = getReplicationGroupServer(numBackups, backupIds,
                                                replicationId);
        if (id.isValid()) {
            return id;
        }
        double waited = Cycles::toSeconds(Cycles::rdtsc() - startTicks);
        if (waited > 0.02) {
            LOG(WARNING, "BackupSelector could not find a suitable server in "
                "the last 20ms; seems to be stuck; waiting for the coordinator "
                "to notify this master of newly enlisted backups");
            return ServerId(/* Invalid */);
        }
    }
}

/**
 * Get a node that does not conflict with an existing set of backups and has
 * the same replication Id.
 * \param numBackups
 *      The number of entries in the \a backupIds array.
 * \param backupIds
 *      An array of numBackups backup Ids, none of which may conflict with
 *      the returned backup or the server itself. All of the replica locations
 *      should be listed (the server Id of the master itself should not
 *      be listed).
 * \param replicationId
 *      The replicationId of the backups.
 */
ServerId
MinCopysetsBackupSelector::getReplicationGroupServer(
    uint32_t numBackups, const ServerId backupIds[], uint64_t replicationId)
{
    auto range = replicationIdMap.equal_range(replicationId);
    replicationIter it;
    for (it = range.first; it != range.second; ++it) {
        if (!conflictWithAny(it->second, numBackups, backupIds)) {
            return it->second;
        }
    }
    return ServerId(/* Invalid */);
}

} // namespace RAMCloud
