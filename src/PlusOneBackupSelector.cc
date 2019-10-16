/* Copyright (c) 2011-2019 Stanford University
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

#include <algorithm>

#include "Cycles.h"
#include "PlusOneBackupSelector.h"
#include "ShortMacros.h"

namespace RAMCloud {

// --- PlusOneBackupSelector ---

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
PlusOneBackupSelector::PlusOneBackupSelector(Context* context,
    const ServerId* serverId, uint32_t numReplicas, bool allowLocalBackup)
    : BackupSelector(context, serverId, numReplicas, allowLocalBackup)
{
}


/**
 * Select a node that's masterServerId+1 with wraparound, or if that fails,
 * keep moving forward one with wraparound until you either find a node or
 * tried them all.
 * \param numBackups
 *      The number of entries in the \a backupIds array.
 * \param backupIds
 *      An array of numBackups backup ids, none of which may conflict with the
 *      returned backup. All existing replica locations as well as the
 *      server id of the master should be listed.
 */
ServerId
PlusOneBackupSelector::selectSecondary(uint32_t numBackups,
                                       const ServerId backupIds[])
{
    applyTrackerChanges();
    uint32_t totalAttempts = std::min(tracker.size(), maxAttempts);
    uint32_t attempts = 0;
    uint32_t index = serverId->indexNumber();

    for (attempts = 0; attempts < totalAttempts; attempts++) {
        applyTrackerChanges();
        index++;
        if (index > tracker.size()) {
            index = 1;
        }
        ServerId id = tracker.getServerIdAtIndexWithService(
            index, WireFormat::BACKUP_SERVICE);
        if (id.isValid() &&
            !conflictWithAny(id, numBackups, backupIds)) {
            okToLogNextProblem = true;
            return id;
        }
    }
    if (okToLogNextProblem) {
        RAMCLOUD_CLOG(WARNING, "PlusOneBackupSelector could not find a "
            "suitable server in %d attempts; may need to wait for additional "
            "servers to enlist",
            attempts);
        okToLogNextProblem = false;
    }
    return ServerId(/* Invalid */);
}

} // namespace RAMCloud
