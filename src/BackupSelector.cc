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
#include "Rpc.h"
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
           Segment::SEGMENT_SIZE /
           1024 / 1024 / expectedReadMBytesPerSec);
}

// --- BackupSelector ---

/**
 * Constructor.
 * \param tracker
 *      The tracker used to find backups and track replica distribution
 *      stats.
 */
BackupSelector::BackupSelector(BackupTracker& tracker)
    : tracker(tracker)
{
}

/**
 * From a set of 5 backups that does not conflict with an existing set of
 * backups choose the one that will minimize expected time to read replicas
 * from disk in the case that this master should crash.
 * \param numBackups
 *      The number of entries in the \a backupIds array.
 * \param backupIds
 *      An array of numBackups backup ids, none of which may conflict with the
 *      returned backup.
 */
ServerId
BackupSelector::selectPrimary(uint32_t numBackups,
                              const ServerId backupIds[])
{
    ServerId primary = selectSecondary(numBackups, backupIds);
    for (uint32_t i = 0; i < 5 - 1; ++i) {
        ServerId candidate = selectSecondary(numBackups, backupIds);
        if (tracker[primary]->getExpectedReadMs() >
            tracker[candidate]->getExpectedReadMs()) {
            primary = candidate;
        }
    }
    BackupStats* stats = tracker[primary];
    LOG(DEBUG, "Chose server %lu with %u primary replicas and %u MB/s disk "
               "bandwidth (expected time to read on recovery is %u ms)",
               primary.getId(), stats->primaryReplicaCount,
               stats->expectedReadMBytesPerSec, stats->getExpectedReadMs());
    ++stats->primaryReplicaCount;

    return primary;
}

/**
 * Choose a random backup that does not conflict with an existing set of
 * backups.
 * \param numBackups
 *      The number of entries in the \a backupIds array.
 * \param backupIds
 *      An array of numBackups backup ids, none of which may conflict with the
 *      returned backup.
 */
ServerId
BackupSelector::selectSecondary(uint32_t numBackups,
                                const ServerId backupIds[])
{
    while (true) {
        applyTrackerChanges();
        ServerId id = tracker.getRandomServerIdWithService(BACKUP_SERVICE);
        if (id.isValid() &&
            !conflictWithAny(id, numBackups, backupIds)) {
            return id;
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
        if (event == SERVER_ADDED) {
           tracker[server.serverId] = new BackupStats;
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
    // TODO(stutsman): This doesn't even capture the notion of a master
    // conflicting with its local backup.  It only prevents us from
    // choosing the same backup more than once in odd edge cases of the
    // algorithm. In order to capture that we'll need to pass down the
    // id of the server this is running on.
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
    return false;
}

} // namespace RAMCloud
