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

#ifndef RAMCLOUD_BACKUPSELECTOR_H
#define RAMCLOUD_BACKUPSELECTOR_H

#include <unordered_map>
#include "Common.h"
#include "ServerTracker.h"

namespace RAMCloud {

/**
 * Tracks speed of backups and count of replicas stored on each which is
 * used to balance placement of replicas across the cluster. Also keeps track
 * of the replication group Ids of the backups. Stored for backup
 * in a BackupTracker.
 */
struct BackupStats {
    BackupStats()
        : primaryReplicaCount(0)
        , expectedReadMBytesPerSec(0)
        , replicationId(0)
    {}

    uint32_t getExpectedReadMs();

    /// Number of primary replicas this master has stored on the backup.
    uint32_t primaryReplicaCount;

    /// Disk bandwidth of the host in MB/s
    uint32_t expectedReadMBytesPerSec;

    /// Replication group Id of the backup.
    uint64_t replicationId;
};

/// Tracks BackupStats; a ReplicaManager processes ServerListChanges.
typedef ServerTracker<BackupStats> BackupTracker;

/**
 * See BackupSelector; base class only used to virtualize some calls for testing.
 */
class BaseBackupSelector {
  PUBLIC:
    virtual ServerId selectPrimary(uint32_t numBackups,
                                   const ServerId backupIds[]) = 0;
    virtual ServerId selectSecondary(uint32_t numBackups,
                                     const ServerId backupIds[]) = 0;
    virtual void signalFreedPrimary(const ServerId backupId) = 0;
    virtual ~BaseBackupSelector() {}
};

/**
 * Selects backups on which to store replicas while obeying replica placement
 * constraints and balancing expected work among backups for recovery.
 * Logically part of the ReplicaManager.
 */
class BackupSelector : public BaseBackupSelector {
  PUBLIC:

    explicit BackupSelector(Context* context, const ServerId* serverId,
                            uint32_t numReplicas, bool allowLocalBackup);
    ServerId selectPrimary(uint32_t numBackups, const ServerId backupIds[]);
    virtual ServerId selectSecondary(uint32_t numBackups,
                                     const ServerId backupIds[]);
    void signalFreedPrimary(const ServerId backupId);

  PROTECTED:
    void applyTrackerChanges();
    bool conflictWithAny(const ServerId backupId,
                         uint32_t numBackups,
                         const ServerId backupIds[]) const;
    /**
     * A ServerTracker used to find backups and track replica distribution
     * stats.  Each entry in the tracker contains a pointer to a BackupStats
     * struct which stores the number of primary replicas stored on that
     * server.
     */
    BackupTracker tracker;

    /**
     * Id of the backup.
     */
    const ServerId* serverId;

    /**
     * Number of replicas for each segment.
     */
    uint32_t numReplicas;

    /**
     * Specifies whether to allow replication to local backup.
     */
    bool allowLocalBackup;

    /**
     * Maps replication groups to servers. Used for selecting secondary
     * replicas with MinCopysets.
     */
    typedef std::unordered_multimap<uint64_t, ServerId> ReplicationIdMap;
    typedef ReplicationIdMap::const_iterator replicationIter;
    ReplicationIdMap replicationIdMap;

    /**
     * Used to avoid redundant log messages. True means that a message
     * should be logged the next time we can't find a suitable server;
     * false means we've already reported a problem since last time
     * selectSecondary return successfully, so there's no need to log
     * again.
     */
    bool okToLogNextProblem;

    /**
     * Indicates the maximum number of attempts to find a secondary 
     * serverId.
     */
    const uint32_t maxAttempts;

  PRIVATE:
    bool conflict(const ServerId backupId,
                  const ServerId otherBackupId) const;
    void eraseReplicationId(uint64_t replicationId,
                            const ServerId backupId);

    DISALLOW_COPY_AND_ASSIGN(BackupSelector);
};

} // namespace RAMCloud

#endif
