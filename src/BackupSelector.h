/* Copyright (c) 2011 Stanford University
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
#include "CoordinatorClient.h"
#include "ServerList.pb.h"

#ifndef RAMCLOUD_BACKUPSELECTOR_H
#define RAMCLOUD_BACKUPSELECTOR_H

namespace RAMCloud {

/**
 * See BackupSelector; base class only used to virtualize some calls for testing.
 */
class BaseBackupSelector {
  PUBLIC:
    typedef ProtoBuf::ServerList::Entry Backup;
    virtual Backup* selectPrimary(uint32_t numBackups,
                                  const uint64_t backupIds[]) = 0;
    virtual Backup* selectSecondary(uint32_t numBackups,
                                    const uint64_t backupIds[]) = 0;
    virtual ~BaseBackupSelector() {}
};

/**
 * Selects backups on which to store replicas while obeying replica placement
 * constraints.  Logically part of the BackupManager.
 */
class BackupSelector : public BaseBackupSelector {
  PUBLIC:
    explicit BackupSelector(CoordinatorClient* coordinator);
    Backup* selectPrimary(uint32_t numBackups, const uint64_t backupIds[]);
    Backup* selectSecondary(uint32_t numBackups, const uint64_t backupIds[]);

  PRIVATE:
    bool conflict(const Backup* backup,
                  const uint64_t otherBackupId) const;
    bool conflictWithAny(const Backup* backup,
                         uint32_t numBackups,
                         const uint64_t backupIds[]) const;
    Backup* getRandomHost();
    void updateHostListFromCoordinator();

    /// Cluster coordinator. May be NULL for testing purposes.
    CoordinatorClient* const coordinator;

    /// The list of backups from which to select.
    ProtoBuf::ServerList hosts;

    /**
     * Used in #getRandomHost(). This is some permutation of the integers
     * between 0 and hosts.size() - 1, inclusive.
     */
    vector<uint32_t> hostsOrder;

    /**
     * Used in #getRandomHost(). This is the number of backups that have
     * been returned by #getRandomHost() since its last pass over the
     * #hosts list.
     */
    uint32_t numUsedHosts;

    /// A hook for testing purposes.
    DelayedThrower<> updateHostListThrower;

    DISALLOW_COPY_AND_ASSIGN(BackupSelector);
};

} // namespace RAMCloud

#endif
