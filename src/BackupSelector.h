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
 * Selects backups on which to store replicas.
 */
class BackupSelector {
  PUBLIC:
    typedef ProtoBuf::ServerList::Entry Backup;
    explicit BackupSelector(CoordinatorClient* coordinator);
    void select(uint32_t numBackups, Backup* backups[]);
    Backup* selectAdditional(uint32_t numBackups,
                             const Backup* const backups[]);
  PRIVATE:
    Backup* getRandomHost();
    bool conflict(const Backup* a, const Backup* b) const;
    bool conflictWithAny(const Backup* a, uint32_t numBackups,
                             const Backup* const backups[]) const;
    void updateHostListFromCoordinator();

    /// A hook for testing purposes.
    DelayedThrower<> updateHostListThrower;

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

    DISALLOW_COPY_AND_ASSIGN(BackupSelector);
};

} // namespace RAMCloud

#endif
