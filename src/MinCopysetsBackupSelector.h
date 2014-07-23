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

#ifndef RAMCLOUD_MINCOPYSETSBACKUPSELECTOR_H
#define RAMCLOUD_MINCOPYSETSBACKUPSELECTOR_H

#include "Common.h"
#include "BackupSelector.h"

namespace RAMCloud {

/**
 * Selects backups to store replicas on the same replication group (i.e.,
 * backups that have the same replication Id).
 */
class MinCopysetsBackupSelector : public BackupSelector {
  PUBLIC:
    explicit MinCopysetsBackupSelector(Context* context,
                                       const ServerId* serverId,
                                       uint32_t numReplicas,
                                       bool allowLocalBackup);
    ServerId selectSecondary(uint32_t numBackups, const ServerId backupIds[]);

  PRIVATE:
    ServerId getReplicationGroupServer(uint32_t numBackups,
                                       const ServerId BackupIds[],
                                       uint64_t replicationId);
    DISALLOW_COPY_AND_ASSIGN(MinCopysetsBackupSelector);
};

} // namespace RAMCloud

#endif
