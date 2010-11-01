/* Copyright (c) 2009-2010 Stanford University
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


#ifndef RAMCLOUD_BACKUPMANAGER_H
#define RAMCLOUD_BACKUPMANAGER_H

#include <map>

#include "BackupClient.h"
#include "Common.h"

namespace RAMCloud {

class MasterServer;

/**
 * A backup consisting of a multiple remote hosts.
 *
 * The precise set of backup hosts is selected by creating BackupClient
 * instances and adding them to the BackupManager instance via
 * addHost().
 *
 * Eventually this will be a more sophisticated not implementing
 * BackupClient, but rather, scheduling and orchestrating the backup
 * servers' for backup and recovery.
 */
class BackupManager {
  public:
    explicit BackupManager(CoordinatorClient* coordinator,
                           uint32_t replicas = 2);
    virtual ~BackupManager();

    void closeSegment(uint64_t masterId, uint64_t segmentId);
    void freeSegment(uint64_t masterId, uint64_t segmentId);
    void openSegment(uint64_t masterId, uint64_t segmentId);
    void recover(MasterServer& recoveryMaster,
                 uint64_t masterId,
                 const ProtoBuf::Tablets& tablets,
                 const ProtoBuf::ServerList& backups);
    void setHostList(const ProtoBuf::ServerList& hosts);
    void writeSegment(uint64_t masterId,
                      uint64_t segmentId,
                      uint32_t offset,
                      const void *buf,
                      uint32_t length);

  private:
    void selectOpenHosts();
    void updateHostListFromCoordinator();

    CoordinatorClient* coordinator;

    /// The host pool to schedule backups from.
    ProtoBuf::ServerList hosts;

    typedef std::list<BackupClient*> OpenHostList;
    /// List of hosts currently containing an open segment for this master.
    OpenHostList openHosts;

    /// The number of backups to replicate each segment on.
    const uint32_t replicas;

    typedef std::multimap<uint64_t, Transport::SessionRef> SegmentMap;
    /// Tells which backup each segment is stored on.
    SegmentMap segments;

    friend class BackupManagerTest;
    DISALLOW_COPY_AND_ASSIGN(BackupManager);
};

} // namespace RAMCloud

#endif
