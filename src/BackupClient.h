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

#ifndef RAMCLOUD_BACKUPCLIENT_H
#define RAMCLOUD_BACKUPCLIENT_H

#include "Client.h"
#include "Common.h"
#include "Object.h"
#include "Transport.h"

namespace RAMCloud {

// TODO(stutsman) delete this soon!
class TabletMap {
};

/**
 * The interface for an object that can act as a backup server no
 * matter the transport or location.
 */
class BackupClient : public Client {
  public:
    struct RecoveredObject {
        uint64_t segmentId;
        //Object object;
    };

    virtual ~BackupClient() {}
    virtual void commitSegment(uint64_t masterId, uint64_t segmentId) = 0;
    virtual void freeSegment(uint64_t masterId, uint64_t segmentId) = 0;

    /** 
     * Get the objects stored for the given tablets of the given server.
     */
    virtual vector<RecoveredObject> getRecoveryData(uint64_t masterId,
                                                    const TabletMap& tablets)=0;

    /**
     * Allocate space to receive backup writes for a segment.
     *
     * \param masterId
     *      Id of this server.
     * \param segmentId
     *      Id of the segment to be backed up.
     */
    virtual void openSegment(uint64_t masterId, uint64_t segmentId) = 0;

    /** 
     * Begin reading the objects stored for the given server from disk.
     * \return
     *      A set of segment IDs for that server which will be read from disk.
     */
    virtual vector<uint64_t> startReadingData(uint64_t masterId) = 0;

    /**
     * Write the byte range specified in an open segment on the backup.
     */
    virtual void writeSegment(uint64_t masterId,
                              uint64_t segmentId,
                              uint32_t offset,
                              const void *buf,
                              uint32_t length) = 0;
};

/**
 * A backup consisting of a single remote host.  BackupHost's primary
 * role is to proxy calls via RPCs to a particular backup server.
 *
 * \implements BackupClient
 */
class BackupHost : public BackupClient {
  public:
    explicit BackupHost(Transport::SessionRef session);
    virtual ~BackupHost();

    virtual void commitSegment(uint64_t masterId, uint64_t segmentId);
    virtual void freeSegment(uint64_t masterId, uint64_t segmentId);
    virtual vector<RecoveredObject> getRecoveryData(uint64_t masterId,
                                                    const TabletMap& tablets);
    virtual void openSegment(uint64_t masterId, uint64_t segmentId);
    virtual vector<uint64_t> startReadingData(uint64_t masterId);
    virtual void writeSegment(uint64_t masterId,
                              uint64_t segmentId,
                              uint32_t offset,
                              const void *bug,
                              uint32_t length);

  private:
    /**
     * Performance metric from the response in the most recent RPC (as
     * requested by selectPerfCounter). If no metric was requested and done
     * most recent RPC, then this value is 0.
     */
    uint32_t counterValue;

    /**
     * A session with a backup server.
     */
    Transport::SessionRef session;

    /**
     * Completion status from the most recent RPC completed for this client.
     */
    Status status;

    DISALLOW_COPY_AND_ASSIGN(BackupHost);
};

/**
 * A backup consisting of a multiple remote hosts.
 *
 * The precise set of backup hosts is selected by creating BackupHost
 * instances and adding them to the BackupManager instance via
 * addHost().
 *
 * Eventually this will be a more sophisticated not implementing
 * BackupClient, but rather, scheduling and orchestrating the backup
 * servers' for backup and recovery.
 *
 * \implements BackupClient
 */
class BackupManager : public BackupClient {
  public:
    explicit BackupManager();
    virtual ~BackupManager();
    void addHost(Transport::SessionRef session);

    virtual void commitSegment(uint64_t masterId, uint64_t segmentId);
    virtual void freeSegment(uint64_t masterId, uint64_t segmentId);
    virtual vector<RecoveredObject> getRecoveryData(uint64_t masterId,
                                                    const TabletMap& tablets);
    virtual void openSegment(uint64_t masterId, uint64_t segmentId);
    virtual vector<uint64_t> startReadingData(uint64_t masterId);
    virtual void writeSegment(uint64_t masterId,
                              uint64_t segmentId,
                              uint32_t offset,
                              const void *buf,
                              uint32_t length);

  private:
    /// The lone host to backup to for this dumb implementation.
    BackupHost *host;
    DISALLOW_COPY_AND_ASSIGN(BackupManager);
};

} // namespace RAMCloud

#endif
