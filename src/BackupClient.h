/* Copyright (c) 2009 Stanford University
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

/**
 * \file
 * Declarations for master server-side backup RPC stubs.  The
 * classes herein send requests to the backup servers transparently to
 * handle all the backup needs of the masters.
 */

#ifndef RAMCLOUD_BACKUPCLIENT_H
#define RAMCLOUD_BACKUPCLIENT_H

#include <Common.h>
#include <Net.h>
#include <backuprpc.h>

namespace RAMCloud {

class BufferPtr;

/**
 * A single unit of metadata for recovery of an object as returned by
 * getSegmentMetadata().
 */
struct RecoveryObjectMetadata {
    uint64_t key;
    uint64_t table;
    uint64_t version;
    uint64_t offset;
    uint64_t length;
};


/**
 * The interface for an object that can act as a backup server no
 * matter the transport or location.
 */
class BackupClient {
  public:
    virtual ~BackupClient() {}
    virtual void heartbeat() = 0;
    virtual void writeSegment(uint64_t segNum, uint32_t offset,
                              const void *data, uint32_t len) = 0;
    virtual void commitSegment(uint64_t segNum) = 0;
    virtual void freeSegment(uint64_t segNum) = 0;
    virtual size_t getSegmentList(uint64_t *list, size_t maxSize) = 0;
    virtual size_t getSegmentMetadata(uint64_t segNum,
                                      RecoveryObjectMetadata *list,
                                      size_t maxSize) = 0;
    virtual void retrieveSegment(uint64_t segNum, void *buf) = 0;
};

/**
 * A backup consisting of a single remote host.
 *
 * \implements BackupClient
 */
class BackupHost : public BackupClient {
  public:
    explicit BackupHost(Net *netimpl);
    virtual ~BackupHost();

    virtual void heartbeat();
    virtual void writeSegment(uint64_t segNum, uint32_t offset,
                              const void *data, uint32_t len);
    virtual void commitSegment(uint64_t segNum);
    virtual void freeSegment(uint64_t segNum);
    virtual size_t getSegmentList(uint64_t *list, size_t maxSize);
    virtual size_t getSegmentMetadata(uint64_t segNum,
                                      RecoveryObjectMetadata *list,
                                      size_t maxSize);
    virtual void retrieveSegment(uint64_t segNum, void *buf);
  private:
    void sendRPC(BufferPtr *rpc);
    void recvRPC(BufferPtr **rpc);
    Net *net;
    DISALLOW_COPY_AND_ASSIGN(BackupHost);
};

/**
 * A backup consisting of a multiple remote hosts.
 *
 * The precise set of backup hosts is selected by creating BackupHost
 * instances and adding them to the MultiBackupClient instance via
 * addHost().
 *
 * \implements BackupClient
 */
class MultiBackupClient : public BackupClient {
  public:
    explicit MultiBackupClient();
    virtual ~MultiBackupClient();
    void addHost(Net *net);

    virtual void heartbeat();
    virtual void writeSegment(uint64_t segNum, uint32_t offset,
                              const void *data, uint32_t len);
    virtual void commitSegment(uint64_t segNum);
    virtual void freeSegment(uint64_t segNum);
    virtual size_t getSegmentList(uint64_t *list, size_t maxSize);
    virtual size_t getSegmentMetadata(uint64_t segNum,
                                      RecoveryObjectMetadata *list,
                                      size_t maxSize);
    virtual void retrieveSegment(uint64_t segNum, void *buf);
  private:
    BackupHost *host;
    DISALLOW_COPY_AND_ASSIGN(MultiBackupClient);
};

} // namespace RAMCloud

#endif
