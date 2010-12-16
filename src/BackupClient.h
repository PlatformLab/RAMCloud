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

#include <list>

#include "Client.h"
#include "Common.h"
#include "CoordinatorClient.h"
#include "Object.h"
#include "ProtoBuf.h"
#include "ServerList.pb.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * A backup consisting of a single remote host.  BackupClient's primary
 * role is to proxy calls via RPCs to a particular backup server.
 */
class BackupClient : public Client {
  public:

    /**
     * Get the objects stored for the given tablets of the given server.  This
     * object is a continuation that blocks until #responseBuffer is populated
     * when invoked.
     */
    class GetRecoveryData {
      public:
        GetRecoveryData(BackupClient& client,
                        uint64_t masterId,
                        uint64_t segmentId,
                        uint64_t partitionId,
                        Buffer& responseBuffer);
        void operator()();

        BackupClient& client;
        Buffer requestBuffer;
        Buffer& responseBuffer;
        AsyncState state;

        friend class BackupClient;
        DISALLOW_COPY_AND_ASSIGN(GetRecoveryData);
    };
    DEF_SYNC_RPC_METHOD(getRecoveryData, GetRecoveryData);

    class WriteSegment {
      public:
        WriteSegment(BackupClient& client,
                     uint64_t masterId,
                     uint64_t segmentId,
                     uint32_t offset,
                     const void *buf,
                     uint32_t length,
                     BackupWriteRpc::Flags flags = BackupWriteRpc::NONE);
        void operator()();
      private:
        BackupClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(WriteSegment);
    };
    DEF_SYNC_RPC_METHOD(writeSegment, WriteSegment);

    explicit BackupClient(Transport::SessionRef session);
    ~BackupClient();

    void closeSegment(uint64_t masterId, uint64_t segmentId) {
        writeSegment(masterId, segmentId,
                     0, static_cast<const void*>(NULL), 0,
                     BackupWriteRpc::CLOSE);
    }

    void freeSegment(uint64_t masterId, uint64_t segmentId);
    Transport::SessionRef getSession();

    void openSegment(uint64_t masterId, uint64_t segmentId) {
        writeSegment(masterId, segmentId,
                     0, static_cast<const void*>(NULL), 0,
                     BackupWriteRpc::OPEN);
    }

    void ping();
    vector<uint64_t> startReadingData(uint64_t masterId,
                                      const ProtoBuf::Tablets& partitions);

  private:
    /**
     * A session with a backup server.
     */
    Transport::SessionRef session;

    DISALLOW_COPY_AND_ASSIGN(BackupClient);
};

} // namespace RAMCloud

#endif
