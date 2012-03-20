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
#include "Memory.h"
#include "Object.h"
#include "ProtoBuf.h"
#include "ServerId.h"
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
     * Free a segment replica stored on a backup.  This object is a
     * continuation that blocks until #responseBuffer is populated
     * when invoked.
     */
    class FreeSegment {
      public:
        FreeSegment(BackupClient& client,
                    ServerId masterId,
                    uint64_t segmentId);
        void cancel() { state.cancel(); }
        bool isReady() { return state.isReady(); }
        void operator()();
      private:
        BackupClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(FreeSegment);
    };
    DEF_SYNC_RPC_METHOD(freeSegment, FreeSegment);

    /**
     * Get the objects stored for the given tablets of the given server.  This
     * object is a continuation that blocks until #responseBuffer is populated
     * when invoked.
     */
    class GetRecoveryData {
      public:
        GetRecoveryData(BackupClient& client,
                        ServerId masterId,
                        uint64_t segmentId,
                        uint64_t partitionId,
                        Buffer& responseBuffer);
        bool isReady() { return state.isReady(); }
        void operator()();
        BackupClient& client;
        Buffer requestBuffer;
        Buffer& responseBuffer;
        AsyncState state;

        friend class BackupClient;
        DISALLOW_COPY_AND_ASSIGN(GetRecoveryData);
    };
    DEF_SYNC_RPC_METHOD(getRecoveryData, GetRecoveryData);

    class StartReadingData {
      public:
        /**
         * The result of a startReadingData RPC, as returned by the backup.
         */
        struct Result {
            Result();
            Result(Result&& other);
            Result& operator=(Result&& other);
            /**
             * A list of the segment IDs for which this backup has a replica,
             * and the length in bytes for those replicas.
             * For closed segments, this length is currently returned as ~OU.
             */
            vector<pair<uint64_t, uint32_t>> segmentIdAndLength;

            /**
             * The number of primary replicas this backup has returned at the
             * start of segmentIdAndLength.
             */
            uint32_t primarySegmentCount;

            /**
             * A buffer containing the LogDigest of the newest open segment
             * replica found on this backup from this master, if one exists.
             */
            std::unique_ptr<char[]> logDigestBuffer;

            /**
             * The number of bytes that make up logDigestBuffer.
             */
            uint32_t logDigestBytes;

            /**
             * The segment ID the log digest came from.
             * This will be -1 if there is no log digest.
             */
            uint64_t logDigestSegmentId;

            /**
             * The number of bytes making up the replica that contains the
             * returned log digest.
             * This will be -1 if there is no log digest.
             */
            uint32_t logDigestSegmentLen;
            DISALLOW_COPY_AND_ASSIGN(Result);
        };

        StartReadingData(BackupClient& client,
                         ServerId masterId,
                         const ProtoBuf::Tablets& partitions);
        bool isReady() { return state.isReady(); }
        BackupClient::StartReadingData::Result operator()();
        BackupClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;

        friend class BackupClient;
        DISALLOW_COPY_AND_ASSIGN(StartReadingData);
    };

    /// A synchronous version of StartReadingData.
    StartReadingData::Result
    startReadingData(ServerId masterId, const ProtoBuf::Tablets& partitions)
    {
        return StartReadingData(*this, masterId, partitions)();
    }

    class WriteSegment {
      public:
        WriteSegment(BackupClient& client,
                     ServerId masterId,
                     uint64_t segmentId,
                     uint32_t offset,
                     const void *buf,
                     uint32_t length,
                     BackupWriteRpc::Flags flags = BackupWriteRpc::NONE,
                     bool atomic = false);
        void cancel() { state.cancel(); }
        bool isReady() { return state.isReady(); }
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

    void closeSegment(ServerId masterId, uint64_t segmentId) {
        writeSegment(masterId, segmentId,
                     0, static_cast<const void*>(NULL), 0,
                     BackupWriteRpc::CLOSE, false);
    }

    Transport::SessionRef getSession();

    void openSegment(ServerId masterId,
                     uint64_t segmentId,
                     bool primary = true, bool atomic = false)
    {
        writeSegment(masterId, segmentId,
                     0, static_cast<const void*>(NULL), 0,
                     primary ? BackupWriteRpc::OPENPRIMARY
                             : BackupWriteRpc::OPEN, atomic);
    }

    void quiesce();

    class RecoveryComplete {
      public:
        RecoveryComplete(BackupClient& client, ServerId masterId);
        bool isReady() { return state.isReady(); }
        void operator()();
      private:
        BackupClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(RecoveryComplete);
    };
    DEF_SYNC_RPC_METHOD(recoveryComplete, RecoveryComplete);

    /**
     * Assign a replicationId to a backup, and notify the backup of its other
     * group members.
     */
    class AssignGroup {
      public:
        AssignGroup(BackupClient& client,
                    uint64_t replicationId,
                    uint32_t numReplicas,
                    const ServerId* replicationGroupIds);
        void cancel() { state.cancel(); }
        bool isReady() { return state.isReady(); }
        void operator()();
      private:
        BackupClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(AssignGroup);
    };
    DEF_SYNC_RPC_METHOD(assignGroup, AssignGroup);


  private:
    /**
     * A session with a backup server.
     */
    Transport::SessionRef session;

    DISALLOW_COPY_AND_ASSIGN(BackupClient);
};

} // namespace RAMCloud

#endif
