/* Copyright (c) 2009-2012 Stanford University
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

#include "Common.h"
#include "ProtoBuf.h"
#include "Segment.h"
#include "ServerId.h"
#include "ServerIdRpcWrapper.h"
#include "ServerList.pb.h"
#include "Tablets.pb.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * Encapsulates the state of a BackupClient::assignGroup operation,
 * allowing it to execute asynchronously.
 */
class AssignGroupRpc : public ServerIdRpcWrapper {
  public:
    AssignGroupRpc(Context* context, ServerId backupId,
            uint64_t replicationId, uint32_t numReplicas,
            const ServerId* replicationGroupIds);
    ~AssignGroupRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(AssignGroupRpc);
};

/**
 * Encapsulates the state of a BackupClient::freeSegment operation,
 * allowing it to execute asynchronously.
 */
class FreeSegmentRpc : public ServerIdRpcWrapper {
  public:
    FreeSegmentRpc(Context* context, ServerId backupId, ServerId masterId,
            uint64_t segmentId);
    ~FreeSegmentRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(FreeSegmentRpc);
};

/**
 * Encapsulates the state of a BackupClient::getRecoveryData operation,
 * allowing it to execute asynchronously.
 */
class GetRecoveryDataRpc : public ServerIdRpcWrapper {
  public:
    GetRecoveryDataRpc(Context* context,
                       uint64_t recoveryId,
                       ServerId backupId,
                       ServerId masterId,
                       uint64_t segmentId,
                       uint64_t partitionId,
                       Buffer* responseBuffer);
    ~GetRecoveryDataRpc() {}
    Segment::Certificate wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetRecoveryDataRpc);
};

/**
 * Encapsulates the state of a BackupClient::quiesce operation,
 * allowing it to execute asynchronously (it has the "Backup" prefix
 * to distinguish it from the CoordinatorClient version of the
 * same call).
 */
class BackupQuiesceRpc : public ServerIdRpcWrapper {
  public:
    BackupQuiesceRpc(Context* context, ServerId backupId);
    ~BackupQuiesceRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(BackupQuiesceRpc);
};

/**
 * Encapsulates the state of a BackupClient::recoveryComplete operation,
 * allowing it to execute asynchronously.
 */
class RecoveryCompleteRpc : public ServerIdRpcWrapper {
  public:
    RecoveryCompleteRpc(Context* context, ServerId backupId,
            ServerId masterId);
    ~RecoveryCompleteRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RecoveryCompleteRpc);
};

/**
 * Encapsulates the state of a BackupClient::startReadingData operation,
 * allowing it to execute asynchronously.
 */
class StartReadingDataRpc : public ServerIdRpcWrapper {
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

    StartReadingDataRpc(Context* context, ServerId backupId,
            ServerId masterId, const ProtoBuf::Tablets* partitions);
    ~StartReadingDataRpc() {}
    Result wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(StartReadingDataRpc);
};

/**
 * Encapsulates the state of a BackupClient::writeSegment operation,
 * allowing it to execute asynchronously.
 */
class WriteSegmentRpc : public ServerIdRpcWrapper {
  public:
    WriteSegmentRpc(Context* context, ServerId backupId,
                    ServerId masterId, uint64_t segmentId,
                    const Segment* segment, uint32_t offset, uint32_t length,
                    const Segment::Certificate* certificate,
                    WireFormat::BackupWrite::Flags flags);
    ~WriteSegmentRpc() {}
    vector<ServerId> wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(WriteSegmentRpc);
};

/**
 * This class implements RPC requests that are sent to backup servers
 * to manage segment replicas. The class contains only static methods,
 * so you shouldn't ever need to instantiate an object.
 */
class BackupClient {
  public:
    static void assignGroup(Context* context, ServerId backupId,
            uint64_t replicationId, uint32_t numReplicas,
            const ServerId* replicationGroupIds);
    static void freeSegment(Context* context, ServerId backupId,
            ServerId masterId, uint64_t segmentId);
    static Segment::Certificate getRecoveryData(Context* context,
                                                uint64_t recoveryId,
                                                ServerId backupId,
                                                ServerId masterId,
                                                uint64_t segmentId,
                                                uint64_t partitionId,
                                                Buffer* response);
    static void quiesce(Context* context, ServerId backupId);
    static void recoveryComplete(Context* context, ServerId backupId,
            ServerId masterId);
    static StartReadingDataRpc::Result startReadingData(Context* context,
            ServerId backupId, ServerId masterId,
            const ProtoBuf::Tablets* partitions);
    static vector<ServerId> writeSegment(Context* context, ServerId backupId,
            ServerId masterId, uint64_t segmentId, const Segment* segment,
            uint32_t offset, uint32_t length,
            const Segment::Certificate* certificate,
            WireFormat::BackupWrite::Flags flags =
                                        WireFormat::BackupWrite::NONE);

  private:
    BackupClient();
};

} // namespace RAMCloud

#endif
