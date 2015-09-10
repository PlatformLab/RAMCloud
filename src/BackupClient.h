/* Copyright (c) 2009-2015 Stanford University
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
#include "RecoveryPartition.pb.h"
#include "Transport.h"

namespace RAMCloud {

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
                       ServerId backupId,
                       uint64_t recoveryId,
                       ServerId masterId,
                       uint64_t segmentId,
                       uint64_t partitionId,
                       Buffer* responseBuffer);
    ~GetRecoveryDataRpc() {}
    SegmentCertificate wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetRecoveryDataRpc);
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
    typedef WireFormat::BackupStartReadingData::Replica Replica;

    /**
     * The result of a startReadingData RPC, as returned by the backup.
     */
    struct Result {
        Result();
        Result(Result&& other);
        Result& operator=(Result&& other);

        /**
         * Information about each of the replicas found on the backup.
         * Includes any details needed to determine whether the replica is
         * consistent and safe to use during recovery. See
         * WireFormat::BackupStartReadingData::Replica for exact fields.
         */
        vector<Replica> replicas;

        /**
         * The number of primary replicas this backup has returned at the
         * start of #replicaDetails.
         */
        uint32_t primaryReplicaCount;

        /**
         * A buffer containing the LogDigest of the newest open segment
         * replica found on this backup from this master, if one exists.
         */
        std::unique_ptr<char[]> logDigestBuffer;

        /**
         * A buffer containing the table stats gathered from the
         * newest open segment replica found on this master, if one exists.
         * These metrics may not be completely up-to-date as the metrics
         * are updated only when a new log head is created.
         */
        std::unique_ptr<char[]> tableStatsBuffer;

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
         * Epoch of the replica from which the log digest was taken.
         * Used by the coordinator to detect if the replica the
         * digest was extracted may be inconsistent. If it might be
         * then the coordinator will discard the returned log digest.
         * This will be -1 if there is no log digest.
         */
        uint64_t logDigestSegmentEpoch;

        /**
         * The number of bytes making up the TabletMetrics.
         * This will be -1 if no metrics were found.
         */
        uint32_t tableStatsBytes;

        DISALLOW_COPY_AND_ASSIGN(Result);
    };

    StartReadingDataRpc(Context* context, ServerId backupId,
                        uint64_t recoveryId, ServerId masterId);
    ~StartReadingDataRpc() {}
    Result wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(StartReadingDataRpc);
};

/**
 * Encapsulates the state of a BackupClient::startPartitioning operation,
 * allowing it to execute asynchronously.
 */
class StartPartitioningRpc : public ServerIdRpcWrapper {
  public:
    StartPartitioningRpc(Context* context, ServerId backupId,
                        uint64_t recoveryId, ServerId masterId,
                        const ProtoBuf::RecoveryPartition* partitions);
    ~StartPartitioningRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(StartPartitioningRpc);
};

/**
 * Encapsulates the state of a BackupClient::writeSegment operation,
 * allowing it to execute asynchronously.
 */
class WriteSegmentRpc : public ServerIdRpcWrapper {
  public:
    WriteSegmentRpc(Context* context, ServerId backupId,
                    ServerId masterId,
                    uint64_t segmentId, uint64_t segmentEpoch,
                    const Segment* segment, uint32_t offset, uint32_t length,
                    const SegmentCertificate* certificate,
                    bool open, bool close, bool primary);
    ~WriteSegmentRpc() {}
    void wait();

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
    static SegmentCertificate getRecoveryData(Context* context,
                                              ServerId backupId,
                                              uint64_t recoveryId,
                                              ServerId masterId,
                                              uint64_t segmentId,
                                              uint64_t partitionId,
                                              Buffer* response);
    static void recoveryComplete(Context* context, ServerId backupId,
            ServerId masterId);
    static StartReadingDataRpc::Result startReadingData(Context* context,
            ServerId backupId, uint64_t recoveryId, ServerId masterId);
    static void StartPartitioningReplicas(Context* context, ServerId backupId,
            uint64_t recoveryId, ServerId masterId,
            const ProtoBuf::RecoveryPartition* partitions);
    static void writeSegment(Context* context, ServerId backupId,
            ServerId masterId, uint64_t segmentId, uint64_t segmentEpoch,
            const Segment* segment, uint32_t offset, uint32_t length,
            const SegmentCertificate* certificate,
            bool open, bool close, bool primary);

  private:
    BackupClient();
};

} // namespace RAMCloud

#endif
