/* Copyright (c) 2009-2011 Stanford University
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

#ifndef RAMCLOUD_MASTERSERVICE_H
#define RAMCLOUD_MASTERSERVICE_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "Log.h"
#include "LogCleaner.h"
#include "HashTable.h"
#include "Object.h"
#include "RecoverySegmentIterator.h"
#include "ReplicaManager.h"
#include "SegmentIterator.h"
#include "Service.h"
#include "ServerConfig.h"
#include "SpinLock.h"
#include "Table.h"

namespace RAMCloud {

#if TESTING
void
detectSegmentRecoveryFailure(const ServerId masterId,
                             const uint64_t partitionId,
                             const ProtoBuf::ServerList& backups);
#endif

/**
 * An object of this class represents a RAMCloud server, which can
 * respond to client RPC requests to manipulate objects stored on the
 * server.
 */
class MasterService : public Service {
  public:
    static const uint64_t TOTAL_READ_REQUESTS_OBJID = 100000UL;

    MasterService(const ServerConfig& config,
                  CoordinatorClient* coordinator);
    virtual ~MasterService();
    void init(ServerId id);
    void dispatch(RpcOpcode opcode,
                  Rpc& rpc);

    /**
     * Used in detectSegmentRecoveryFailure() and MasterService::recover() to
     * mark and check getRecoveryData() requests statuses.
     */
    enum { REC_REQ_NOT_STARTED, REC_REQ_WAITING, REC_REQ_FAILED, REC_REQ_OK };

  PRIVATE:
    void create(const CreateRpc::Request& reqHdr,
                CreateRpc::Response& respHdr,
                Rpc& rpc);
    void fillWithTestData(const FillWithTestDataRpc::Request& reqHdr,
                          FillWithTestDataRpc::Response& respHdr,
                          Rpc& rpc);
    void multiRead(const MultiReadRpc::Request& reqHdr,
                   MultiReadRpc::Response& respHdr,
                   Rpc& rpc);
    void read(const ReadRpc::Request& reqHdr,
              ReadRpc::Response& respHdr,
              Rpc& rpc);
    void recover(const RecoverRpc::Request& reqHdr,
                 RecoverRpc::Response& respHdr,
                 Rpc& rpc);

    void recoverSegmentPrefetcher(RecoverySegmentIterator& i);
    void recoverSegment(uint64_t segmentId, const void *buffer,
                        uint32_t bufferLength);

    void recover(ServerId masterId,
                 uint64_t partitionId,
                 ProtoBuf::ServerList& backups);

    void remove(const RemoveRpc::Request& reqHdr,
                RemoveRpc::Response& respHdr,
                Rpc& rpc);
    void rereplicateSegments(const RereplicateSegmentsRpc::Request& reqHdr,
                             RereplicateSegmentsRpc::Response& respHdr,
                             Rpc& rpc);
    void setTablets(const ProtoBuf::Tablets& newTablets);
    void setTablets(const SetTabletsRpc::Request& reqHdr,
                    SetTabletsRpc::Response& respHdr,
                    Rpc& rpc);
    void write(const WriteRpc::Request& reqHdr,
               WriteRpc::Response& respHdr,
               Rpc& rpc);

    const ServerConfig& config;

  public:
    CoordinatorClient* coordinator;

    Tub<ServerId> serverId;

  PRIVATE:
    /// Maximum number of bytes per partition. For Will calculation.
    static const uint64_t maxBytesPerPartition = 640UL * 1024 * 1024;

    /// Maximum number of referents (objs) per partition. For Will calculation.
    static const uint64_t maxReferentsPerPartition = 10UL * 1000 * 1000;

    /// Creates and tracks replicas of in-memory log segments on remote backups.
    ReplicaManager replicaManager;

    /// Track total bytes of object data written (not including log overhead).
    uint64_t bytesWritten;

    /**
     * The main in-memory data structure holding all of the data stored
     * on this server.
     */
    Log log;

    /**
     * The (table ID, object ID) to #RAMCloud::Object pointer map for all
     * objects stored on this server. Before accessing objects via the hash
     * table, you usually need to check that the tablet still lives on this
     * server; objects from deleted tablets are not immediately purged from the
     * hash table.
     */
    HashTable<LogEntryHandle> objectMap;

    /**
     * Tablets this master owns.
     * The user_data field in each tablet points to a Table object.
     */
    ProtoBuf::Tablets tablets;

    /**
     * Used to ensure that init() is invoked before the dispatcher runs.
     */
    bool initCalled;

    /**
     * Used to identify the first write request, so that we can initialize
     * connections to all backups at that time (this is a temporary kludge
     * that needs to be replaced with a better solution).  False means this
     * service has not yet processed any write requests.
     */
    bool anyWrites;

    /**
     * Lock that serialises all object updates (creations, overwrites,
     * deletions, and cleaning relocations). This protects regular RPC
     * operations from the log cleaner. When we work on multithreaded
     * writes we'll need to revisit this.
     */
    SpinLock objectUpdateLock;

    /* Tombstone cleanup method used after recovery. */
    void removeTombstones();

    friend void recoveryCleanup(LogEntryHandle maybeTomb, void *cookie);
    friend bool objectLivenessCallback(LogEntryHandle handle, void* cookie);
    friend bool objectRelocationCallback(LogEntryHandle oldHandle,
                                         LogEntryHandle newHandle,
                                         void* cookie);
    friend void objectScanCallback(LogEntryHandle handle, void* cookie);
    friend bool tombstoneLivenessCallback(LogEntryHandle handle, void* cookie);
    friend bool tombstoneRelocationCallback(LogEntryHandle oldHandle,
                                            LogEntryHandle newHandle,
                                            void* cookie);
    friend void tombstoneScanCallback(LogEntryHandle handle, void* cookie);
    friend void segmentReplayCallback(Segment* seg, void* cookie);
    Table* getTable(uint32_t tableId, uint64_t objectId)
        __attribute__((warn_unused_result));
    Status rejectOperation(const RejectRules& rejectRules, uint64_t version)
        __attribute__((warn_unused_result));
    Status storeData(uint64_t table, uint64_t id,
                     const RejectRules* rejectRules, Buffer* data,
                     uint32_t dataOffset, uint32_t dataLength,
                     uint64_t* newVersion, bool async)
        __attribute__((warn_unused_result));
    friend class RecoverSegmentBenchmark;
    DISALLOW_COPY_AND_ASSIGN(MasterService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTERSERVICE_H
