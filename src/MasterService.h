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
#include "ServerList.h"
#include "ServerStatistics.pb.h"
#include "Service.h"
#include "ServerConfig.h"
#include "SpinLock.h"
#include "Table.h"

namespace RAMCloud {

// forward declaration
namespace MasterServiceInternal {
class RecoveryTask;
}

/**
 * An object of this class represents a RAMCloud server, which can
 * respond to client RPC requests to manipulate objects stored on the
 * server.
 */
class MasterService : public Service {
  public:
    MasterService(const ServerConfig& config,
                  CoordinatorClient* coordinator,
                  ServerList& serverList);
    virtual ~MasterService();
    void init(ServerId id);
    void dispatch(RpcOpcode opcode,
                  Rpc& rpc);

  PRIVATE:

    /**
     * Represents a known segment replica during recovery and the state
     * of fetching it from backups.
     */
    struct Replica {
        enum class State {
            NOT_STARTED = 0,
            WAITING,
            FAILED,
            OK,
        };
        Replica(uint64_t backupId, uint64_t segmentId,
                State state = State::NOT_STARTED);
        /**
         * The backup containing the replica.
         */
        ServerId backupId;
        /**
         * The segment ID for this replica.
         */
        uint64_t segmentId;
        /**
         * Used in recovery routines to keep track of the status of requesting
         * the data from this replica.
         */
        State state;
    };

    void fillWithTestData(const FillWithTestDataRpc::Request& reqHdr,
                          FillWithTestDataRpc::Response& respHdr,
                          Rpc& rpc);
    void increment(const IncrementRpc::Request& reqHdr,
                 IncrementRpc::Response& respHdr,
                 Rpc& rpc);
    void isReplicaNeeded(const IsReplicaNeededRpc::Request& reqHdr,
                         IsReplicaNeededRpc::Response& respHdr,
                         Rpc& rpc);
    void getHeadOfLog(const GetHeadOfLogRpc::Request& reqHdr,
                      GetHeadOfLogRpc::Response& respHdr,
                      Rpc& rpc);
    void multiRead(const MultiReadRpc::Request& reqHdr,
                   MultiReadRpc::Response& respHdr,
                   Rpc& rpc);
    void read(const ReadRpc::Request& reqHdr,
              ReadRpc::Response& respHdr,
              Rpc& rpc);
    void getServerStatistics(const GetServerStatisticsRpc::Request& reqHdr,
                             GetServerStatisticsRpc::Response& respHdr,
                             Rpc& rpc);
    void dropTabletOwnership(const DropTabletOwnershipRpc::Request& reqHdr,
                             DropTabletOwnershipRpc::Response& respHdr,
                             Rpc& rpc);
    void takeTabletOwnership(const TakeTabletOwnershipRpc::Request& reqHdr,
                             TakeTabletOwnershipRpc::Response& respHdr,
                             Rpc& rpc);
    void prepForMigration(const PrepForMigrationRpc::Request& reqHdr,
                          PrepForMigrationRpc::Response& respHdr,
                          Rpc& rpc);
    void migrateTablet(const MigrateTabletRpc::Request& reqHdr,
                       MigrateTabletRpc::Response& respHdr,
                       Rpc& rpc);
    void receiveMigrationData(const ReceiveMigrationDataRpc::Request& reqHdr,
                              ReceiveMigrationDataRpc::Response& respHdr,
                              Rpc& rpc);
    void purgeObjectsFromUnknownTablets();
    void recover(const RecoverRpc::Request& reqHdr,
                 RecoverRpc::Response& respHdr,
                 Rpc& rpc);
    void recoverSegmentPrefetcher(RecoverySegmentIterator& i);
    void recoverSegment(uint64_t segmentId, const void *buffer,
                        uint32_t bufferLength);
    void recover(ServerId masterId,
                 uint64_t partitionId,
                 vector<Replica>& replicas);
    void remove(const RemoveRpc::Request& reqHdr,
                RemoveRpc::Response& respHdr,
                Rpc& rpc);
    void splitMasterTablet(const SplitMasterTabletRpc::Request& reqHdr,
                SplitMasterTabletRpc::Response& respHdr,
                Rpc& rpc);
    void write(const WriteRpc::Request& reqHdr,
               WriteRpc::Response& respHdr,
               Rpc& rpc);

    const ServerConfig& config;

  public:
    CoordinatorClient* coordinator;

    ServerId serverId;

  PRIVATE:
    /// A reference to the global ServerList.
    ServerList& serverList;

    /**
     * Creates and tracks replicas of in-memory log segments on remote backups.
     * Its BackupFailureMonitor must be started after the log is created
     * and halted before the log is destroyed.
     */
    ReplicaManager replicaManager;

    /// Maximum number of bytes per partition. For Will calculation.
    static const uint64_t maxBytesPerPartition = 640UL * 1024 * 1024;

    /// Maximum number of referents (objs) per partition. For Will calculation.
    static const uint64_t maxReferentsPerPartition = 10UL * 1000 * 1000;

    /// Track total bytes of object data written (not including log overhead).
    uint64_t bytesWritten;

    /**
     * The main in-memory data structure holding all of the data stored
     * on this server.
     */
    Log log;

    /**
     * The (table ID, key, keyLength) to #RAMCloud::Object pointer map for all
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

  PRIVATE:
    void incrementReadAndWriteStatistics(Table* table);

    static void
    detectSegmentRecoveryFailure(
                        const ServerId masterId,
                        const uint64_t partitionId,
                        const vector<MasterService::Replica>& replicas);

    friend void recoveryCleanup(LogEntryHandle maybeTomb, void *cookie);
    friend void removeObjectIfFromUnknownTablet(LogEntryHandle entry,
                                                void *cookie);
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
    Table* getTable(uint64_t tableId, const char* key, uint16_t keyLength)
        __attribute__((warn_unused_result));
    Table* getTableForHash(uint64_t tableId, HashType keyHash)
        __attribute__((warn_unused_result));
    ProtoBuf::Tablets::Tablet const* getTabletForHash(uint64_t tableId,
                                                      HashType keyHash)
        __attribute__((warn_unused_result));
    Status rejectOperation(const RejectRules& rejectRules, uint64_t version)
        __attribute__((warn_unused_result));
    Status storeData(uint64_t table,
                     const RejectRules* rejectRules, Buffer* data,
                     uint32_t keyOffset, uint16_t keyLength,
                     uint32_t dataLength,
                     uint64_t* newVersion, bool async)
        __attribute__((warn_unused_result));
    friend class RecoverSegmentBenchmark;
    friend class MasterServiceInternal::RecoveryTask;
    DISALLOW_COPY_AND_ASSIGN(MasterService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTERSERVICE_H
