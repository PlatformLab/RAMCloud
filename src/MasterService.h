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
#include "LogEntryHandlers.h"
#include "HashTable.h"
#include "Object.h"
#include "SegmentIterator.h"
#include "SegmentManager.h"
#include "ReplicaManager.h"
#include "ServerList.h"
#include "ServerStatistics.pb.h"
#include "Service.h"
#include "ServerConfig.h"
#include "SpinLock.h"
#include "Table.h"
#include "WireFormat.h"

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
class MasterService : public Service, LogEntryHandlers {
  public:
    MasterService(Context& context,
                  const ServerConfig& config);
    virtual ~MasterService();
    void init(ServerId id);
    void dispatch(WireFormat::Opcode opcode,
                  Rpc& rpc);

    uint32_t getTimestamp(LogEntryType type, Buffer& buffer);
    bool checkLiveness(LogEntryType type, Buffer& buffer);
    bool relocate(LogEntryType type,
                  Buffer& oldBuffer,
                  HashTable::Reference newReference);

  PRIVATE:
    /**
     * Comparison functor used by the HashTable to compare a key
     * we're querying with a potential match.
     */
    class LogKeyComparer : public HashTable::KeyComparer {
      public:
        explicit LogKeyComparer(Log& log);
        bool doesMatch(Key& key, HashTable::Reference reference);

      private:
        Log& log;
    };

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

    void enumeration(const WireFormat::Enumerate::Request& reqHdr,
                     WireFormat::Enumerate::Response& respHdr,
                     Rpc& rpc);
    void fillWithTestData(const WireFormat::FillWithTestData::Request& reqHdr,
                          WireFormat::FillWithTestData::Response& respHdr,
                          Rpc& rpc);
    void increment(const WireFormat::Increment::Request& reqHdr,
                 WireFormat::Increment::Response& respHdr,
                 Rpc& rpc);
    void isReplicaNeeded(const WireFormat::IsReplicaNeeded::Request& reqHdr,
                         WireFormat::IsReplicaNeeded::Response& respHdr,
                         Rpc& rpc);
    void getHeadOfLog(const WireFormat::GetHeadOfLog::Request& reqHdr,
                      WireFormat::GetHeadOfLog::Response& respHdr,
                      Rpc& rpc);
    void multiRead(const WireFormat::MultiRead::Request& reqHdr,
                   WireFormat::MultiRead::Response& respHdr,
                   Rpc& rpc);
    void read(const WireFormat::Read::Request& reqHdr,
              WireFormat::Read::Response& respHdr,
              Rpc& rpc);
    void getServerStatistics(
        const WireFormat::GetServerStatistics::Request& reqHdr,
        WireFormat::GetServerStatistics::Response& respHdr,
        Rpc& rpc);
    void dropTabletOwnership(
        const WireFormat::DropTabletOwnership::Request& reqHdr,
        WireFormat::DropTabletOwnership::Response& respHdr,
        Rpc& rpc);
    void takeTabletOwnership(
            const WireFormat::TakeTabletOwnership::Request& reqHdr,
            WireFormat::TakeTabletOwnership::Response& respHdr,
            Rpc& rpc);
    void prepForMigration(const WireFormat::PrepForMigration::Request& reqHdr,
                          WireFormat::PrepForMigration::Response& respHdr,
                          Rpc& rpc);
    void migrateTablet(const WireFormat::MigrateTablet::Request& reqHdr,
                       WireFormat::MigrateTablet::Response& respHdr,
                       Rpc& rpc);
    void receiveMigrationData(
        const WireFormat::ReceiveMigrationData::Request& reqHdr,
        WireFormat::ReceiveMigrationData::Response& respHdr,
        Rpc& rpc);
    void purgeObjectsFromUnknownTablets();
    void recover(const WireFormat::Recover::Request& reqHdr,
                 WireFormat::Recover::Response& respHdr,
                 Rpc& rpc);
    void recoverSegmentPrefetcher(SegmentIterator& i);
    void recoverSegment(uint64_t segmentId, const void *buffer,
                        uint32_t bufferLength);
    void recover(ServerId masterId,
                 uint64_t partitionId,
                 vector<Replica>& replicas);
    void remove(const WireFormat::Remove::Request& reqHdr,
                WireFormat::Remove::Response& respHdr,
                Rpc& rpc);
    void splitMasterTablet(const WireFormat::SplitMasterTablet::Request& reqHdr,
                WireFormat::SplitMasterTablet::Response& respHdr,
                Rpc& rpc);
    void write(const WireFormat::Write::Request& reqHdr,
               WireFormat::Write::Response& respHdr,
               Rpc& rpc);

  public:
    /// Shared RAMCloud information.
    Context& context;

    const ServerConfig& config;

    /// The identifier assigned to this service by the coordinator.  Initial
    /// state (before enlistment) is ???.
    ServerId serverId;

  PRIVATE:

    /// Track total bytes of object data written (not including log overhead).
    uint64_t bytesWritten;

    /**
     * Creates and tracks replicas of in-memory log segments on remote backups.
     * Its BackupFailureMonitor must be started after the log is created
     * and halted before the log is destroyed.
     */
    ReplicaManager replicaManager;

    /**
     * Allocator used by the SegmentManager to obtain main memory for log
     * segments.
     */
    SegletAllocator allocator;

    /**
     * The SegmentManager manages all segments in the log and interfaces
     * between the log and the cleaner modules.
     */
    SegmentManager segmentManager;

    /**
     * The log stores all of our objects and tombstones. It is used to append
     * new data is notified of dead data. Garbage collection ("cleaning") takes
     * place concurrently with server execution and may cause live data to be
     * reshuffled to new locations in memory.
     *
     * The log is not constructed until init() is called, since we don't know
     * our own ServerId until then and the log's constructor will ensure that
     * the first segment is replicated before returning (if needed).
     */
    Log* log;

    /**
     * Comparison functor used by the hash table to compare keys for equality.
     */
    LogKeyComparer* keyComparer;

    /**
     * The (table ID, key, keyLength) to #RAMCloud::Object pointer map for all
     * objects stored on this server. Before accessing objects via the hash
     * table, you usually need to check that the tablet still lives on this
     * server; objects from deleted tablets are not immediately purged from the
     * hash table.
     */
    HashTable* objectMap;

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

    /**
     * Determines the maximum size of the response buffer for multiRead
     * operations. Normally MAX_RPC_LEN, but can be modified during tests
     * to simplify testing.
     */
    uint32_t maxMultiReadResponseSize;

    /* Tombstone cleanup method used after recovery. */
    void removeTombstones();

  PRIVATE:
    void incrementReadAndWriteStatistics(Table* table);

    static void
    detectSegmentRecoveryFailure(
                        const ServerId masterId,
                        const uint64_t partitionId,
                        const vector<MasterService::Replica>& replicas);

    friend void recoveryCleanup(HashTable::Reference maybeTomb, void *cookie);
    friend void removeObjectIfFromUnknownTablet(HashTable::Reference reference,
                                                void *cookie);

    Table* getTable(Key& key) __attribute__((warn_unused_result));
    Table* getTableForHash(uint64_t tableId, HashType keyHash)
        __attribute__((warn_unused_result));
    ProtoBuf::Tablets::Tablet const* getTabletForHash(uint64_t tableId,
                                                      HashType keyHash)
        __attribute__((warn_unused_result));
    Status rejectOperation(const RejectRules& rejectRules, uint64_t version)
        __attribute__((warn_unused_result));
    bool checkObjectLiveness(Buffer& buffer);
    bool relocateObject(Buffer& oldBuffer,
                        HashTable::Reference newReference);
    uint32_t getObjectTimestamp(Buffer& buffer);
    bool checkTombstoneLiveness(Buffer& buffer);
    bool relocateTombstone(Buffer& oldBuffer,
                           HashTable::Reference newReference);
    uint32_t getTombstoneTimestamp(Buffer& buffer);
    Status storeObject(Key& key,
                       const RejectRules* rejectRules,
                       Buffer& data,
                       uint64_t* newVersion,
                       bool sync) __attribute__((warn_unused_result));
    bool lookup(Key& key, LogEntryType& type, Buffer& buffer);
    bool lookup(Key& key,
                LogEntryType& type,
                Buffer& buffer,
                HashTable::Reference& reference);

    friend class RecoverSegmentBenchmark;
    friend class MasterServiceInternal::RecoveryTask;

    DISALLOW_COPY_AND_ASSIGN(MasterService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTERSERVICE_H
