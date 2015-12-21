/* Copyright (c) 2014-2015 Stanford University
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

#ifndef RAMCLOUD_OBJECTMANAGER_H
#define RAMCLOUD_OBJECTMANAGER_H

#include "Common.h"
#include "Log.h"
#include "SideLog.h"
#include "LogEntryHandlers.h"
#include "HashTable.h"
#include "IndexKey.h"
#include "Object.h"
#include "PreparedOps.h"
#include "SegmentManager.h"
#include "SegmentIterator.h"
#include "ReplicaManager.h"
#include "RpcResult.h"
#include "ServerConfig.h"
#include "SpinLock.h"
#include "TabletManager.h"
#include "TxDecisionRecord.h"
#include "TxRecoveryManager.h"
#include "MasterTableMetadata.h"
#include "UnackedRpcResults.h"
#include "LockTable.h"

namespace RAMCloud {

/**
 * The ObjectManager class is responsible for storing objects in a master
 * server. It is essentially the union of the Log, HashTable, TabletMap,
 * and ReplicaManager classes.
 *
 * Each MasterService instance has a single ObjectManager that encapsulates
 * the details of object storage and consistency. MasterService knows that
 * tablets and objects exist, but is unaware of the details of their storage -
 * it merely translates RPCs to ObjectManager method calls.
 *
 * ObjectManager is thread-safe. Multiple worker threads in MasterService may
 * call into it simultaneously.
 */
class ObjectManager : public LogEntryHandlers,
                      public AbstractLog::ReferenceFreer {
  public:

    ObjectManager(Context* context, ServerId* serverId,
                const ServerConfig* config,
                TabletManager* tabletManager,
                MasterTableMetadata* masterTableMetadata,
                UnackedRpcResults* unackedRpcResults,
                PreparedOps* preparedOps,
                TxRecoveryManager* txRecoveryManager);
    virtual ~ObjectManager();
    virtual void freeLogEntry(Log::Reference ref);
    void initOnceEnlisted();

    void readHashes(const uint64_t tableId, uint32_t reqNumHashes,
                Buffer* pKHashes, uint32_t initialPKHashesOffset,
                uint32_t maxLength, Buffer* response, uint32_t* respNumHashes,
                uint32_t* numObjects);
    void prefetchHashTableBucket(SegmentIterator* it);
    Status readObject(Key& key, Buffer* outBuffer,
                RejectRules* rejectRules, uint64_t* outVersion,
                bool valueOnly = false);
    Status removeObject(Key& key, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer = NULL,
                RpcResult* rpcResult = NULL, uint64_t* rpcResultPtr = NULL);
    void removeOrphanedObjects();
    void replaySegment(SideLog* sideLog, SegmentIterator& it,
                std::unordered_map<uint64_t, uint64_t>* nextNodeIdMap);
    void replaySegment(SideLog* sideLog, SegmentIterator& it);
    void syncChanges();
    Status writeObject(Object& newObject, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer = NULL,
                RpcResult* rpcResult = NULL, uint64_t* rpcResultPtr = NULL);
    bool keyPointsAtReference(Key& k, AbstractLog::Reference oldReference);
    void writePrepareFail(RpcResult* rpcResult, uint64_t* rpcResultPtr);
    void writeRpcResultOnly(RpcResult* rpcResult, uint64_t* rpcResultPtr);
    Status logTransactionParticipantList(ParticipantList& participantList,
                uint64_t* participantListLogRef);
    Status prepareOp(PreparedOp& newOp, RejectRules* rejectRules,
                uint64_t* newOpPtr, bool* isCommitVote,
                RpcResult* rpcResult, uint64_t* rpcResultPtr);
    Status prepareReadOnly(PreparedOp& newOp, RejectRules* rejectRules,
                bool* isCommitVote);
    Status tryGrabTxLock(Object& objToLock, Log::Reference& ref);
    Status writeTxDecisionRecord(TxDecisionRecord& record);
    Status commitRead(PreparedOp& op, Log::Reference& refToPreparedOp);
    Status commitRemove(PreparedOp& op, Log::Reference& refToPreparedOp,
                        Buffer* removedObjBuffer = NULL);
    Status commitWrite(PreparedOp& op, Log::Reference& refToPreparedOp,
                        Buffer* removedObjBuffer = NULL);

    /**
     * The following three methods are used when multiple log entries
     * need to be committed to the log atomically.
     */

    bool flushEntriesToLog(Buffer *logBuffer, uint32_t& numEntries);
    Status prepareForLog(Object& newObject, Buffer *logBuffer,
                uint32_t* offset, bool *tombstoneAdded);
    Status writeTombstone(Key& key, Buffer *logBuffer);

    /**
     * The following two methods are used by the log cleaner. They aren't
     * intended to be called from any other modules.
     */
    uint32_t getTimestamp(LogEntryType type, Buffer& buffer);
    void relocate(LogEntryType type, Buffer& oldBuffer,
                Log::Reference oldReference, LogEntryRelocator& relocator);

    /**
     * The following methods exist because our current abstraction doesn't quite
     * cut it in terms of hiding object storage information from MasterService.
     * Sometimes MasterService needs to poke at the log, the replica manager, or
     * the object map.
     *
     * If you're considering using these methods, please think twice.
     */
    Log* getLog() { return &log; }
    ReplicaManager* getReplicaManager() { return &replicaManager; }
    HashTable* getObjectMap() { return &objectMap; }

    /**
     * An object of this class must be held by any activity that places
     * tombstones in the hash table temporarily (e.g., anyone who calls
     * replaySegment). While there exist any of these objects, tombstones
     * will not be removed from the hash table; however, once there are no
     * more objects of this class, a background activity will be initiated
     * to remove the tombstones.
     */
    class TombstoneProtector {
      public:
        explicit TombstoneProtector(ObjectManager* objectManager);
        ~TombstoneProtector();

      PRIVATE:
        // Saved copy of the constructor argument.
        ObjectManager* objectManager;

        DISALLOW_COPY_AND_ASSIGN(TombstoneProtector);
    };

  PRIVATE:
    /**
     * An instance of this class locks the bucket of the hash table that a given
     * key maps into. The lock is taken in the constructor and released in the
     * destructor.
     *
     * Taking the lock for a particular key serializes modifications to objects
     * belonging to that key. ObjectManager maintains a number of fine-grained
     * locks to reduce the likelihood of contention between operations on
     * different keys (see ObjectManager::hashTableBucketLocks).
     */
    class HashTableBucketLock {
      public:
        /**
         * This constructor finds the bucket a given key maps to in the hash
         * table and acquires the lock.
         *
         * \param objectManager
         *      The ObjectManager that owns the hash table bucket to lock.
         * \param key
         *      Key whose corresponding bucket in the hash table will be locked.
         */
        HashTableBucketLock(ObjectManager& objectManager, Key& key)
            : lock(NULL)
        {
            uint64_t unused;
            uint64_t bucket = HashTable::findBucketIndex(
                        objectManager.objectMap.getNumBuckets(),
                        key.getHash(), &unused);
            takeBucketLock(objectManager, bucket);
        }

        /**
         * This constructor acquires the lock for a particular bucket index
         * in the hash table.
         *
         * \param objectManager
         *      The ObjectManager that owns the hash table bucket to lock.
         * \param bucket
         *      Index of the hash table bucket to lock.
         */
        HashTableBucketLock(ObjectManager& objectManager, uint64_t bucket)
            : lock(NULL)
        {
            takeBucketLock(objectManager, bucket);
        }

        ~HashTableBucketLock()
        {
            lock->unlock();
        }

      PRIVATE:
        /**
         * Helper method that actually acquires the appropriate bucket lock.
         * Used by both constructors.
         *
         * \param objectManager
         *      The ObjectManager that owns the hash table bucket to lock.
         * \param bucket
         *      Index of the hash table bucket to lock.
         */
        void
        takeBucketLock(ObjectManager& objectManager, uint64_t bucket)
        {
            assert(lock == NULL);
            uint32_t numLocks = arrayLength(objectManager.hashTableBucketLocks);
            assert(BitOps::isPowerOfTwo(numLocks));
            uint64_t lockIndex = bucket & (numLocks - 1);
            lock = &objectManager.hashTableBucketLocks[lockIndex];
            lock->lock();
        }

        /// The hash table bucket spinlock this object acquired in the
        /// constructor and will release in the destructor.
        SpinLock* lock;

        DISALLOW_COPY_AND_ASSIGN(HashTableBucketLock);
    };

    /**
     * Struct used to pass parameters into the removeIfOrphanedObject and
     * removeIfTombstone methods through the generic HashTable::forEachInBucket
     * method.
     */
    struct CleanupParameters {
        /// Pointer to the ObjectManager class owning the hash table.
        ObjectManager* objectManager;

        /// Pointer to the locking object that is keeping the hash table bucket
        /// currently begin iterated thread-safe.
        ObjectManager::HashTableBucketLock* lock;
    };

    /**
     * This object executes in the background (as a WorkerTimer) to remove
     * tombstones that were added to the objectMap by replaySegment().
     */
    class TombstoneRemover : public WorkerTimer {
      public:
        TombstoneRemover(ObjectManager* objectManager,
                        HashTable* objectMap);
        void handleTimerEvent();

      PRIVATE:
        /// Which bucket of #objectMap should be cleaned out next.
        uint64_t currentBucket;

        /// The ObjectManager that owns the hash table to remove tombstones
        /// from in the #recoveryCleanup callback.
        ObjectManager* objectManager;

        /// The hash table to be purged of tombstones.
        HashTable* objectMap;

        friend class TombstoneProtector;

        DISALLOW_COPY_AND_ASSIGN(TombstoneRemover);
    };

    static string dumpSegment(Segment* segment);
    uint32_t getObjectTimestamp(Buffer& buffer);
    uint32_t getTombstoneTimestamp(Buffer& buffer);
    uint32_t getTxDecisionRecordTimestamp(Buffer& buffer);
    bool lookup(HashTableBucketLock& lock, Key& key,
                LogEntryType& outType, Buffer& buffer,
                uint64_t* outVersion = NULL,
                Log::Reference* outReference = NULL,
                HashTable::Candidates* outCandidates = NULL);
    friend void recoveryCleanup(uint64_t maybeTomb, void *cookie);
    bool remove(HashTableBucketLock& lock, Key& key);
    static void removeIfOrphanedObject(uint64_t reference, void *cookie);
    static void removeIfTombstone(uint64_t maybeTomb, void *cookie);
    void removeTombstones();
    Status rejectOperation(const RejectRules* rejectRules, uint64_t version)
                __attribute__((warn_unused_result));
    void relocateObject(Buffer& oldBuffer, Log::Reference oldReference,
                LogEntryRelocator& relocator);
    void relocatePreparedOp(Buffer& oldBuffer, Log::Reference oldReference,
                LogEntryRelocator& relocator);
    void relocatePreparedOpTombstone(Buffer& oldBuffer,
                                     LogEntryRelocator& relocator);
    void relocateRpcResult(Buffer& oldBuffer, LogEntryRelocator& relocator);
    void relocateTombstone(Buffer& oldBuffer, Log::Reference oldReference,
            LogEntryRelocator& relocator);
    void relocateTxDecisionRecord(
            Buffer& oldBuffer, LogEntryRelocator& relocator);
    void relocateTxParticipantList(
            Buffer& oldBuffer, LogEntryRelocator& relocator);
    bool replace(HashTableBucketLock& lock, Key& key, Log::Reference reference);

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * The runtime configuration for the server this ObjectManager is in. Used
     * to pass parameters to various subsystems such as the log, hash table,
     * cleaner, and so on.
     */
    const ServerConfig* config;

    /**
     * The TabletManager keeps track of table hash ranges that belong to this
     * server. ObjectManager uses this information to avoid returning objects
     * that are still in the hash table, but whose tablets are not assigned to
     * the server. This occurs, for instance, during recovery before a tablet's
     * ownership is taken and after a tablet is dropped.
     */
    TabletManager* tabletManager;

    /**
     * Used to update table statistics.
     */
    MasterTableMetadata* masterTableMetadata;

    /**
     * Used to managed cleaning and recovery of RpcResult objects.
     */
    UnackedRpcResults* unackedRpcResults;

    /**
     * Used to manage cleaning and recovery of PreparedOp objects.
     */
    PreparedOps* preparedOps;

    /**
     * Used to managed cleaning and recovery of RpcResult objects.
     */
    TxRecoveryManager* txRecoveryManager;

    /**
     * Allocator used by the SegmentManager to obtain main memory for log
     * segments.
     */
    SegletAllocator allocator;

    /**
     * Creates and tracks replicas of in-memory log segments on remote backups.
     * Its BackupFailureMonitor must be started after the log is created
     * and halted before the log is destroyed.
     */
    ReplicaManager replicaManager;

    /**
     * The SegmentManager manages all segments in the log and interfaces
     * between the log and the cleaner modules.
     */
    SegmentManager segmentManager;

    /**
     * The log stores all of our objects and tombstones both in memory and on
     * backups.
     */
    Log log;

    /**
     * The (table ID, key, keyLength) to #RAMCloud::Object pointer map for all
     * objects stored on this server. Before accessing objects via the hash
     * table, you usually need to check that the tablet still lives on this
     * server; objects from deleted tablets are not immediately purged from the
     * hash table.
     */
    HashTable objectMap;

    /**
     * Used to identify the first write request, so that we can initialize
     * connections to all backups at that time (this is a temporary kludge
     * that needs to be replaced with a better solution).  False means this
     * service has not yet processed any write requests.
     */
    bool anyWrites;

    /**
     * Locks that serialise all object updates (creations, overwrites,
     * deletions, and cleaning relocations) for the same key. This protects
     * regular, parallel RPC operations from one another and from the log
     * cleaner.
     */
    UnnamedSpinLock hashTableBucketLocks[1024];

    /**
     * Locks objects during transactions.
     */
    LockTable lockTable;

    /**
     * Protects access to tombstoneRemover and tombstoneProtectorCount.
     */
    SpinLock mutex;

    /**
     * This object automatically garbage collects tombstones that were added
     * to the hash table during replaySegment() calls.
     */
    TombstoneRemover tombstoneRemover;

    /**
     * Number of TombstoneProtector objects that currently exist for this
     * ObjectsManager.
     */
    int tombstoneProtectorCount;

    friend class CleanerCompactionBenchmark;
    friend class ObjectManagerBenchmark;

    DISALLOW_COPY_AND_ASSIGN(ObjectManager);
};

} // namespace RAMCloud

#endif // RAMCLOUD_OBJECTMANAGER_H
