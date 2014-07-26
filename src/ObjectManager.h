/* Copyright (c) 2014 Stanford University
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
#include "SegmentManager.h"
#include "SegmentIterator.h"
#include "ReplicaManager.h"
#include "ServerConfig.h"
#include "SpinLock.h"
#include "TabletManager.h"
#include "MasterTableMetadata.h"

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
class ObjectManager : public LogEntryHandlers {
  public:

    ObjectManager(Context* context, ServerId* serverId,
                const ServerConfig* config,
                TabletManager* tabletManager,
                MasterTableMetadata* masterTableMetadata);
    virtual ~ObjectManager();
    void initOnceEnlisted();

    void indexedRead(const uint64_t tableId, uint32_t reqNumHashes,
                Buffer* pKHashes, uint32_t initialPKHashesOffset,
                IndexKey::IndexKeyRange* keyRange, uint32_t maxLength,
                Buffer* response, uint32_t* respNumHashes,
                uint32_t* numObjects);
    void prefetchHashTableBucket(SegmentIterator* it);
    Status readObject(Key& key, Buffer* outBuffer,
                RejectRules* rejectRules, uint64_t* outVersion,
                bool valueOnly = false);
    Status removeObject(Key& key, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer = NULL);
    void removeOrphanedObjects();
    void replaySegment(SideLog* sideLog, SegmentIterator& it,
                std::unordered_map<uint64_t, uint64_t>& highestBTreeIdMap);
    void replaySegment(SideLog* sideLog, SegmentIterator& it);
    void syncChanges();
    Status writeObject(Object& newObject, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer = NULL);

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
     * A Dispatch::Poller that lazily removes tombstones that were added to the
     * objectMap during calls to replaySegment(). ObjectManager instantiates one
     * on creation that runs automatically as needed.
     */
    class RemoveTombstonePoller : public Dispatch::Poller {
      public:
        RemoveTombstonePoller(ObjectManager* objectManager,
                        HashTable* objectMap);
        virtual void poll();

      PRIVATE:
        /// Which bucket of #objectMap should be cleaned out next.
        uint64_t currentBucket;

        /// Number of times this tombstone remover has gone through every single
        /// bucket in the objectMap. Used only for logging.
        uint64_t passes;

        /// Count of the number of times ObjectManager::replaySegment() was
        /// called (and completed) at the beginning of the most recent pass
        /// through the hash table. This is used to avoid scanning the hash
        /// table when it does not need to be scanned. That is, when a full
        /// pass through has been done after the last replaySegment call
        /// completed, we can be sure that no more tombstones are left to be
        /// removed.
        uint64_t lastReplaySegmentCount;

        /// The ObjectManager that owns the hash table to remove tombstones
        /// from in the #recoveryCleanup callback.
        ObjectManager* objectManager;

        /// The hash table to be purged of tombstones.
        HashTable* objectMap;

        DISALLOW_COPY_AND_ASSIGN(RemoveTombstonePoller);
    };

    static string dumpSegment(Segment* segment);
    uint32_t getObjectTimestamp(Buffer& buffer);
    uint32_t getTombstoneTimestamp(Buffer& buffer);
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
    void relocateTombstone(Buffer& oldBuffer, LogEntryRelocator& relocator);
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
    SpinLock hashTableBucketLocks[1024];

    /**
     * Number of times the replaySegment() method returned (or threw an
     * exception). This is used by the RemoveTombstonePoller to decide when
     * there may be tombstones in the objectMap that need removal and when it
     * need not scan. This mechanism is simpler than trying to maintain an
     * exact count of outstanding tombstones in the hash table.
     */
    std::atomic<uint64_t> replaySegmentReturnCount;

    /**
     * This object automatically garbage collects tombstones that were added to
     * the hash table during replaySegment() calls. Wrapped in a Tub so that we
     * can construct it after grabbing the Dispatch lock (required by the parent
     * constructor).
     */
    Tub<RemoveTombstonePoller> tombstoneRemover;

    friend class CleanerCompactionBenchmark;
    friend class ObjectManagerBenchmark;

    DISALLOW_COPY_AND_ASSIGN(ObjectManager);
};

} // namespace RAMCloud

#endif // RAMCLOUD_OBJECTMANAGER_H
