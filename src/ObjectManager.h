/* Copyright (c) 2012 Stanford University
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
#include "Object.h"
#include "SegmentManager.h"
#include "SegmentIterator.h"
#include "ReplicaManager.h"
#include "ServerConfig.h"
#include "SpinLock.h"
#include "Table.h"
#include "TabletsOnMaster.h"

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
    ObjectManager(Context* context,
                  ServerId serverId,
                  const ServerConfig* config);
    virtual ~ObjectManager();
    Status readObject(Key& key, Buffer* outBuffer, RejectRules& rejectRules, uint64_t* outVersion = NULL);
    Status writeObject(Key& key, Buffer& value, RejectRules& rejectRules, uint64_t* outVersion = NULL);
    void syncWrites();
    Status removeObject(Key& key, RejectRules& rejectRules);
    void prefetchHashTableBucket(SegmentIterator* it);
    void replaySegment(SideLog* sideLog, SegmentIterator& it);

    TabletsOnMaster* getTable(Key& key) __attribute__((warn_unused_result));
    TabletsOnMaster* getTable(uint64_t tableId, KeyHash keyHash) __attribute__((warn_unused_result));
    ProtoBuf::Tablets::Tablet const* getTablet(uint64_t tableId, KeyHash keyHash) __attribute__((warn_unused_result));

    // should be private?
    void incrementReadAndWriteStatistics(TabletsOnMaster* table);

    /**
     * The following two methods are used by the log cleaner. They aren't
     * intended to be called from any other modules.
     */
    uint32_t getTimestamp(LogEntryType type, Buffer& buffer);
    void relocate(LogEntryType type,
                  Buffer& oldBuffer,
                  LogEntryRelocator& relocator);

    /**
     * Tablets this master owns.
     * The user_data field in each tablet points to a Table object.
     */
    ProtoBuf::Tablets tablets;

  PRIVATE:
    /**
     * An instance of this class locks the bucket of the hash table that a given
     * key maps into. The lock is taken in the constructor and released in the
     * destructor.
     *
     * Taking the lock for a particular key serializes modifications to objects
     * belonging to that key. ObjectManager maintains a number of fine-grained
     * locks to reduce the likelihood of contention between operations on
     * different keys.
     */
    class HashTableBucketLock {
      public:
        HashTableBucketLock(ObjectManager& manager, Key& key)
            : lock(NULL)
        {
            uint32_t numLocks = arrayLength(manager.hashTableBucketLocks);
            assert(BitOps::isPowerOfTwo(numLocks));

            uint64_t unused;
            uint64_t bucket = HashTable::findBucketIndex(
                manager.objectMap.getNumBuckets(), key, &unused);
            uint64_t lockIndex = bucket & (numLocks - 1);

            lock = &manager.hashTableBucketLocks[lockIndex]; 
            lock->lock();
        }

        ~HashTableBucketLock()
        {
            lock->unlock();
        }

      PRIVATE:
        SpinLock* lock;

        DISALLOW_COPY_AND_ASSIGN(HashTableBucketLock);
    };

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
     * Creates and tracks replicas of in-memory log segments on remote backups.
     * Its BackupFailureMonitor must be started after the log is created
     * and halted before the log is destroyed.
     */
// WAR for isReplicaNeeded
public:
    ReplicaManager replicaManager;
PRIVATE:

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
     * The log stores all of our objects and tombstones both in memory and on
     * backups.
     */
// WAR enumeration needing access to this.
public:
    Log log;
PRIVATE:

    /**
     * The (table ID, key, keyLength) to #RAMCloud::Object pointer map for all
     * objects stored on this server. Before accessing objects via the hash
     * table, you usually need to check that the tablet still lives on this
     * server; objects from deleted tablets are not immediately purged from the
     * hash table.
     */
// WAR enumeration needing access to this.
public:
    HashTable objectMap;
PRIVATE:

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
    SpinLock hashTableBucketLocks[1024];

    /**
     * Look up an object in the hash table, then extract the entry from the
     * log. Since tombstones are stored in the hash table during recovery,
     * this method may return either an object or a tombstone.
     *
     * \param key
     *      Key of the object being looked up.
     * \param[out] outType
     *      The type of the log entry is returned here.
     * \param buffer
     *      The entry, if found, is appended to this buffer.
     * \param outReference
     *      The log reference to the entry, if found, is stored in this optional
     *      parameter.
     * \return
     *      True if an entry is found matching the given key, otherwise false.
     */
    bool
    lookup(Key& key,
           LogEntryType& outType,
           Buffer& buffer,
           uint64_t* outVersion = NULL,
           Log::Reference* outReference = NULL)
    {
        HashTable::Candidates candidates;
        objectMap.lookup(key, &candidates);
        while (!candidates.isDone()) {
            Buffer candidateBuffer;
            Log::Reference candidateRef(candidates.getReference());
            LogEntryType type = log.getEntry(candidateRef, candidateBuffer);

            Key candidateKey(type, candidateBuffer);
            if (key == candidateKey) {
                outType = type;
                // TODO: A proper Buffer to Buffer virtual copy method.
                buffer.append(candidateBuffer.getRange(0, candidateBuffer.getTotalLength()),
                              candidateBuffer.getTotalLength());
                if (outVersion != NULL) {
                    if (type == LOG_ENTRY_TYPE_OBJ) {
                        Object o(candidateBuffer);
                        *outVersion = o.getVersion();
                    } else {
                        ObjectTombstone o(candidateBuffer);
                        *outVersion = o.getObjectVersion();
                    }
                }
                if (outReference != NULL)
                    *outReference = candidateRef;
                return true;
            }

            candidates.next();
        }

        return false;
    }

    bool
    remove(Key& key)
    {
        HashTable::Candidates candidates;
        objectMap.lookup(key, &candidates);
        while (!candidates.isDone()) {
            Buffer buffer;
            Log::Reference candidateRef(candidates.getReference());
            LogEntryType type = log.getEntry(candidateRef, buffer);
            Key candidateKey(type, buffer);
            if (key == candidateKey) {
                candidates.remove();
                return true;
            }
            candidates.next();
        }
        return false;
    }

    bool
    replace(Key& key, Log::Reference reference)
    {
        HashTable::Candidates candidates;
        objectMap.lookup(key, &candidates);
        while (!candidates.isDone()) {
            Buffer buffer;
            Log::Reference candidateRef(candidates.getReference());
            LogEntryType type = log.getEntry(candidateRef, buffer);
            Key candidateKey(type, buffer);
            if (key == candidateKey) {
                candidates.setReference(reference.toInteger());
                return true;
            }
            candidates.next();
        }

        objectMap.insert(key, reference.toInteger());
        return false;
    }

    friend void recoveryCleanup(uint64_t maybeTomb, void *cookie);
    friend void removeObjectIfFromUnknownTablet(uint64_t reference,
                                                void *cookie);

    uint32_t getObjectTimestamp(Buffer& buffer);
    uint32_t getTombstoneTimestamp(Buffer& buffer);
    Status rejectOperation(const RejectRules& rejectRules, uint64_t version)
        __attribute__((warn_unused_result));
    void relocateObject(Buffer& oldBuffer,
                        LogEntryRelocator& relocator);
    void relocateTombstone(Buffer& oldBuffer,
                           LogEntryRelocator& relocator);
    void removeTombstones();

    DISALLOW_COPY_AND_ASSIGN(ObjectManager);
};

} // namespace RAMCloud

#endif // RAMCLOUD_OBJECTMANAGER_H
