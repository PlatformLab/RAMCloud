/* Copyright (c) 2012-2014 Stanford University
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

#include "Buffer.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "Enumeration.h"
#include "EnumerationIterator.h"
#include "IndexletManager.h"
#include "LogEntryRelocator.h"
#include "ObjectManager.h"
#include "Object.h"
#include "PerfStats.h"
#include "ShortMacros.h"
#include "RawMetrics.h"
#include "Tub.h"
#include "ProtoBuf.h"
#include "Segment.h"
#include "TimeTrace.h"
#include "Transport.h"
#include "WallTime.h"

namespace RAMCloud {

/**
 * Construct an ObjectManager.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param serverId
 *      Pointer to the ServerId of the master server that is instantiating this
 *      object manager. The ServerId pointed to need not be valid when this
 *      constructor is invoked, but it must be valid before any other methods
 *      are called.
 * \param config
 *      Contains various parameters that configure the operation of this server.
 * \param tabletManager
 *      Pointer to the master's TabletManager instance. This defines which
 *      tablets are owned by the master and affects which objects can we read
 *      from this ObjectManager. For example, if an object is written and its
 *      tablet is deleted before the object is removed, reads on that object
 *      will fail because the tablet is no longer owned by the master.
 * \param masterTableMetadata
 *      Pointer to the master's MasterTableMetadata instance.  This keeps track
 *      of per table metadata located on this master.
 */
ObjectManager::ObjectManager(Context* context, ServerId* serverId,
                const ServerConfig* config,
                TabletManager* tabletManager,
                MasterTableMetadata* masterTableMetadata)
    : context(context)
    , config(config)
    , tabletManager(tabletManager)
    , masterTableMetadata(masterTableMetadata)
    , allocator(config)
    , replicaManager(context, serverId,
                     config->master.numReplicas,
                     config->master.useMinCopysets,
                     config->master.allowLocalBackup)
    , segmentManager(context, config, serverId,
                     allocator, replicaManager, masterTableMetadata)
    , log(context, config, this, &segmentManager, &replicaManager)
    , objectMap(config->master.hashTableBytes / HashTable::bytesPerCacheLine())
    , anyWrites(false)
    , hashTableBucketLocks()
    , replaySegmentReturnCount(0)
    , tombstoneRemover()
{
    for (size_t i = 0; i < arrayLength(hashTableBucketLocks); i++)
        hashTableBucketLocks[i].setName("hashTableBucketLock");
}

/**
 * The destructor does nothing particularly interesting right now.
 */
ObjectManager::~ObjectManager()
{
    replicaManager.haltFailureMonitor();
}

/**
 * Perform any initialization that needed to wait until after the server has
 * enlisted. This must be called only once.
 *
 * Any actions performed here must not block the process or dispatch thread,
 * otherwise the server may be timed out and declared failed by the coordinator.
 */
void
ObjectManager::initOnceEnlisted()
{
    assert(!tombstoneRemover);

    replicaManager.startFailureMonitor();

    if (!config->master.disableLogCleaner)
        log.enableCleaner();

    Dispatch::Lock lock(context->dispatch);
    tombstoneRemover.construct(this, &objectMap);
}

/**
 * Read object(s) matching the given parameters, previously written by
 * ObjectManager.
 *
 * We'd typically expect one object to match one primary key hash, but it is
 * possible that zero (if that object was removed after client did
 * lookupIndexKeys() preceding this call), one, or multiple (if there are
 * multiple objects with the same primary key hash that also have index
 * keys in the current read range) objects match.
 * 
 * \param tableId
 *      Id of the table containing the object(s).
 * \param reqNumHashes
 *      Number of key hashes (see pKHashes) in the request.
 * \param pKHashes
 *      Key hashes of the primary keys of the object(s).
 * \param initialPKHashesOffset
 *      Offset indicating location of first key hash in the pKHashes buffer.
 * \param keyRange
 *      IndexKeyRange that will be used to compare the object's index key
 *      to determine whether it is a match.
 * \param maxLength
 *      Maximum length of response that can be appended to the response
 *      buffer.
 *
 * \param[out] response
 *      Buffer to which response for each object will be appended in the
 *      readNext() method.
 * \param[out] respNumHashes
 *      Num of hashes corresponding to which objects are being returned.
 * \param[out] numObjects
 *      Num of objects being returned.
 */
void
ObjectManager::indexedRead(const uint64_t tableId, uint32_t reqNumHashes,
            Buffer* pKHashes, uint32_t initialPKHashesOffset,
            IndexKey::IndexKeyRange* keyRange, uint32_t maxLength,
            Buffer* response, uint32_t* respNumHashes, uint32_t* numObjects)
{
    // The current length of the response buffer in bytes. This is the
    // cumulative length of all the objects that have been appended to response
    // till now.
    // This length should be less than maxLength before returning to the client.
    uint32_t currentLength = 0;
    // The length for the data corresponding to the object that was just
    // appended to the response buffer.
    uint32_t partLength = 0;
    // The primary key hash being processed (that is, for which the objects
    // are being looked up) in the current iteration of the loop below.
    uint64_t pKHash;
    // Offset into pKHashes buffer where the next pKHash to be processed
    // is available.
    uint32_t pKHashesOffset = initialPKHashesOffset;

    *numObjects = 0;

    for (*respNumHashes = 0; *respNumHashes < reqNumHashes;
            *respNumHashes += 1) {

        pKHash = *(pKHashes->getOffset<uint64_t>(pKHashesOffset));
        pKHashesOffset += sizeof32(pKHash);

        // Instead of calling a private lookup function as in a "normal" read,
        // doing the work here directly, since the abstraction breaks down
        // as multiple objects having the same primary key hash may match
        // the index key range.
        objectMap.prefetchBucket(pKHash);
        HashTableBucketLock lock(*this, pKHash);

        // If the tablet doesn't exist in the NORMAL state,
        // we must plead ignorance.
        // Client can refresh its tablet information and retry.
        TabletManager::Tablet tablet;
        if (!tabletManager->getTablet(tableId, pKHash, &tablet))
            return;
        if (tablet.state != TabletManager::NORMAL)
            return;

        HashTable::Candidates candidates;
        objectMap.lookup(pKHash, candidates);

        while (!candidates.isDone()) {
            Buffer candidateBuffer;
            Log::Reference candidateRef(candidates.getReference());
            LogEntryType type = log.getEntry(candidateRef, candidateBuffer);

            if (type != LOG_ENTRY_TYPE_OBJ)
                continue;

            Object object(candidateBuffer);
            bool isInRange = IndexKey::isKeyInRange(&object, keyRange);

            if (isInRange == true) {
                *numObjects += 1;
                response->emplaceAppend<uint64_t>(object.getVersion());
                response->emplaceAppend<uint32_t>(
                        object.getKeysAndValueLength());
                object.appendKeysAndValueToBuffer(*response);

                KeyLength pKLength;
                const void* pKString = object.getKey(1, &pKLength);
                Key pK(tableId, pKString, pKLength);
                tabletManager->incrementReadCount(pK);
                ++PerfStats::threadStats.readCount;
            }

            candidates.next();
        }

        partLength = response->size() - currentLength;
        if (currentLength > maxLength) {
            response->truncate(response->size() - partLength);
            break;
        }
        currentLength += partLength;
    }
}

/**
 * This method is used by replaySegment() to prefetch the hash table bucket
 * corresponding to the next entry to be replayed. Doing so avoids a cache
 * miss for subsequent hash table lookups and significantly speeds up replay.
 *
 * \param it
 *      SegmentIterator to use for prefetching. Whatever is currently pointed
 *      to by this iterator will be used to prefetch, if possible. Some entries
 *      do not contain keys; they are safely ignored.
 */
inline void
ObjectManager::prefetchHashTableBucket(SegmentIterator* it)
{
    if (expect_false(it->isDone()))
        return;

    if (expect_true(it->getType() == LOG_ENTRY_TYPE_OBJ)) {
        const Object::Header* obj =
            it->getContiguous<Object::Header>(NULL, 0);

        Object prefetchObj(obj, it->getLength());
        KeyLength primaryKeyLen = 0;
        const void *primaryKey = prefetchObj.getKey(0, &primaryKeyLen);

        Key key(obj->tableId, primaryKey, primaryKeyLen);
        objectMap.prefetchBucket(key.getHash());
    } else if (it->getType() == LOG_ENTRY_TYPE_OBJTOMB) {
        const ObjectTombstone::Header* tomb =
            it->getContiguous<ObjectTombstone::Header>(NULL, 0);
        Key key(tomb->tableId, tomb->key,
            downCast<uint16_t>(it->getLength() - sizeof32(*tomb)));
        objectMap.prefetchBucket(key.getHash());
    }
}

/**
 * Read an object previously written to this ObjectManager.
 *
 * \param key
 *      Key of the object being read.
 * \param outBuffer
 *      Buffer to populate with the value of the object, if found.
 * \param rejectRules
 *      If non-NULL, use the specified rules to perform a conditional read. See
 *      the RejectRules class documentation for more details.
 * \param outVersion
 *      If non-NULL and the object is found, the version is returned here. If
 *      the reject rules failed the read, the current object's version is still
 *      returned.
 * \param valueOnly
 *      If true, then only the value portion of the object is written to
 *      outBuffer. Otherwise, keys and value are written to outBuffer.
 * \return
 *      Returns STATUS_OK if the lookup succeeded and the reject rules did not
 *      preclude this read. Other status values indicate different failures
 *      (object not found, tablet doesn't exist, reject rules applied, etc).
 */
Status
ObjectManager::readObject(Key& key, Buffer* outBuffer,
                RejectRules* rejectRules, uint64_t* outVersion,
                bool valueOnly)
{
    objectMap.prefetchBucket(key.getHash());
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    if (!tabletManager->checkAndIncrementReadCount(key))
        return STATUS_UNKNOWN_TABLET;

    Buffer buffer;
    LogEntryType type;
    uint64_t version;
    Log::Reference reference;
    bool found = lookup(lock, key, type, buffer, &version, &reference);
    if (!found || type != LOG_ENTRY_TYPE_OBJ)
        return STATUS_OBJECT_DOESNT_EXIST;

    if (outVersion != NULL)
        *outVersion = version;

    if (rejectRules != NULL) {
        Status status = rejectOperation(rejectRules, version);
        if (status != STATUS_OK)
            return status;
    }

    Object object(buffer);
    if (valueOnly) {
        uint16_t valueOffset = 0;
        object.getValueOffset(&valueOffset);
        object.appendValueToBuffer(outBuffer, valueOffset);
    } else {
        object.appendKeysAndValueToBuffer(*outBuffer);
    }
    ++PerfStats::threadStats.readCount;

    return STATUS_OK;
}

/**
 * Remove an object previously written to this ObjectManager.
 *
 * Note that just like writeObject(), this operation will not be stably commited
 * to backups until the syncChanges() method is called. This allows many remove
 * operations to be batched (to support, for example, the multiRemove RPC). If
 * this method returns anything other than STATUS_OK, there were no changes made
 * and the caller need not sync.
 *
 * \param key
 *      Key of the object to remove.
 * \param rejectRules
 *      If non-NULL, use the specified rules to perform a conditional remove.
 *      See the RejectRules class documentation for more details.
 *
 * \param[out] outVersion
 *      If non-NULL, the version of the current object version is returned here.
 *      Unless rejectRules prevented the operation, this object will have been
 *      deleted. If the rejectRules did prevent removal, the current object's
 *      version is still returned.
 * \param[out] removedObjBuffer
 *      If non-NULL, pointer to the buffer in log for the object being removed
 *      is returned.
 * \return
 *      Returns STATUS_OK if the remove succeeded. Other status values indicate
 *      different failures (tablet doesn't exist, reject rules applied, etc).
 */
Status
ObjectManager::removeObject(Key& key, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer)
{
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    LogEntryType type;
    Buffer buffer;
    Log::Reference reference;
    if (!lookup(lock, key, type, buffer, NULL, &reference) ||
            type != LOG_ENTRY_TYPE_OBJ) {
        static RejectRules defaultRejectRules;
        if (rejectRules == NULL)
            rejectRules = &defaultRejectRules;
        return rejectOperation(rejectRules, VERSION_NONEXISTENT);
    }

    Object object(buffer);
    if (outVersion != NULL)
        *outVersion = object.getVersion();

    // Abort if we're trying to delete the wrong version.
    if (rejectRules != NULL) {
        Status status = rejectOperation(rejectRules, object.getVersion());
        if (status != STATUS_OK)
            return status;
    }

    // Return a pointer to the buffer in log for the object being removed.
    if (removedObjBuffer != NULL) {
        removedObjBuffer->appendExternal(&buffer);
    }

    ObjectTombstone tombstone(object,
                              log.getSegmentId(reference),
                              WallTime::secondsTimestamp());
    Buffer tombstoneBuffer;
    tombstone.assembleForLog(tombstoneBuffer);

    // Write the tombstone into the Log, increment the tablet version
    // number, and remove from the hash table.
    if (!log.append(LOG_ENTRY_TYPE_OBJTOMB, tombstoneBuffer)) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_RETRY;
    }

    TableStats::increment(masterTableMetadata,
                          tablet.tableId,
                          tombstoneBuffer.size(),
                          1);
    segmentManager.raiseSafeVersion(object.getVersion() + 1);
    log.free(reference);
    remove(lock, key);
    return STATUS_OK;
}

/**
 * Scan the hashtable and remove all objects that do not belong to a
 * tablet currently owned by this master. Used to clean up any objects
 * created as part of an aborted recovery.
 */
void
ObjectManager::removeOrphanedObjects()
{
    for (uint64_t i = 0; i < objectMap.getNumBuckets(); i++) {
        HashTableBucketLock lock(*this, i);
        CleanupParameters params = { this , &lock };
        objectMap.forEachInBucket(removeIfOrphanedObject, &params, i);
    }
}

/**
 * This class is used by replaySegment to increment the number of times that
 * that method returns, regardless of the return path. That counter is used
 * by the RemoveTombstonePoller class to know when it need not bother scanning
 * the hash table.
 */
template<typename T>
class DelayedIncrementer {
  public:
    /**
     * \param incrementee
     *      Pointer to the object to increment when the destructor is called.
     */
    explicit DelayedIncrementer(T* incrementee)
        : incrementee(incrementee)
    {
    }

    /**
     * Destroy this object and increment the incrementee.
     */
    ~DelayedIncrementer()
    {
        (*incrementee)++;
    }

  PRIVATE:
    /// Pointer to the object that will be incremented in the destructor.
    T* incrementee;

    DISALLOW_COPY_AND_ASSIGN(DelayedIncrementer);
};

/**
 * A wrapper function for replaySegment
 *
 * \param sideLog
 *      Pointer to the SideLog in which replayed data will be stored.
 * \param it
 *       SegmentIterator which is pointing to the start of the recovery segment
 *       to be replayed into the log.
 */
void
ObjectManager::replaySegment(SideLog* sideLog, SegmentIterator& it)
{
  std::unordered_map<uint64_t, uint64_t> highestBTreeIdMap;
  replaySegment(sideLog, it, highestBTreeIdMap);
}

/**
 * Replay the entries within a segment and store the appropriate objects.
 * This method is used during recovery to replay a portion of a failed
 * master's log. It is also used during tablet migration to receive objects
 * from another master.
 *
 * To support out-of-order replay (necessary for performance), ObjectManager
 * will keep track of tombstones during replay and remove any older objects
 * encountered to maintain delete consistency.
 *
 * Objects being replayed should belong to existing tablets in the RECOVERING
 * state. ObjectManager uses the state of the tablets to determine when it is
 * safe to prune tombstones created during replaySegment calls. In particular,
 * tombstones referring to unknown tablets or to tablets not in the RECOVERING
 * state will be pruned. The caller should ensure that when replaying objects
 * for a particular tablet, the tablet already exists in the RECOVERING state
 * before the first invocation of replaySegment() and that the state is changed
 * (or the tablet is dropped) after the last call.
 *
 * \param sideLog
 *      Pointer to the SideLog in which replayed data will be stored.
 * \param it
 *       SegmentIterator which is pointing to the start of the recovery segment
 *       to be replayed into the log.
 * \param highestBTreeIdMap
 *       A unordered map that keeps track of the highest used BTree ID in
 *       each indexlet table.
 */
void
ObjectManager::replaySegment(SideLog* sideLog, SegmentIterator& it,
    std::unordered_map<uint64_t, uint64_t>& highestBTreeIdMap)
{
    uint64_t startReplicationTicks = metrics->master.replicaManagerTicks;
    uint64_t startReplicationPostingWriteRpcTicks =
        metrics->master.replicationPostingWriteRpcTicks;
    CycleCounter<RawMetric> _(&metrics->master.recoverSegmentTicks);

    // Metrics can be very expense (they're atomic operations), so we aggregate
    // as much as we can in local variables and update the counters once at the
    // end of this method.
    uint64_t verifyChecksumTicks = 0;
    uint64_t segmentAppendTicks = 0;
    uint64_t recoverySegmentEntryCount = 0;
    uint64_t recoverySegmentEntryBytes = 0;
    uint64_t objectAppendCount = 0;
    uint64_t tombstoneAppendCount = 0;
    uint64_t liveObjectCount = 0;
    uint64_t liveObjectBytes = 0;
    uint64_t objectDiscardCount = 0;
    uint64_t tombstoneDiscardCount = 0;
    uint64_t safeVersionRecoveryCount = 0;
    uint64_t safeVersionNonRecoveryCount = 0;

    // Keep track of the number of times this method returns (or throws). See
    // RemoveTombstonePoller for how this count is used.
    DelayedIncrementer<std::atomic<uint64_t>>
        returnCountIncrementer(&replaySegmentReturnCount);

    SegmentIterator prefetcher = it;
    prefetcher.next();

    uint64_t bytesIterated = 0;
    while (expect_true(!it.isDone())) {
        prefetchHashTableBucket(&prefetcher);
        prefetcher.next();

        LogEntryType type = it.getType();

        if (bytesIterated > 50000) {
            bytesIterated = 0;
            replicaManager.proceed();
        }
        bytesIterated += it.getLength();

        recoverySegmentEntryCount++;
        recoverySegmentEntryBytes += it.getLength();

        if (expect_true(type == LOG_ENTRY_TYPE_OBJ)) {
            // The recovery segment is guaranteed to be contiguous, so we need
            // not provide a copyout buffer.

            const Object::Header* recoveryObj =
                it.getContiguous<Object::Header>(NULL, 0);

            Object replayObj(recoveryObj, it.getLength());
            KeyLength primaryKeyLen = 0;
            const void *primaryKey = replayObj.getKey(0, &primaryKeyLen);

            Key key(recoveryObj->tableId, primaryKey, primaryKeyLen);

            // If table is an BTree table,i.e., tableId exists in
            // highestBTreeIdMap, update highestBTreeId of its table.
            std::unordered_map<uint64_t, uint64_t>::iterator iter
                = highestBTreeIdMap.find(recoveryObj->tableId);
            if (iter != highestBTreeIdMap.end()) {
                const uint64_t *bTreeKey =
                    reinterpret_cast<const uint64_t*>(primaryKey);
                uint64_t bTreeId = *bTreeKey;
                if (bTreeId > iter->second)
                    iter->second = bTreeId;
            }

            bool checksumIsValid = ({
                CycleCounter<uint64_t> c(&verifyChecksumTicks);
                Object::computeChecksum(recoveryObj, it.getLength()) ==
                    recoveryObj->checksum;
            });
            if (expect_false(!checksumIsValid)) {
                LOG(WARNING, "bad object checksum! key: %s, version: %lu",
                    key.toString().c_str(), recoveryObj->version);
                // TODO(Stutsman): Should throw and try another segment replica?
            }

            HashTableBucketLock lock(*this, key);

            uint64_t minSuccessor = 0;
            bool freeCurrentEntry = false;

            LogEntryType currentType;
            Buffer currentBuffer;
            Log::Reference currentReference;
            if (lookup(lock, key, currentType, currentBuffer, 0,
                                                        &currentReference)) {
                uint64_t currentVersion;

                if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
                    ObjectTombstone currentTombstone(currentBuffer);
                    currentVersion = currentTombstone.getObjectVersion();
                } else {
                    Object currentObject(currentBuffer);
                    currentVersion = currentObject.getVersion();
                    freeCurrentEntry = true;
                }

                minSuccessor = currentVersion + 1;
            }

            if (recoveryObj->version >= minSuccessor) {
                // write to log (with lazy backup flush) & update hash table
                Log::Reference newObjReference;
                {
                    CycleCounter<uint64_t> _(&segmentAppendTicks);
                    sideLog->append(LOG_ENTRY_TYPE_OBJ,
                                    recoveryObj,
                                    it.getLength(),
                                    &newObjReference);
                    TableStats::increment(masterTableMetadata,
                                          key.getTableId(),
                                          it.getLength(),
                                          1);
                }

                // TODO(steve/ryan): what happens if the log is full? won't an
                //      exception here just cause the master to try another
                //      backup?

                objectAppendCount++;
                liveObjectBytes += it.getLength();

                replace(lock, key, newObjReference);

                // nuke the old object, if it existed
                // TODO(steve): put tombstones in the HT and have this free them
                //              as well
                if (freeCurrentEntry) {
                    liveObjectBytes -= currentBuffer.size();
                    sideLog->free(currentReference);
                } else {
                    liveObjectCount++;
                }
            } else {
                objectDiscardCount++;
            }
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            Key key(type, buffer);

            // If table is an BTree table,i.e., tableId exists in
            // highestBTreeIdMap, update highestBTreeId of its table.
            std::unordered_map<uint64_t, uint64_t>::iterator iter
                = highestBTreeIdMap.find(key.getTableId());
            if (iter != highestBTreeIdMap.end()) {
                const uint64_t *bTreeKey =
                    reinterpret_cast<const uint64_t*>(key.getStringKey());
                uint64_t bTreeId = *bTreeKey;
                if (bTreeId > iter->second)
                    iter->second = bTreeId;
            }

            ObjectTombstone recoverTomb(buffer);
            bool checksumIsValid = ({
                CycleCounter<uint64_t> c(&verifyChecksumTicks);
                recoverTomb.checkIntegrity();
            });
            if (expect_false(!checksumIsValid)) {
                LOG(WARNING, "bad tombstone checksum! key: %s, version: %lu",
                    key.toString().c_str(), recoverTomb.getObjectVersion());
                // TODO(Stutsman): Should throw and try another segment replica?
            }

            HashTableBucketLock lock(*this, key);

            uint64_t minSuccessor = 0;
            bool freeCurrentEntry = false;

            LogEntryType currentType;
            Buffer currentBuffer;
            Log::Reference currentReference;
            if (lookup(lock, key, currentType, currentBuffer, 0,
                                                        &currentReference)) {
                if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
                    ObjectTombstone currentTombstone(currentBuffer);
                    minSuccessor = currentTombstone.getObjectVersion() + 1;
                } else {
                    Object currentObject(currentBuffer);
                    minSuccessor = currentObject.getVersion();
                    freeCurrentEntry = true;
                }
            }

            if (recoverTomb.getObjectVersion() >= minSuccessor) {
                tombstoneAppendCount++;
                Log::Reference newTombReference;
                {
                    CycleCounter<uint64_t> _(&segmentAppendTicks);
                    sideLog->append(LOG_ENTRY_TYPE_OBJTOMB,
                                    buffer,
                                    &newTombReference);
                    TableStats::increment(masterTableMetadata,
                                          key.getTableId(),
                                          buffer.size(),
                                          1);
                }

                // TODO(steve/ryan): append could fail here!

                replace(lock, key, newTombReference);

                // nuke the object, if it existed
                if (freeCurrentEntry) {
                    liveObjectCount++;
                    liveObjectBytes -= currentBuffer.size();
                    sideLog->free(currentReference);
                }
            } else {
                tombstoneDiscardCount++;
            }
        } else if (type == LOG_ENTRY_TYPE_SAFEVERSION) {
            // LOG_ENTRY_TYPE_SAFEVERSION is duplicated to all the
            // partitions in BackupService::buildRecoverySegments()
            Buffer buffer;
            it.appendToBuffer(buffer);

            ObjectSafeVersion recoverSafeVer(buffer);
            uint64_t safeVersion = recoverSafeVer.getSafeVersion();

            bool checksumIsValid = ({
                CycleCounter<uint64_t> _(&verifyChecksumTicks);
                recoverSafeVer.checkIntegrity();
            });
            if (expect_false(!checksumIsValid)) {
                LOG(WARNING, "bad objectSafeVer checksum! version: %lu",
                    safeVersion);
                // TODO(Stutsman): Should throw and try another segment replica?
            }

            // Copy SafeVerObject to the recovery segment.
            // Sync can be delayed, because recovery can be replayed
            // with the same backup data when the recovery crashes on the way.
            {
                CycleCounter<uint64_t> _(&segmentAppendTicks);
                sideLog->append(LOG_ENTRY_TYPE_SAFEVERSION, buffer);
            }

            // recover segmentManager.safeVersion (Master safeVersion)
            if (segmentManager.raiseSafeVersion(safeVersion)) {
                // true if log.safeVersion is revised.
                safeVersionRecoveryCount++;
                LOG(DEBUG, "SAFEVERSION %lu recovered", safeVersion);
            } else {
                safeVersionNonRecoveryCount++;
                LOG(DEBUG, "SAFEVERSION %lu discarded", safeVersion);
            }
        }

        it.next();
    }

    metrics->master.backupInRecoverTicks +=
        metrics->master.replicaManagerTicks - startReplicationTicks;
    metrics->master.recoverSegmentPostingWriteRpcTicks +=
        metrics->master.replicationPostingWriteRpcTicks -
        startReplicationPostingWriteRpcTicks;
    metrics->master.verifyChecksumTicks += verifyChecksumTicks;
    metrics->master.segmentAppendTicks += segmentAppendTicks;
    metrics->master.recoverySegmentEntryCount += recoverySegmentEntryCount;
    metrics->master.recoverySegmentEntryBytes += recoverySegmentEntryBytes;
    metrics->master.objectAppendCount += objectAppendCount;
    metrics->master.tombstoneAppendCount += tombstoneAppendCount;
    metrics->master.liveObjectCount += liveObjectCount;
    metrics->master.liveObjectBytes += liveObjectBytes;
    metrics->master.objectDiscardCount += objectDiscardCount;
    metrics->master.tombstoneDiscardCount += tombstoneDiscardCount;
    metrics->master.safeVersionRecoveryCount += safeVersionRecoveryCount;
    metrics->master.safeVersionNonRecoveryCount += safeVersionNonRecoveryCount;
}

/**
 * Sync any previous writes or removes. This operation is required after any
 * writeObject() or removeObject() invocation if the caller wants to ensure that
 * the change is committed to stable storage. Prior to invoking this, no
 * guarantees are made about the consistency of backup and master views of the
 * log since the previous syncChanges() operation.
 */
void
ObjectManager::syncChanges()
{
    log.sync();
}

/**
 * Write an object to this ObjectManager, replacing a previous one if necessary.
 *
 * This method will do everything needed to store an object associated with
 * a particular key. This includes allocating or incrementing version numbers,
 * writing a tombstone if a previous version exists, storing to the log,
 * and adding or replacing an entry in the hash table.
 *
 * Note, however, that the write is not be guaranteed to have completed on
 * backups until the syncChanges() method is called. This allows callers to
 * issue multiple object writes and batch backup writes by syncing once per
 * batch, rather than for each object. If this method returns anything other
 * than STATUS_OK, there were no changes made and the caller need not sync.
 *
 * \param newObject
 *      The new object to be written to the log. The object does not have
 *      a valid version and timestamp. So this function will update the version,
 *      timestamp and the checksum of the object before writing to the log.
 * \param rejectRules
 *      Specifies conditions under which the write should be aborted with an
 *      error. May be NULL if no special reject conditions are desired.
 *
 * \param[out] outVersion
 *      If non-NULL, the version number of the new object is returned here. If
 *      the operation was successful this will be the new version for the
 *      object; if this object has ever existed previously the new version is
 *      guaranteed to be greater than any previous version of the object. If the
 *      operation failed then the version number returned is the current version
 *      of the object, or VERSION_NONEXISTENT if the object does not exist.
 * \param[out] removedObjBuffer
 *      If non-NULL, pointer to the buffer in log for the object being removed
 *      is returned.
 * \return
 *      STATUS_OK if the object was written. Otherwise, for example,
 *      STATUS_UKNOWN_TABLE may be returned.
 */
Status
ObjectManager::writeObject(Object& newObject, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer)
{
    if (!anyWrites) {
        // This is the first write; use this as a trigger to update the
        // cluster configuration information and open a session with each
        // backup, so it won't slow down recovery benchmarks.  This is a
        // temporary hack, and needs to be replaced with a more robust
        // approach to updating cluster configuration information.
        anyWrites = true;

        // Empty coordinator locator means we're in test mode, so skip this.
        if (!context->coordinatorSession->getLocation().empty()) {
            ProtoBuf::ServerList backups;
            CoordinatorClient::getBackupList(context, &backups);
            TransportManager& transportManager =
                *context->transportManager;
            foreach(auto& backup, backups.server())
                transportManager.getSession(backup.service_locator());
        }
    }

    uint16_t keyLength = 0;
    const void *keyString = newObject.getKey(0, &keyLength);
    Key key(newObject.getTableId(), keyString, keyLength);

    objectMap.prefetchBucket(key.getHash());
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    LogEntryType currentType = LOG_ENTRY_TYPE_INVALID;
    Buffer currentBuffer;
    Log::Reference currentReference;
    uint64_t currentVersion = VERSION_NONEXISTENT;

    HashTable::Candidates currentHashTableEntry;

    if (lookup(lock, key, currentType, currentBuffer, 0,
               &currentReference, &currentHashTableEntry)) {
        if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
            removeIfTombstone(currentReference.toInteger(), this);
        } else {
            Object currentObject(currentBuffer);
            currentVersion = currentObject.getVersion();
            // Return a pointer to the buffer in log for the object being
            // overwritten.
            if (removedObjBuffer != NULL) {
                removedObjBuffer->appendExternal(&currentBuffer);
            }
        }
    }

    if (rejectRules != NULL) {
        Status status = rejectOperation(rejectRules, currentVersion);
        if (status != STATUS_OK) {
            if (outVersion != NULL)
                *outVersion = currentVersion;
            return status;
        }
    }

    // Existing objects get a bump in version, new objects start from
    // the next version allocated in the table.
    uint64_t newObjectVersion = (currentVersion == VERSION_NONEXISTENT) ?
            segmentManager.allocateVersion() : currentVersion + 1;

    newObject.setVersion(newObjectVersion);
    newObject.setTimestamp(WallTime::secondsTimestamp());

    assert(currentVersion == VERSION_NONEXISTENT ||
           newObject.getVersion() > currentVersion);

    Tub<ObjectTombstone> tombstone;
    if (currentVersion != VERSION_NONEXISTENT &&
      currentType == LOG_ENTRY_TYPE_OBJ) {
        Object object(currentBuffer);
        tombstone.construct(object,
                            log.getSegmentId(currentReference),
                            WallTime::secondsTimestamp());
    }

    // Create a vector of appends in case we need to write a tombstone and
    // an object. This is necessary to ensure that both tombstone and object
    // are written atomically. The log makes no atomicity guarantees across
    // multiple append calls and we don't want a tombstone going to backups
    // before the new object, or the new object going out without a tombstone
    // for the old deleted version. Both cases lead to consistency problems.
    Log::AppendVector appends[2];

    newObject.assembleForLog(appends[0].buffer);
    appends[0].type = LOG_ENTRY_TYPE_OBJ;

    if (tombstone) {
        tombstone->assembleForLog(appends[1].buffer);
        appends[1].type = LOG_ENTRY_TYPE_OBJTOMB;
    }

    if (!log.append(appends, tombstone ? 2 : 1)) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_RETRY;
    }

    if (tombstone) {
        currentHashTableEntry.setReference(appends[0].reference.toInteger());
        log.free(currentReference);
    } else {
        objectMap.insert(key.getHash(), appends[0].reference.toInteger());
    }

    if (outVersion != NULL)
        *outVersion = newObject.getVersion();

    tabletManager->incrementWriteCount(key);
    ++PerfStats::threadStats.writeCount;

    TEST_LOG("object: %u bytes, version %lu",
        appends[0].buffer.size(), newObject.getVersion());

    if (tombstone) {
        TEST_LOG("tombstone: %u bytes, version %lu",
            appends[1].buffer.size(), tombstone->getObjectVersion());
    }

    {
        uint64_t byteCount = appends[0].buffer.size();
        uint64_t recordCount = 1;
        if (tombstone) {
            byteCount += appends[1].buffer.size();
            recordCount += 1;
        }

        TableStats::increment(masterTableMetadata,
                              tablet.tableId,
                              byteCount,
                              recordCount);
    }

    return STATUS_OK;
}

/**
 * Flushes all the log entries from the given buffer to the log
 * atomically and updates the hash table with the corresponding
 * log reference for each entry
 *
 * \param logBuffer
 *      The buffer which contains various log entries
 * \param numEntries
 *      Number of log entries in the buffer
 * \return
 *      True, if successful, false otherwise.
 */
bool
ObjectManager::flushEntriesToLog(Buffer *logBuffer, uint32_t& numEntries)
{
    if (numEntries == 0)
        return true;

    // This array will hold the references of all the entries
    // that get written to the log. This will be used
    // subsequently to update the hash table
    Log::Reference references[numEntries];

    // atomically flush all the entries to the log
    if (!log.append(logBuffer, references, numEntries)) {
        return false;
        // TODO(arjung): how to propagate this error
        // into the tree code and then back to the caller
        // of the tree code ??
    }

    LogEntryType type;

    // keeps track of the current log entry header in the logBuffer
    uint32_t offset = 0;
    uint32_t entryLength = 0;
    // marks the starting offset of a serialized object in the logBuffer
    uint32_t objectOffset;
    uint32_t serializedObjectLength = 0;

    // update hash table for each entry that was written to the log
    for (uint32_t i = 0; i < numEntries; i++) {
        serializedObjectLength = 0;
        // Don't call getRange on logBuffer because we only need the
        // key for each object/tombstone
        type = Segment::getEntry(logBuffer, offset,
                                 &serializedObjectLength, &entryLength);
        objectOffset = offset + entryLength - serializedObjectLength;

        LogEntryType currentType = LOG_ENTRY_TYPE_INVALID;
        Buffer currentBuffer;
        Log::Reference currentReference;
        uint64_t currentVersion = VERSION_NONEXISTENT;

        HashTable::Candidates currentHashTableEntry;

        if (type == LOG_ENTRY_TYPE_OBJ) {

            Object object(*logBuffer, objectOffset, serializedObjectLength);
            KeyLength keyLength;
            const void* keyString = object.getKey(0, &keyLength);

            Key key(object.getTableId(), keyString, keyLength);

            objectMap.prefetchBucket(key.getHash());
            HashTableBucketLock lock(*this, key);

            if (lookup(lock, key, currentType, currentBuffer, &currentVersion,
                       &currentReference, &currentHashTableEntry)) {

                if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
                    removeIfTombstone(currentReference.toInteger(), this);
                    objectMap.insert(key.getHash(), references[i].toInteger());
                }

                if (currentType == LOG_ENTRY_TYPE_OBJ) {
                    currentHashTableEntry.setReference(
                                    references[i].toInteger());
                    log.free(currentReference);
                }
            } else {
                objectMap.insert(key.getHash(), references[i].toInteger());
            }
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {

            ObjectTombstone tombstone(*logBuffer, objectOffset,
                                      serializedObjectLength);

            KeyLength keyLength = tombstone.getKeyLength();
            const void* keyString = tombstone.getKey();

            Key key(tombstone.getTableId(), keyString, keyLength);

            objectMap.prefetchBucket(key.getHash());
            HashTableBucketLock lock(*this, key);

            uint64_t currentVersion = 0;
            if (lookup(lock, key, currentType, currentBuffer, &currentVersion,
                       &currentReference, &currentHashTableEntry)) {

                // this is a tombstone for the most recent version of
                // the object so far in the log
                if (currentVersion == tombstone.getObjectVersion()) {
                    remove(lock, key);
                    log.free(currentReference);
                    segmentManager.raiseSafeVersion(currentVersion + 1);
                }
            }
        }
        offset = offset + entryLength;
    }
    // sync to backups
    syncChanges();
    logBuffer->reset();
    numEntries = 0;
    return true;
}

/**
 * Adds a log entry header, an object header and the object contents
 * to a buffer. This is preparatory work so that eventually, all the
 * entries in the buffer can be flushed to the log atomically.
 * 
 * This method is currently used by the Btree module to prepare a buffer
 * for the log. The idea is that eventually, each of the log entries in
 * the buffer can be flushed atomically by the log.
 * It is general enough to be used by any other module.
 * 
 * 
 * \param newObject
 *      The object for which the object header and the log entry
 *      header need to be constructed.
 * \param logBuffer
 *      The buffer to which the log entry header, the object
 *      header and the object contents will be appended (in that
 *      order)
 * \param [out] offset
 *      offset of the new object's value in #logBuffer
 * \param [out] tombstoneAdded
 *      true if an older version of this object exists, false
 *      otherwise
 * \return
 *      the status of the operation. As long as the table is in
 *      the proper state, this should return STATUS_OK
 */
Status
ObjectManager::prepareForLog(Object& newObject, Buffer *logBuffer,
                             uint32_t* offset, bool *tombstoneAdded)
{
    uint16_t keyLength = 0;
    const void *keyString = newObject.getKey(0, &keyLength);
    Key key(newObject.getTableId(), keyString, keyLength);

    objectMap.prefetchBucket(key.getHash());
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead
    // ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    LogEntryType currentType = LOG_ENTRY_TYPE_INVALID;
    Buffer currentBuffer;
    Log::Reference currentReference;
    uint64_t currentVersion = VERSION_NONEXISTENT;

    HashTable::Candidates currentHashTableEntry;

    if (lookup(lock, key, currentType, currentBuffer, 0,
               &currentReference, &currentHashTableEntry)) {

        if (currentType != LOG_ENTRY_TYPE_OBJTOMB) {
            Object currentObject(currentBuffer);
            currentVersion = currentObject.getVersion();
        }
    }

    // Existing objects get a bump in version, new objects start from
    // the next version allocated in the table.
    uint64_t newObjectVersion = (currentVersion == VERSION_NONEXISTENT) ?
            segmentManager.allocateVersion() : currentVersion + 1;

    newObject.setVersion(newObjectVersion);
    newObject.setTimestamp(WallTime::secondsTimestamp());

    assert(currentVersion == VERSION_NONEXISTENT ||
           newObject.getVersion() > currentVersion);

    Tub<ObjectTombstone> tombstone;
    if (currentVersion != VERSION_NONEXISTENT &&
      currentType == LOG_ENTRY_TYPE_OBJ) {
        Object object(currentBuffer);
        tombstone.construct(object,
                            log.getSegmentId(currentReference),
                            WallTime::secondsTimestamp());
    }

    Segment::appendLogHeader(LOG_ENTRY_TYPE_OBJ,
                             newObject.getSerializedLength(),
                             logBuffer);

    uint32_t objectOffset = 0;
    uint32_t lengthBefore = logBuffer->size();
    uint16_t valueOffset = 0;

    newObject.getValueOffset(&valueOffset);
    objectOffset = lengthBefore + sizeof32(Object::Header) + valueOffset;

    void* target = logBuffer->alloc(newObject.getSerializedLength());
    newObject.assembleForLog(target);

    // tombstone goes after the new object
    if (tombstone) {
        Segment::appendLogHeader(LOG_ENTRY_TYPE_OBJTOMB,
                                 tombstone->getSerializedLength(),
                                 logBuffer);
        void* target = logBuffer->alloc(tombstone->getSerializedLength());
        tombstone->assembleForLog(target);
        *tombstoneAdded = true;
    }

    // TODO(arjung):
    // When do we update this ? Now or later when flushing to the log
    // If later, how would we do it ?
#if 0
    tabletManager->incrementWriteCount(key);
    uint64_t byteCount = appends[0].buffer.size();
    uint64_t recordCount = 1;
    if (tombstone) {
        byteCount += appends[1].buffer.size();
        recordCount += 1;
    }

    TableStats::increment(masterTableMetadata,
                          tablet.tableId,
                          byteCount,
                          recordCount);
#endif

    if (offset)
        *offset = objectOffset;

    return STATUS_OK;
}

/**
 * Write a tombstone including the corresponding log entry header
 * into a buffer based on the primary key of the object.
 *
 * \param key
 *      Key of the object for which a tombstone needs to be written.
 * \param [out] logBuffer
 *      Buffer that will contain the newly written tombstone entry.
 * \return
 *      the status of the operation. If the tablet is in the NORMAL
 *      state, it returns STATUS_OK.
 */
Status
ObjectManager::writeTombstone(Key& key, Buffer *logBuffer)
{
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead
    // ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    LogEntryType type;
    Buffer buffer;
    Log::Reference reference;
    if (!lookup(lock, key, type, buffer, NULL, &reference) ||
            type != LOG_ENTRY_TYPE_OBJ) {
        return STATUS_OK;
    }

    Object object(buffer);

    ObjectTombstone tombstone(object,
                              log.getSegmentId(reference),
                              WallTime::secondsTimestamp());

    Segment::appendLogHeader(LOG_ENTRY_TYPE_OBJTOMB,
                             tombstone.getSerializedLength(),
                             logBuffer);
    void* target = logBuffer->alloc(tombstone.getSerializedLength());
    tombstone.assembleForLog(target);

// TODO(arjung):
// When do we update this ? Now or later when flushing to the log
// If later, how would we do it ?
#if 0
    TableStats::increment(masterTableMetadata,
                          tablet.tableId,
                          tombstoneBuffer.size(),
                          1);
#endif
    return STATUS_OK;
}

/**
 * Extract the timestamp from an entry written into the log. Used by the log
 * code do more efficient cleaning.
 *
 * \param type
 *      Type of the object being queried.
 * \param buffer
 *      Buffer pointing to the object in the log being queried.
 */
uint32_t
ObjectManager::getTimestamp(LogEntryType type, Buffer& buffer)
{
    if (type == LOG_ENTRY_TYPE_OBJ)
        return getObjectTimestamp(buffer);
    else if (type == LOG_ENTRY_TYPE_OBJTOMB)
        return getTombstoneTimestamp(buffer);
    else
        return 0;
}

/**
 * Relocate and update metadata for an object or tombstone that is being
 * cleaned. The cleaner invokes this method for every entry it comes across
 * when processing a segment. If the entry is no longer needed, nothing needs
 * to be done. If it is needed, the provided relocator should be used to copy
 * it to a new location and any metadata pointing to the old entry must be
 * updated before returning.
 *
 * \param type
 *      Type of the entry being cleaned.
 * \param oldBuffer
 *      Buffer pointing to the entry in the log being cleaned. This is the
 *      location that will soon be invalid due to garbage collection.
 * \param oldReference
 *      Log reference pointing to the entry being cleaned.
 * \param relocator
 *      The relocator is used to copy a live entry to a new location in the
 *      log and get a reference to that new location. If the entry is not
 *      needed, the relocator should not be used.
 */
void
ObjectManager::relocate(LogEntryType type, Buffer& oldBuffer,
                Log::Reference oldReference, LogEntryRelocator& relocator)
{
    if (type == LOG_ENTRY_TYPE_OBJ)
        relocateObject(oldBuffer, oldReference, relocator);
    else if (type == LOG_ENTRY_TYPE_OBJTOMB)
        relocateTombstone(oldBuffer, relocator);
}

/**
 * Clean tombstones from #objectMap lazily and in the background.
 *
 * Instances of this class must be allocated with new since they
 * delete themselves when the #objectMap scan is completed which
 * automatically deregisters it from Dispatch.
 *
 * \param objectManager
 *      The instance of ObjectManager which owns the #objectMap.
 * \param objectMap
 *      The HashTable which will be purged of tombstones.
 */
ObjectManager::RemoveTombstonePoller::RemoveTombstonePoller(
                ObjectManager* objectManager,
                HashTable* objectMap)
    : Dispatch::Poller(objectManager->context->dispatch, "TombstoneRemover")
    , currentBucket(0)
    , passes(0)
    , lastReplaySegmentCount(0)
    , objectManager(objectManager)
    , objectMap(objectMap)
{
    LOG(DEBUG, "Starting cleanup of tombstones in background");
}

/**
 * Remove tombstones from a single bucket and yield to other work
 * in the system.
 */
void
ObjectManager::RemoveTombstonePoller::poll()
{
    if (lastReplaySegmentCount == objectManager->replaySegmentReturnCount &&
      currentBucket == 0) {
        return;
    }

    // At the start of a new pass, record the number of replaySegment()
    // calls that have completed by this point. We will then keep doing
    // passes until this number remains constant at the beginning and
    // end of a pass.
    //
    // A recovery is likely to issue many replaySegment calls, but
    // should complete much faster than one pass here, so at worst we
    // should hopefully only traverse the hash table an extra time per
    // recovery.
    if (currentBucket == 0)
        lastReplaySegmentCount = objectManager->replaySegmentReturnCount;

    HashTableBucketLock lock(*objectManager, currentBucket);
    CleanupParameters params = { objectManager, &lock };
    objectMap->forEachInBucket(removeIfTombstone, &params, currentBucket);

    ++currentBucket;
    if (currentBucket == objectMap->getNumBuckets()) {
        LOG(DEBUG, "Cleanup of tombstones completed pass %lu", passes);
        currentBucket = 0;
        passes++;
    }
}

/**
 * Produce a human-readable description of the contents of a segment.
 * Intended primarily for use in unit tests.
 *
 * \param segment
 *       Segment whose contents should be dumped.
 *
 * \result
 *       A string describing the contents of the segment
 */
string
ObjectManager::dumpSegment(Segment* segment)
{
    const char* separator = "";
    string result;
    SegmentIterator it(*segment);
    while (!it.isDone()) {
        LogEntryType type = it.getType();
        if (type == LOG_ENTRY_TYPE_OBJ) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            Object object(buffer);
            result += format("%sobject at offset %u, length %u with tableId "
                    "%lu, key '%.*s'",
                    separator, it.getOffset(), it.getLength(),
                    object.getTableId(), object.getKeyLength(),
                    static_cast<const char*>(object.getKey()));
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            ObjectTombstone tombstone(buffer);
            result += format("%stombstone at offset %u, length %u with tableId "
                    "%lu, key '%.*s'",
                    separator, it.getOffset(), it.getLength(),
                    tombstone.getTableId(),
                    tombstone.getKeyLength(),
                    static_cast<const char*>(tombstone.getKey()));
        } else if (type == LOG_ENTRY_TYPE_SAFEVERSION) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            ObjectSafeVersion safeVersion(buffer);
            result += format("%ssafeVersion at offset %u, length %u with "
                    "version %lu",
                    separator, it.getOffset(), it.getLength(),
                    safeVersion.getSafeVersion());
        }
        it.next();
        separator = " | ";
    }
    return result;
}

/**
 * Callback used by the Log to determine the modification timestamp of an
 * Object. Timestamps are stored in the Object itself, rather than in the
 * Log, since not all Log entries need timestamps and other parts of the
 * system (or clients) may care about Object modification times.
 *
 * \param buffer
 *      Buffer pointing to the object the timestamp is to be extracted from.
 * \return
 *      The Object's modification timestamp.
 */
uint32_t
ObjectManager::getObjectTimestamp(Buffer& buffer)
{
    Object object(buffer);
    return object.getTimestamp();
}

/**
 * Callback used by the Log to determine the age of Tombstone.
 *
 * \param buffer
 *      Buffer pointing to the tombstone the timestamp is to be extracted from.
 * \return
 *      The tombstone's creation timestamp.
 */
uint32_t
ObjectManager::getTombstoneTimestamp(Buffer& buffer)
{
    ObjectTombstone tomb(buffer);
    return tomb.getTimestamp();
}

/**
 * Look up an object in the hash table, then extract the entry from the
 * log. Since tombstones are stored in the hash table during recovery,
 * this method may return either an object or a tombstone.
 *
 * \param lock
 *      This method must be invoked with the appropriate hash table bucket
 *      lock already held. This parameter exists to help ensure correct
 *      caller behaviour.
 * \param key
 *      Key of the object being looked up.
 * \param[out] outType
 *      The type of the log entry is returned here.
 * \param[out] buffer
 *      The entry, if found, is appended to this buffer. Note that the data
 *      pointed to by this buffer will be exactly the data in the log. The
 *      cleaner uses this fact to check whether an object in a segment is
 *      alive by comparing the pointer in the hash table (see #relocateObject).
 * \param[out] outVersion
 *      The version of the object or tombstone, when one is found, stored in
 *      this optional parameter.
 * \param[out] outReference
 *      The log reference to the entry, if found, is stored in this optional
 *      parameter.
 * \param[out] outCandidates
 *      If this option parameter is specified and the key being looked up
 *      is found, the HashTable::Candidates object provided will be set to
 *      point to that key's entry in the hash table. This is useful when an
 *      object is being updated. The caller may update the hash table's
 *      reference directly, rather than having to first perform another
 *      lookup after the new object is written to the log.
 * \return
 *      True if an entry is found matching the given key, otherwise false.
 */
bool
ObjectManager::lookup(HashTableBucketLock& lock, Key& key,
                LogEntryType& outType, Buffer& buffer,
                uint64_t* outVersion,
                Log::Reference* outReference,
                HashTable::Candidates* outCandidates)
{
    HashTable::Candidates candidates;
    objectMap.lookup(key.getHash(), candidates);
    while (!candidates.isDone()) {
        Buffer candidateBuffer;
        Log::Reference candidateRef(candidates.getReference());
        LogEntryType type = log.getEntry(candidateRef, candidateBuffer);

        Key candidateKey(type, candidateBuffer);
        if (key == candidateKey) {
            outType = type;
            buffer.appendExternal(&candidateBuffer);
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
            if (outCandidates != NULL)
                *outCandidates = candidates;
            return true;
        }

        candidates.next();
    }

    return false;
}

/**
 * Remove an object from the hash table, if it exists in it. Return whether or
 * not it was found and removed.
 *
 * \param lock
 *      This method must be invoked with the appropriate hash table bucket
 *      lock already held. This parameter exists to help ensure correct
 *      caller behaviour.
 * \param key
 *      Key of the object being removed.
 * \return
 *      True if the key was found and the object removed. False if it was not
 *      in the hash table.
 */
bool
ObjectManager::remove(HashTableBucketLock& lock, Key& key)
{
    HashTable::Candidates candidates;
    objectMap.lookup(key.getHash(), candidates);
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

/**
 * Removes an object from the hash table and frees it from the log if
 * it belongs to a tablet that doesn't exist in the master's TabletManager.
 * Used by deleteOrphanedObjects().
 *
 * \param reference
 *      Reference into the log for an object as returned from the master's
 *      objectMap->lookup() or on callback from objectMap->forEachInBucket().
 *      This object is removed from the objectMap and freed from the log if it
 *      doesn't belong to any tablet the master lists among its tablets.
 * \param cookie
 *      Pointer to the MasterService where this object is currently
 *      stored.
 */
void
ObjectManager::removeIfOrphanedObject(uint64_t reference, void *cookie)
{
    CleanupParameters* params = reinterpret_cast<CleanupParameters*>(cookie);
    ObjectManager* objectManager = params->objectManager;
    LogEntryType type;
    Buffer buffer;

    type = objectManager->log.getEntry(Log::Reference(reference), buffer);
    if (type != LOG_ENTRY_TYPE_OBJ)
        return;

    Key key(type, buffer);
    if (!objectManager->tabletManager->getTablet(key)) {
        TEST_LOG("removing orphaned object at ref %lu", reference);
        bool r = objectManager->remove(*params->lock, key);
        assert(r);
        objectManager->log.free(Log::Reference(reference));
    }
}

/**
 * This function is a callback used to purge the tombstones from the hash
 * table after a recovery has taken place. It is invoked by HashTable::
 * forEach via the RemoveTombstonePoller class that runs in the dispatch
 * thread.
 *
 * This function must be called with the appropriate HashTableBucketLock
 * held.
 */
void
ObjectManager::removeIfTombstone(uint64_t maybeTomb, void *cookie)
{
    CleanupParameters* params = reinterpret_cast<CleanupParameters*>(cookie);
    ObjectManager* objectManager = params->objectManager;
    LogEntryType type;
    Buffer buffer;

    type = objectManager->log.getEntry(Log::Reference(maybeTomb), buffer);
    if (type == LOG_ENTRY_TYPE_OBJTOMB) {
        Key key(type, buffer);

        // We can remove tombstones so long as they meet one of the two
        // following criteria:
        //  1) Tablet is not assigned to us (not in TabletManager, so we don't
        //     care about it).
        //  2) Tablet is not in the RECOVERING state (replaySegment won't be
        //     called for objects in that tablet anymore).
        bool discard = false;

        TabletManager::Tablet tablet;
        if (!objectManager->tabletManager->getTablet(key, &tablet) ||
          tablet.state != TabletManager::RECOVERING) {
            discard = true;
        }

        if (discard) {
            TEST_LOG("discarding");
            bool r = objectManager->remove(*params->lock, key);
            assert(r);
        }

        // Tombstones are not explicitly freed in the log. The cleaner will
        // figure out that they're dead.
    }
}

/**
 * Synchronously remove leftover tombstones in the hash table added during
 * replaySegment calls (for example, as caused by a recovery). This private
 * method exists for testing purposes only, since asynchronous removal raises
 * hell in unit tests.
 */
void
ObjectManager::removeTombstones()
{
    for (uint64_t i = 0; i < objectMap.getNumBuckets(); i++) {
        HashTableBucketLock lock(*this, i);
        CleanupParameters params = { this , &lock };
        objectMap.forEachInBucket(removeIfTombstone, &params, i);
    }
}

/**
 * Check a set of RejectRules against the current state of an object
 * to decide whether an operation is allowed.
 *
 * \param rejectRules
 *      Specifies conditions under which the operation should fail.
 * \param version
 *      The current version of an object, or VERSION_NONEXISTENT
 *      if the object does not currently exist (used to test rejectRules)
 *
 * \return
 *      The return value is STATUS_OK if none of the reject rules
 *      indicate that the operation should be rejected. Otherwise
 *      the return value indicates the reason for the rejection.
 */
Status
ObjectManager::rejectOperation(const RejectRules* rejectRules, uint64_t version)
{
    if (version == VERSION_NONEXISTENT) {
        if (rejectRules->doesntExist)
            return STATUS_OBJECT_DOESNT_EXIST;
        return STATUS_OK;
    }
    if (rejectRules->exists)
        return STATUS_OBJECT_EXISTS;
    if (rejectRules->versionLeGiven && version <= rejectRules->givenVersion)
        return STATUS_WRONG_VERSION;
    if (rejectRules->versionNeGiven && version != rejectRules->givenVersion)
        return STATUS_WRONG_VERSION;
    return STATUS_OK;
}

/**
 * Callback used by the LogCleaner when it's cleaning a Segment and comes
 * across an Object.
 *
 * This callback will decide if the object is still alive. If it is, it must
 * use the relocator to move it to a new location and atomically update the
 * hash table.
 *
 * \param oldBuffer
 *      Buffer pointing to the object's current location, which will soon be
 *      invalidated.
 * \param oldReference
 *      Reference to the old object in the log. This is used to make a fast
 *      liveness check (the hash table points to this iff the object is alive).
 * \param relocator
 *      The relocator may be used to store the object in a new location if it
 *      is still alive. It also provides a reference to the new location and
 *      keeps track of whether this call wanted the object anymore or not.
 *
 *      It is possible that relocation may fail (because more memory needs to
 *      be allocated). In this case, the callback should just return. The
 *      cleaner will note the failure, allocate more memory, and try again.
 */
void
ObjectManager::relocateObject(Buffer& oldBuffer, Log::Reference oldReference,
                LogEntryRelocator& relocator)
{
    Key key(LOG_ENTRY_TYPE_OBJ, oldBuffer);
    HashTableBucketLock lock(*this, key);

    // Note that we do not query the TabletManager to see if this object
    // belongs to a live tablet since that would create considerable
    // contention for the TabletManager. We can do this safely because
    // tablet drops synchronously purge the hash table of any objects
    // corresponding to the tablet. Were we not to purge the objects,
    // we could migrate a tablet away and back again while maintaining
    // references to old objects (that could have been deleted on the
    // intermediate master before migrating back).

    // It's much faster not to use lookup and replace here, but to
    // scan the hash table bucket ourselves. We already have the
    // reference, so there's no need for a key comparison, and we
    // can easily avoid looping over the bucket twice this way for
    // live objects.
    HashTable::Candidates candidates;
    objectMap.lookup(key.getHash(), candidates);
    while (!candidates.isDone()) {
        if (candidates.getReference() != oldReference.toInteger()) {
            candidates.next();
            continue;
        }

        // Try to relocate this live object. If we fail, just return. The
        // cleaner will allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_OBJ, oldBuffer))
            return;

        candidates.setReference(relocator.getNewReference().toInteger());
        return;
    }

    // No reference was found meaning object will be cleaned.  We should update
    // the stats accordingly.
    TableStats::decrement(masterTableMetadata,
                          key.getTableId(),
                          oldBuffer.size(),
                          1);
}

/**
 * Callback used by the LogCleaner when it's cleaning a Segment and comes
 * across a Tombstone.
 *
 * This callback will decide if the tombstone is still alive. If it is, it must
 * use the relocator to move it to a new location and atomically update the
 * hash table.
 *
 * \param oldBuffer
 *      Buffer pointing to the tombstone's current location, which will soon be
 *      invalidated.
 * \param relocator
 *      The relocator may be used to store the tombstone in a new location if it
 *      is still alive. It also provides a reference to the new location and
 *      keeps track of whether this call wanted the tombstone anymore or not.
 *
 *      It is possible that relocation may fail (because more memory needs to
 *      be allocated). In this case, the callback should just return. The
 *      cleaner will note the failure, allocate more memory, and try again.
 */
void
ObjectManager::relocateTombstone(Buffer& oldBuffer,
                LogEntryRelocator& relocator)
{
    ObjectTombstone tomb(oldBuffer);

    // See if the object this tombstone refers to is still in the log.
    bool keepNewTomb = log.segmentExists(tomb.getSegmentId());

    if (keepNewTomb) {
        // Try to relocate it. If it fails, just return. The cleaner will
        // allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_OBJTOMB, oldBuffer))
            return;
    } else {
        // Tombstone will be dropped/"cleaned" so stats should be updated.
        TableStats::decrement(masterTableMetadata,
                              tomb.getTableId(),
                              oldBuffer.size(),
                              1);
    }
}

/**
 * Insert an object reference into the hash table, or replace the object
 * reference currently associated with the key if one already exists in the
 * table.
 *
 * \param lock
 *      This method must be invoked with the appropriate hash table bucket
 *      lock already held. This parameter exists to help ensure correct
 *      caller behaviour.
 * \param key
 *      The key to add to update a reference for.
 * \param reference
 *      The reference to store in the hash table under the given key.
 * \return
 *      Returns true if the key already existed in the hash table and the
 *      reference was updated. False indicates that the key did not already
 *      exist. In either case, the hash table will refer to the given reference.
 */
bool
ObjectManager::replace(HashTableBucketLock& lock, Key& key,
                Log::Reference reference)
{
    HashTable::Candidates candidates;
    objectMap.lookup(key.getHash(), candidates);
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

    objectMap.insert(key.getHash(), reference.toInteger());
    return false;
}

} //enamespace RAMCloud
