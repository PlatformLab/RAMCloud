/* Copyright (c) 2012-2015 Stanford University
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
#include "UnackedRpcResults.h"
#include "WallTime.h"
#include "MasterService.h"

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
 * \param unackedRpcResults
 *      Pointer to the master's UnackedRpcResults instance.  This keeps track
 *      of data stored to ensure client rpcs are linearizable.
 * \param preparedOps
 *      Pointer to the master's PreparedWrites instance.  This keeps track
 *      of data stored during transaction prepare stage.
 * \param txRecoveryManager
 *      Pointer to the master's TxRecoveryManager instance.  This keeps track
 *      of ongoing transaction recoveries; these recoveries may need records
 *      stored in the log.
 */
ObjectManager::ObjectManager(Context* context, ServerId* serverId,
                const ServerConfig* config,
                TabletManager* tabletManager,
                MasterTableMetadata* masterTableMetadata,
                UnackedRpcResults* unackedRpcResults,
                PreparedOps* preparedOps,
                TxRecoveryManager* txRecoveryManager)
    : context(context)
    , config(config)
    , tabletManager(tabletManager)
    , masterTableMetadata(masterTableMetadata)
    , unackedRpcResults(unackedRpcResults)
    , preparedOps(preparedOps)
    , txRecoveryManager(txRecoveryManager)
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
    , lockTable(1000, log)
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
 * Implementation of AbstractLog::ReferenceFreer. This method will be used
 * by default on regular linearizable RPC handling.
 * Linearizable RPC handler should pass "this" objectManager to the constructor
 * of UnackedRpcResults.
 *
 * \param ref
 *      Log reference for log entry to be freed.
 */
void
ObjectManager::freeLogEntry(Log::Reference ref)
{
    assert(ref.toInteger());
    log.free(ref);
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
 * Read object(s) with the given primary key hashes, previously written by
 * ObjectManager.
 *
 * We'd typically expect one object to match one primary key hash, but it is
 * possible that zero (if that object was removed after client got the key
 * hash), one, or multiple (if there are multiple objects with the same primary
 * key hash) objects match.
 *
 * \param tableId
 *      Id of the table containing the object(s).
 * \param reqNumHashes
 *      Number of key hashes (see pKHashes) in the request.
 * \param pKHashes
 *      Key hashes of the primary keys of the object(s).
 * \param initialPKHashesOffset
 *      Offset indicating location of first key hash in the pKHashes buffer.
 * \param maxLength
 *      Maximum length of response that can be put in the response buffer.
 *
 * \param[out] response
 *      Buffer of objects whose primary key hashes match those requested.
 * \param[out] respNumHashes
 *      Number of hashes corresponding to objects being returned.
 * \param[out] numObjects
 *      Number of objects being returned.
 */
void
ObjectManager::readHashes(const uint64_t tableId, uint32_t reqNumHashes,
            Buffer* pKHashes, uint32_t initialPKHashesOffset,
            uint32_t maxLength, Buffer* response, uint32_t* respNumHashes,
            uint32_t* numObjects)
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
        HashTableBucketLock lock(*this, pKHash); // unlocks self on destruct

        // If the tablet doesn't exist in the NORMAL state,
        // we must plead ignorance.
        // Client can refresh its tablet information and retry.
        TabletManager::Tablet tablet;
        if (!tabletManager->getTablet(tableId, pKHash, &tablet))
            return;
        if (tablet.state != TabletManager::NORMAL) {
            if (tablet.state == TabletManager::LOCKED_FOR_MIGRATION)
                throw RetryException(HERE, 1000, 2000,
                        "Tablet is currently locked for migration!");
            return;
        }

        HashTable::Candidates candidates;
        objectMap.lookup(pKHash, candidates);
        for (; !candidates.isDone(); candidates.next()) {
            Buffer candidateBuffer;
            Log::Reference candidateRef(candidates.getReference());
            LogEntryType type = log.getEntry(candidateRef, candidateBuffer);

            if (type != LOG_ENTRY_TYPE_OBJ)
                continue;

            Object object(candidateBuffer);

            // Candidate may have only partially matching primary key hash.
            if (object.getPKHash() == pKHash) {
                *numObjects += 1;
                response->emplaceAppend<uint64_t>(object.getVersion());
                response->emplaceAppend<uint32_t>(
                        object.getKeysAndValueLength());
                object.appendKeysAndValueToBuffer(*response);

                tabletManager->incrementReadCount(object.getTableId(),
                        object.getPKHash());
                ++PerfStats::threadStats.readCount;
                uint32_t valueLength = object.getValueLength();
                PerfStats::threadStats.readObjectBytes += valueLength;
                PerfStats::threadStats.readKeyBytes +=
                        object.getKeysAndValueLength() - valueLength;
            }
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
        object.appendValueToBuffer(outBuffer);
    } else {
        object.appendKeysAndValueToBuffer(*outBuffer);
    }
    ++PerfStats::threadStats.readCount;
    uint32_t valueLength = object.getValueLength();
    PerfStats::threadStats.readObjectBytes += valueLength;
    PerfStats::threadStats.readKeyBytes +=
            object.getKeysAndValueLength() - valueLength;

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
 * \param rpcResult
 *      If non-NULL, this method appends rpcResult to the log atomically with
 *      the other record(s) for the write. The extra record is used to ensure
 *      linearizability.
 * \param[out] rpcResultPtr
 *      If non-NULL, pointer to the RpcResult in log is returned.
 * \return
 *      Returns STATUS_OK if the remove succeeded. Other status values indicate
 *      different failures (tablet doesn't exist, reject rules applied, etc).
 */
Status
ObjectManager::removeObject(Key& key, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer,
                RpcResult* rpcResult, uint64_t* rpcResultPtr)
{
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL) {
        if (tablet.state == TabletManager::LOCKED_FOR_MIGRATION)
            throw RetryException(HERE, 1000, 2000,
                    "Tablet is currently locked for migration!");
        return STATUS_UNKNOWN_TABLET;
    }

    // If key is locked due to an in-progress transaction, we must wait.
    if (lockTable.isLockAcquired(key)) {
        return STATUS_RETRY;
    }

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
        removedObjBuffer->append(&buffer);
    }

    ObjectTombstone tombstone(object,
                              log.getSegmentId(reference),
                              WallTime::secondsTimestamp());

    // Create a vector of appends in case we need to write multiple log entries
    // including a tombstone and a linearizability record.
    // This is necessary to ensure that both tombstone and rpcResult
    // are written atomically. The log makes no atomicity guarantees across
    // multiple append calls and we don't want a tombstone going to backups
    // before the RpcResult, or vice versa.
    Log::AppendVector appends[(rpcResult ? 2 : 1)];

    tombstone.assembleForLog(appends[0].buffer);
    appends[0].type = LOG_ENTRY_TYPE_OBJTOMB;
    if (rpcResult) {
        rpcResult->assembleForLog(appends[1].buffer);
        appends[1].type = LOG_ENTRY_TYPE_RPCRESULT;
    }

    if (!log.append(appends, (rpcResult ? 2 : 1))) {
        // The log is out of space. Tell the client to retry and hope
        // that the cleaner makes space soon.
        return STATUS_RETRY;
    }

    if (rpcResult && rpcResultPtr)
        *rpcResultPtr = appends[1].reference.toInteger();

    TableStats::increment(masterTableMetadata,
                          tablet.tableId,
                          appends[0].buffer.size(),
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
    replaySegment(sideLog, it, NULL);
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
 * \param nextNodeIdMap
 *       A unordered map that keeps track of the nextNodeId in
 *       each indexlet table.
 */
void
ObjectManager::replaySegment(SideLog* sideLog, SegmentIterator& it,
    std::unordered_map<uint64_t, uint64_t>* nextNodeIdMap)
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
    for (; expect_true(!it.isDone()); it.next()) {
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
            // nextNodeIdMap, update nextNodeId of its table.
            if (nextNodeIdMap) {
                std::unordered_map<uint64_t, uint64_t>::iterator iter
                    = nextNodeIdMap->find(recoveryObj->tableId);
                if (iter != nextNodeIdMap->end()) {
                    const uint64_t *bTreeKey =
                        reinterpret_cast<const uint64_t*>(primaryKey);
                    uint64_t bTreeId = *bTreeKey;
                    if (bTreeId >= iter->second)
                        iter->second = bTreeId+1;
                }
            }

            bool checksumIsValid = ({
                CycleCounter<uint64_t> c(&verifyChecksumTicks);
                Object::computeChecksum(recoveryObj, it.getLength()) ==
                    recoveryObj->checksum;
            });
            if (expect_false(!checksumIsValid)) {
                LOG(WARNING, "bad object checksum! key: %s, version: %lu",
                    key.toString().c_str(), recoveryObj->version);
                // JIRA Issue: RAM-673:
                // Should throw and try another segment replica.
            }

            HashTableBucketLock lock(*this, key);


            LogEntryType currentType;
            Buffer currentBuffer;
            Log::Reference currentReference;
            // Handle the case where there is a current object in the hash table
            if (lookup(lock, key, currentType, currentBuffer, 0,
                                                        &currentReference)) {
                bool currentEntryIsObject = false;
                uint64_t currentVersion;

                if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
                    ObjectTombstone currentTombstone(currentBuffer);
                    currentVersion = currentTombstone.getObjectVersion();
                } else {
                    Object currentObject(currentBuffer);
                    currentVersion = currentObject.getVersion();
                    currentEntryIsObject = true;
                }

                // Throw new object away if the hash table version is newer
                if (recoveryObj->version <= currentVersion) {
                    objectDiscardCount++;
                    continue;
                }
                if (currentEntryIsObject) {
                    // Create a tombstone to prevent rebirth after more crashes
                    Object object(currentBuffer);
                    ObjectTombstone tombstone(object,
                            log.getSegmentId(currentReference),
                            WallTime::secondsTimestamp());

                    Buffer tombstoneBuffer;
                    tombstone.assembleForLog(tombstoneBuffer);
                    sideLog->append(LOG_ENTRY_TYPE_OBJTOMB, tombstoneBuffer);
                    tombstoneAppendCount++;
                    TableStats::increment(masterTableMetadata,
                            key.getTableId(),
                            tombstoneBuffer.size(),
                            1);

                    // Track the death of the object
                    liveObjectBytes -= currentBuffer.size();
                    sideLog->free(currentReference);
                    liveObjectCount--;
                }
            }

            // Add the incoming object or tombstone to our log and update
            // the hash table to refer to it.
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
            replace(lock, key, newObjReference);

            // JIRA Issue: RAM-674:
            // If master runs out of space during recovery, this master
            // should abort the recovery. Coordinator will then try
            // another master.

            liveObjectCount++;
            objectAppendCount++;
            liveObjectBytes += it.getLength();
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            Key key(type, buffer);

            // TODO(syang0) A B+ Tree nextNodeId check was removed here because
            // we only need to set the nextNodeId to the highest live node;
            // any (deleted) nodeId's higher than that can be overwritten.

            ObjectTombstone recoverTomb(buffer);
            bool checksumIsValid = ({
                CycleCounter<uint64_t> c(&verifyChecksumTicks);
                recoverTomb.checkIntegrity();
            });
            if (expect_false(!checksumIsValid)) {
                LOG(WARNING, "bad tombstone checksum! key: %s, version: %lu",
                    key.toString().c_str(), recoverTomb.getObjectVersion());
                // JIRA Issue: RAM-673:
                // Should throw and try another segment replica.
            }
            uint64_t recoverVersion = recoverTomb.getObjectVersion();

            HashTableBucketLock lock(*this, key);


            LogEntryType currentType;
            Buffer currentBuffer;
            Log::Reference currentReference;
            if (lookup(lock, key, currentType, currentBuffer, 0,
                                                        &currentReference)) {
                bool currentEntryIsObject = false;
                uint64_t currentVersion;
                if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
                    ObjectTombstone currentTombstone(currentBuffer);
                    currentVersion = currentTombstone.getObjectVersion();
                } else {
                    Object currentObject(currentBuffer);
                    currentVersion = currentObject.getVersion();
                    currentEntryIsObject = true;
                }
                // Throw new tombstone away if the hash table version is
                // strictly newer
                if (recoverVersion < currentVersion) {
                    tombstoneDiscardCount++;
                    continue;
                }
                // We assume we will not see an exact duplicate tombstone and
                // do not handle that case.
                assert(currentEntryIsObject ||
                        recoverVersion != currentVersion);

                // Raise safeVersion if that in the recovered tombstone is
                // bigger than current safeVersion. (RAM-677 fix)
                segmentManager.raiseSafeVersion(recoverVersion + 1);

                // Synthesize a new tombstone specifically so as to maintain
                // the following invariant
                // Any time a logically deleted object is present in the log
                // and not yet cleaned, there exists a corresponding tombstone
                // which refers specifically to it.
                if (currentEntryIsObject) {
                    Log::Reference newTombReference;
                    // Create a tombstone to prevent rebirth after more crashes
                    Object object(currentBuffer);
                    ObjectTombstone tombstone(object,
                            log.getSegmentId(currentReference),
                            WallTime::secondsTimestamp());

                    Buffer tombstoneBuffer;
                    tombstone.assembleForLog(tombstoneBuffer);
                    sideLog->append(LOG_ENTRY_TYPE_OBJTOMB,
                            tombstoneBuffer,
                            &newTombReference);

                    tombstoneAppendCount++;
                    TableStats::increment(masterTableMetadata,
                            key.getTableId(),
                            tombstoneBuffer.size(),
                            1);

                    // Track the death of the object
                    liveObjectBytes -= currentBuffer.size();
                    sideLog->free(currentReference);
                    liveObjectCount--;

                    // Optimization to avoid appending two tombstones with the
                    // same version for the same key to the log.
                    if (recoverVersion == currentVersion) {
                        replace(lock, key, newTombReference);
                        continue;
                    }

                }

            }

            // Reaching this point means that there was either no entry in the
            // hash table or this tombstone is newer than the current object in
            // the hash table.
            // Thus, we add the received tombstone to the hash table to block
            // earlier version objects and tombstones.
            recoverTomb.header.timestamp = WallTime::secondsTimestamp();

            // A segmentId of 0 indicates that this tombstone can be cleaned as
            // soon as the hash table ceases to reference it.
            recoverTomb.header.segmentId = 0;
            recoverTomb.header.checksum = 0;
            recoverTomb.header.checksum = recoverTomb.computeChecksum();

            // Create a new Buffer to avoid reading and writing from the same
            // Buffer.
            Log::Reference newTombReference;
            Buffer tombstoneBuffer;
            recoverTomb.assembleForLog(tombstoneBuffer);
            sideLog->append(LOG_ENTRY_TYPE_OBJTOMB,
                    tombstoneBuffer,
                    &newTombReference);

            tombstoneAppendCount++;
            TableStats::increment(masterTableMetadata,
                    key.getTableId(),
                    buffer.size(),
                    1);
            replace(lock, key, newTombReference);
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
                // JIRA Issue: RAM-673:
                // Should throw and try another segment replica.
            }

            // Copy SafeVerObject to the recovery segment.
            // Sync can be delayed, because recovery can be replayed
            // with the same backup data when the recovery crashes on the way.
            {
                CycleCounter<uint64_t> _(&segmentAppendTicks);
                sideLog->append(LOG_ENTRY_TYPE_SAFEVERSION, buffer);
            }
            // JIRA Issue: RAM-674:
            // If master runs out of space during recovery, this master
            // should abort the recovery. Coordinator will then try
            // another master.

            // recover segmentManager.safeVersion (Master safeVersion)
            if (segmentManager.raiseSafeVersion(safeVersion)) {
                // true if log.safeVersion is revised.
                safeVersionRecoveryCount++;
                LOG(DEBUG, "SAFEVERSION %lu recovered", safeVersion);
            } else {
                safeVersionNonRecoveryCount++;
                LOG(DEBUG, "SAFEVERSION %lu discarded", safeVersion);
            }
        } else if (type == LOG_ENTRY_TYPE_RPCRESULT) {
            Buffer buffer;
            it.appendToBuffer(buffer);

            RpcResult rpcResult(buffer);

            if (unackedRpcResults->shouldRecover(
                    rpcResult.getLeaseId(),
                    rpcResult.getRpcId(),
                    rpcResult.getAckId())) {
                Log::Reference newRpcResultReference;
                {
                    CycleCounter<uint64_t> _(&segmentAppendTicks);
                    sideLog->append(LOG_ENTRY_TYPE_RPCRESULT,
                                    buffer,
                                    &newRpcResultReference);
                    TableStats::increment(masterTableMetadata,
                            rpcResult.getTableId(),
                            buffer.size(),
                            1);
                }

                unackedRpcResults->recoverRecord(
                        rpcResult.getLeaseId(),
                        rpcResult.getRpcId(),
                        rpcResult.getAckId(),
                        reinterpret_cast<void*>(
                                newRpcResultReference.toInteger()));
            }
        } else if (type == LOG_ENTRY_TYPE_PREP) {
            // We cannot grab lock in this code, which can cause deadlock.
            // We may see out-of-order prepare records on a same key, even
            // before object is recover. (Which is not supported by current
            // LockTable implementation.)
            //
            // We should grab all locks after we replay all segments
            // (by traversing PreparedWrites)
            Buffer buffer;
            it.appendToBuffer(buffer);

            PreparedOp op(buffer, 0, buffer.size());

            KeyLength pKeyLen = 0;
            const void *pKey = op.object.getKey(0, &pKeyLen);

            Key key(op.object.header.tableId, pKey, pKeyLen);

            if (expect_false(!op.checkIntegrity())) {
                LOG(WARNING, "bad preparedOp checksum! key: %s, leaseId: %lu"
                             ", rpcId: %lu",
                             key.toString().c_str(),
                             op.header.clientId,
                             op.header.rpcId);
            }

            HashTableBucketLock lock(*this, key);

            uint64_t minSuccessor = 0;

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
                }
                minSuccessor = currentVersion + 1;
            }

            if (!preparedOps->isDeleted(op.header.clientId,
                                           op.header.rpcId) &&
                !preparedOps->peekOp(op.header.clientId,
                                        op.header.rpcId) &&
                op.object.header.version >= minSuccessor) {

                // write to log (with lazy backup flush) & update hash table
                Log::Reference newReference;
                {
                    CycleCounter<uint64_t> _(&segmentAppendTicks);
                    sideLog->append(LOG_ENTRY_TYPE_PREP,
                                    buffer,
                                    &newReference);
                    TableStats::increment(masterTableMetadata,
                            key.getTableId(),
                            buffer.size(),
                            1);
                }
                preparedOps->bufferOp(op.header.clientId,
                                         op.header.rpcId,
                                         newReference.toInteger(),
                                         true);
            }
        } else if (type == LOG_ENTRY_TYPE_PREPTOMB) {
            Buffer buffer;
            it.appendToBuffer(buffer);

            PreparedOpTombstone opTomb(buffer, 0);

            if (expect_false(!opTomb.checkIntegrity())) {
                LOG(WARNING, "bad preparedOpTombstone checksum! tableId: %lu, "
                             "keyHash: %lu, leaseId: %lu, rpcId: %lu",
                             opTomb.header.tableId,
                             opTomb.header.keyHash,
                             opTomb.header.clientLeaseId,
                             opTomb.header.rpcId);
            }

            if (!preparedOps->isDeleted(opTomb.header.clientLeaseId,
                                           opTomb.header.rpcId) &&
                !preparedOps->peekOp(opTomb.header.clientLeaseId,
                                        opTomb.header.rpcId)) {
                // PreparedOp log entry is either deleted or
                // not yet recovered. We will check whether it is marked
                // for deletion and skip its recovery, so no need to recover
                // this tombstone.
                preparedOps->markDeleted(opTomb.header.clientLeaseId,
                                            opTomb.header.rpcId);
            } else {
                // write to log (with lazy backup flush)
                Log::Reference newReference;
                {
                    CycleCounter<uint64_t> _(&segmentAppendTicks);
                    sideLog->append(LOG_ENTRY_TYPE_PREPTOMB,
                                    buffer,
                                    &newReference);
                    TableStats::increment(masterTableMetadata,
                            opTomb.header.tableId,
                            buffer.size(),
                            1);
                }
                preparedOps->popOp(opTomb.header.clientLeaseId,
                                      opTomb.header.rpcId);

                // For idempodency of replaySegment.
                preparedOps->markDeleted(opTomb.header.clientLeaseId,
                                            opTomb.header.rpcId);
            }
        } else if (type == LOG_ENTRY_TYPE_TXDECISION) {
            Buffer buffer;
            it.appendToBuffer(buffer);

            TxDecisionRecord record(buffer);

            bool checksumIsValid = ({
                CycleCounter<uint64_t> c(&verifyChecksumTicks);
                record.checkIntegrity();
            });
            if (expect_false(!checksumIsValid)) {
                LOG(ERROR, "bad TxDecisionRecord checksum! leaseId: %lu",
                    record.getLeaseId());
                // TODO(cstlee): Should throw and try another segment replica?
            }
            if (txRecoveryManager->recoverRecovery(record)) {
                CycleCounter<uint64_t> _(&segmentAppendTicks);
                sideLog->append(LOG_ENTRY_TYPE_TXDECISION, buffer);
                // TODO(cstlee) : What should we do if the append fails?
                TableStats::increment(masterTableMetadata,
                                      record.getTableId(),
                                      buffer.size(),
                                      1);
            }
        } else if (type == LOG_ENTRY_TYPE_TXPLIST) {
            Buffer buffer;
            it.appendToBuffer(buffer);

            ParticipantList participantList(buffer);

            bool checksumIsValid = ({
                CycleCounter<uint64_t> c(&verifyChecksumTicks);
                participantList.checkIntegrity();
            });

            TransactionId txId = participantList.getTransactionId();

            if (expect_false(!checksumIsValid)) {
                LOG(ERROR,
                        "bad ParticipantList checksum! "
                        "(leaseId: %lu, txId: %lu)",
                        txId.clientLeaseId, txId.clientTransactionId);
                // TODO(cstlee): Should throw and try another segment replica?
            }
            if (unackedRpcResults->shouldRecover(txId.clientLeaseId,
                                                 txId.clientTransactionId,
                                                 0)) {
                CycleCounter<uint64_t> _(&segmentAppendTicks);
                Log::Reference logRef;
                if (sideLog->append(LOG_ENTRY_TYPE_TXPLIST, buffer, &logRef)) {
                    unackedRpcResults->recoverRecord(
                            txId.clientLeaseId,
                            txId.clientTransactionId,
                            0,
                            reinterpret_cast<void*>(logRef.toInteger()));
                    // Participant List records are not accounted for in the
                    // table stats. The assumption is that the Participant List
                    // records should occupy a relatively small fraction of the
                    // server's log and thus should not significantly affect
                    // table stats estimate.
                } else {
                    LOG(ERROR,
                            "Could not append ParticipantList! "
                            "(leaseId: %lu, txId: %lu)",
                            txId.clientLeaseId, txId.clientTransactionId);
                }
            }
        }
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
 * \param rpcResult
 *      If non-NULL, this method appends rpcResult to the log atomically with
 *      the other record(s) for the write. The extra record is used to ensure
 *      linearizability.
 * \param[out] rpcResultPtr
 *      If non-NULL, pointer to the RpcResult in log is returned.
 * \return
 *      STATUS_OK if the object was written. Otherwise, for example,
 *      STATUS_UKNOWN_TABLE may be returned.
 */
Status
ObjectManager::writeObject(Object& newObject, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer,
                RpcResult* rpcResult, uint64_t* rpcResultPtr)
{
    uint16_t keyLength = 0;
    const void *keyString = newObject.getKey(0, &keyLength);
    Key key(newObject.getTableId(), keyString, keyLength);

    objectMap.prefetchBucket(key.getHash());
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet)) {
        return STATUS_UNKNOWN_TABLET;
    }
    if (tablet.state != TabletManager::NORMAL) {
        if (tablet.state == TabletManager::LOCKED_FOR_MIGRATION)
            throw RetryException(HERE, 1000, 2000,
                    "Tablet is currently locked for migration!");
        return STATUS_UNKNOWN_TABLET;
    }

    // If key is locked due to an in-progress transaction, we must wait.
    if (lockTable.isLockAcquired(key)) {
        RAMCLOUD_CLOG(NOTICE, "Retrying because of transaction lock");
        return STATUS_RETRY;
    }

    LogEntryType currentType = LOG_ENTRY_TYPE_INVALID;
    Buffer currentBuffer;
    Log::Reference currentReference;
    uint64_t currentVersion = VERSION_NONEXISTENT;

    HashTable::Candidates currentHashTableEntry;

    if (lookup(lock, key, currentType, currentBuffer, 0,
               &currentReference, &currentHashTableEntry)) {
        if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
            CleanupParameters params = { this , &lock };
            removeIfTombstone(currentReference.toInteger(), &params);
        } else {
            Object currentObject(currentBuffer);
            currentVersion = currentObject.getVersion();
            // Return a pointer to the buffer in log for the object being
            // overwritten.
            if (removedObjBuffer != NULL) {
                removedObjBuffer->append(&currentBuffer);
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

    // Create a vector of appends in case we need to write multiple log entries
    // including a tombstone, an object and a linearizability record.
    // This is necessary to ensure that both tombstone, object and rpcResult
    // are written atomically. The log makes no atomicity guarantees across
    // multiple append calls and we don't want a tombstone going to backups
    // before the new object, or the new object going out without a tombstone
    // for the old deleted version. Both cases lead to consistency problems.
    // The same argument holds for linearizability records; the linearizability
    // record should exist if and only if new object is written.
    Log::AppendVector appends[2 + (rpcResult ? 1 : 0)];

    newObject.assembleForLog(appends[0].buffer);
    appends[0].type = LOG_ENTRY_TYPE_OBJ;

    // Note: only check for enough space for the object (tombstones
    // don't get included in the limit, since they can be cleaned).
    if (!log.hasSpaceFor(appends[0].buffer.size())) {
        throw RetryException(HERE, 1000, 2000, "Memory capacity exceeded");
    }

    if (tombstone) {
        tombstone->assembleForLog(appends[1].buffer);
        appends[1].type = LOG_ENTRY_TYPE_OBJTOMB;
    }

    if (outVersion != NULL)
        *outVersion = newObject.getVersion();

    int rpcResultIndex = 1 + (tombstone ? 1 : 0);
    if (rpcResult) {
        rpcResult->assembleForLog(appends[rpcResultIndex].buffer);
        appends[rpcResultIndex].type = LOG_ENTRY_TYPE_RPCRESULT;
    }

    if (!log.append(appends, (tombstone ? 2 : 1) + (rpcResult ? 1 : 0))) {
        // The log is out of space. Tell the client to retry and hope
        // that the cleaner makes space soon.
        throw RetryException(HERE, 1000, 2000, "Must wait for cleaner");
    }

    if (tombstone) {
        currentHashTableEntry.setReference(appends[0].reference.toInteger());
        log.free(currentReference);
    } else {
        objectMap.insert(key.getHash(), appends[0].reference.toInteger());
    }

    if (rpcResult && rpcResultPtr)
        *rpcResultPtr = appends[rpcResultIndex].reference.toInteger();

    tabletManager->incrementWriteCount(key);
    ++PerfStats::threadStats.writeCount;
    uint32_t valueLength = newObject.getValueLength();
    PerfStats::threadStats.writeObjectBytes += valueLength;
    PerfStats::threadStats.writeKeyBytes +=
            newObject.getKeysAndValueLength() - valueLength;

    TEST_LOG("object: %u bytes, version %lu",
        appends[0].buffer.size(), newObject.getVersion());

    if (tombstone) {
        TEST_LOG("tombstone: %u bytes, version %lu",
            appends[1].buffer.size(), tombstone->getObjectVersion());
    }
    if (rpcResult) {
        TEST_LOG("rpcResult: %u bytes",
            appends[rpcResultIndex].buffer.size());
    }

    {
        uint64_t byteCount = appends[0].buffer.size();
        uint64_t recordCount = 1;
        if (tombstone) {
            byteCount += appends[1].buffer.size();
            recordCount += 1;
        }
        if (rpcResult) {
            byteCount += appends[rpcResultIndex].buffer.size();
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
 * Write the RpcResult log-entry indicating that transaction prepare has failed
 * and transition should be aborted.
 * This function is used in txPrepare or handler of TxRequestAbort.
 * \param rpcResult
 *      This method appends rpcResult to the log atomically with
 *      the other record(s) for the write. The extra record is used to ensure
 *      linearizability.
 * \param[out] rpcResultPtr
 *      The pointer to the RpcResult in log is returned.
 */
void
ObjectManager::writePrepareFail(RpcResult* rpcResult, uint64_t* rpcResultPtr)
{
    Log::AppendVector av;
    *(reinterpret_cast<WireFormat::TxPrepare::Vote*>(
            const_cast<void*>(rpcResult->getResp()))) =
                    WireFormat::TxPrepare::ABORT;
    rpcResult->assembleForLog(av.buffer);
    av.type = LOG_ENTRY_TYPE_RPCRESULT;

    if (!log.hasSpaceFor(av.buffer.size()) || !log.append(&av, 1)) {
        throw RetryException(HERE, 1000, 2000,
                "Log is out of space! Transaction abort-vote wasn't logged.");
        //TODO(seojin): Possible safety violation. Abort-vote is not written.
        //              Network-layer retry may cause in-consistency.
        //              Suspect this is unlikely.
    }

    *rpcResultPtr = av.reference.toInteger();
    TableStats::increment(masterTableMetadata,
            rpcResult->getTableId(),
            av.buffer.size(),
            1);
}

/**
 * Write the RpcResult log-entry with result of Linearizable RPC.
 * This method should be only used for failed RPCs. (So that RpcResult is
 * the only log-entry to write on log.)
 * \param rpcResult
 *      This method appends rpcResult to the log. The record is used to ensure
 *      linearizability.
 * \param[out] rpcResultPtr
 *      The pointer to the RpcResult in log is returned.
 */
void
ObjectManager::writeRpcResultOnly(RpcResult* rpcResult, uint64_t* rpcResultPtr)
{
    Log::AppendVector av;
    rpcResult->assembleForLog(av.buffer);
    av.type = LOG_ENTRY_TYPE_RPCRESULT;

    if (!log.hasSpaceFor(av.buffer.size()) || !log.append(&av, 1)) {
        // The log is out of space. Tell the client to retry and hope
        // that the cleaner makes space soon.
        throw RetryException(HERE, 1000, 2000,
                "Log is out of space! RpcResult wasn't logged. "
                "Cannot just return failure of operation.");
    }

    *rpcResultPtr = av.reference.toInteger();
    TableStats::increment(masterTableMetadata,
            rpcResult->getTableId(),
            av.buffer.size(),
            1);
}

/**
 * Append the provided ParticipantList to the log.
 *
 * \param participantList
 *      ParticipantList to be appended.
 * \param participantListLogRef
 *      Log reference to the appended ParticipantList.
 * \return
 *      STATUS_OK if the ParticipantList could be appended.
 *      STATUS_RETRY otherwise.
 */
Status
ObjectManager::logTransactionParticipantList(ParticipantList& participantList,
                                             uint64_t* participantListLogRef)
{
    Buffer participantListBuffer;
    Log::Reference reference;
    participantList.assembleForLog(participantListBuffer);

    // Write the ParticipantList into the Log, update the table.
    if (!log.append(LOG_ENTRY_TYPE_TXPLIST, participantListBuffer, &reference))
    {
        // The log is out of space. Tell the client to retry and hope
        // that the cleaner makes space soon.
        return STATUS_RETRY;
    }

    *participantListLogRef = reference.toInteger();

    // Participant List records are not accounted for in the table stats.  The
    // assumption is that the Participant List records should occupy a
    // relatively small fraction of the server's log and thus should not
    // significantly affect table stats estimate.

    return STATUS_OK;
}

/**
 * Prepares an operation during transaction prepare stage.
 * It locks the corresponding object and writes logs for PreparedOp
 * and RpcResult (for linearizablity of vote).
 *
 * \param newOp
 *      The preparedOperation to be written to the log. It does not have
 *      a valid version and timestamp. So this function will update the version,
 *      timestamp and the checksum of the object before writing to the log.
 * \param rejectRules
 *      Specifies conditions under which the prepare should be aborted with an
 *      error. May be NULL if no special reject conditions are desired.
 *
 * \param[out] newOpPtr
 *      The pointer to the PreparedOp in log is returned.
 * \param[out] isCommitVote
 *      Vote result after prepare is returned.
 * \param rpcResult
 *      This method appends rpcResult to the log atomically with
 *      the other record(s) for the write. The extra record is used to ensure
 *      linearizability.
 * \param[out] rpcResultPtr
 *      The pointer to the RpcResult in log is returned.
 * \return
 *      STATUS_OK if the object was written. Otherwise, for example,
 *      STATUS_UNKNOWN_TABLE may be returned.
 */
Status
ObjectManager::prepareOp(PreparedOp& newOp, RejectRules* rejectRules,
                uint64_t* newOpPtr, bool* isCommitVote,
                RpcResult* rpcResult, uint64_t* rpcResultPtr)
{
    *isCommitVote = false;
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
    const void *keyString = newOp.object.getKey(0, &keyLength);
    Key key(newOp.object.getTableId(), keyString, keyLength);

    objectMap.prefetchBucket(key.getHash());
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    // If the key is already locked, abort.
    if (lockTable.isLockAcquired(key)) {
        RAMCLOUD_LOG(DEBUG,
                "TxPrepare fail. Key: %.*s, object is already locked",
                keyLength, reinterpret_cast<const char*>(keyString));
        writePrepareFail(rpcResult, rpcResultPtr);
        return STATUS_OK;
    }

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
        }
    }

    if (rejectRules != NULL) {
        Status status = rejectOperation(rejectRules, currentVersion);
        if (status != STATUS_OK) {
            RAMCLOUD_LOG(DEBUG, "TxPrepare fail. Type: %d Key: %.*s, "
                "RejectRule outcome: %s rejectRule.givenVersion %lu "
                "currentVersion %lu",
                    newOp.header.type,
                    keyLength, reinterpret_cast<const char*>(keyString),
                    statusToString(status),
                    rejectRules->givenVersion, currentVersion);
            writePrepareFail(rpcResult, rpcResultPtr);
            return STATUS_OK;
        }
    }

    // Existing objects get a bump in version, new objects start from
    // the next version allocated in the table.
    uint64_t newObjectVersion = (currentVersion == VERSION_NONEXISTENT) ?
            segmentManager.allocateVersion() : currentVersion + 1;

    newOp.object.setVersion(newObjectVersion);
    newOp.object.setTimestamp(WallTime::secondsTimestamp());

    assert(currentVersion == VERSION_NONEXISTENT ||
           newOp.object.getVersion() > currentVersion);

    Log::AppendVector appends[2];

    newOp.assembleForLog(appends[0].buffer);
    appends[0].type = LOG_ENTRY_TYPE_PREP;

    int rpcResultIndex = 1;
    assert(rpcResult);
    rpcResult->assembleForLog(appends[1].buffer);
    appends[1].type = LOG_ENTRY_TYPE_RPCRESULT;

    if (!log.hasSpaceFor(appends[0].buffer.size() + appends[1].buffer.size())) {
        // We must bound the amount of live data to ensure deletes are possible
        writePrepareFail(rpcResult, rpcResultPtr);
        return STATUS_OK;
    }


    if (!log.append(appends, 2)) {
        writePrepareFail(rpcResult, rpcResultPtr);
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_OK;
    }

    // Lock the key now that we know the prepare op has been logged.
    if (!lockTable.tryAcquireLock(key, appends[0].reference)) {
        // If we were not able to aquire the lock there is a bug somewhere.
        RAMCLOUD_LOG(ERROR,
                     "While preparing transaction, lock already acquired "
                     "after checking lock was free. Key: %.*s",
                     keyLength, reinterpret_cast<const char*>(keyString));
    }

    *newOpPtr = appends[0].reference.toInteger();

    assert(rpcResult && rpcResultPtr);
    *rpcResultPtr = appends[rpcResultIndex].reference.toInteger();

    //tabletManager->incrementWriteCount(key);

    TEST_LOG("preparedOp: %u bytes", appends[0].buffer.size());
    TEST_LOG("rpcResult: %u bytes", appends[rpcResultIndex].buffer.size());

    {
        uint64_t byteCount = appends[0].buffer.size();
        uint64_t recordCount = 2;
        byteCount += appends[rpcResultIndex].buffer.size();

        TableStats::increment(masterTableMetadata,
                              tablet.tableId,
                              byteCount,
                              recordCount);
    }

    *isCommitVote = true;
    return STATUS_OK;
}

/**
 * Process prepare request for ReadOnly operation.
 * It just checks the lock of the corresponding object and compares version
 * (by rejectRules). It writes no data on the log.
 *
 * \param newOp
 *      The preparedOperation to be written to the log. It does not have
 *      a valid version and timestamp. So this function will update the version,
 *      timestamp and the checksum of the object before writing to the log.
 * \param rejectRules
 *      Specifies conditions under which the prepare should be aborted with an
 *      error. May be NULL if no special reject conditions are desired.
 *
 * \param[out] isCommitVote
 *      Vote result after prepare is returned.
 * \return
 *      STATUS_OK if we can decide commit-vote or abort-vote.
 *      STATUS_UNKNOWN_TABLE may be returned.
 */
Status
ObjectManager::prepareReadOnly(PreparedOp& newOp, RejectRules* rejectRules,
                bool* isCommitVote)
{
    *isCommitVote = false;
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
    const void *keyString = newOp.object.getKey(0, &keyLength);
    Key key(newOp.object.getTableId(), keyString, keyLength);

    objectMap.prefetchBucket(key.getHash());
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    // If the key is already locked, abort.
    if (lockTable.isLockAcquired(key)) {
        RAMCLOUD_LOG(DEBUG,
                "TxPrepare(readOnly) fail. Key: %.*s, object is already locked",
                keyLength, reinterpret_cast<const char*>(keyString));
        return STATUS_OK;
    }

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
        }
    }

    if (rejectRules != NULL) {
        Status status = rejectOperation(rejectRules, currentVersion);
        if (status != STATUS_OK) {
            RAMCLOUD_LOG(DEBUG, "TxPrepare(readOnly) fail. Type: %d Key: %.*s, "
                "RejectRule outcome: %s rejectRule.givenVersion %lu "
                "currentVersion %lu",
                    newOp.header.type,
                    keyLength, reinterpret_cast<const char*>(keyString),
                    statusToString(status),
                    rejectRules->givenVersion, currentVersion);
            return STATUS_OK;
        }
    }
    *isCommitVote = true;
    return STATUS_OK;
}

/**
 * Try to acquire transaction lock for an object.
 * This function is used while finalizing recovery.
 *
 * \param objToLock
 *      Object which contains tableId and key which we lock for.
 * \param ref
 *      Log reference to the PrepareOp object that represents the lock.
 */
Status
ObjectManager::tryGrabTxLock(Object& objToLock, Log::Reference& ref)
{
    uint16_t keyLength = 0;
    const void *keyString = objToLock.getKey(0, &keyLength);
    Key key(objToLock.getTableId(), keyString, keyLength);

    objectMap.prefetchBucket(key.getHash());
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;

    lockTable.tryAcquireLock(key, ref);
    return STATUS_OK;
}

/**
 * Write a persistent transaction decision record to the log.
 *
 * \param record
 *      The transaction decision record that will be written to the log.
 * \return
 *      the status of the operation. If the tablet is in the NORMAL
 *      state, it returns STATUS_OK.
 */
Status
ObjectManager::writeTxDecisionRecord(TxDecisionRecord& record)
{
    // Lock the HashTableBucket to ensure that migration doesn't rip the tablet
    // out from under us.
    objectMap.prefetchBucket(record.getKeyHash());
    uint64_t dummy;
    uint64_t bucketIndex = objectMap.findBucketIndex(
            objectMap.getNumBuckets(),
            record.getKeyHash(),
            &dummy);
    HashTableBucketLock lock(*this, bucketIndex);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(
            record.getTableId(), record.getKeyHash(), &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    Buffer recordBuffer;
    record.assembleForLog(recordBuffer);

    if (!log.append(LOG_ENTRY_TYPE_TXDECISION, recordBuffer)) {
        return STATUS_RETRY;
    }

    TEST_LOG("tansactionDecisionRecord: %u bytes", recordBuffer.size());
    TableStats::increment(masterTableMetadata,
                          tablet.tableId,
                          recordBuffer.size(),
                          1);

    return STATUS_OK;
}

/**
 * Transaction commit for a prepared read operation on an object.
 * All this function does is removing TX lock held on the object.
 * This is also used for aborting.
 *
 * \param op
 *      Prepared read operation to commit.
 * \param refToPreparedOp
 *      Reference to the preparedOp entry in log.
 *
 * \return
 *      Returns STATUS_OK if the remove succeeded. Other status values indicate
 *      different failures (tablet doesn't exist, reject rules applied, etc).
 */
Status
ObjectManager::commitRead(PreparedOp& op, Log::Reference& refToPreparedOp)
{
    uint16_t keyLength = 0;
    const void *keyString = op.object.getKey(0, &keyLength);
    Key key(op.object.getTableId(), keyString, keyLength);

    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    PreparedOpTombstone prepOpTombstone(op, log.getSegmentId(refToPreparedOp));

    Buffer prepTombBuffer;
    prepOpTombstone.assembleForLog(prepTombBuffer);

    if (!log.append(LOG_ENTRY_TYPE_PREPTOMB, prepTombBuffer)) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_RETRY;
    }

    // Release the lock now that the transaction is committed to log.
    if (!lockTable.releaseLock(key, refToPreparedOp)) {
        // If we were not able to release the lock there is a bug somewhere.
        RAMCLOUD_LOG(ERROR,
                     "While committing transaction, lock already released "
                     "when it should not be. Key: %.*s PrepareRef: %lu",
                     keyLength, reinterpret_cast<const char*>(keyString),
                     refToPreparedOp.toInteger());
    }

    TableStats::increment(masterTableMetadata,
            prepOpTombstone.header.tableId,
            prepTombBuffer.size(),
            1);
    log.free(refToPreparedOp);
    return STATUS_OK;
}

/**
 * Transaction commit for a prepared remove operation on an object.
 *
 * \param op
 *      Prepared remove operation to commit.
 * \param refToPreparedOp
 *      Reference to the preparedOp entry in log.
 *
 * \param[out] removedObjBuffer
 *      If non-NULL, pointer to the buffer in log for the object being removed
 *      is returned.
 * \return
 *      Returns STATUS_OK if the remove succeeded. Other status values indicate
 *      different failures (tablet doesn't exist, reject rules applied, etc).
 */
Status
ObjectManager::commitRemove(PreparedOp& op,
                            Log::Reference& refToPreparedOp,
                            Buffer* removedObjBuffer)
{
    uint16_t keyLength = 0;
    const void *keyString = op.object.getKey(0, &keyLength);
    Key key(op.object.getTableId(), keyString, keyLength);

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
        return rejectOperation(&defaultRejectRules, VERSION_NONEXISTENT);
    }

    Object object(buffer);

    // Return a pointer to the buffer in log for the object being removed.
    if (removedObjBuffer != NULL) {
        removedObjBuffer->appendExternal(&buffer);
    }

    PreparedOpTombstone prepOpTombstone(op, log.getSegmentId(refToPreparedOp));
    ObjectTombstone tombstone(object,
                              log.getSegmentId(reference),
                              WallTime::secondsTimestamp());

    Log::AppendVector appends[2];
    tombstone.assembleForLog(appends[0].buffer);
    appends[0].type = LOG_ENTRY_TYPE_OBJTOMB;

    prepOpTombstone.assembleForLog(appends[1].buffer);
    appends[1].type = LOG_ENTRY_TYPE_PREPTOMB;

    // Write the tombstone into the Log, increment the tablet version
    // number, and remove from the hash table.
    if (!log.append(appends, 2)) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_RETRY;
    }

    // Release the lock now that the transaction is committed to log.
    if (!lockTable.releaseLock(key, refToPreparedOp)) {
        // If we were not able to release the lock there is a bug somewhere.
        RAMCLOUD_LOG(ERROR,
                     "While committing transaction, lock already released "
                     "when it should not be. Key: %.*s PrepareRef: %lu",
                     keyLength, reinterpret_cast<const char*>(keyString),
                     refToPreparedOp.toInteger());
    }

    {
        uint64_t byteCount = appends[0].buffer.size();
        uint64_t recordCount = 2;
        byteCount += appends[1].buffer.size();

        TableStats::increment(masterTableMetadata,
                              tombstone.getTableId(),
                              byteCount,
                              recordCount);
    }

    segmentManager.raiseSafeVersion(object.getVersion() + 1);
    log.free(reference);
    log.free(refToPreparedOp);
    remove(lock, key);
    return STATUS_OK;
}

/**
 * Transaction commit for a prepared write operation on an object.
 *
 * \param op
 *      Prepared write operation to commit.
 * \param refToPreparedOp
 *      Reference to the preparedOp entry in log.
 *
 * \param[out] removedObjBuffer
 *      If non-NULL, pointer to the buffer in log for the object being removed
 *      is returned.
 * \return
 *      STATUS_OK if the object was written. Otherwise, for example,
 *      STATUS_UKNOWN_TABLE may be returned.
 */
Status
ObjectManager::commitWrite(PreparedOp& op,
                           Log::Reference& refToPreparedOp,
                           Buffer* removedObjBuffer)
{
    uint16_t keyLength = 0;
    const void *keyString = op.object.getKey(0, &keyLength);
    bool newKey = false;
    Key key(op.object.getTableId(), keyString, keyLength);

    objectMap.prefetchBucket(key.getHash());
    HashTableBucketLock lock(*this, key);

    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet))
        return STATUS_UNKNOWN_TABLET;
    if (tablet.state != TabletManager::NORMAL)
        return STATUS_UNKNOWN_TABLET;

    LogEntryType type;
    Buffer buffer;
    Log::Reference oldReference;
    HashTable::Candidates currentHashTableEntry;
    if (!lookup(lock, key, type, buffer, NULL,
                &oldReference, &currentHashTableEntry) ||
            type != LOG_ENTRY_TYPE_OBJ) {
        newKey = true;
    }

    Tub<Object> oldObject;
    if (!newKey) {
        oldObject.construct(buffer);
        // Return a pointer to the buffer in log for the object being removed.
        if (removedObjBuffer != NULL) {
            removedObjBuffer->appendExternal(&buffer);
        }
    }

    PreparedOpTombstone prepOpTombstone(op, log.getSegmentId(refToPreparedOp));
    Tub<ObjectTombstone> tombstone;
    if (!newKey) {
        tombstone.construct(*oldObject,
                            log.getSegmentId(oldReference),
                            WallTime::secondsTimestamp());
    }

    uint64_t byteCount = 0;
    uint64_t recordCount = 0;
    int size = 2 + (newKey ? 0 : 1);
    Log::AppendVector appends[size];

    prepOpTombstone.assembleForLog(appends[0].buffer);
    appends[0].type = LOG_ENTRY_TYPE_PREPTOMB;
    byteCount += appends[0].buffer.size();
    recordCount++;

    op.object.assembleForLog(appends[1].buffer);
    appends[1].type = LOG_ENTRY_TYPE_OBJ;
    byteCount += appends[1].buffer.size();
    recordCount++;

    if (!newKey) {
        tombstone->assembleForLog(appends[2].buffer);
        appends[2].type = LOG_ENTRY_TYPE_OBJTOMB;
        byteCount += appends[2].buffer.size();
        recordCount++;
    }

    if (!log.hasSpaceFor(appends[1].buffer.size())) {
        // We must bound the amount of live data to ensure deletes are possible
        throw RetryException(HERE, 1000, 2000, "Log is out of space!");
    }

    if (!log.append(appends, size)) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_RETRY;
    }

    // Release the lock now that the transaction is committed to log.
    if (!lockTable.releaseLock(key, refToPreparedOp)) {
        // If we were not able to release the lock there is a bug somewhere.
        RAMCLOUD_LOG(ERROR,
                     "While committing transaction, lock already released "
                     "when it should not be. Key: %.*s PrepareRef: %lu",
                     keyLength, reinterpret_cast<const char*>(keyString),
                     refToPreparedOp.toInteger());
    }

    TableStats::increment(masterTableMetadata,
                          prepOpTombstone.header.tableId,
                          byteCount,
                          recordCount);
    ++PerfStats::threadStats.writeCount;
    uint32_t valueLength = op.object.getValueLength();
    PerfStats::threadStats.writeObjectBytes += valueLength;
    PerfStats::threadStats.writeKeyBytes +=
            op.object.getKeysAndValueLength() - valueLength;

    log.free(refToPreparedOp);

    if (!newKey) {
        currentHashTableEntry.setReference(appends[1].reference.toInteger());
        log.free(oldReference);
    } else {
        objectMap.insert(key.getHash(), appends[1].reference.toInteger());
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

    if (!log.hasSpaceFor(logBuffer->size())) {
        // We must bound the amount of live data to ensure deletes are possible
        return false;
    }

    // This array will hold the references of all the entries
    // that get written to the log. This will be used
    // subsequently to update the hash table
    Log::Reference references[numEntries];

    // atomically flush all the entries to the log
    if (!log.append(logBuffer, references, numEntries)) {
        return false;
        // JIRA Issue: RAM-675
        // How to propagate this error
        // into the tree code and then back to the caller
        // of the tree code??
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

            uint64_t tableId = object.getTableId();
            Key key(tableId, keyString, keyLength);

            objectMap.prefetchBucket(key.getHash());
            HashTableBucketLock lock(*this, key);

            if (lookup(lock, key, currentType, currentBuffer, &currentVersion,
                       &currentReference, &currentHashTableEntry)) {

                if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
                    CleanupParameters params = { this , &lock };
                    removeIfTombstone(currentReference.toInteger(), &params);
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

            tabletManager->incrementWriteCount(key);
            TableStats::increment(masterTableMetadata,
                                  tableId,
                                  entryLength, 1);

        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {

            ObjectTombstone tombstone(*logBuffer, objectOffset,
                                      serializedObjectLength);

            KeyLength keyLength = tombstone.getKeyLength();
            const void* keyString = tombstone.getKey();

            uint64_t tableId = tombstone.getTableId();
            Key key(tableId, keyString, keyLength);

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

            tabletManager->incrementWriteCount(key);
            TableStats::increment(masterTableMetadata,
                                  tableId,
                                  entryLength, 1);

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
    if (tablet.state != TabletManager::NORMAL) {
        if (tablet.state == TabletManager::LOCKED_FOR_MIGRATION)
            DIE("Tablet is currently locked for migration!");
        return STATUS_UNKNOWN_TABLET;
    }

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
    uint32_t valueOffset = 0;

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
    if (tablet.state != TabletManager::NORMAL) {
        if (tablet.state == TabletManager::LOCKED_FOR_MIGRATION)
            throw RetryException(HERE, 1000, 2000,
                    "Tablet is currently locked for migration!");
        return STATUS_UNKNOWN_TABLET;
    }

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
    else if (type == LOG_ENTRY_TYPE_TXDECISION)
        return getTxDecisionRecordTimestamp(buffer);
    else
        return 0;
}

/**
 * Relocate and update metadata for an object, tombstone, etc. that is being
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
        relocateTombstone(oldBuffer, oldReference, relocator);
    else if (type == LOG_ENTRY_TYPE_RPCRESULT)
        relocateRpcResult(oldBuffer, relocator);
    else if (type == LOG_ENTRY_TYPE_PREP)
        relocatePreparedOp(oldBuffer, oldReference, relocator);
    else if (type == LOG_ENTRY_TYPE_PREPTOMB)
        relocatePreparedOpTombstone(oldBuffer, relocator);
    else if (type == LOG_ENTRY_TYPE_TXDECISION)
        relocateTxDecisionRecord(oldBuffer, relocator);
    else if (type == LOG_ENTRY_TYPE_TXPLIST)
        relocateTxParticipantList(oldBuffer, relocator);
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
int
ObjectManager::RemoveTombstonePoller::poll()
{
    if (lastReplaySegmentCount == objectManager->replaySegmentReturnCount &&
      currentBucket == 0) {
        return 0;
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
    return 1;
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
        } else if (type == LOG_ENTRY_TYPE_RPCRESULT) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            RpcResult rpcResult(buffer);
            result += format("%srpcResult at offset %u, length %u with tableId "
                    "%lu, keyHash 0x%016" PRIX64 ", leaseId %lu, rpcId %lu",
                    separator, it.getOffset(), it.getLength(),
                    rpcResult.getTableId(), rpcResult.getKeyHash(),
                    rpcResult.getLeaseId(), rpcResult.getRpcId());
        } else if (type == LOG_ENTRY_TYPE_PREP) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            PreparedOp op(buffer, 0, buffer.size());
            result += format("%spreparedOp at offset %u, length %u with "
                    "tableId %lu, key '%.*s', leaseId %lu, rpcId %lu",
                    separator, it.getOffset(), it.getLength(),
                    op.object.getTableId(), op.object.getKeyLength(),
                    static_cast<const char*>(op.object.getKey()),
                    op.header.clientId, op.header.rpcId);
        } else if (type == LOG_ENTRY_TYPE_PREPTOMB) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            PreparedOpTombstone opTomb(buffer, 0);
            result += format("%spreparedOpTombstone at offset %u, length %u "
                    "with tableId %lu, keyHash 0x%016" PRIX64 ", leaseId %lu, "
                    "rpcId %lu",
                    separator, it.getOffset(), it.getLength(),
                    opTomb.header.tableId, opTomb.header.keyHash,
                    opTomb.header.clientLeaseId, opTomb.header.rpcId);
        } else if (type == LOG_ENTRY_TYPE_TXDECISION) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            TxDecisionRecord decisionRecord(buffer);
            result += format("%stxDecision at offset %u, length %u with tableId"
                    " %lu, keyHash 0x%016" PRIX64 ", leaseId %lu",
                    separator, it.getOffset(), it.getLength(),
                    decisionRecord.getTableId(), decisionRecord.getKeyHash(),
                    decisionRecord.getLeaseId());

        } else if (type == LOG_ENTRY_TYPE_TXPLIST) {
            Buffer buffer;
            it.appendToBuffer(buffer);
            ParticipantList participantList(buffer);
            result += format("%sparticipantList at offset %u, length %u with "
                    "TxId: (leaseId %lu, rpcId %lu) containing %u entries",
                    separator, it.getOffset(), it.getLength(),
                    participantList.getTransactionId().clientLeaseId,
                    participantList.getTransactionId().clientTransactionId,
                    participantList.header.participantCount);
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
 * Method used by the Lod to determine the age of a TxDecisionRecord.
 *
 * \param buffer
 *      Buffer pointing to the transaction decision record from which the
 *      timestamp is to be extracted.
 * \return
 *      The record's creation timestamp.
 */
uint32_t
ObjectManager::getTxDecisionRecordTimestamp(Buffer& buffer)
{
    TxDecisionRecord record(buffer);
    return record.getTimestamp();
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
            buffer.append(&candidateBuffer);
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
 * Returns true iff the given key still points at the given reference.
 *
 * \param key
 *      The key to look up in the hash table.
 * \param reference
 *      The reference to verify for presence in the hash table.
 */
bool
ObjectManager::keyPointsAtReference(Key& key, AbstractLog::Reference reference)
{

    HashTableBucketLock lock(*this, key);

    HashTable::Candidates candidates;
    objectMap.lookup(key.getHash(), candidates);
    while (!candidates.isDone()) {
        if (candidates.getReference() != reference.toInteger()) {
            candidates.next();
            continue;
        }
        return true;
    }
    return false;
}


/**
 * Method used by the LogCleaner when it's cleaning a Segment and comes across
 * an RpcResult.
 *
 * This method will decide if the RpcResult is still alive. If it is, it must
 * move the record to a new location and update the UnackedRpcResults module.
 *
 * \param oldBuffer
 *      Buffer pointing to the RpcResult's current location, which will soon be
 *      invalidated.
 * \param relocator
 *      The relocator may be used to store the RpcResult in a new location if it
 *      is still alive. It also provides a reference to the new location and
 *      keeps track of whether this call wanted the RpcResult anymore or not.
 *
 *      It is possible that relocation may fail (because more memory needs to
 *      be allocated). In this case, the callback should just return. The
 *      cleaner will note the failure, allocate more memory, and try again.
 */
void
ObjectManager::relocateRpcResult(Buffer& oldBuffer,
        LogEntryRelocator& relocator)
{
    RpcResult rpcResult(oldBuffer);

    // See if the rpc that this record records is still not acked and thus needs
    // to be kept.
    bool keepRpcResult = !unackedRpcResults->isRpcAcked(
            rpcResult.getLeaseId(), rpcResult.getRpcId());

    if (keepRpcResult) {
        // Try to relocate it. If it fails, just return. The cleaner will
        // allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_RPCRESULT, oldBuffer))
            return;

        unackedRpcResults->recordCompletion(
                rpcResult.getLeaseId(),
                rpcResult.getRpcId(),
                reinterpret_cast<void*>(
                        relocator.getNewReference().toInteger()),
                true);
    } else {
        // Rpc Record will be dropped/"cleaned" so stats should be updated.
        TableStats::decrement(masterTableMetadata,
                              rpcResult.getTableId(),
                              oldBuffer.size(),
                              1);
    }
}

/**
 * Method used by the LogCleaner when it's cleaning a Segment and comes across
 * an PreparedOp.
 *
 * This method will decide if the PreparedOp is still alive. If it is, it must
 * move the record to a new location and update the PreparedWrites module.
 *
 * \param oldBuffer
 *      Buffer pointing to the PreparedOp's current location, which will soon be
 *      invalidated.
 * \param oldReference
 *      Reference to the old PreparedOp in the log. This is used to properly
 *      update the LockTable if needed.
 * \param relocator
 *      The relocator may be used to store the PreparedOp in a new location if
 *      it is still alive. It also provides a reference to the new location and
 *      keeps track of whether this call wanted the PreparedOp anymore or not.
 *
 *      It is possible that relocation may fail (because more memory needs to
 *      be allocated). In this case, the callback should just return. The
 *      cleaner will note the failure, allocate more memory, and try again.
 */
void
ObjectManager::relocatePreparedOp(Buffer& oldBuffer,
        Log::Reference oldReference,
        LogEntryRelocator& relocator)
{
    PreparedOp op(oldBuffer, 0, oldBuffer.size());
    Key key(op.object.getTableId(),
            op.object.getKey(),
            op.object.getKeyLength());
    HashTableBucketLock lock(*this, key);

    uint64_t opPtr = preparedOps->peekOp(op.header.clientId,
                                            op.header.rpcId);
    if (opPtr) {
        // Try to relocate it. If it fails, just return. The cleaner will
        // allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_PREP, oldBuffer))
            return;

        preparedOps->updatePtr(op.header.clientId,
                                  op.header.rpcId,
                                  relocator.getNewReference().toInteger());
        // Move transaction LockTable lock to new location.
        if (lockTable.releaseLock(key, oldReference)) {
            lockTable.acquireLock(key, relocator.getNewReference());
        }
    } else {
        // PreparedOp will be dropped/"cleaned" so stats should be updated.
        TableStats::decrement(masterTableMetadata,
                              op.object.getTableId(),
                              oldBuffer.size(),
                              1);
    }
}

/**
 * Method used by the LogCleaner when it's cleaning a Segment and comes across
 * an PreparedOpTombstone.
 *
 * This method will decide if the PreparedOpTombstone is still alive.
 * If it is, it must move the record to a new location and update
 * the PreparedWrites module.
 *
 * \param oldBuffer
 *      Buffer pointing to the PreparedOp's current location, which will soon be
 *      invalidated.
 * \param relocator
 *      The relocator may be used to store the PreparedOp in a new location if
 *      it is still alive. It also provides a reference to the new location and
 *      keeps track of whether this call wanted the PreparedOp anymore or not.
 *
 *      It is possible that relocation may fail (because more memory needs to
 *      be allocated). In this case, the callback should just return. The
 *      cleaner will note the failure, allocate more memory, and try again.
 */
void
ObjectManager::relocatePreparedOpTombstone(Buffer& oldBuffer,
        LogEntryRelocator& relocator)
{
    PreparedOpTombstone opTomb(oldBuffer, 0);

    if (log.segmentExists(opTomb.header.segmentId)) {
        // Try to relocate it. If it fails, just return. The cleaner will
        // allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_PREPTOMB, oldBuffer))
            return;
    } else {
        // Tombstone will be dropped/"cleaned" so stats should be updated.
        TableStats::decrement(masterTableMetadata,
                              opTomb.header.tableId,
                              oldBuffer.size(),
                              1);
    }
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
 * \param oldReference
 *      Reference to the old tombstone in the log. This is used to check whether
 *      the hash table still points at the tombstone, because we must copy the
 *      tombstone and update the reference if that is the case.
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
ObjectManager::relocateTombstone(Buffer& oldBuffer, Log::Reference oldReference,
                LogEntryRelocator& relocator)
{
    ObjectTombstone tomb(oldBuffer);

    // See if the object this tombstone refers to is still in the log.
    bool objectExists = log.segmentExists(tomb.getSegmentId());
    bool hashReferenceExists = false;

    // Check if the hash table still references this tombstone and update the
    // pointer if it does. For efficiency, we perform the lookup here so we can
    // do the update inline if it turns out to be necessary.
    Key key(LOG_ENTRY_TYPE_OBJTOMB, oldBuffer);
    HashTableBucketLock lock(*this, key);
    HashTable::Candidates candidates;
    objectMap.lookup(key.getHash(), candidates);
    while (!candidates.isDone()) {
        if (candidates.getReference() != oldReference.toInteger()) {
            candidates.next();
            continue;
        }
        hashReferenceExists = true;
        break;
    }

    bool keepNewTomb = objectExists || hashReferenceExists;

    if (keepNewTomb) {
        // Try to relocate it. If it fails, just return. The cleaner will
        // allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_OBJTOMB, oldBuffer))
            return;
        if (hashReferenceExists)
            candidates.setReference(relocator.getNewReference().toInteger());
    } else {
        // Tombstone will be dropped/"cleaned" so stats should be updated.
        TableStats::decrement(masterTableMetadata,
                              tomb.getTableId(),
                              oldBuffer.size(),
                              1);
    }
}


/**
 * Method used by the LogCleaner when it's cleaning a Segment and comes across
 * an TxDecisionRecord.
 *
 * This method will decide if the TxDecisionRecord still needs to be kept. If
 * so, it must move the record to a new location.
 *
 * \param oldBuffer
 *      Buffer pointing to the TxDecisionRecord's current location.
 * \param relocator
 *      The relocator may be used to store the TxDecisionRecord in a new
 *      location if it is still alive. It also provides a reference to the new
 *      location and keeps track of whether this call wanted the
 *      TxDecisionRecord anymore or not.
 *
 *      It is possible that relocation may fail (because more memory needs to
 *      be allocated). In this case, the callback should just return. The
 *      cleaner will note the failure, allocate more memory, and try again.
 */
void
ObjectManager::relocateTxDecisionRecord(
        Buffer& oldBuffer, LogEntryRelocator& relocator)
{
    TxDecisionRecord record(oldBuffer);

    bool needed = txRecoveryManager->isTxDecisionRecordNeeded(record);
    if (needed) {
        // Try to relocate it. If it fails, just return. The cleaner will
        // allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_TXDECISION, oldBuffer))
            return;
    } else {
        // Decision Record will be dropped/"cleaned" so stats should be updated.
        TableStats::decrement(masterTableMetadata,
                              record.getTableId(),
                              oldBuffer.size(),
                              1);
    }
}

/**
 * Method used by the LogCleaner when it's cleaning a Segment and comes across
 * a ParticipantList record.
 *
 * This method will decide if the ParticipantList is still alive. If it is, it
 * must move the record to a new location and update the PreparedOps module.
 *
 * \param oldBuffer
 *      Buffer pointing to the ParticipantList's current location, which will
 *      soon be invalidated.
 * \param relocator
 *      The relocator may be used to store the ParticipantList in a new location
 *      if it is still alive. It also provides a reference to the new location
 *      and keeps track of whether this call wanted the ParticipantList anymore
 *      or not.
 *
 *      It is possible that relocation may fail (because more memory needs to
 *      be allocated). In this case, the callback should just return. The
 *      cleaner will note the failure, allocate more memory, and try again.
 */
void
ObjectManager::relocateTxParticipantList(Buffer& oldBuffer,
        LogEntryRelocator& relocator)
{
    ParticipantList participantList(oldBuffer);

    // See if this transaction is still going on and thus if the participant
    // list should be kept.
    TransactionId txId = participantList.getTransactionId();

    bool keep = !unackedRpcResults->isRpcAcked(txId.clientLeaseId,
                                               txId.clientTransactionId);

    if (keep) {
        // Try to relocate it. If it fails, just return. The cleaner will
        // allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_TXPLIST, oldBuffer))
            return;

        unackedRpcResults->recordCompletion(
                txId.clientLeaseId,
                txId.clientTransactionId,
                reinterpret_cast<void*>(
                        relocator.getNewReference().toInteger()),
                true);
    } else {
        // Participant List will be dropped/"cleaned"

        // Participant List records are not accounted for in the table stats.
        // The assumption is that the Participant List records should occupy a
        // relatively small fraction of the server's log and thus should not
        // significantly affect table stats estimate.
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
