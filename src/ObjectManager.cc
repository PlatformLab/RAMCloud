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

#include "Buffer.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "Enumeration.h"
#include "EnumerationIterator.h"
#include "LogEntryRelocator.h"
#include "ObjectManager.h"
#include "ShortMacros.h"
#include "RawMetrics.h"
#include "Tub.h"
#include "ProtoBuf.h"
#include "Segment.h"
#include "Transport.h"
#include "WallTime.h"

namespace RAMCloud {

/**
 * Construct an ObjectManager.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param serverId
 *      ServerId of the master server that is instantiating this object manager.
 * \param config
 *      Contains various parameters that configure the operation of this server.
 */
ObjectManager::ObjectManager(Context* context,
                             ServerId serverId,
                             const ServerConfig* config)
    : tablets() 
    , context(context)
    , config(config)
    , replicaManager(context, serverId,
                     config->master.numReplicas,
                     config->master.useMinCopysets)
    , allocator(config)
    , segmentManager(context, config, serverId,
                     allocator, replicaManager)
    , log(context, config, this, &segmentManager, &replicaManager)
    , objectMap(config->master.hashTableBytes / HashTable::bytesPerCacheLine())
    , anyWrites(false)
    , hashTableBucketLocks()
{
    replicaManager.startFailureMonitor();

    if (!config->master.disableLogCleaner)
        log.enableCleaner();
}

ObjectManager::~ObjectManager()
{
    replicaManager.haltFailureMonitor();

    std::set<TabletsOnMaster*> tables;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
        tables.insert(reinterpret_cast<TabletsOnMaster*>(tablet.user_data()));
    foreach (TabletsOnMaster* table, tables)
        delete table;
}

/**
 * This method will do everything needed to store an object associated with
 * a particular key. This includes allocating or incrementing version numbers,
 * writing a tombstone if a previous version exists, storing to the log,
 * and adding or replacing an entry in the hash table.
 *
 * \param key
 *      Key that will refer to the object being stored.
 * \param rejectRules
 *      Specifies conditions under which the write should be aborted with an
 *      error.
 *
 *      Must not be NULL. The reason this is a pointer and not a reference is
 *      to (dubiously?) work around an issue where we pass in from a packed
 *      Rpc wire format struct.
 * \param value
 *      The value portion of the key-value pair that a stored object represents.
 *      This is an uninterpreted sequence of bytes.
 * \param outVersion
 *      The version number of the new object is returned here. If the operation
 *      was successful this will be the new version for the object; if this
 *      object has ever existed previously the new version is guaranteed to be
 *      greater than any previous version of the object. If the operation failed
 *      then the version number returned is the current version of the object,
 *      or VERSION_NONEXISTENT if the object does not exist.
 *
 *      Must not be NULL. The reason this is a pointer and not a reference is
 *      to (dubiously?) work around an issue where we pass out to a packed
 *      Rpc wire format struct.
 * \param sync
 *      If true, this write will be replicated to backups before return.
 *      If false, the replication may happen sometime later.
 * \return
 *      STATUS_OK if the object was written. Otherwise, for example,
 *      STATUS_UKNOWN_TABLE may be returned.
 */
Status
ObjectManager::writeObject(Key& key,
                           Buffer& value,
                           RejectRules& rejectRules,
                           uint64_t* outVersion)
{
    TabletsOnMaster* table = getTable(key);
    if (table == NULL)
        return STATUS_UNKNOWN_TABLET;

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
                transportManager.getSession(backup.service_locator().c_str());
        }
    }

    HashTableBucketLock lock(*this, key);

    LogEntryType currentType = LOG_ENTRY_TYPE_INVALID;
    Buffer currentBuffer;
    Log::Reference currentReference;
    uint64_t currentVersion = VERSION_NONEXISTENT;

    if (lookup(key, currentType, currentBuffer, NULL, &currentReference)) {
        if (currentType == LOG_ENTRY_TYPE_OBJTOMB) {
            recoveryCleanup(currentReference.toInteger(), this);
        } else {
            Object currentObject(currentBuffer);
            currentVersion = currentObject.getVersion();
        }
    }

    Status status = rejectOperation(rejectRules, currentVersion);
    if (status != STATUS_OK) {
        *outVersion = currentVersion;
        return status;
    }

    // Existing objects get a bump in version, new objects start from
    // the next version allocated in the table.
    uint64_t newObjectVersion = (currentVersion == VERSION_NONEXISTENT) ?
            segmentManager.allocateVersion() : currentVersion + 1;

    Object newObject(key, value, newObjectVersion, WallTime::secondsTimestamp());

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

    newObject.serializeToBuffer(appends[0].buffer);
    appends[0].type = LOG_ENTRY_TYPE_OBJ;
    appends[0].timestamp = newObject.getTimestamp();

    if (tombstone) {
        tombstone->serializeToBuffer(appends[1].buffer);
        appends[1].type = LOG_ENTRY_TYPE_OBJTOMB;
        appends[1].timestamp = tombstone->getTimestamp();
    }

    if (!log.append(appends, tombstone ? 2 : 1)) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_RETRY;
    }

    replace(key, appends[0].reference);
    if (tombstone)
        log.free(currentReference);
    *outVersion = newObject.getVersion();
    return STATUS_OK;
}

/**
 * Sync any previous writes.
 */
void
ObjectManager::syncWrites()
{
    log.sync();
}

Status
ObjectManager::readObject(Key& key, Buffer* outBuffer, RejectRules& rejectRules, uint64_t* outVersion)
{
    HashTableBucketLock lock(*this, key);

    // We must return table doesn't exist if the table does not exist. Also, we
    // might have an entry in the hash table that's invalid because its tablet
    // no longer lives here.
    if (getTable(key) == NULL)
        return STATUS_UNKNOWN_TABLET;

    LogEntryType type;
    bool found = lookup(key, type, *outBuffer, outVersion);
    if (!found || type != LOG_ENTRY_TYPE_OBJ)
        return STATUS_OBJECT_DOESNT_EXIST;

    Status status = rejectOperation(rejectRules, *outVersion);
    if (status != STATUS_OK)
        return status;

    return STATUS_OK;
}

Status
ObjectManager::removeObject(Key& key, RejectRules& rejectRules)
{
    TabletsOnMaster* table = getTable(key);
    if (table == NULL)
        return STATUS_UNKNOWN_TABLET;

    HashTableBucketLock lock(*this, key);

    LogEntryType type;
    Buffer buffer;
    Log::Reference reference;
    if (!lookup(key, type, buffer, NULL, &reference) || type != LOG_ENTRY_TYPE_OBJ)
        return rejectOperation(rejectRules, VERSION_NONEXISTENT);

    Object object(buffer);

    // Abort if we're trying to delete the wrong version.
    Status status = rejectOperation(rejectRules, object.getVersion());
    if (status != STATUS_OK)
        return status;

    ObjectTombstone tombstone(object,
                              log.getSegmentId(reference),
                              WallTime::secondsTimestamp());
    Buffer tombstoneBuffer;
    tombstone.serializeToBuffer(tombstoneBuffer);

    // Write the tombstone into the Log, increment the tablet version
    // number, and remove from the hash table.
    if (!log.append(LOG_ENTRY_TYPE_OBJTOMB,
                     tombstone.getTimestamp(),
                     tombstoneBuffer)) {
        // The log is out of space. Tell the client to retry and hope
        // that either the cleaner makes space soon or we shift load
        // off of this server.
        return STATUS_RETRY;
    }
    log.sync();

    segmentManager.raiseSafeVersion(object.getVersion() + 1);
    log.free(reference);
    remove(key);
    return STATUS_OK;
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
        const Object::SerializedForm* obj =
            it->getContiguous<Object::SerializedForm>(NULL, 0);
        Key key(obj->tableId, obj->keyAndData, obj->keyLength);
        objectMap.prefetchBucket(key);
    } else if (it->getType() == LOG_ENTRY_TYPE_OBJTOMB) {
        const ObjectTombstone::SerializedForm* tomb =
            it->getContiguous<ObjectTombstone::SerializedForm>(NULL, 0);
        Key key(tomb->tableId, tomb->key, tomb->keyLength);
        objectMap.prefetchBucket(key);
    }
}

void
ObjectManager::replaySegment(SideLog* sideLog, SegmentIterator& it)
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
            const Object::SerializedForm* recoveryObj =
                it.getContiguous<Object::SerializedForm>(NULL, 0);
            Key key(recoveryObj->tableId,
                    recoveryObj->keyAndData,
                    recoveryObj->keyLength);

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

            uint64_t minSuccessor = 0;
            bool freeCurrentEntry = false;

            LogEntryType currentType;
            Buffer currentBuffer;
            Log::Reference currentReference;
            if (lookup(key, currentType, currentBuffer, 0, &currentReference)) {
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
                                    recoveryObj->timestamp,
                                    recoveryObj,
                                    it.getLength(),
                                    &newObjReference);
                }

                // TODO(steve/ryan): what happens if the log is full? won't an
                //      exception here just cause the master to try another
                //      backup?

                objectAppendCount++;
                liveObjectBytes += it.getLength();

                replace(key, newObjReference);

                // nuke the old object, if it existed
                // TODO(steve): put tombstones in the HT and have this free them
                //              as well
                if (freeCurrentEntry) {
                    liveObjectBytes -= currentBuffer.getTotalLength();
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

            uint64_t minSuccessor = 0;
            bool freeCurrentEntry = false;

            LogEntryType currentType;
            Buffer currentBuffer;
            Log::Reference currentReference;
            if (lookup(key, currentType, currentBuffer, 0, &currentReference)) {
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
                                    recoverTomb.getTimestamp(),
                                    buffer,
                                    &newTombReference);
                }

                // TODO(steve/ryan): append could fail here!

                replace(key, newTombReference);

                // nuke the object, if it existed
                if (freeCurrentEntry) {
                    liveObjectCount++;
                    liveObjectBytes -= currentBuffer.getTotalLength();
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
                sideLog->append(LOG_ENTRY_TYPE_SAFEVERSION, 0, buffer);
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
 * Ensures that this master owns the tablet for the given object (based on its
 * tableId and string key) and returns the corresponding Table.
 *
 * \param key
 *      Key to look up the corresponding table for.
 * \return
 *      The Table of which the tablet containing this key is a part, or NULL if
 *      this master does not own the tablet.
 */
TabletsOnMaster*
ObjectManager::getTable(Key& key)
{
    ProtoBuf::Tablets::Tablet const* tablet = getTablet(key.getTableId(),
                                                        key.getHash());
    if (tablet == NULL)
        return NULL;

    TabletsOnMaster* table = reinterpret_cast<TabletsOnMaster*>(tablet->user_data());
    incrementReadAndWriteStatistics(table);
    return table;
}

/**
 * Ensures that this master owns the tablet for any object corresponding
 * to the given hash value of its string key and returns the
 * corresponding Table.
 *
 * \param tableId
 *      Identifier for a desired table.
 * \param keyHash
 *      Hash value of the variable length key of the object.
 *
 * \return
 *      The Table of which the tablet containing this object is a part,
 *      or NULL if this master does not own the tablet.
 */
TabletsOnMaster*
ObjectManager::getTable(uint64_t tableId, KeyHash keyHash)
{
    ProtoBuf::Tablets::Tablet const* tablet = getTablet(tableId,
                                                        keyHash);
    if (tablet == NULL)
        return NULL;

    TabletsOnMaster* table = reinterpret_cast<TabletsOnMaster*>(tablet->user_data());
    return table;
}

/**
 * Ensures that this master owns the tablet for any object corresponding
 * to the given hash value of its string key and returns the
 * corresponding Table.
 *
 * \param tableId
 *      Identifier for a desired table.
 * \param keyHash
 *      Hash value of the variable length key of the object.
 *
 * \return
 *      The Table of which the tablet containing this object is a part,
 *      or NULL if this master does not own the tablet.
 */
ProtoBuf::Tablets::Tablet const*
ObjectManager::getTablet(uint64_t tableId, KeyHash keyHash)
{
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet()) {
        if (tablet.table_id() == tableId &&
            tablet.start_key_hash() <= keyHash &&
            keyHash <= tablet.end_key_hash()) {
            return &tablet;
        }
    }
    return NULL;
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
ObjectManager::rejectOperation(const RejectRules& rejectRules, uint64_t version)
{
    if (version == VERSION_NONEXISTENT) {
        if (rejectRules.doesntExist)
            return STATUS_OBJECT_DOESNT_EXIST;
        return STATUS_OK;
    }
    if (rejectRules.exists)
        return STATUS_OBJECT_EXISTS;
    if (rejectRules.versionLeGiven && version <= rejectRules.givenVersion)
        return STATUS_WRONG_VERSION;
    if (rejectRules.versionNeGiven && version != rejectRules.givenVersion)
        return STATUS_WRONG_VERSION;
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
 * \param relocator
 *      The relocator is used to copy a live entry to a new location in the
 *      log and get a reference to that new location. If the entry is not
 *      needed, the relocator should not be used.
 */
void
ObjectManager::relocate(LogEntryType type,
                        Buffer& oldBuffer,
                        LogEntryRelocator& relocator)
{
    if (type == LOG_ENTRY_TYPE_OBJ)
        relocateObject(oldBuffer, relocator);
    else if (type == LOG_ENTRY_TYPE_OBJTOMB)
        relocateTombstone(oldBuffer, relocator);
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
ObjectManager::relocateObject(Buffer& oldBuffer,
                              LogEntryRelocator& relocator)
{
    Key key(LOG_ENTRY_TYPE_OBJ, oldBuffer);

    TabletsOnMaster* table = getTable(key);
    if (table == NULL) {
        // That tablet doesn't exist on this server anymore.
        // Just remove the hash table entry, if it exists.
        remove(key);
        return;
    }

    HashTableBucketLock lock(*this, key);

    bool keepNewObject = false;

    LogEntryType currentType;
    Buffer currentBuffer;
    if (lookup(key, currentType, currentBuffer)) {
        assert(currentType == LOG_ENTRY_TYPE_OBJ);

        keepNewObject = (currentBuffer.getStart<uint8_t>() ==
                         oldBuffer.getStart<uint8_t>());
        if (keepNewObject) {
            // Try to relocate it. If it fails, just return. The cleaner will
            // allocate more memory and retry.
            uint32_t timestamp = getObjectTimestamp(oldBuffer);
            if (!relocator.append(LOG_ENTRY_TYPE_OBJ, oldBuffer, timestamp))
                return;
            replace(key, relocator.getNewReference());
        }
    }

    // Update table statistics.
    if (!keepNewObject) {
        table->objectCount--;
        table->objectBytes -= oldBuffer.getTotalLength();
    }
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
        uint32_t timestamp = getTombstoneTimestamp(oldBuffer);
        if (!relocator.append(LOG_ENTRY_TYPE_OBJTOMB, oldBuffer, timestamp))
            return;
    } else {
        Key key(LOG_ENTRY_TYPE_OBJTOMB, oldBuffer);
        TabletsOnMaster* table = getTable(key);
        if (table != NULL) {
            table->tombstoneCount--;
            table->tombstoneBytes -= oldBuffer.getTotalLength();
        }
    }
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
 * Increments the access statistics for each read and write operation
 * on the repsective tablet.
 *
 * \param *table
 *      Pointer to the table object that is associated with each tablet.
 */
void
ObjectManager::incrementReadAndWriteStatistics(TabletsOnMaster* table)
{
    table->statEntry.set_number_read_and_writes(
        table->statEntry.number_read_and_writes() + 1);
}

} // namespace RAMCloud
