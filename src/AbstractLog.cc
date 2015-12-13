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

#include <assert.h>
#include <stdint.h>

#include "Log.h"
#include "LogCleaner.h"
#include "PerfStats.h"
#include "ServerConfig.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for AbstractLog.
 *
 * \param entryHandlers
 *      Class to query for various bits of per-object information. For instance,
 *      the log may want to know whether an object is still needed or if it can
 *      be garbage collected. Methods on the given class instance will be
 *      invoked to field such queries.
 * \param segmentManager
 *      The SegmentManager this log should allocate its head segments from.
 * \param replicaManager
 *      The ReplicaManager that will be used to make each of this Log's
 *      Segments durable.
 * \param segmentSize
 *      The size, in bytes, of segments this log will use.
 */
AbstractLog::AbstractLog(LogEntryHandlers* entryHandlers,
                         SegmentManager* segmentManager,
                         ReplicaManager* replicaManager,
                         uint32_t segmentSize)
    : entryHandlers(entryHandlers),
      segmentManager(segmentManager),
      replicaManager(replicaManager),
      segmentSize(segmentSize),
      head(NULL),
      appendLock("AbstractLog::appendLock"),
      totalLiveBytes(0),
      maxLiveBytes(0),
      metrics()
{
    // This is a placeholder value; the real value will get computed
    // shortly by allocNewWritableHead.
    maxLiveBytes = 10000000;
}

/**
 * Append multiple entries to the log atomically. This ensures that either
 * everything is written, or none of it is. Furthermore, recovery is
 * guaranteed to see either none or all of the entries.
 *
 * Note that the append operation is not synchronous with respect to backups.
 * To ensure that the data appended has been safely written to backups, the
 * sync() method must be invoked after appending. Until sync() is called, the
 * data may or may not have been made durable.
 *
 * \param appends
 *      Array containing the entries to append. References to the entries
 *      are also returned here.
 * \param numAppends
 *      Number of entries in the appends array.
 * \return
 *      True if the append succeeded, false if there was insufficient space
 *      to complete the operation.
 */
bool
AbstractLog::append(AppendVector* appends, uint32_t numAppends)
{
    CycleCounter<uint64_t> _(&metrics.totalAppendTicks);
    Lock lock(appendLock);
    metrics.totalAppendCalls++;

    uint32_t lengths[numAppends];
    for (uint32_t i = 0; i < numAppends; i++)
        lengths[i] = appends[i].buffer.size();

    if (head == NULL || !head->hasSpaceFor(lengths, numAppends)) {
        if (!allocNewWritableHead())
            return false;
    }

    if (head->isEmergencyHead)
        return false;

    if (!head->hasSpaceFor(lengths, numAppends))
        throw FatalError(HERE, "too much data to append to one segment");

    LogSegment* headBefore = head;
    for (uint32_t i = 0; i < numAppends; i++) {
        bool enoughSpace = append(lock,
                                  appends[i].type,
                                  appends[i].buffer,
                                  &appends[i].reference);
        if (!enoughSpace)
            throw FatalError(HERE, "Guaranteed append managed to fail");
    }
    assert(head == headBefore);

    return true;
}

/**
 * Flushes all the log entries from the given buffer to the log
 * atomically.
 *
 * \param logBuffer
 *      The buffer which contains various log entries
 * \param[out] references
 *      This will hold the reference for each log entry that
 *      is written to the log. The caller must have allocated
 *      memory for this
 * \param numEntries
 *      Number of log entries in the buffer
 */
bool
AbstractLog::append(Buffer *logBuffer, Reference *references,
                    uint32_t numEntries)
{
    CycleCounter<uint64_t> _(&metrics.totalAppendTicks);
    Lock lock(appendLock);
    metrics.totalAppendCalls++;

    if (head == NULL || !head->hasSpaceFor(logBuffer->size())) {
        if (!allocNewWritableHead())
            return false;
    }

    if (head->isEmergencyHead)
        return false;

    if (!head->hasSpaceFor(logBuffer->size()))
        throw FatalError(HERE, "too much data to append to one segment");

    LogSegment* headBefore = head;

    // Makes sense to call getRange on the entire logBuffer here because
    // everything n the buffer has to be written out before this function
    // can return
    const uint8_t* buffer = reinterpret_cast<const uint8_t*>(logBuffer->
                                getRange(0, logBuffer->size()));
    if (!buffer) {
        throw FatalError(HERE, "Ill-formed log entries in the buffer");
    }

    uint32_t entryLength = 0;
    uint32_t offset = 0;
    for (uint32_t i = 0; i < numEntries; i++) {
        bool enoughSpace = append(lock,
                                  buffer + offset,
                                  &entryLength,
                                  &references[i]);
        if (!enoughSpace)
            throw FatalError(HERE, "Guaranteed append managed to fail");
        offset+= entryLength;
    }

    assert(head == headBefore);

    return true;
}

/**
 * This method is invoked when a log entry is no longer needed (for example, an
 * object that has been deleted). This method does not change anything in the
 * log itself, but it is used to keep track of free space in segments to help
 * the cleaner identify good candidates for cleaning.
 *
 * Free should be called only once for each entry. Behaviour is undefined if
 * it is called more than once on the same reference.
 *
 * \param reference
 *      Reference to the entry being freed.
 */
void
AbstractLog::free(Reference reference)
{
    TEST_LOG("free on reference %lu", reference.toInteger());
    LogSegment* segment = getSegment(reference);
    uint32_t lengthWithMetadata;
    LogEntryType type = reference.getEntry(&segmentManager->getAllocator(),
                                           NULL,
                                           &lengthWithMetadata);
    segment->trackDeadEntry(type, lengthWithMetadata);
    if (type == LOG_ENTRY_TYPE_OBJ ||
        type == LOG_ENTRY_TYPE_RPCRESULT ||
        type == LOG_ENTRY_TYPE_PREP ||
        type == LOG_ENTRY_TYPE_TXPLIST)
        totalLiveBytes -= lengthWithMetadata;
    //TODO(seojin): handle RpcResult and PreparedOp.
}

/**
 * Populate the given protocol buffer with various log metrics.
 *
 * \param[out] m
 *      The protocol buffer to fill with metrics.
 */
void
AbstractLog::getMetrics(ProtoBuf::LogMetrics& m)
{
    m.set_total_append_calls(metrics.totalAppendCalls);
    m.set_ticks_per_second(Cycles::perSecond());
    m.set_total_append_ticks(metrics.totalAppendTicks);
    m.set_total_no_space_ticks(metrics.totalNoSpaceTicks);
    m.set_total_bytes_appended(metrics.totalBytesAppended);
    m.set_total_metadata_bytes_appended(metrics.totalMetadataBytesAppended);

    segmentManager->getMetrics(*m.mutable_segment_metrics());
    segmentManager->getAllocator().getMetrics(*m.mutable_seglet_metrics());
}

/**
 * Given a reference to an entry previously appended to the log, return the
 * entry's type and fill in a buffer that points to the entry's contents.
 * This is the method to use to access something after it is appended to the
 * log.
 *
 * In the common case (small entries, especially) this method is very fast as
 * the reference already points to a contiguous entry. When this is not the
 * case, we take a slower path to look up the subsequent discontiguous pieces
 * of the entry, which typically incurs several additional cache misses.
 *
 * \param reference
 *      Reference to the entry requested. This value is returned in the append
 *      method. If this reference is invalid behaviour is undefined. The log
 *      will indicate when references become invalid via the LogEntryHandlers
 *      class.
 * \param outBuffer
 *      Buffer to append the entry being looked up to.
 * \return
 *      The type of the entry being looked up is returned here.
 */
LogEntryType
AbstractLog::getEntry(Reference reference, Buffer& outBuffer)
{
    return reference.getEntry(&segmentManager->getAllocator(), &outBuffer);
}

/**
 * Given a reference to an appended entry, return the identifier of the segment
 * that contains the entry. An example use of this is tombstones, which mark
 * themselves with the segment id of the object they're deleting. When that
 * segment leaves the system, the tombstone may be garbage collected.
 */
uint64_t
AbstractLog::getSegmentId(Reference reference)
{
    return getSegment(reference)->id;
}

/**
 * Given the size of a new live object, check whether we have enough
 * space to put it into the log without risking being unable to clean.
 * A false reply means some existing objects must be deleted before
 * any new objects can be created (i.e., the current problem can't be
 * fixed by the cleaner). If false is returned, a log message is
 * generated.
 *
 * \param objectSize
 *       The total amount of log space that will be consumed by the object and
 *       its metadata
 */
bool
AbstractLog::hasSpaceFor(uint64_t objectSize) {
    if ((totalLiveBytes + objectSize) <= maxLiveBytes) {
        return true;
    }
    RAMCLOUD_CLOG(WARNING, "Memory capacity exceeded; must delete objects "
            "before any more new objects can be created");
    return false;
}
/**
 * Check if a segment is still in the system. This method can be used to
 * determine if data once written to the log is no longer present in the
 * RAMCloud system and hence will not appear again during either normal
 * operation or recovery. Tombstones use this to determine when they are
 * eligible for garbage collection.
 *
 * \param segmentId
 *      The Segment identifier to check for liveness.
 * \return
 *      True if the given segment is present in the log, otherwise false.
 */
bool
AbstractLog::segmentExists(uint64_t segmentId)
{
    TEST_LOG("%lu", segmentId);
    return segmentManager->doesIdExist(segmentId);
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

LogSegment*
AbstractLog::getSegment(Reference reference)
{
    return segmentManager->getAllocator().getOwnerSegment(
        reinterpret_cast<const void*>(reference.toInteger()));
}

/**
 * Append a typed entry to the log by copying in the data. Entries are binary
 * blobs described by a simple <type, length> tuple.
 *
 * Note that the append operation is not synchronous with respect to backups.
 * To ensure that the data appended has been safely written to backups, the
 * sync() method must be invoked after appending. Until sync() is called, the
 * data may or may not have been made durable.
 *
 * \param appendLock
 *      The append lock must have already been acquired before entering this
 *      method. This parameter exists in the hopes that you don't forget to
 *      do that.
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param buffer
 *      Pointer to buffer containing the entry to be appended.
 * \param length
 *      Size of the entry pointed to by #buffer in bytes.
 * \param[out] outReference
 *      If the append succeeds, a reference to the created entry is returned
 *      here. This reference may be used to access the appended entry via the
 *      lookup method. It may also be inserted into a HashTable.
 * \param[out] outTickCounter
 *      If non-NULL, store the number of processor ticks spent executing this
 *      method.
 * \return
 *      True if the append succeeded, false if there was insufficient space
 *      to complete the operation.
 */
bool
AbstractLog::append(Lock& appendLock,
            LogEntryType type,
            const void* buffer,
            uint32_t length,
            Reference* outReference,
            uint64_t* outTickCounter)
{
    CycleCounter<uint64_t> _(outTickCounter);

    // Note that we do not increment metrics.totalAppendCalls here, but rather
    // in the public methods that invoke this. The reason is that we consider
    // a single append of multiple entries (which invokes this method several
    // times) to be a single call.

    // This is only possible once after construction.
    if (head == NULL) {
        if (!allocNewWritableHead())
            throw FatalError(HERE, "Could not allocate initial head segment");
    }

    // Try to append. If we can't, try to allocate a new head to get more space.
    Reference reference;
    uint32_t bytesUsedBefore = head->getAppendedLength();
    bool enoughSpace = head->append(type, buffer, length, &reference);
    if (!enoughSpace) {
        if (!allocNewWritableHead())
            return false;

        bytesUsedBefore = head->getAppendedLength();
        if (!head->append(type, buffer, length, &reference)) {
            LOG(ERROR, "Entry too big to append to log: %u bytes of type %d",
                length, static_cast<int>(type));
            throw FatalError(HERE, "Entry too big to append to log");
        }
    }

    if (outReference != NULL)
        *outReference = reference;

    uint32_t lengthWithMetadata = head->getAppendedLength() - bytesUsedBefore;

    // Update log statistics so that the cleaner can make intelligent decisions
    // when trying to reclaim memory.
    head->trackNewEntry(type, lengthWithMetadata);
    if (type == LOG_ENTRY_TYPE_OBJ ||
        type == LOG_ENTRY_TYPE_RPCRESULT ||
        type == LOG_ENTRY_TYPE_PREP ||
        type == LOG_ENTRY_TYPE_TXPLIST)
        totalLiveBytes += lengthWithMetadata;
    //TODO(seojin): handle RpcResult and PreparedOp.

    PerfStats::threadStats.logBytesAppended += lengthWithMetadata;

    return true;
}

/**
 * Append a a complete log entry to the log by copying in the data.
 *
 * Note that the append operation is not synchronous with respect to backups.
 * To ensure that the data appended has been safely written to backups, the
 * sync() method must be invoked after appending. Until sync() is called, the
 * data may or may not have been made durable.
 *
 * \param appendLock
 *      The append lock must have already been acquired before entering this
 *      method. This parameter exists in the hopes that you don't forget to
 *      do that.
 * \param buffer
 *      Pointer to buffer containing the entry to be appended.
 * \param[out] entryLength
 *      Size of the entry including the entry header information which
 *      starts at the location pointer to by #buffer
 * \param[out] outReference
 *      If the append succeeds, a reference to the created entry is returned
 *      here. This reference may be used to access the appended entry via the
 *      lookup method. It may also be inserted into a HashTable.
 * \param[out] outTickCounter
 *      If non-NULL, store the number of processor ticks spent executing this
 *      method.
 * \return
 *      True if the append succeeded, false if there was insufficient space
 *      to complete the operation.
 */
bool
AbstractLog::append(Lock& appendLock,
            const void* buffer,
            uint32_t *entryLength,
            Reference* outReference,
            uint64_t* outTickCounter)
{
    CycleCounter<uint64_t> _(outTickCounter);

    // Note that we do not increment metrics.totalAppendCalls here, but rather
    // in the public methods that invoke this. The reason is that we consider
    // a single append of multiple entries (which invokes this method several
    // times) to be a single call.

    // This is only possible once after construction.
    if (head == NULL) {
        if (!allocNewWritableHead())
            throw FatalError(HERE, "Could not allocate initial head segment");
    }

    // Try to append. If we can't, try to allocate a new head to get more space.
    Reference reference;
    LogEntryType type;
    uint32_t entryDataLength = 0;
    uint32_t bytesUsedBefore = head->getAppendedLength();
    bool enoughSpace = head->append(buffer, &entryDataLength,
                                    &type, &reference);
    if (!enoughSpace) {
        if (!allocNewWritableHead())
            return false;

        bytesUsedBefore = head->getAppendedLength();
        if (!head->append(buffer, &entryDataLength, &type, &reference)) {
            LOG(ERROR, "Entry too big to append to log: %u bytes of type %d",
                entryDataLength, static_cast<int>(type));
            throw FatalError(HERE, "Entry too big to append to log");
        }
    }

    if (outReference != NULL)
        *outReference = reference;

    uint32_t lengthWithMetadata = head->getAppendedLength() - bytesUsedBefore;

    if (entryLength)
        *entryLength = lengthWithMetadata;

    // Update log statistics so that the cleaner can make intelligent decisions
    // when trying to reclaim memory.
    head->trackNewEntry(type, lengthWithMetadata);
    if (type == LOG_ENTRY_TYPE_OBJ ||
        type == LOG_ENTRY_TYPE_RPCRESULT ||
        type == LOG_ENTRY_TYPE_PREP ||
        type == LOG_ENTRY_TYPE_TXPLIST)
        totalLiveBytes += lengthWithMetadata;
    //TODO(seojin): handle RpcResult and PreparedOp.

    PerfStats::threadStats.logBytesAppended += lengthWithMetadata;

    return true;
}

/**
 * Append a typed entry to the log by coping in the data. Entries are binary
 * blobs described by a simple <type, length> tuple.
 *
 * Note that the append operation is not synchronous with respect to backups.
 * To ensure that the data appended has been safely written to backups, the
 * sync() method must be invoked after appending. Until sync() is called, the
 * data may or may not have been made durable.
 *
 * \param appendLock
 *      The append lock must have already been acquired before entering this
 *      method. This parameter exists in the hopes that you don't forget to
 *      do that.
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param buffer
 *      Buffer object containing the entry to be appended.
 * \param[out] outReference
 *      If the append succeeds, a reference to the created entry is returned
 *      here. This reference may be used to access the appended entry via the
 *      lookup method. It may also be inserted into a HashTable.
 * \param[out] outTickCounter
 *      If non-NULL, store the number of processor ticks spent executing this
 *      method.
 * \return
 *      True if the append succeeded, false if there was insufficient space to
 *      complete the operation.
 */
bool
AbstractLog::append(Lock& appendLock,
            LogEntryType type,
            Buffer& buffer,
            Reference* outReference,
            uint64_t* outTickCounter)
{
    return append(appendLock,
                  type,
                  buffer.getRange(0, buffer.size()),
                  buffer.size(),
                  outReference,
                  outTickCounter);
}

/**
 * Allocate a new head segment, changing the ``head'' field. If the allocation
 * succeeds and the allocated segment is writable (that is, not an emergency
 * head segment), return true. Otherwise, return false.
 *
 * This method centralizes a bit of subtle logic shared between the append
 * methods (the ``head'' field should never be NULL after the first segment
 * has been allocated).
 */
bool
AbstractLog::allocNewWritableHead()
{
    LogSegment* newHead = allocNextSegment(false);
    if (newHead != NULL)
        head = newHead;

    // If we're entirely out of memory or were allocated an emergency head
    // segment due to memory pressure, we can't service the append. Return
    // failure and let the client retry. Hopefully the cleaner will free up
    // more memory soon.
    if (newHead == NULL || head->isEmergencyHead) {
        if (!metrics.noSpaceTimer)
            metrics.noSpaceTimer.construct(&metrics.totalNoSpaceTicks);
        RAMCLOUD_CLOG(NOTICE, "No clean segments available; deferring "
                "operations until cleaner runs");
        return false;
    }

    if (metrics.noSpaceTimer)
        metrics.noSpaceTimer.destroy();

    // Recompute maxLiveBytes. We do this here because the value can
    // change over time due to seglets being removed from the default
    // pool for other uses. Steve Rumble suggests that the correct
    // percentage of memory we allow to be used for live data is 98%,
    // but this code is more conservative because we expect performance
    // to degrade at even lower utilizations.

    SegletAllocator& alloc = segmentManager->getAllocator();
    maxLiveBytes = static_cast<uint64_t>(0.95 * alloc.getSegletSize() *
            static_cast<double>(alloc.getTotalCount(SegletAllocator::DEFAULT)));

    return true;
}

} // namespace
