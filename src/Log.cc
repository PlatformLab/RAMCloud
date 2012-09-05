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

#include <assert.h>
#include <stdint.h>

#include "Log.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for Log. No segments are allocated in the constructor, so if
 * replicas are being used no backups will have been contacted yet and there
 * will be no durable evidence of this log having existed. Call sync() or make
 * a synchronous append() to allocate and force the head segment to backups.
 *
 * The reason for this behaviour is complicated. We want to ensure that the
 * log is made durable before assigning any tablets to the master, since we
 * want a lack of the log on disk to unambiguously mean data loss if the
 * coordinator thinks we have any tablets. However, the constructor cannot
 * allocate and replicate the first head, since there is the potential for
 * deadlock (transport manager isn't running yet, so we can't learn of any
 * backups from the coordinator).
 *
 * \param context
 *      Overall information about the RAMCloud server.
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
 */
Log::Log(Context& context,
         const ServerConfig& config,
         LogEntryHandlers& entryHandlers,
         SegmentManager& segmentManager,
         ReplicaManager& replicaManager)
    : context(context),
      entryHandlers(entryHandlers),
      segmentManager(segmentManager),
      replicaManager(replicaManager),
      cleaner(context, config, segmentManager, replicaManager, entryHandlers),
      head(NULL),
      appendLock(),
      metrics()
{
}

/**
 * Clean up after the Log.
 */
Log::~Log()
{
}

/**
 * Enable the cleaner if it isn't already running.
 */
void
Log::enableCleaner()
{
    cleaner.start();
}

/**
 * Disable the cleaner if it's running. Blocks until the cleaner thread has
 * quiesced.
 */
void
Log::disableCleaner()
{
    cleaner.stop();
}

/**
 * Populate the given protocol buffer with various log metrics.
 *
 * \param[out] metrics
 *      The protocol buffer to fill with metrics.
 */
void
Log::getMetrics(ProtoBuf::LogMetrics& m)
{
    m.set_ticks_per_second(Cycles::perSecond());
    m.set_total_append_ticks(metrics.totalAppendTicks);
    m.set_total_sync_ticks(metrics.totalSyncTicks);
    m.set_total_no_space_ticks(metrics.totalNoSpaceTicks);
    m.set_total_bytes_appended(metrics.totalBytesAppended);
    m.set_total_metadata_bytes_appended(metrics.totalMetadataBytesAppended);

    cleaner.getMetrics(*m.mutable_cleaner_metrics());
    segmentManager.getMetrics(*m.mutable_segment_metrics());
    segmentManager.getAllocator().getMetrics(*m.mutable_seglet_metrics());
}

/**
 * Append a typed entry to the log by coping in the data. Entries are binary
 * blobs described by a simple <type, length> tuple.
 *
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param buffer
 *      Buffer object describing the entry to be appended.
 * \param offset
 *      Byte offset within the buffer object to begin appending from.
 * \param length
 *      Number of bytes to append starting from the given offset in the buffer.
 * \param sync
 *      If true, do not return until the append has been replicated to backups.
 *      If false, may return before any replication has been done.
 * \param[out] outReference
 *      If the append succeeds, a reference to the created entry is returned
 *      here. This reference may be used to access the appended entry via the
 *      lookup method. It may also be inserted into a HashTable.
 * \return
 *      True if the append succeeded, false if there was either insufficient
 *      space to complete the operation or the requested append was larger
 *      than the system supports.
 */
bool
Log::append(LogEntryType type,
            Buffer& buffer,
            uint32_t offset,
            uint32_t length,
            bool sync,
            HashTable::Reference& outReference)
{
    Lock lock(appendLock);
    CycleCounter<uint64_t> _(&metrics.totalAppendTicks);

    // This is only possible once after construction.
    if (head == NULL) {
        head = segmentManager.allocHead(false);
        if (head == NULL)
            throw FatalError(HERE, "Could not allocate initial head segment");
    }

    // Try to append. If we can't, try to allocate a new head to get more space.
    uint32_t segmentOffset;
    uint32_t bytesUsedBefore = head->getAppendedLength();
    bool success = head->append(type, buffer, offset, length, segmentOffset);
    if (!success) {
        LogSegment* newHead = segmentManager.allocHead(false);
        if (newHead != NULL)
            head = newHead;

        // If we're entirely out of memory or were allocated an emergency head
        // segment due to memory pressure, we can't service the append. Return
        // failure and let the client retry. Hopefully the cleaner will free up
        // more memory soon.
        if (newHead == NULL || head->isEmergencyHead) {
            if (!metrics.noSpaceTimer)
                metrics.noSpaceTimer.construct(&metrics.totalNoSpaceTicks);
            return false;
        }

        bytesUsedBefore = head->getAppendedLength();
        if (!head->append(type, buffer, offset, length, segmentOffset)) {
            // TODO(Steve): We should probably just permit up to 1/N'th of the
            // size of a segment in any single append. Say, 1/2th or 1/4th as
            // a ceiling. Then we could ensure that after opening a new head
            // we have at least as much space, or else throw a fatal error.
            LOG(ERROR, "Entry too big to append to log: %u bytes of type %d",
                length, static_cast<int>(type));
            throw FatalError(HERE, "Entry too big to append to log");
        }
    }

    if (metrics.noSpaceTimer)
        metrics.noSpaceTimer.destroy();

    if (sync)
        Log::sync();

    outReference = buildReference(head->slot, segmentOffset);

    // TODO(Steve): Should this just be passed in? It's a bummer that we build
    // a new buffer just to query the callback. Yet we still need the callback
    // for the cleaner to query the timestamp.
    Buffer inLogBuffer;
    head->getEntry(segmentOffset, inLogBuffer);
    uint32_t timestamp = entryHandlers.getTimestamp(type, inLogBuffer);
    head->statistics.increment(head->getAppendedLength() - bytesUsedBefore,
                               timestamp);

    metrics.totalBytesAppended += length;
    metrics.totalMetadataBytesAppended +=
        (head->getAppendedLength() - bytesUsedBefore) - length;

    return true;
}

/**
 * Abbreviated append method for convenience. See the above append method for
 * documentation.
 */
bool
Log::append(LogEntryType type,
            Buffer& buffer,
            bool sync,
            HashTable::Reference& outReference)
{
    return append(type, buffer, 0, buffer.getTotalLength(), sync, outReference);
}

/**
 * Abbreviated append method primarily for convenience in tests.
 *
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param data
 *      Pointer to data to be appended.
 * \param length
 *      Number of bytes to append from the given pointer.
 * \param sync
 *      If true, do not return until the append has been replicated to backups.
 *      If false, may return before any replication has been done.
 * \return
 *      True if the append succeeded, false if there was either insufficient
 *      space to complete the operation or the requested append was larger
 *      than the system supports.
 *  
 */
bool
Log::append(LogEntryType type, const void* data, uint32_t length, bool sync)
{
    Buffer buffer;
    buffer.appendTo(data, length);
    HashTable::Reference dummy;
    return append(type, buffer, true, dummy);
}

/**
 * Mark bytes in log as freed. When a previously-appended entry is no longer
 * needed, this method may be used to notify the log that it may garbage
 * collect it.
 */
void
Log::free(HashTable::Reference reference)
{
    uint32_t slot = referenceToSlot(reference);
    uint32_t offset = referenceToOffset(reference);
    LogSegment& segment = segmentManager[slot];
    Buffer buffer;
    LogEntryType type = segment.getEntry(offset, buffer);
    uint32_t timestamp = entryHandlers.getTimestamp(type, buffer);
    segment.statistics.decrement(buffer.getTotalLength(), timestamp);
}

/**
 * Given a reference to an entry previously appended to the log, return the
 * entry's type and fill in a buffer that points to the entry's contents.
 * This is the method to use to access something after it is appended to the
 * log.
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
Log::getEntry(HashTable::Reference reference, Buffer& outBuffer)
{
    uint32_t slot = referenceToSlot(reference);
    uint32_t offset = referenceToOffset(reference);
    return segmentManager[slot].getEntry(offset, outBuffer);
}

/**
 * Wait for all segments to be fully replicated. If there has never been a head
 * segment, allocate one and sync it to backups. This method must be invoked
 * before any appends to the log are permitted.
 */
void
Log::sync()
{
    CycleCounter<uint64_t> __(&metrics.totalSyncTicks);

    // The only time 'head' should be NULL is after construction and before the
    // initial call to this method. Even if we run out of memory in the future,
    // head will remain valid.
    if (head == NULL) {
        head = segmentManager.allocHead(true);
        if (head == NULL)
            throw FatalError(HERE, "Could not allocate initial head segment");
    }

    head->replicatedSegment->sync(head->getAppendedLength());
    TEST_LOG("log synced");
}

/**
 * Get the current position of the log head. This can be used when adding
 * tablets in order to preclude any prior log data from being considered
 * part of the tablet. This is important in tablet migration and when
 * creating, deleting, and re-creating the same tablet on a master since we
 * don't want recovery to resurrect old objects.
 */
Log::Position
Log::getHeadPosition()
{
    // TODO(Steve): This will interact poorly with iteration - it will not
    // return until iteration is done, since callers to append() will block
    // holding this lock!
    Lock lock(appendLock);

    if (head == NULL) {
        // If invoked before the first call to append or sync, then we can say
        // the log is currently at the initial position.
        return { 0, 0 };
    }

    return { head->id, head->getAppendedLength() };
}

/**
 * Given a reference to an appended entry, return the identifier of the segment
 * that contains the entry. An example use of this is tombstones, which mark
 * themselves with the segment id of the object they're deleting. When that
 * segment leaves the system, the tombstone may be garbage collected.
 */
uint64_t
Log::getSegmentId(HashTable::Reference reference)
{
    uint32_t slot = referenceToSlot(reference);
    return segmentManager[slot].id;
}

/**
 * Allocate a new head Segment and write the LogDigest before returning if
 * the provided \a segmentId is still the current log head.
 * The current head is closed and replaced with the new one.  All the
 * usual log durability constraints are enforced by the underlying
 * ReplicaManager for safety during the transition to the new head.
 *
 * \param segmentId
 *      Only allocate a new log head if the current log head is the one
 *      specified.  If \a segmentId is empty a new head is allocated
 *      regardless of which segment is currently the log head.  This is
 *      used to prevent useless allocations in the case that multiple
 *      callers try to allocate new log heads at the same time.
 * \throw LogOutOfMemoryException
 *      If no Segments are free.
 */
void
Log::allocateHeadIfStillOn(Tub<uint64_t> segmentId)
{
    Lock lock(appendLock);

    if (!segmentId || (head != NULL && head->id == *segmentId))
        head = segmentManager.allocHead(true);
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
Log::containsSegment(uint64_t segmentId)
{
    TEST_LOG("%lu", segmentId);
    return segmentManager.doesIdExist(segmentId);
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Build a HashTable::Reference pointing to an entry in the log.
 *
 * \param slot
 *      Slot of the segment containing the entry. This it the temporary,
 *      reusable identifier that SegmentManager allocates.
 * \param offset
 *      Byte offset of the entry in the segment referred to by 'slot'.
 */
HashTable::Reference
Log::buildReference(uint32_t slot, uint32_t offset)
{
    // TODO(Steve): Just calculate how many bits we need for the offset, rather
    // than statically allocate.
    return HashTable::Reference((static_cast<uint64_t>(slot) << 24) | offset);
}

/**
 * Given a HashTable::Reference pointing to a log entry, extract the segment
 * slot number.
 */
uint32_t
Log::referenceToSlot(HashTable::Reference reference)
{
    return downCast<uint32_t>(reference.get() >> 24);
}

/**
 * Given a HashTable::Reference pointing to a log entry, extract the entry's
 * segment byte offset.
 */
uint32_t
Log::referenceToOffset(HashTable::Reference reference)
{
    return downCast<uint32_t>(reference.get() & 0xffffff);
}

} // namespace
