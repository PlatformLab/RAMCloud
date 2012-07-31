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
 * Constructor for Log. Upon returning, the first segment of the log will be
 * opened and synced to any backup replicas. Doing so avoids ambiguity if no
 * log segments are found after a crash. So long as the log is opened before
 * any tablets are assigned, failure to find segments can only mean loss of
 * the log.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param logId
 *      A unique numerical identifier for this Log. This should be globally
 *      unique in the RAMCloud system.
 * \param replicaManager
 *      The ReplicaManager that will be used to make each of this Log's
 *      Segments durable.
 * \param disableCleaner
 *      If true, do not do any cleaning. This will keep the log from garbage
 *      collecting freed space. It will eventually run out of memory forever.
 */
Log::Log(Context& context,
         EntryHandlers& entryHandlers,
         SegmentManager& segmentManager,
         ReplicaManager& replicaManager,
         bool disableCleaner)
    : context(context),
      entryHandlers(entryHandlers),
      segmentManager(segmentManager),
      replicaManager(replicaManager),
      cleaner(),
      head(segmentManager.allocHead()),
      appendLock()
{
    if (head == NULL)
        throw LogException(HERE, "Could not allocate initial head segment");

    if (!disableCleaner)
        cleaner.construct(context, segmentManager, replicaManager, 4);

    sync();
}

/**
 * Clean up after the Log.
 */
Log::~Log()
{
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

    // Try to append. If we can't, try to allocate a new head to get more space.
    uint32_t segmentOffset;
    bool success = head->append(type, buffer, offset, length, segmentOffset);
    if (!success) {
        LogSegment* newHead = segmentManager.allocHead();
        if (newHead == NULL)
            return false;

        head = newHead;

        if (!head->append(type, buffer, offset, length, segmentOffset)) {
            // If we still can't append it, the caller must be appending more
            // than we can fit in a segment. This is something that just
            // shouldn't happen. Should we throw FatalError instead?
            LOG(WARNING, "Entry too big to append to log: %u bytes", length);
            return false;
        }
    }

    if (sync) {
        // XXX ...
    }

    outReference = buildReference(head->slot, segmentOffset);
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
 * \return
 *      True if the append succeeded, false if there was either insufficient
 *      space to complete the operation or the requested append was larger
 *      than the system supports.
 *  
 */
bool
Log::append(LogEntryType type, const void* data, uint32_t length)
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
    segmentManager[slot].free(offset);
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
 *      will indicate when references become invalid via the EntryHandlers
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
 * Wait for all segments to be fully replicated.
 */
void
Log::sync()
{
    // ---XXXXX sync (teehee!) with Ryan---
#if 0
    if (head)
        head->sync();
#endif

    TEST_LOG("synced");
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
    return { head->id, head->getTailOffset() };
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
 *      specified.  This is used to prevent useless allocations in the
 *      case that multiple callers try to allocate new log heads at the
 *      same time.
 */
void
Log::allocateHeadIfStillOn(uint64_t segmentId)
{
    Lock lock(appendLock);
    if (head->id == segmentId)
        head = segmentManager.allocHead();

    // XXX What if we're out of space? The above could return NULL, in which
    //     case we haven't actually closed it. Could we return false to replica
    //     manager and rely on it retrying? The previous code could have thrown
    //     an exception, but we never caught it...
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
