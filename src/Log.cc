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
#include "TransportManager.h" // for Log memory 0-copy registration hack

namespace RAMCloud {


/**
 * Constructor for Log.
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
      head(NULL),
      appendLock()
{
    if (!disableCleaner)
        cleaner.construct(context, segmentManager, replicaManager, 4);
}

/**
 * Clean up after the Log.
 */
Log::~Log()
{
}

bool
Log::append(LogEntryType type,
            Buffer& buffer,
            uint32_t offset,
            uint32_t length,
            bool sync,
            HashTable::Reference& outReference)
{
    Lock lock(appendLock);

    // If no head, try to allocate one
    if (head == NULL)
        head = segmentManager.allocHead();

    // If allocation failed, out of space - return error
    if (head == NULL)
        return false;

    // Try to append. If we can't, try to allocate a new head to get more space
    uint32_t segmentOffset;
    bool success = head->append(type, buffer, offset, length, segmentOffset);
    if (!success) {
        head = segmentManager.allocHead();
        if (head == NULL)
            return false;

        if (!head->append(type, buffer, offset, length, segmentOffset)) {
            // If we still can't append it, the caller must appending more
            // than we can fit in a segment.
            return false;
        }
    }

    outReference = buildReference(head->slot, segmentOffset);
    return true;
}

bool
Log::append(LogEntryType type,
            Buffer& buffer,
            bool sync,
            HashTable::Reference& outReference)
{
    return append(type, buffer, 0, buffer.getTotalLength(), sync, outReference);
}

/**
 * Mark bytes in Log as freed. This simply maintains a per-Segment tally that
 * can be used to compute utilisation of individual Log Segments.
 * \param[in] entry
 *      A LogEntryHandle as returned by an #append call.
 * \throw LogException
 *      An exception is thrown if the pointer provided is not valid.
 */
void
Log::free(HashTable::Reference reference)
{
    uint32_t slot = referenceToSlot(reference);
    uint32_t offset = referenceToOffset(reference);
    segmentManager[slot].free(offset);
}

void
Log::lookup(HashTable::Reference reference, LogEntryType& outType, Buffer& outBuffer)
{
    uint32_t slot = referenceToSlot(reference);
    uint32_t offset = referenceToOffset(reference);
    LogSegment& segment = segmentManager[slot];
    outType = segment.getEntryTypeAt(offset);
    segment.appendEntryToBuffer(offset, outBuffer);
}

/**
 * Wait for all segments to be fully replicated.
 */
void
Log::sync()
{
    // ---XXXXX---
#if 0
    if (head)
        head->sync();
#endif
}

/**
 * Get the current position of the log head. This can be used when adding
 * tablets in order to preclude any prior log data from being considered
 * part of the tablet. This is important in tablet migration and when
 * creating, deleting, and re-creating the same tablet on a master since we
 * don't want recovery to resurrect old objects.
 */
Log::Position
Log::headOfLog()
{
// XXX- this will interact poorly with iteration (i.e. not return until iteration done),
//      since callers to append() will block holding this lock!
    Lock lock(appendLock);

    if (head == NULL)
        return { 0, 0 };

    return { head->id, head->getTailOffset() };
}

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
 * \throw LogOutOfMemoryException
 *      If no Segments are free.
 */
void
Log::allocateHeadIfStillOn(uint64_t segmentId)
{
    Lock lock(appendLock);
    if (head && head->id == segmentId)
        head = segmentManager.allocHead();
}

/**
 * Determine whether or not the provided Segment identifier is currently
 * live. A live Segment is one that is still being used by the Log for
 * storage. This method can be used to determine if data once written to the
 * Log is no longer present in the RAMCloud system and hence will not appear
 * again during either normal operation or recovery.
 * \param[in] segmentId
 *      The Segment identifier to check for liveness.
 */
bool
Log::isSegmentLive(uint64_t segmentId)
{
    TEST_LOG("%lu", segmentId);
    return segmentManager.doesIdExist(segmentId);
}


/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

HashTable::Reference
Log::buildReference(uint32_t slot, uint32_t offset)
{
    return HashTable::Reference((static_cast<uint64_t>(slot) << 24) | offset);
}

uint32_t
Log::referenceToSlot(HashTable::Reference reference)
{
    return downCast<uint32_t>(reference.get() >> 24);
}

uint32_t
Log::referenceToOffset(HashTable::Reference reference)
{
    return downCast<uint32_t>(reference.get() & 0xffffff);
}

} // namespace
