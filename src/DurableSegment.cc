/* Copyright (c) 2011 Stanford University
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

#include "DurableSegment.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Convenience method for calling #write() with the most recently queued
 * offset and queueing the close flag.
 */
void
OpenSegment::close()
{
    segment.close();
}

/**
 * Eventually replicate the \a len bytes of data starting at \a offset into the
 * segment.
 * Guarantees that no replica will see this write until it has seen all
 * previous writes on this segment.
 * \pre
 *      All previous segments have been closed (at least locally).
 * \param offset
 *      The number of bytes into the segment through which to replicate.
 * \param closeSegment
 *      Whether to close the segment after writing this data. If this is true,
 *      the caller's DurableSegment pointer is invalidated upon the return of this
 *      function.
 */
void
OpenSegment::write(uint32_t offset, bool closeSegment)
{
    segment.write(offset, closeSegment);
}

/**
 * Constructor.
 * Must be constructed on #sizeOf(backupManager.replicas) bytes of space.
 * The arguments are the same as those to #BackupManager::openSegment.
 */
DurableSegment::DurableSegment(BackupManager& backupManager,
                               uint64_t segmentId,
                               const void* data,
                               uint32_t len,
                               uint32_t numReplicas)
    : listEntries()
    , openSegment(*this)
    , backupManager(backupManager)
    , segmentId(segmentId)
    , data(data)
    , openLen(len)
    , queued(true, len, false)
    , backups(numReplicas)
{
}

/// See OpenSegment::write.
void
DurableSegment::write(uint32_t offset,
                      bool closeSegment)
{
    // offset monotonically increases
    assert(offset >= queued.bytes);
    queued.bytes = offset;

    // immutable after close
    assert(!queued.close);
    queued.close = closeSegment;
    if (queued.close) {
        LOG(DEBUG, "Segment %lu closed (length %d)", segmentId, queued.bytes);
        ++metrics->master.segmentCloseCount;
    }
}

} // namespace RAMCloud
