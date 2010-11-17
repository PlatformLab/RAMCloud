/* Copyright (c) 2009, 2010 Stanford University
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rabinpoly.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "LogTypes.h"

namespace RAMCloud {

/**
 * Constructor for Segment.
 * \param[in] logId
 *      The unique identifier for the Log to which this Segment belongs.
 * \param[in] segmentId
 *      The unique identifier for this Segment.
 * \param[in] baseAddress
 *      A pointer to memory that will back this Segment.
 * \param[in] capacity
 *      The size of the backing memory pointed to by baseAddress in bytes.
 * \param[in] backup
 *      The BackupManager responsible for this Segment's durability.
 * \return
 *      The newly constructed Segment object.
 */
Segment::Segment(uint64_t logId, uint64_t segmentId, void *baseAddress,
    uint64_t capacity, BackupManager *backup)
    : backup(backup),
      syncOffset(0),
      baseAddress(baseAddress),
      logId(logId),
      id(segmentId),
      capacity(capacity),
      tail(0),
      bytesFreed(0),
      rabinPoly(RABIN_POLYNOMIAL),
      checksum(0),
      closed(false)
{
    if (backup)
        backup->openSegment(logId, id);
    SegmentHeader header = { logId, id, capacity };
    const void *p = append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));
    assert(p != NULL);
}

/**
 * Clean up after the Segment. Since Segments currently do not allocate
 * any memory, this is a no-op.
 */
Segment::~Segment()
{
    static_assert(sizeof(SegmentEntry) == 8);
    static_assert(sizeof(SegmentHeader) == 20);
    static_assert(sizeof(SegmentFooter) == 8);

    if (backup)
        backup->freeSegment(logId, id);
}

/**
 * Append an entry to this Segment. Entries consist of a typed header, followed
 * by the user-specified contents. Note that this operation makes no guarantees
 * about data alignment.
 * \param[in] type
 *      The type of entry to append. All types except LOG_ENTRY_TYPE_SEGFOOTER
 *      are permitted.
 * \param[in] buffer
 *      Data to be appended to this Segment.
 * \param[in] length
 *      Length of the data to be appended in bytes.
 * \param[in] sync
 *      If true then this write to replicated to backups before return,
 *      otherwise the replication will happen on a subsequent append()
 *      where sync is true or when the segment is closed.  This defaults
 *      to true.
 * \return
 *      On success, a const pointer into the Segment's backing memory with
 *      the same contents as `buffer'. On failure, NULL. 
 */
const void *
Segment::append(LogEntryType type, const void *buffer, uint64_t length,
    bool sync)
{
    if (closed || type == LOG_ENTRY_TYPE_SEGFOOTER ||
      appendableBytes() < length)
        return NULL;

    return forceAppendWithEntry(type, buffer, length, sync);
}

/**
 * Mark bytes used by a single entry in this Segment as freed. This simply
 * maintains a tally that can be used to compute utilisation of the Segment.
 * \param[in] p
 *      A pointer into the Segment as returned by an #append call.
 */
void
Segment::free(const void *p)
{
    assert((uintptr_t)p >= ((uintptr_t)baseAddress + sizeof(SegmentEntry)));
    assert((uintptr_t)p <  ((uintptr_t)baseAddress + capacity));

    const SegmentEntry *entry = (const SegmentEntry *)
        ((const uintptr_t)p - sizeof(SegmentEntry));

    // be sure to account for SegmentEntry structs before each append
    uint64_t length = entry->length + sizeof(SegmentEntry);

    assert((bytesFreed + length) <= tail);

    bytesFreed += length;
}

/**
 * Close the Segment. Once a Segment has been closed, it is considered
 * closed, i.e. it cannot be appended to. Calling #free on a closed
 * Segment to maintain utilisation counts is still permitted. 
 * \throw SegmentException
 *      An exception is thrown if the Segment has already been closed.
 */
void
Segment::close()
{
    if (closed)
        throw SegmentException(HERE, "Segment has already been closed");

    SegmentEntry entry = { LOG_ENTRY_TYPE_SEGFOOTER, sizeof(SegmentFooter) };
    const void *p = forceAppendBlob(&entry, sizeof(entry));
    assert(p != NULL);

    SegmentFooter footer = { checksum };
    p = forceAppendBlob(&footer, sizeof(footer), false);
    assert(p != NULL);

    // ensure that any future append() will fail
    closed = true;

    syncToBackup();
    if (backup)
        backup->closeSegment(logId, id);
}

/**
 * Obtain a const pointer to the first byte of backing memory for this Segment.
 */
const void *
Segment::getBaseAddress() const
{
    return baseAddress;
}

/**
 * Obtain the Segment's Id, which was originally specified in the constructor.
 */
uint64_t
Segment::getId() const
{
    return id;
}

/**
 * Obtain the number of bytes of backing memory that this Segment represents.
 */
uint64_t
Segment::getCapacity() const
{
    return capacity;
}

/**
 * Obtain the maximum number of bytes that can be appended to this Segment
 * using the #append method. Buffers equal to this size or smaller are
 * guaranteed to succeed, whereas buffers larger will fail to be appended.
 */
uint64_t
Segment::appendableBytes() const
{
    if (closed)
        return 0;

    uint64_t freeBytes = capacity - tail;
    uint64_t headRoom  = sizeof(SegmentEntry) + sizeof(SegmentFooter);

    assert(freeBytes >= headRoom);

    if ((freeBytes - headRoom) < sizeof(SegmentEntry))
        return 0;

    return freeBytes - headRoom - sizeof(SegmentEntry);
}

/**
 * Iterate over all entries in this Segment and pass them to the callback
 * provided. This is simply a convenience wrapper around #SegmentIterator.
 * \param[in] cb
 *      The callback to use on each entry.
 * \param[in] cookie
 *      A void* argument to be passed with the specified callback.
 */
void
Segment::forEachEntry(SegmentEntryCallback cb, void *cookie) const
{
    for (SegmentIterator i(this); !i.isDone(); i.next())
        cb(i.getType(), i.getPointer(), i.getLength(), cookie);
}

/**
 * Return the Segment's utilisation as an integer percentage. This is
 * calculated by taking into account the number of live bytes written to
 * the Segment minus the freed bytes in proportion to its capacity.
 */
int
Segment::getUtilisation() const
{
    return (100ULL * (tail - bytesFreed)) / capacity;
}

////////////////////////////////////////
/// Private Methods
////////////////////////////////////////

/**
 * Append exactly the provided raw bytes to the memory backing this Segment.
 * Note that no SegmentEntry is written and the only sanity check is to ensure
 * that the backing memory is not overrun.
 * \param[in] buffer
 *      Pointer to the data to be appended to the Segment's backing memory.
 * \param[in] length
 *      Length of the buffer to be appended in bytes.
 * \param[in] updateChecksum
 *      Optional boolean to disable updates to the Segment checksum. The
 *      default is to update the running checksum while appending data, but
 *      this can be stopped when appending the SegmentFooter, for instance.
 * \return
 *      A pointer into the Segment corresponding to the first byte that was
 *      copied in to.
 */
const void *
Segment::forceAppendBlob(const void *buffer, uint64_t length,
    bool updateChecksum)
{
    assert((tail + length) <= capacity);
    assert(!closed);

    const uint8_t *src = reinterpret_cast<const uint8_t *>(buffer);
    uint8_t       *dst = reinterpret_cast<uint8_t *>(baseAddress) + tail;

    if (updateChecksum) {
        for (uint64_t i = 0; i < length; i++) {
            dst[i] = src[i];
            checksum = rabinPoly.append8(checksum, src[i]);
        }
    } else {
        memcpy(dst, src, length);
    }

    tail += length;
    return reinterpret_cast<void *>(dst);
}

/**
 * Append an entry of any type to the Segment. This function will always
 * succeed so long as there is sufficient room left in the tail of the Segment.
 * \param[in] type
 *      The type of entry to append.
 * \param[in] buffer
 *      Data to be appended to this Segment.
 * \param[in] length
 *      Length of the data to be appended in bytes.
 * \param[in] sync
 *      If true then this write to replicated to backups before return,
 *      otherwise the replication will happen on a subsequent append()
 *      where sync is true or when the segment is closed.  This defaults
 *      to true.
 * \return
 *      A pointer into the Segment corresponding to the first data byte that
 *      was copied in to (i.e. the contents are the same as #buffer).
 */
const void *
Segment::forceAppendWithEntry(LogEntryType type, const void *buffer,
    uint64_t length, bool sync)
{
    assert(!closed);

    uint64_t freeBytes = capacity - tail;
    uint64_t needBytes = sizeof(SegmentEntry) + length;
    if (freeBytes < needBytes)
        return NULL;

    SegmentEntry entry = { type, length };
    forceAppendBlob(&entry, sizeof(entry));
    const void *datap = forceAppendBlob(buffer, length);

    if (sync)
        syncToBackup();

    return datap;
}

/**
 * Ensure all segment data is replicated to backups.
 */
void
Segment::syncToBackup()
{
    if (syncOffset == tail || !backup)
        return;

    uint32_t syncLength = tail - syncOffset;
    backup->writeSegment(
        logId, id, syncOffset,
        reinterpret_cast<const uint8_t*>(baseAddress) + syncOffset, syncLength);
    syncOffset = tail;
}

} // namespace
