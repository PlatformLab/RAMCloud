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

#include "Crc32C.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "Log.h"
#include "LogTypes.h"

namespace RAMCloud {

/**
 * Constructor for Segment.
 * \param[in] log
 *      Pointer to the Log this Segment is a part of.
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
Segment::Segment(Log *log, uint64_t segmentId, void *baseAddress,
    uint32_t capacity, BackupManager *backup)
    : backup(backup),
      baseAddress(baseAddress),
      log(log),
      logId(log->getId()),
      id(segmentId),
      capacity(capacity),
      tail(0),
      bytesFreed(0),
      checksum(),
      closed(false),
      backupSegment(NULL)
{
    commonConstructor();
}

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
    uint32_t capacity, BackupManager *backup)
    : backup(backup),
      baseAddress(baseAddress),
      log(NULL),
      logId(logId),
      id(segmentId),
      capacity(capacity),
      tail(0),
      bytesFreed(0),
      checksum(),
      closed(false),
      backupSegment(NULL)
{
    commonConstructor();
}

/**
 * Perform actions common to all Segment constructors, including writing
 * the header and opening the backup.
 */
void
Segment::commonConstructor()
{
    assert(capacity >= sizeof(SegmentEntry) + sizeof(SegmentHeader) +
                       sizeof(SegmentEntry) + sizeof(SegmentFooter));

    new(static_cast<uint8_t*>(baseAddress) + tail)
        SegmentEntry { LOG_ENTRY_TYPE_SEGHEADER, sizeof(SegmentHeader) };
    tail += sizeof(SegmentEntry);

    new(static_cast<uint8_t*>(baseAddress) + tail)
        SegmentHeader { logId, id, capacity };
    tail += sizeof(SegmentHeader);

    if (log)
        log->stats.totalBytesAppended += tail;

#ifndef PERF_DEBUG_RECOVERY_NO_CKSUM
    checksum.update(baseAddress, tail);
#endif

    if (backup)
        backupSegment = backup->openSegment(id, baseAddress, tail);
}

Segment::~Segment()
{
    // TODO(ongaro): I'm not convinced this makes any sense. sync is probably
    // better.
    if (backup)
        backup->freeSegment(id);
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
 * \param[out] lengthInSegment
 *      If non-NULL, the actual number of bytes consumed by this append to
 *      the Segment is stored to this address. Note that this size includes
 *      all Log and Segment overheads, so it will be greater than the
 *      ``length'' parameter. 
 * \param[out] offsetInSegment
 *      If non-NULL, the offset in this Segment at which the operation was
 *      performed is returned here. Note that this offset does not correspond
 *      to where the contents of ``buffer'' was written, but to the preceding
 *      metadata for this operation.
 * \return
 *      On success, a SegmentEntryHandle is returned, which points to the
 *      ``buffer'' written. On failure, the handle is NULL. We avoid using
 *      slow exceptions since this can be on the fast path.
 */
SegmentEntryHandle
Segment::append(LogEntryType type, const void *buffer, uint32_t length,
    uint64_t *lengthInSegment, uint64_t *offsetInSegment, bool sync)
{
    if (closed || type == LOG_ENTRY_TYPE_SEGFOOTER ||
      appendableBytes() < length)
        return NULL;

    if (offsetInSegment != NULL)
        *offsetInSegment = tail;

    return forceAppendWithEntry(type, buffer, length, lengthInSegment, sync);
}

/**
 * Mark bytes used by a single entry in this Segment as freed. This simply
 * maintains a tally that can be used to compute utilisation of the Segment.
 * \param[in] entry
 *      A SegmentEntryHandle as returned by an #append call.
 */
void
Segment::free(SegmentEntryHandle entry)
{
    assert((uintptr_t)entry >= ((uintptr_t)baseAddress + sizeof(SegmentEntry)));
    assert((uintptr_t)entry <  ((uintptr_t)baseAddress + capacity));

    // be sure to account for SegmentEntry structs before each append
    uint32_t length = entry->totalLength();

    assert((bytesFreed + length) <= tail);

    bytesFreed += length;
}

/**
 * Close the Segment. Once a Segment has been closed, it is considered
 * closed, i.e. it cannot be appended to. Calling #free on a closed
 * Segment to maintain utilisation counts is still permitted. 
 * \param sync
 *      Whether to wait for the replicas to acknowledge that the segment is
 *      closed.
 * \throw SegmentException
 *      An exception is thrown if the Segment has already been closed.
 */
void
Segment::close(bool sync)
{
    if (closed)
        throw SegmentException(HERE, "Segment has already been closed");

    SegmentEntry entry = { LOG_ENTRY_TYPE_SEGFOOTER, sizeof(SegmentFooter) };
    const void *p = forceAppendBlob(&entry, sizeof(entry));
    assert(p != NULL);

    SegmentFooter footer = { checksum.getResult() };
    p = forceAppendBlob(&footer, sizeof(footer), false);
    assert(p != NULL);

    // ensure that any future append() will fail
    closed = true;

    if (backup) {
        backupSegment->write(tail, true); // start replicating immediately
        backupSegment = NULL;
        if (sync) // sync determines whether to wait for the acks
            backup->sync();
    }
}

/**
 * Wait for the segment to be fully replicated.
 */
void
Segment::sync()
{
    if (backup) {
        if (backupSegment) {
            backupSegment->write(tail, closed);
            if (closed)
                backupSegment = NULL;
        }
        backup->sync();
    }
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
Segment::forceAppendBlob(const void *buffer, uint32_t length,
    bool updateChecksum)
{
    assert((tail + length) <= capacity);
    assert(!closed);

    const uint8_t *src = reinterpret_cast<const uint8_t *>(buffer);
    uint8_t       *dst = reinterpret_cast<uint8_t *>(baseAddress) + tail;

#ifdef PERF_DEBUG_RECOVERY_NO_CKSUM
    updateChecksum = false;
#endif // PERF_DEBUG_RECOVERY_NO_CKSUM
    if (updateChecksum)
        checksum.update(src, length);
    memcpy(dst, src, length);

    if (log)
        log->stats.totalBytesAppended += length;

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
 * \param[out] lengthOfAppend
 *      If non-NULL, the actual number of bytes consumed by this append to
 *      the Segment is stored to this address. Note that this size includes all
 *      Log and Segment overheads, so it will be greater than the ``length''
 *      parameter. 
 * \param[in] sync
 *      If true then this write to replicated to backups before return,
 *      otherwise the replication will happen on a subsequent append()
 *      where sync is true or when the segment is closed.  This defaults
 *      to true.
 * \return
 *      A SegmentEntryHandle corresponding to the data just written. 
 */
SegmentEntryHandle
Segment::forceAppendWithEntry(LogEntryType type, const void *buffer,
    uint32_t length, uint64_t *lengthOfAppend, bool sync)
{
    assert(!closed);

    uint64_t freeBytes = capacity - tail;
    uint64_t needBytes = sizeof(SegmentEntry) + length;
    if (freeBytes < needBytes)
        return NULL;

    SegmentEntry entry = { type, length };
    const void* entryPointer = forceAppendBlob(&entry, sizeof(entry));
    forceAppendBlob(buffer, length);

    if (sync)
        this->sync();

    if (lengthOfAppend != NULL)
        *lengthOfAppend = needBytes;

    return reinterpret_cast<SegmentEntryHandle>(entryPointer);
}

} // namespace
