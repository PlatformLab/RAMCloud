/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <ext/algorithm>

#include "RecoverySegment.h"

namespace RAMCloud {

// --- RecoverySegment::Iterator ---

/**
 * \param segment
 *      The RecoverySegment to be iterated over.
 */
RecoverySegment::Iterator::Iterator(const RecoverySegment& segment)
    : offset()
    , segment(segment.get())
    , size(segment.size())
{
}

/**
 * Create an iterator for a buffer which will make a best effort to
 * walk it despite any important RAMCloud invariants it might violate.
 *
 * \param segment
 *      The recovery segment or segment to be iterated over.
 * \param size
 *      The size buffer at #segment.
 */
RecoverySegment::Iterator::Iterator(const void* segment, uint32_t size)
    : offset()
    , segment(static_cast<const char*>(segment))
    , size(size)
{
}

/**
 * \return
 *      true if there are no more entries left to iterate, else false.
 */
bool
RecoverySegment::Iterator::isDone() const
{
    return offset >= size;
}

/**
 * Progress the iterator to the next entry, if there is one.
 * Future calls to #getType, #getLength, #getPointer, and #getOffset will
 * reflect the next SegmentEntry's parameters.
 */
void
RecoverySegment::Iterator::next()
{
    if (isDone())
        return;
    offset += getLength() + sizeof(SegmentEntry);
    bool prefetching = true;
    if (prefetching) {
        const char* entry = &segment[offset + sizeof(SegmentEntry) + getLength()];
        _mm_prefetch(entry, _MM_HINT_T0);
        _mm_prefetch(entry + 64, _MM_HINT_T0);
        _mm_prefetch(entry + 128, _MM_HINT_T0);
    }
}

/**
 * \return
 *      The current entry.
 */
const SegmentEntry&
RecoverySegment::Iterator::getEntry() const
{
    return *reinterpret_cast<const SegmentEntry*>(&segment[offset]);
}

/**
 * \return
 *      The type of the current entry.
 */
LogEntryType
RecoverySegment::Iterator::getType() const
{
    return getEntry().type;
}

/**
 * \return
 *      The length in bytes of the current entry.
 */
uint64_t
RecoverySegment::Iterator::getLength() const
{
    return getEntry().length;
}

/**
 * \return
 *      A pointer to entry data following its SegmentEntry header.
 */
const void*
RecoverySegment::Iterator::getPointer() const
{
    return reinterpret_cast<const char*>(&segment[getOffset()]);
}

/**
 * \return
 *      The offset of the current data following its SegmentEntry header.
 */
uint64_t
RecoverySegment::Iterator::getOffset() const
{
    return offset + sizeof(SegmentEntry);
}

// --- RecoverySegment ---

/**
 * Create a recovery segment.
 *
 * \param startSize
 *      A hint for memory allocation used to choose the size of the
 *      first allocation done to back this recovery segment.  This
 *      should probably be some function of the stored segment size
 *      over the number of partitions that will be used to recover.
 */
RecoverySegment::RecoverySegment(uint32_t startSize)
    : offset()
{
}

/**
 * Append an entry to this RecoverySegment.
 *
 * \param type
 *      The type of entry to append.
 * \param buffer
 *      Data to be appended to this Segment.
 * \param] length
 *      Length of the data to be appended in bytes.
 */
void
RecoverySegment::append(LogEntryType type, const void* buffer, uint32_t length)
{
    SegmentEntry entry{type, length};
    memcpy(&data[offset],
           reinterpret_cast<const char*>(&entry), sizeof(entry));
    memcpy(&data[offset + sizeof(entry)],
           reinterpret_cast<const char*>(buffer), length);
    offset += sizeof(entry) + length;
}

/**
 * Copy the recovery segment into a buffer.
 *
 * \param buf
 *      A buffer of at least size() bytes where the recovery segment
 *      will be stored.
 */
void
RecoverySegment::copy(char* buf) const
{
    memcpy(buf, data, offset);
}

/**
 * \return
 *      A pointer to a contiguous block of memory with all the data
 *      appended to it so far.  This pointer is invalidated by future
 *      appends.
 */
const char*
RecoverySegment::get() const
{
    return &data[0];
}

/**
 * \return
 *      The number of bytes appended to this recovery segment so far.
 */
uint32_t
RecoverySegment::size() const
{
    return offset;
}

} // namespace RAMCloud

