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

#include "RecoverySegmentIterator.h"

namespace RAMCloud {

// --- RecoverySegmentIterator ---

/**
 * Create an iterator for a recovery segment buffer.
 *
 * \param segment
 *      The recovery segment to be iterated over.
 * \param size
 *      The size buffer at #segment.
 */
RecoverySegmentIterator::RecoverySegmentIterator(const void* segment,
                                                 uint32_t size)
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
RecoverySegmentIterator::isDone() const
{
    return offset >= size;
}

/**
 * Progress the iterator to the next entry, if there is one.
 * Future calls to #getType, #getLength, #getPointer, and #getOffset will
 * reflect the next SegmentEntry's parameters.
 */
void
RecoverySegmentIterator::next()
{
    if (isDone())
        return;
    offset += getLength() + downCast<uint32_t>(sizeof(SegmentEntry));
    bool prefetching = true;
    if (prefetching && !isDone()) {
        const char* entry = &segment[offset +
                                     sizeof(SegmentEntry) +
                                     getLength()];
        prefetch(entry, 128);
    }
}

/**
 * \return
 *      The current entry.
 */
const SegmentEntry&
RecoverySegmentIterator::getEntry() const
{
    return *reinterpret_cast<const SegmentEntry*>(&segment[offset]);
}

/**
 * \return
 *      The type of the current entry.
 */
LogEntryType
RecoverySegmentIterator::getType() const
{
    return getEntry().type;
}

/**
 * \return
 *      The length in bytes of the current entry.
 */
uint32_t
RecoverySegmentIterator::getLength() const
{
    return getEntry().length;
}

/**
 * \return
 *      A pointer to entry data following its SegmentEntry header.
 */
const void*
RecoverySegmentIterator::getPointer() const
{
    return reinterpret_cast<const char*>(&segment[getOffset()]);
}

/**
 * \return
 *      The offset of the current data following its SegmentEntry header.
 */
uint64_t
RecoverySegmentIterator::getOffset() const
{
    return offset + sizeof(SegmentEntry);
}

/**
 * \return
 *      The checksum stored with the current SegmentEntry. Note that it could
 *      be invalid if data corruption occurred.
 */
SegmentChecksum::ResultType
RecoverySegmentIterator::checksum() const
{
    return reinterpret_cast<SegmentEntryHandle>(&getEntry())->checksum();
}

/**
 * \return
 *      The checksum calculated for the current SegmentEntry.
 */
SegmentChecksum::ResultType
RecoverySegmentIterator::generateChecksum() const
{
    return reinterpret_cast<SegmentEntryHandle>(
        &getEntry())->generateChecksum();
}

/**
 * \return
 *      true if the checksum is valid for the current entry.
 */
bool
RecoverySegmentIterator::isChecksumValid() const
{
    return reinterpret_cast<SegmentEntryHandle>(&getEntry())->isChecksumValid();
}

} // namespace RAMCloud

