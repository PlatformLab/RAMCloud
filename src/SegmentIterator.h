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

#ifndef RAMCLOUD_SEGMENTITERATOR_H
#define RAMCLOUD_SEGMENTITERATOR_H

#include <stdint.h>
#include <vector>
#include "Crc32C.h"
#include "Log.h"
#include "Segment.h"

using std::vector;

namespace RAMCloud {

/**
 * An exception that is thrown when the SegmentIterator class cannot iterate
 * over a segment due to corruption.
 */
struct SegmentIteratorException : public Exception {
    SegmentIteratorException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

/**
 * A SegmentIterator provides the necessary state and methods to step through
 * each entry in a segment in order.
 *
 * Note that the segments being iterated over must not change while the iterator
 * exists, otherwise behaviour is undefined.
 *
 * Copy and assignment is defined on SegmentIterators.
 */
class SegmentIterator {
  public:
    explicit SegmentIterator(Segment& segment);
    SegmentIterator(const void* buffer, uint32_t length,
                    const SegmentCertificate& certificate);
    SegmentIterator(const SegmentIterator& other);
    SegmentIterator& operator=(const SegmentIterator& other);
    ~SegmentIterator();
    void next();
    LogEntryType getType();
    uint32_t getLength();
    uint32_t getOffset();
    void setOffset(uint32_t offset);
    void setLimit(uint32_t limit);
    Segment::Reference getReference();
    uint32_t appendToBuffer(Buffer& buffer);
    uint32_t setBufferTo(Buffer& buffer);
    void checkMetadataIntegrity();

    /**
     * Test if the SegmentIterator has exhausted all entries. More concretely, if
     * the current entry is valid, this will return false. After next() has been
     * called on the last valid entry, this will return true.
     *
     * \return
     *      true if there are no more entries left to iterate, else false.
     */
    bool
    isDone()
    {
        // This is defined in the header file so that the logic is inlined,
        // rather than duplicated in the .cc file to avoid extra method calls.
        return currentOffset >= offsetLimit;
    }

    /**
     * Return a pointer to the current entry being iterated over. This will
     * point to contiguous memory and may be accessed directly, without
     * needing to construct and go through a Buffer.
     *
     * Since the entry in the segment may not be contiguous, a buffer large
     * enough to hold however much of it will be accessed should be passed in.
     * This buffer will only be used to copy the (partial) entry into a
     * contiguous chunk of memory in the event that it isn't already contiguous.
     *
     * This method exists for very hot paths that cannot afford to wrap entries
     * in Buffer objects. It assumes you know what you're doing and won't access
     * past the amount of contiguous memory guaranteed by the buffer you pass
     * in.
     *
     * \param buffer
     *      Buffer used to copy part of all of the entry into if the desired
     *      length is not already contiguous in memory.
     * \param length
     *      Length of the buffer. Or, in other words, the maximum number of
     *      contiguous bytes desired. If the whole entry needs to be accessed,
     *      this must be large enough to hold all of it. One simple way to
     *      ensure this is to allocate a single buffer the size of the segment
     *      being iterated over that can be passed into this method.
     *
     *      If only the front of the entry will be accessed (for example, just
     *      the object header and key), a smaller length can be specified so
     *      that large objects are not copied in their entirety when not being
     *      accessed in full.
     * \return
     *      NULL if the iterator is out of objects, otherwise a pointer to the
     *      contiguous entry (either in the segment, or copied into the caller-
     *      provided buffer). The number of contiguous bytes is guaranteed to be
     *      at most #length.
     */
    template<typename T>
    const T*
    getContiguous(void* buffer, uint32_t length)
    {
        if (isDone())
            return NULL;

        uint32_t entryOffset = currentOffset +
                               sizeof32(currentHeader) +
                               currentHeader.getLengthBytes();
        const void* pointer = NULL;
        uint32_t contigBytes = segment->peek(entryOffset, &pointer);
        if (contigBytes < getLength() && contigBytes < length) {
            assert(getLength() <= length);
            segment->copyOut(entryOffset,
                             buffer,
                             getLength());
            pointer = buffer;
        }

        return reinterpret_cast<const T*>(pointer);
    }

  PRIVATE:
    /// If the constructor was called on a void pointer, we'll create a wrapper
    /// segment to access the data in a common way using segment object calls.
    Tub<Segment> wrapperSegment;

    /// Buffer used to construct #wrapperSegment, if any. Only used in copy
    /// constructor to construct an identical #wrapperSegment if populated.
    const void* buffer;

    /// Length used to construct #wrapperSegment, if any. Only used in copy
    /// constructor to construct an identical #wrapperSegment if populated.
    uint32_t length;

    /// Pointer to the segment we're iterating on. This points either to the
    /// segment object passed in to the constructor, or wrapperSegment above if
    /// we're iterating over a void buffer.
    Segment* segment;

    /// Contains information to verify the integrity of the metadata of
    /// the segment.
    /// checkMetadataIntegrity() must be called explicitly before iterating
    /// over the segment, otherwise, the metadata of the segment is trusted.
    SegmentCertificate certificate;

    /// Current offset into the segment. This points to the entry we're on and
    /// will use in the getType, getLength, appendToBuffer, etc. calls.
    uint32_t currentOffset;

    /// Don't consider any log entries whose first byte has a higher
    /// offset within the segments than this.
    uint32_t offsetLimit;

    /// Copy of the current log entry's header (the one at currentOffset).
    Segment::EntryHeader currentHeader;

    /// Cache of the length of the entry at currentOffset. Set the first time
    /// getLength() is called and destroyed when next() is called.
    Tub<uint32_t> currentLength;
};

} // namespace

#endif // !RAMCLOUD_SEGMENTITERATOR_H
