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

#ifndef RAMCLOUD_SEGMENTITERATOR_H
#define RAMCLOUD_SEGMENTITERATOR_H

#include <stdint.h>
#include <vector>
#include "Segment.h"

using std::vector;

namespace RAMCloud {

/**
 * An exception that is thrown when the SegmentIterator class is provided
 * invalid method arguments.
 */
struct SegmentIteratorException : public Exception {
    SegmentIteratorException() : Exception() {}
    explicit SegmentIteratorException(std::string msg) : Exception(msg) {}
    explicit SegmentIteratorException(int errNo) : Exception(errNo) {}
};

class SegmentIterator {
  public:
    explicit SegmentIterator(const Segment *segment);
    SegmentIterator(const void *buffer, uint64_t length);

    bool         isDone() const;
    void         next();
    LogEntryType getType() const;
    uint64_t     getLength() const;

    /**
     * Obtain a const T* to the data associated with the current SegmentEntry.
     * \tparam T
     *      The type to cast the pointer as for the return.
     * \return
     *      A const T* to the current data.
     * \throw SegmentIteratorException
     *      An exception is thrown if the iterator has no more entries.
     */
    template <typename T>
    const T*
    get() const
    {
        if (currentEntry == NULL)
            throw SegmentIteratorException("getPointer while isDone");
        return reinterpret_cast<const T*>(blobPtr);
    }

    const void  *getPointer() const;
    uint64_t     getOffset() const;
    bool         isChecksumValid() const;

    /**
     * Given the backing memory for a Segment, calculate the checksum.
     * The checksum includes everything up to the SegmentFooter. If the
     * Segment has not been closed (i.e. contains no SegmentFooter), the
     * caller may specify the stopOffset field, in which case the calculation
     * will cease at the stopOffset'th byte without including it in the
     * checksum's calculation.
     * \param[in] buffer
     *      Pointer to the first byte of a Segment's backing memory.
     * \param[in] segmentCapacity
     *      The total capacity of the Segment in bytes.
     * \param[in] stopOffset
     *      An optional early checksum termination offset. The byte at this
     *      offset is not included in the checksum returned.
     * \return
     *      The Segment checksum corresponding to the provided parameters.
     */
    static uint64_t
    generateChecksum(const void *buffer, uint64_t segmentCapacity,
                     const uint64_t stopOffset = ~(0ull))
    {
        uint64_t offset = 0;
        uint64_t checksum = 0;
        rabinpoly rabinPoly(Segment::RABIN_POLYNOMIAL);

        for (SegmentIterator i(buffer, segmentCapacity); !i.isDone(); i.next()){
            LogEntryType type = i.getType();
            uint32_t length   = i.getLength();
            SegmentEntry e    = { type, length };

#define u64 uint64_t

            const uint8_t *p = reinterpret_cast<const uint8_t *>(&e);
            for (u64 j = 0; j < sizeof(e) && offset < stopOffset; j++, offset++)
                checksum = rabinPoly.append8(checksum, p[j]);

            // checksum cannot include itself!
            if (type == LOG_ENTRY_TYPE_SEGFOOTER)
                break;

            p = reinterpret_cast<const uint8_t *>(i.getPointer());
            for (u64 j = 0; j < length && offset < stopOffset; j++, offset++)
                checksum = rabinPoly.append8(checksum, p[j]);

#undef u64

            if (offset == stopOffset)
                break;
        }

        return checksum;
    }

  private:
    void            CommonConstructor(void);
    bool            isEntryValid(const SegmentEntry *entry) const;

    const void     *baseAddress;     // base address for the Segment
    uint64_t        segmentCapacity; // maximum length of the segment in bytes
    uint64_t        id;              // segment identification number

    // current iteration state
    LogEntryType     type;
    uint64_t         length;
    const void      *blobPtr;
    bool             sawFooter;

    const SegmentEntry    *firstEntry;
    const SegmentEntry    *currentEntry;

    friend class SegmentIteratorTest;

    DISALLOW_COPY_AND_ASSIGN(SegmentIterator);
};

} // namespace

#endif // !RAMCLOUD_SEGMENTITERATOR_H
