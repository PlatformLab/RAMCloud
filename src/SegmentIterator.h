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
#include "Crc32C.h"
#include "Log.h"

using std::vector;

namespace RAMCloud {

/**
 * An exception that is thrown when the SegmentIterator class is provided
 * invalid method arguments.
 */
struct SegmentIteratorException : public Exception {
    explicit SegmentIteratorException(const CodeLocation& where)
        : Exception(where) {}
    SegmentIteratorException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    SegmentIteratorException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    SegmentIteratorException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

class SegmentIterator {
  public:
    SegmentIterator();
    explicit SegmentIterator(const Segment *segment);
    SegmentIterator(const void *buffer, uint64_t capacity,
                    bool ignoreCapacityMismatch = false);
    VIRTUAL_FOR_TESTING ~SegmentIterator();

    VIRTUAL_FOR_TESTING bool         isDone() const;
    VIRTUAL_FOR_TESTING void         next();
    VIRTUAL_FOR_TESTING LogEntryType getType() const;
    VIRTUAL_FOR_TESTING uint32_t     getLength() const;
    VIRTUAL_FOR_TESTING uint32_t     getLengthInLog() const;
    VIRTUAL_FOR_TESTING LogPosition  getLogPosition() const;

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
            throw SegmentIteratorException(HERE, "getPointer while isDone");
        return reinterpret_cast<const T*>(blobPtr);
    }

    VIRTUAL_FOR_TESTING SegmentEntryHandle          getHandle() const;
    VIRTUAL_FOR_TESTING const SegmentHeader&        getHeader() const;
    VIRTUAL_FOR_TESTING const void                 *getPointer() const;
    VIRTUAL_FOR_TESTING uint64_t                    getOffset() const;
    VIRTUAL_FOR_TESTING SegmentChecksum::ResultType generateChecksum() const;
    VIRTUAL_FOR_TESTING bool                        isCleanerSegment() const;
    VIRTUAL_FOR_TESTING bool                        isChecksumValid() const;
    VIRTUAL_FOR_TESTING bool                        isSegmentChecksumValid()
                                                                        const;

  PRIVATE:
    void            commonConstructor(bool ignoreCapacityMismatch);
    bool            isEntryValid(const SegmentEntry *entry) const;

    /// Base address for the segment being iterated over.
    const void     *baseAddress;

    /// Maximum length of the segment in bytes.
    uint64_t        segmentCapacity;

    /// Number of bytes actually used in the segment. This is used to tell
    /// when we're done iterating on an open segment.
    uint64_t        segmentBytesUsed;

    /// Segment identification number.
    uint64_t        id;

    // current iteration state
    LogEntryType     type;
    uint32_t         length;
    const void      *blobPtr;
    bool             sawFooter;

    const SegmentEntry    *firstEntry;
    const SegmentEntry    *currentEntry;

    DISALLOW_COPY_AND_ASSIGN(SegmentIterator);
};

} // namespace

#endif // !RAMCLOUD_SEGMENTITERATOR_H
