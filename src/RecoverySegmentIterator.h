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

#ifndef RAMCLOUD_RECOVERYSEGMENTITERATOR_H
#define RAMCLOUD_RECOVERYSEGMENTITERATOR_H

#include "Common.h"
#include "LogTypes.h"
#include "Segment.h"

namespace RAMCloud {

/**
 * Walks recovery segments.  Enumerates a recovery segment's SegmentEntries
 * as generated from a call to getRecoveryData.
 * This class is similar to SegmentIterator but is far more permissive
 * and is expected to be used only when the length of valid recovery
 * segment data is known.
 */
class RecoverySegmentIterator {
  public:
    explicit RecoverySegmentIterator(const void* segment, uint32_t size);
    bool isDone() const;
    void next();
    const SegmentEntry& getEntry() const;
    LogEntryType getType() const;
    uint64_t getLength() const;

    /**
     * Obtain a const T* to the data for the current SegmentEntry.
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
        return reinterpret_cast<const T*>(getPointer());
    }

    const void* getPointer() const;
    uint64_t getOffset() const;

  PRIVATE:
    /// Bytes from #segment where the current SegmentEntry is.
    uint32_t offset;

    /// The buffer containing the recovery segment to iterate over.
    const char* segment;

    /// The number of bytes that are part of the recovery segment.
    uint32_t size;

    DISALLOW_COPY_AND_ASSIGN(RecoverySegmentIterator);
};

} // namespace RAMCloud

#endif // RAMCLOUD_RECOVERYSEGMENT_H
