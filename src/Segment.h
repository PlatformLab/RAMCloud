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

// RAMCloud pragma [CPPLINT=0]

#ifndef RAMCLOUD_SEGMENT_H
#define RAMCLOUD_SEGMENT_H

#include <stdint.h>

#include "rabinpoly.h"
#include "LogTypes.h"

namespace RAMCloud {

struct SegmentEntry {
    LogEntryType type;
    uint32_t     length;
} __attribute__((__packed__));

struct SegmentHeader {
    uint64_t logId;
    uint64_t segmentId;
    uint32_t segmentCapacity;
} __attribute__((__packed__));

struct SegmentFooter {
    uint64_t checksum;
} __attribute__((__packed__));

typedef void (*SegmentEntryCallback)(LogEntryType, const void *,
                                     uint64_t, void *);

/**
 * An exception that is thrown when the Segment class is provided invalid
 * method arguments or mutating operations are attempted on a closed Segment.
 */
struct SegmentException : public Exception {
    SegmentException() : Exception() {}
    explicit SegmentException(std::string msg) : Exception(msg) {}
    explicit SegmentException(int errNo) : Exception(errNo) {}
};

class Segment {
  public:
    Segment(uint64_t logId, uint64_t segmentId, void *baseAddress,
            uint64_t capacity);
   ~Segment();

    const void      *append(LogEntryType type, const void *buffer,
                            uint64_t length);
    void             free(const void *p);
    void             close();
    const void      *getBaseAddress() const;
    uint64_t         getId() const;
    uint64_t         getCapacity() const;
    uint64_t         appendableBytes() const;
    void             forEachEntry(SegmentEntryCallback cb, void *cookie) const;
    int              getUtilisation() const;

    static const uint32_t  SEGMENT_SIZE = 8 * 1024 * 1024;
    static const uint64_t  INVALID_SEGMENT_ID = ~(0ull);
    static const uint64_t  RABIN_POLYNOMIAL = 0x92d42091a28158a5ull;

  private:
    const void      *forceAppendBlob(const void *buffer, uint64_t length,
                                     bool updateChecksum = true);
    const void      *forceAppendWithEntry(LogEntryType type,
                                          const void *buffer, uint64_t length);

    void            *baseAddress;    // base address for the Segment
    uint64_t         id;             // segment identification number
    const uint64_t   capacity;       // total byte length of segment when empty
    uint64_t         tail;           // offset to the next free byte in Segment
    uint64_t         bytesFreed;     // bytes free()'d in this Segment
    rabinpoly        rabinPoly;      // Rabin Polynomial class used for checksum
    uint64_t         checksum;       // Latest Segment checksum
    bool             closed;         // when true, no appends permitted

    DISALLOW_COPY_AND_ASSIGN(Segment);

    friend class SegmentTest;
    friend class SegmentIteratorTest;
    friend class LogTest;
};

} // namespace

#endif // !RAMCLOUD_SEGMENT_H
