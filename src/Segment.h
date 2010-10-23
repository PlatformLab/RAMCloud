/* Copyright (c) 2009 Stanford University
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

#ifndef RAMCLOUD_SEGMENT_H
#define RAMCLOUD_SEGMENT_H

#include "Common.h"

#include "BackupClient.h"

namespace RAMCloud {

#define SEGMENT_INVALID_ID  ((uint64_t)0)

class Segment {
  public:
    Segment(void *, const uint64_t, BackupClient *);
    ~Segment();
    void        ready(uint64_t);
    void        reset();
    const void *append(const void *, const uint64_t);
    void        free(uint64_t);
    const void *getBase() const;
    uint64_t getId() const;
    uint64_t getFreeTail() const;
    uint64_t getLength() const;
    uint64_t getUtilization() const;
    bool checkRange(const void *, uint64_t) const;
    void finalize();
    Segment *link(Segment *n);
    Segment *unlink();
    void setUsedBytes(uint64_t ub) {
        freeBytes = totalBytes - ub;
    }
  private:
    void     *base;
    bool      isMutable;
    uint64_t  id;                  // segment id
    const uint64_t  totalBytes;    // capacity of the segment
    uint64_t  freeBytes;           // bytes free in segment (anywhere)
    uint64_t  tailBytes;           // bytes free in tail of segment (i.e.
                                   // never written to)
    BackupClient *backup;

    Segment  *next, *prev;

    friend class SegmentTest;
    friend class LogTest;
    DISALLOW_COPY_AND_ASSIGN(Segment);
};

} // namespace

#endif // !_SEGMENT_H_
