/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_SEGLET_H
#define RAMCLOUD_SEGLET_H

#include "Common.h"
#include "Tub.h"

namespace RAMCloud {

// Forward declare around circular dependency.
class SegletAllocator;

/**
 * This class wraps a single contiguous block of memory and associates it with
 * the allocator it should be returned to when no longer in use. Seglets are
 * owned by the allocator and loaned out to instances of the Segment class. The
 * Segment class uses one or more seglets to create a larger, logically
 * contiguous block of memory to store log data in.
 *
 * Seglets are always owned by their allocator and should never be deleted by
 * any other module. When a segment is destroyed or instructed to free unused
 * seglets, the seglets' free() methods are invoked, returning them to the
 * allocator whence they came.
 */
class Seglet {
  public:
    /// The default seglet size is a reasonable trade-off between reducing
    /// fragmentation and keeping the number of seglets per segment relatively
    /// small.
    enum { DEFAULT_SEGLET_SIZE = 64 * 1024 };

    Seglet(SegletAllocator& segletAllocator, void* buffer, uint32_t length);
    void free();
    void* get() const;
    uint32_t getLength() const;
    void setSourcePool(const vector<Seglet*>* newSourcePool);
    const vector<Seglet*>* getSourcePool() const;

  PRIVATE:
    /// SegletAllocator to return this memory to when free() is called.
    SegletAllocator& segletAllocator;

    /// Pointer to this seglet's contiguous block of memory.
    void* const buffer;

    /// Length of the contiguous block in bytes.
    const uint32_t length;

    /// Used by the SegletAlloctor to keep track of the pool a seglet was last
    /// allocated from. The SegletAllocator does not need to track all seglets,
    /// so this field may sometimes be NULL.
    const vector<Seglet*>* sourcePool;

    DISALLOW_COPY_AND_ASSIGN(Seglet);
};

} // end RAMCloud

#endif // RAMCLOUD_SEGLET_H
