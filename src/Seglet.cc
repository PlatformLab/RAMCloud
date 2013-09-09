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

#include "Common.h"
#include "Seglet.h"
#include "SegletAllocator.h"

namespace RAMCloud {

/**
 * Construct a new seglet owned by the given SegletAllocator instance.
 *
 * \param segletAllocator
 *      The SegletAllocator that owns this seglet. When the free() method is
 *      invoked, the seglet will be returned to this object.
 * \param buffer
 *      The contiguous block of memory this seglet describes.
 * \param length
 *      Length of the contiguous memory block in bytes.
 */
Seglet::Seglet(SegletAllocator& segletAllocator,
               void* buffer,
               uint32_t length)
    : segletAllocator(segletAllocator),
      buffer(buffer),
      length(length),
      sourcePool(NULL)
{
}

/**
 * Return this seglet to its allocator. The caller must not use this seglet
 * after making this call.
 */
void
Seglet::free()
{
    segletAllocator.free(this);
}

/**
 * Return a pointer to this seglet's contiguous memory block.
 */
void*
Seglet::get() const
{
    return buffer;
}

/**
 * Return the length of this seglet's contiguous memory block.
 */
uint32_t
Seglet::getLength() const
{
    return length;
}

/**
 * Mark this seglet as having been allocated from the given pool. This must
 * only be invoked by the SegletAllocator for its own bookkeeping.
 *
 * \param newSourcePool
 *      The pool to associate with this seglet.
 */
void
Seglet::setSourcePool(const vector<Seglet*>* newSourcePool)
{
    sourcePool = newSourcePool;
}

/**
 * Return a pointer to the pool this seglet was last allocated from. The
 * SegletAllocator may use this information to decide which pool to return
 * it to when the seglet is freed.
 */
const vector<Seglet*>*
Seglet::getSourcePool() const
{
    return sourcePool;
}

} // end RAMCloud
