/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_POOL_H
#define RAMCLOUD_POOL_H

#include <new>

#include "Common.h"

namespace RAMCloud {

/**
 * A Pool manages allocation of set of fixed-size blocks, caching them for
 * reuse to avoid the memory allocator.  Pool does not perform allocations
 * itself and does not take responsibility for freeing the underlying memory;
 * see GrowablePool to get this behavior.
 *
 * For type safety prefer new(size_t, Pool*) to allocate().
 *
 * Additional memory regions can be added to a Pool using addAllocation().
 */
class Pool {
  public:
    explicit Pool(uint64_t blockSize);
    Pool(uint64_t blockSize, void *base, uint64_t totalBytes);
    virtual ~Pool();
    void addAllocation(void* allocation, uint64_t allocationSize);
    void* allocate();
    void free(void* block);
    uint64_t getBlockSize() const;
    uint64_t getFreeBlockCount() const;

  private:
    vector<void *> blocks;
    uint64_t blockSize;

    friend class PoolTest;
    DISALLOW_COPY_AND_ASSIGN(Pool);
};

class GrowablePool : public Pool {
  public:
    GrowablePool(uint64_t blockSize, uint64_t allocationSize);
    virtual ~GrowablePool();
    void* allocate();

  private:
    void grow();

    vector<void*> allocations;
    uint64_t allocationSize;

    friend class GrowablePoolTest;
    DISALLOW_COPY_AND_ASSIGN(GrowablePool);
};

} // namespace

void* operator new(size_t size, RAMCloud::Pool* pool);
void* operator new[](size_t size, RAMCloud::Pool* pool);

#endif // RAMCLOUD_POOL_H
