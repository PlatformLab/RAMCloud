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

#include <stdlib.h>

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

/**
 * Allocates memory using the standard memory allocator.  Provides an
 * interface satisfing Pools.
 */
struct Mallocator {
    /**
     * Returns a pointer to the newly allocated block of bytes bytes.
     *
     * \param bytes
     *      Number of consecutive bytes to allocate in the returned block.
     */
    void* allocate(size_t bytes) { return xmalloc(bytes); }

    /**
     * Release allocation from this Mallocator to the system.
     *
     * \param addr
     *      The start of the address block to return.
     */
    void deallocate(void* addr) { free(addr); }
};

/// An allocator which just uses the standard system allocator to allocate.
extern Mallocator mallocator;

/**
 * Allocates memory aligned on a specific byte multiple.  Provides an
 * interface satisfing Pools.
 */
class AlignedAllocator {
  public:
    /**
     * Create an AlignedAllocator.
     *
     * \param alignment
     *      Allocate objects on these multiples and in multiples of this size.
     */
    explicit AlignedAllocator(uint64_t alignment)
        : alignment(alignment)
    {
    }

    /**
     * Returns a pointer to the newly allocated block of bytes bytes.
     *
     * \param bytes
     *      Number of consecutive bytes to allocate in the returned block.
     */
    void*
    allocate(size_t bytes)
    {
        void* addr;
        int ret = posix_memalign(&addr, alignment, bytes);
        if (ret != 0)
            throw std::bad_alloc();
        return addr;
    }

    /**
     * Release allocation from this AlignedAllocator to the system.
     *
     * \param addr
     *      The start of the address block to return.
     */
    void
    deallocate(void* addr)
    {
        free(addr);
    }

  private:
    size_t alignment;
};

/// An allocator which allocates page aligned allocations from the system.
extern AlignedAllocator pageAlignedAllocator;
/// An allocator which allocates segment aligned allocations from the system.
extern AlignedAllocator segmentAlignedAllocator;

/**
 * A Pool which automatically manages memory via a memory allocator provided
 * as a template argument.
 *
 * \tparam T
 *      A type which provides an allocate and deallocate
 *      method to acquire and release allocations.
 * \tparam allocator
 *      An instance of type T from which fetch/return allocations.
 */
template <typename T, T& allocator>
class GrowablePool : public Pool {
  public:
    /**
     * Creates a GrowablePool.
     *
     * \param blockSize
     *      The size of the block to be returned by allocate().
     * \param allocationSize
     *      The number of bytes to allocate for form the initial set of blocks.
     *      Note: If allocationSize is not divisible by blockSize then some
     *      bytes will not be used.
     */
    GrowablePool(uint64_t blockSize, uint64_t allocationSize)
        : Pool(blockSize)
        , allocations()
        , allocationSize(allocationSize)
    {
    }

    /**
     * Frees up all the allocations backing the blocks in this GrowablePool.
     */
    virtual ~GrowablePool()
    {
        foreach (void* allocation, allocations)
            allocator.deallocate(allocation);
    }

    /**
     * Return a block from the Pool and mark it as in use; for type safety
     * prefer new(size_t_, Pool*).  If this GrowablePool is out of free blocks
     * more will be allocated to facilitate the request.
     */
    void*
    allocate()
    {
        void* addr = Pool::allocate();
        if (addr)
            return addr;
        grow();
        return Pool::allocate();
    }

  private:
    /**
     * Grow this GrowablePool by allocating allocationSize more memory, dividing
     * it into blocks, and adding it to the pool.  This is guaranteed to succeed
     * or crash if no memory is available.
     */
    void
    grow()
    {
        void* allocation = allocator.allocate(allocationSize);
        allocations.push_back(allocation);
        addAllocation(allocation, allocationSize);
    }

    /// List of addresses allocated by this pool.
    vector<void*> allocations;

    /// Bytes this pool allocates each time it runs out of space.
    uint64_t allocationSize;

    friend class GrowablePoolTest;
    DISALLOW_COPY_AND_ASSIGN(GrowablePool);
};

typedef GrowablePool<Mallocator, mallocator> AutoPool;
typedef GrowablePool<AlignedAllocator,
                     pageAlignedAllocator>
        PageAlignedAutoPool;

} // namespace

void* operator new(size_t size, RAMCloud::Pool* pool);
void* operator new[](size_t size, RAMCloud::Pool* pool);

#endif // RAMCLOUD_POOL_H
