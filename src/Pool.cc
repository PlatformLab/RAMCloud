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

#include "Common.h"
#include "Pool.h"

namespace RAMCloud {

Mallocator mallocator;
AlignedAllocator pageAlignedAllocator(getpagesize());
AlignedAllocator segmentAlignedAllocator(SEGMENT_SIZE);

/**
 * Creates a Pool without any blocks.  They can be added later via
 * addAllocation().
 *
 * \param blockSize
 *      The size of the block to be returned by allocate().
 */
Pool::Pool(uint64_t blockSize)
    : blocks()
    , blockSize(blockSize)
{
}

/**
 * Create a Pool using the provided memory region for the initial set of
 * blocks.
 *
 * \param blockSize
 *      The number of bytes in each block returned by allocate().
 * \param base
 *      The start of the memory region to be managed by the Pool.
 * \param totalBytes
 *      The length of the memory region starting at base to be managed by the
 *      Pool.
 */
Pool::Pool(uint64_t blockSize, void *base, uint64_t totalBytes)
    : blocks()
    , blockSize(blockSize)
{
    addAllocation(base, totalBytes);
}

Pool::~Pool()
{
}

/**
 * Divide a memory region into blockSize blocks and add them to the Pool.
 *
 * \param allocation
 *      A memory region to be incorporated into the Pool of size allocationSize.
 * \param allocationSize
 *      The size of the memory region starting at allocation.  Note: If
 *      allocationSize is not divisible by blockSize then some bytes will
 *      not be used.
 */
void
Pool::addAllocation(void* allocation, uint64_t allocationSize)
{
    for (uint64_t i = 0; i < (allocationSize / blockSize); i++) {
        void* blockBase = reinterpret_cast<char*>(allocation) + (blockSize * i);
        blocks.push_back(blockBase);
    }
}

/**
 * Return a block from the Pool and mark it as in use; for type safety prefer
 * new(size_t_, Pool*).
 */
void *
Pool::allocate()
{
    if (blocks.empty())
        return NULL;
    void *p = *(blocks.end() - 1);     // end() points just past last element
    blocks.pop_back();
    return p;
}

/**
 * Release a block that is no longer in use back to this Pool.
 */
void
Pool::free(void *block)
{
    blocks.push_back(block);
}

/**
 * Return the size of the block returned by allocate().
 */
uint64_t
Pool::getBlockSize() const
{
    return blockSize;
}

/**
 * Return the number of blocks free in the Pool.
 */
uint64_t
Pool::getFreeBlockCount() const
{
    return blocks.size();
}

} // namespace RAMCloud

/**
 * Allocate a block from pool and construct an object on it.  Objects allocated
 * using this operator need to call pool->free() to return the allocated block
 * to the Pool.  Also, the caller is responsible for calling the object's
 * destructor.
 *
 * \throws std::bad_alloc
 *      If the requested type is larger than the block size of the pool.
 */
void* operator new(size_t size, RAMCloud::Pool* pool)
{
    void* addr = pool->allocate();
    if (!addr || size > pool->getBlockSize())
        throw std::bad_alloc();
    return addr;
}

/**
 * Allocate a block from pool and construct an array of objects on it.
 * Objects allocated using this operator need to call pool->free() to return
 * the allocated block to the Pool.  Also, the caller is responsible for
 * calling the objects' destructors.
 *
 * \throws std::bad_alloc
 *      If the requested array is larger than the block size of the pool.
 */
void* operator new[](size_t size, RAMCloud::Pool* pool)
{
    void* addr = pool->allocate();
    if (!addr || size > pool->getBlockSize())
        throw std::bad_alloc();
    return addr;
}

