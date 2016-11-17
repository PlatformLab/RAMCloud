/* Copyright (c) 2016 Stanford University
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

#include "RpcRequestPool.h"
#include "Memory.h"
#include "ObjectRpcWrapper.h"
#include "ShortMacros.h"
#include "ThreadId.h"

namespace RAMCloud {

/// Definitions for constants.
const int32_t RpcRequestPool::BYTES_PER_REQUEST;
const int32_t RpcRequestPool::SLABS_TOTAL;
const int32_t RpcRequestPool::BYTES_TOTAL;

/**
 * Default constructor.
 * Only the thread calling this constructor will be able to invoke #alloc().
 */
RpcRequestPool::RpcRequestPool()
    : basePointer(0)
    , freelist()
    , ownerThreadId(ThreadId::get())
    , freelistForAlienThreads()
    , mutexForFreelistForAlienThreads()
{
    basePointer = reinterpret_cast<char*>(Memory::xmemalign(
                            HERE, BYTES_PER_REQUEST, BYTES_TOTAL));
    for (uint i = 0; i < SLABS_TOTAL; ++i) {
        freelist.push(basePointer + i * BYTES_PER_REQUEST);
    }
}

/**
 * Default destructor
 */
RpcRequestPool::~RpcRequestPool()
{
    Lock lock(mutexForFreelistForAlienThreads);
    if (freelist.size() + freelistForAlienThreads.size() < SLABS_TOTAL) {
        LOG(WARNING, "RpcRequestPool is being destroyed while %lu memory slabs"
                " are not returned to allocator yet.",
                SLABS_TOTAL - freelist.size());
    }
    free(basePointer);
}

/**
 * Allocates memory for RPC request. Only owner thread should invoke this
 * function.
 *
 * \param size
 *      Size of RPC request.
 *
 * \return pointer to allocated memory for RPC request.
 */
void*
RpcRequestPool::alloc(int size)
{
    assert(ThreadId::get() == ownerThreadId);
    if (size > BYTES_PER_REQUEST) {
        return Memory::xmalloc(HERE, size);
    }
    if (freelist.empty()) {
        Lock lock(mutexForFreelistForAlienThreads);
        if (freelistForAlienThreads.empty()) {
            LOG(WARNING, "RpcRequestPool is out of slabs. Using malloc.");
            return Memory::xmalloc(HERE, size);
        }
        // TODO(seojin): write test for this... Not sure this is what I want.
        freelist.swap(freelistForAlienThreads);
    }

    void* ptr = freelist.top();
    freelist.pop();
    return ptr;
}

/**
 * Re-claims memory for RPC request. Although any threads may invoke this
 * function, calling free() from alien thread may be slow. (~ 20ns)
 *
 * \param ptr
 *      Pointer to the memory slab for RPC request.
 */
void
RpcRequestPool::free(void* ptr)
{
    if (ptr < basePointer || ptr >= basePointer + BYTES_TOTAL) {
        // Call POSIX_free
        ::free(ptr);
    } else if (ThreadId::get() == ownerThreadId) {
        freelist.push(ptr);
    } else {
        Lock lock(mutexForFreelistForAlienThreads);
        // TODO(seojin): write test for this... Interesting.
        freelistForAlienThreads.push(ptr);
    }
}

} // namespace RAMCloud
