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

#ifndef RAMCLOUD_RPCREQUESTPOOL_H
#define RAMCLOUD_RPCREQUESTPOOL_H

#include <stack>
#include <unordered_map>
#include "Common.h"
#include "ObjectRpcWrapper.h"
#include "Transport.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * Efficient memory allocator for RPC requests. Primarily designed for
 * client-side linearizable non-durable RPCs, which requires the lifetime of
 * RPC requests to exceed the lifetime of RpcWrapper.
 * For regular RPCs, it is recommended to use standard Buffer approach.
 */
class RpcRequestPool {
  PUBLIC:
    explicit RpcRequestPool();
    ~RpcRequestPool();
    void* alloc(uint32_t size);
    void free(void* ptr);

  PRIVATE:
    /**
     * The number of bytes per memory slab used for an RPC request.
     */
    static const uint32_t BYTES_PER_REQUEST = 8192;

    /**
     * The number of memory slabs allocatable.
     */
    static const int32_t SLABS_TOTAL = 1000;

    /**
     * The number of bytes of total memory used by this allocator.
     */
    static const uint32_t BYTES_TOTAL = 8192 * SLABS_TOTAL;

    /**
     * Pointer to the entire memory pool.
     */
    char* basePointer;

    /**
     * List of free 8KB memory slabs.
     */
    std::stack<void*> freelist;

    /**
     * To avoid expensive synchronization for every alloc() or free(), this
     * pool is primarily owned by one thread (main client thread). Only owner
     * can call alloc(). But any other alien threads can free() which might be
     * a little slow.
     */
    int ownerThreadId;

    /**
     * Temporary holder of slabs freed by alien threads.
     * This is the only shared data structure between owner thread and aliens.
     */
    std::stack<void*> freelistForAlienThreads;

    /**
     * Lock for freelistForAlienThreads.
     */
    std::mutex mutexForFreelistForAlienThreads;
    typedef std::lock_guard<std::mutex> Lock;

    DISALLOW_COPY_AND_ASSIGN(RpcRequestPool);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_RPCREQUESTPOOL_H
