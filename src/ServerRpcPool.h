/* Copyright (c) 2011-2015 Stanford University
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

#ifndef RAMCLOUD_SERVERRPCPOOL_H
#define RAMCLOUD_SERVERRPCPOOL_H

#include "Common.h"
#include "Dispatch.h"
#include "ObjectPool.h"
#include "Transport.h"
#include "LogProtector.h"

namespace RAMCloud {

/**
 * ServerRpcPool is a fast allocator for Transport-specific subclasses
 * of ServerRpc. It also automatically tracks outstanding ServerRpcs
 * in the system by epoch, enabling other parts of the system to query
 * if RPCs less than or equal to a given epoch are still present. This
 * can be used, for example, to ensure that its safe to reclaim zero-
 * copy buffers.
 */
template<typename T = Transport::ServerRpc>
class ServerRpcPool : public LogProtector::EpochProvider {
  public:
    /**
     * Construct a new ServerRpcPool.
     */
    ServerRpcPool()
        : outstandingServerRpcs(),
          pool(),
          outstandingAllocations(0)
    {
    }

    /**
     * Destroy the ServerRpcPool.
     */
    ~ServerRpcPool()
    {
        // ServerRpcs should not just be dropped on the floor. If you are
        // hitting this, then you're not being careful cleaning up.
        assert(outstandingAllocations == 0);
    }

    /**
     * Allocate space for, construct, and return a pointer to an object
     * from this pool.
     */
    template<typename... Args>
    T*
    construct(Args&&... args)
    {
        T* rpc = pool.construct(static_cast<Args&&>(args)...);
        outstandingServerRpcs.push_back(*rpc);
        outstandingAllocations++;
        return rpc;
    }

    /**
     * Destroy an object previously constructed by this pool and return
     * its memory to the pool.
     */
    void
    destroy(T* const rpc)
    {
        outstandingServerRpcs.erase(outstandingServerRpcs.iterator_to(*rpc));
        outstandingAllocations--;
        pool.destroy(rpc);
    }

    // See LogProtector::EpochProvider for documentation.
    // Caller must hold dispatch lock.
    uint64_t
    getEarliestEpoch(int activityMask)
    {
        uint64_t earliest = -1;

        ServerRpcList::iterator it = outstandingServerRpcs.begin();
        while (it != outstandingServerRpcs.end()) {
            if (((it->activities & activityMask) != 0) & (it->epoch != 0)) {
                earliest = std::min(it->epoch, earliest);
            }
            it++;
        }

        return earliest;
    }

  PRIVATE:
    INTRUSIVE_LIST_TYPEDEF(Transport::ServerRpc, outstandingRpcListHook)
        ServerRpcList;

    // List of ServerRpcs that are being processed. RPCs are added to the
    // list when allocated in ServerRpcPool::construct and are removed
    // when they are destroyed in ServerRpcPool::destroy. This list may
    // be walked to check whether a given epoch is still represented by
    // any outstanding RPC in this pool.
    ServerRpcList outstandingServerRpcs;

    /// Pool allocator backing the actual ServerRpc classes this class returns.
    ObjectPool<T> pool;

    /// Count of the number of ServerRpcs that have been constructed, but not
    /// yet destroyed. The new ObjectPool does this, but the old one did not.
    /// Keep this simple check around just in case the interface changes.
    uint64_t outstandingAllocations;
};

/**
 * Guard that destroys an RPC when its destructor is called.
 */
template<typename T>
class ServerRpcPoolGuard {
  public:
    /**
     * \param pool
     *      The pool to call destroy on.
     * \param rpc
     *      The rpc to pass to the pool's destroy method.
     */
    ServerRpcPoolGuard(ServerRpcPool<T>& pool, T* rpc)
        : pool(pool),
          rpc(rpc)
    {
    }

    ~ServerRpcPoolGuard()
    {
        pool.destroy(rpc);
    }

  PRIVATE:
    ServerRpcPool<T>& pool;     /// The pool the rpc came from (and returns to).
    T* rpc;                     /// The rpc to destroy.

    DISALLOW_COPY_AND_ASSIGN(ServerRpcPoolGuard);
};

} // namespace RAMCloud

#endif // RAMCLOUD_SERVERRPCPOOL_H
