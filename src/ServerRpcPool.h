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

namespace RAMCloud {

namespace ServerRpcPoolInternal {
    INTRUSIVE_LIST_TYPEDEF(Transport::ServerRpc, outstandingRpcListHook)
        ServerRpcList;

    // List of ServerRpcs that are being processed. RPCs are added to the
    // list when allocated in ServerRpcPool::construct and are removed
    // when they are destroyed in ServerRpcPool::destroy. This list may
    // be walked to check whether a given epoch is still represented by
    // any outstanding RPC in the system.
    extern ServerRpcList outstandingServerRpcs;

    // An unsigned integer representing the current epoch. Epochs are
    // just monotonically increasing values that represent some point
    // in time. All ServerRpcs are tagged with currentEpoch and all
    // RPCs in the system can be queried to find the oldest RPC still
    // being processed. Epochs are a coarse-grained means to determining
    // when all RPCs encountered after some point in time have left
    // the system.
    extern uint64_t currentEpoch;
};

/**
 * ServerRpcPool is a fast allocator for Transport-specific subclasses
 * of ServerRpc. It also automatically tracks outstanding ServerRpcs
 * in the system by epoch, enabling other parts of the system to query
 * if RPCs less than or equal to a given epoch are still present. This
 * can be used, for example, to ensure that its safe to reclaim zero-
 * copy buffers.
 */
template<typename T = Transport::ServerRpc>
class ServerRpcPool {
  public:
    /**
     * Construct a new ServerRpcPool.
     */
    ServerRpcPool()
        : pool(),
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
        ServerRpcPoolInternal::outstandingServerRpcs.push_back(*rpc);
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
        ServerRpcPoolInternal::outstandingServerRpcs.erase(
            ServerRpcPoolInternal::outstandingServerRpcs.iterator_to(*rpc));
        outstandingAllocations--;
        pool.destroy(rpc);
    }

    /**
     * Return the current epoch.
     */
    static uint64_t
    getCurrentEpoch()
    {
        return ServerRpcPoolInternal::currentEpoch;
    }

    /**
     * Obtain the earliest (lowest value) epoch of any outstanding RPC in the
     * system. If there are no RPCs, then the return value is -1 (i.e. the
     * largest 64-bit unsigned integer).
     *
     * Note that this method is not particularly efficient and should not be
     * called very frequently (it will grab the Dispatch lock and query all
     * outstanding server RPCs).
     * 
     * \param context
     *      Overall information about the RAMCloud server; used to lock the
     *      dispatcher.
     * \param activities
     *      A bit mask of flags such as Transport::READ_ACTIVITY. Only
     *      RPCs performing at least one of to consider all active RPCs,
     *      independent of their activities.
     */
    static uint64_t
    getEarliestOutstandingEpoch(Context* context, int activities)
    {
        Dispatch::Lock lock(context->dispatch);
        uint64_t earliest = -1;

        ServerRpcPoolInternal::ServerRpcList::iterator it =
            ServerRpcPoolInternal::outstandingServerRpcs.begin();
        while (it != ServerRpcPoolInternal::outstandingServerRpcs.end()) {
            if (((it->activities & activities) != 0) & (it->epoch != 0)) {
                earliest = std::min(it->epoch, earliest);
            }
            it++;
        }

        return earliest;
    }

    /**
     * Log information about the epochs for all outstanding RPCs. Intended
     * for debugging.
     *
     * \param context
     *      Overall information about the RAMCloud server; used to lock the
     *      dispatcher.
     */
    static void
    logEpochs(Context* context)
    {
        Dispatch::Lock lock(context->dispatch);

        ServerRpcPoolInternal::ServerRpcList::iterator it =
            ServerRpcPoolInternal::outstandingServerRpcs.begin();
        int count = 0;
        while (it != ServerRpcPoolInternal::outstandingServerRpcs.end()) {
            RAMCLOUD_LOG(NOTICE, "Epoch: %lu", it->epoch);
            it++;
            count++;
        }
        RAMCLOUD_LOG(NOTICE, "Finished with scan: %d RPCs scanned", count);
    }

    /**
     * Increment the current epoch by one.
     * 
     * \return
     *      The new epoch value.
     */
    static uint64_t
    incrementCurrentEpoch()
    {
        // atomically increment the epoch (with a gcc builtin)
        return __sync_add_and_fetch(&ServerRpcPoolInternal::currentEpoch, 1);
    }

  PRIVATE:
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
