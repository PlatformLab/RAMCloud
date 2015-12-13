/* Copyright (c) 2014-2015 Stanford University
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

#ifndef RAMCLOUD_RPCTRACKER_H
#define RAMCLOUD_RPCTRACKER_H

namespace RAMCloud {

class RamCloud;

/**
 * A storage for outstanding rpc status whether the result of each rpc is
 * received or not.
 *
 * Each client (ramcloud object) should keep an instance of this class
 * to keep necessary information for sending linearizable RPCs.
 * By using RpcTracker, client can fill up necessary information
 * (rpcId and ackId) on Rpc header.
 */
class RpcTracker {
  PUBLIC:
    /**
     * Abstract class to be inherited by any class that wishes to be tracked by
     * the RpcTracker.
     */
    class TrackedRpc {
      PUBLIC:
        virtual ~TrackedRpc() {}

      PRIVATE:
        /**
         * Push the tracked rpc toward completion.
         *
         * This method must be implemented by tracked operations and is used by
         * the RpcTracker to prod an unfinished operation.  For example, the
         * RpcTracker will use this method when newRpcId has run out of RPC IDs
         * to allocate.
         *
         * Though the rpc does not have to finish before this call returns, each
         * call of this method should make progress and eventually cause
         * rpcFinished() to be called.
         */
        virtual void tryFinish() = 0;

        friend class RpcTracker;
    };

    /**
     * Constructor which initializes internal data structure.
     */
    RpcTracker()
        : firstMissing(1)
        , nextRpcId(1)
        , rpcs()
    {}
    ~RpcTracker();

    void rpcFinished(uint64_t rpcId);
    uint64_t newRpcId(TrackedRpc* ptr);
    uint64_t newRpcIdBlock(TrackedRpc* ptr, size_t size);
    uint64_t ackId();
    TrackedRpc* oldestOutstandingRpc();

    /**
     * Return true if there is a least one rpc that has started but not yet
     * finished; false otherwise.
     */
    bool hasUnfinishedRpc() {
        return firstMissing < nextRpcId;
    }

  PRIVATE:
    void resizeRpcs(int increment);

    /**
     * Smallest rpcId among rpcs that haven't received results.
     */
    uint64_t firstMissing;

    /**
     * Next rpc id to be used for new RPC. This value increases monotonically.
     * The value 0 is reserved for error handling.
     */
    uint64_t nextRpcId;

    /**
     * Maximum allowed distance between ackId and rpcId.
     * Client should block until it receives reply for its oldest rpc
     * and advances its firstMissing value (same as ackId + 1).
     * This hard limit guarantees that only finite memory (#rpcs) is needed
     * to keep records of all outstanding rpcs.
     *
     * Assuming slow RPC takes 4 times more than average, we can have
     * at least 32 concurrent outstanding linearizable RPCs with the maximum
     * distance value 128. Relatively small value was chosen to limit the effect
     * of bad clients on master's storage burden.
     *
     * For fast indexing in window (using ANDing), the size should be always
     * a power of two.
     */
    static const int windowSize = 512;

    /**
     * Bitmask to calculate index in rpcs array from rpcId.
     */
    static const int indexMask = windowSize - 1;

    /**
     * Array keeping pointers to RPC wrappers for RPCs whose id is
     * between firstMissing and (nextRpcId - 1).
     * The record of RPC with id i is stored at i % sizeof(rpcs).
     * NULL value of the entry means the result of the RPC is received.
     * Non-null pointer means still waiting for the result.
     *
     * At a timepoint, a consecutive (maybe looped around once) portion of
     * array is used. (From <firstMissing % sizeof(rpcs)> to
     *                 <(nextRpcId - 1) % sizeof(rpcs)>)
     * As a rpcFinished is called to record receipt, it checks whether
     * we can advance firstMissing value.
     */
    TrackedRpc* rpcs[windowSize];

    DISALLOW_COPY_AND_ASSIGN(RpcTracker);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_RPCTRACKER_H
