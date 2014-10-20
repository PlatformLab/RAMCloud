/* Copyright (c) 2014 Stanford University
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

#include "Common.h"

namespace RAMCloud {

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
     * Constructor which initializes internal data structure.
     */
    RpcTracker() : firstMissing(1),
                   nextRpcId(1) {
    }
    ~RpcTracker();

    void rpcFinished(uint64_t rpcId);
    uint64_t newRpcId();
    uint64_t ackId();

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
     * at least 25 concurrent outstanding linearizable RPCs with the maximum
     * distance value 100.
     */
    static const int windowSize = 100;

    /**
     * Array keeping RPC status (result received or not) for RPCs whose id is
     * between firstMissing and (nextRpcId - 1).
     * The record of RPC with id i is stored at i % sizeof(rpcs).
     * True value of the entry means the result of the RPC is received.
     * False value means still waiting for the result.
     *
     * At a timepoint, a consecutive (maybe looped around once) portion of
     * array is used. (From <firstMissing % sizeof(rpcs)> to 
     *                 <(nextRpcId - 1) % sizeof(rpcs)>)
     * As a rpcFinished is called to record receipt, it checks whether
     * we can advance firstMissing value.
     */
    bool rpcs[windowSize];

    DISALLOW_COPY_AND_ASSIGN(RpcTracker);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_RPCTRACKER_H
