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

#include "RamCloud.h"
#include "RpcTracker.h"

namespace RAMCloud {

/**
 * Clean up allocated resources.
 */
RpcTracker::~RpcTracker() {
}

/**
 * Mark the receipt of the result for an RPC.
 *
 * \param rpcId
 *      The id of an Rpc.
 */
void
RpcTracker::rpcFinished(uint64_t rpcId) {
    assert(rpcs[rpcId & indexMask]);
    rpcs[rpcId & indexMask] = NULL;
    if (firstMissing == rpcId) {
        firstMissing++;
        while (!rpcs[firstMissing & indexMask] && firstMissing < nextRpcId)
            firstMissing++;
    }
}

/**
 * Gets a unique RPC id for new Tracked RPC.  This method may block waiting for
 * the oldest TrackedRpc to complete if it is running too far behind.  This
 * effectively bounds the number of outstanding requests a client may have.
 *
 * \param ptr
 *      Pointer to TrackedRpc to which we assign a new rpcId.
 *
 * \return
 *      The id for new RPC.
 */
uint64_t
RpcTracker::newRpcId(TrackedRpc* ptr) {
    assert(ptr != NULL);
    while (firstMissing + windowSize <= nextRpcId) {
        RAMCLOUD_CLOG(NOTICE, "Waiting for response of RPC with id: %ld",
                      firstMissing);
        TrackedRpc* oldest = oldestOutstandingRpc();
        oldest->tryFinish();
    }
    assert(firstMissing + windowSize > nextRpcId);
    rpcs[nextRpcId & indexMask] = ptr;
    return nextRpcId++;
}

/**
 * Gets the current acknowledgment id, which indicates RPCs with id smaller
 * than this number have all received results.
 *
 * \return
 *      The ackId value to be sent with new RPC.
 */
uint64_t
RpcTracker::ackId() {
    return firstMissing - 1;
}

/**
 * Return the pointer to the oldest outstanding linearizable RPC.
 * \return
 *      Pointer to linearizable RPC wrapper with smallest rpdId.
 */
RpcTracker::TrackedRpc*
RpcTracker::oldestOutstandingRpc() {
    assert(rpcs[firstMissing & indexMask]);
    return rpcs[firstMissing & indexMask];
}

} // namespace RAMCloud
