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
#include "ObjectRpcWrapper.h"
#include "RpcTracker.h"

#ifndef RAMCLOUD_LINEARIZABLEOBJECTRPCWRAPPER_H
#define RAMCLOUD_LINEARIZABLEOBJECTRPCWRAPPER_H

namespace RAMCloud {

/**
 * LinearizableObjectRpcWrapper packages linearizability features for
 * the client side of RPCs that must be sent to the server that stores
 * a particular object.
 *
 * To make an RPC linearizable, two things should be done.
 * 1) Inherit from this wrapper instead of ObjectRpcWrapper.
 * 2) Invoke #fillLinearizabilityHeader in the constructor of the RPC.
 */
class LinearizableObjectRpcWrapper
    : public ObjectRpcWrapper
    , public RpcTracker::TrackedRpc {
  public:
    explicit LinearizableObjectRpcWrapper(RamCloud* ramcloud, bool linearizable,
            uint64_t tableId, const void* key, uint16_t keyLength,
            uint32_t responseHeaderLength, Buffer* response = NULL);
    explicit LinearizableObjectRpcWrapper(RamCloud* ramcloud, bool linearizable,
            uint64_t tableId, uint64_t keyHash, uint32_t responseHeaderLength,
            Buffer* response = NULL);
    virtual ~LinearizableObjectRpcWrapper();
    void cancel();
    virtual bool isReady();

    /**
     * This flag allows users of linearizable RPCs to specific whether to send
     * linearizable RPC or non-linearizable RPC.
     */
    const bool linearizabilityOn;

  PROTECTED:
    template <typename RpcRequest>
    void fillLinearizabilityHeader(RpcRequest* reqHdr);

    bool waitInternal(Dispatch* dispatch, uint64_t abortTime = ~0UL);

    // General client information.
    RamCloud* ramcloud;

    /**
     * If the linearizability feature is on, we save the rpcId assigned in
     * #fillLinearizabilityHeader function.
     *
     * Value of 0 means rpcId is not assigned yet, or the rpc is already
     * finished. (#RpcTracker::rpcFinished() is invoked.)
     */
    uint64_t assignedRpcId;

  PRIVATE:
    virtual void tryFinish();

    DISALLOW_COPY_AND_ASSIGN(LinearizableObjectRpcWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_LINEARIZABLEOBJECTRPCWRAPPER_H
