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

#include "Common.h"
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
 * 2) Invoke #fillLinearizabilityHeader in constructor of the RPC.
 */
class LinearizableObjectRpcWrapper : public ObjectRpcWrapper {
  public:
    explicit LinearizableObjectRpcWrapper(RamCloud* ramcloud, bool linearizable,
            uint64_t tableId, const void* key, uint16_t keyLength,
            uint32_t responseHeaderLength, Buffer* response = NULL);
    explicit LinearizableObjectRpcWrapper(RamCloud* ramcloud, bool linearizable,
            uint64_t tableId, uint64_t keyHash, uint32_t responseHeaderLength,
            Buffer* response = NULL);
    virtual ~LinearizableObjectRpcWrapper();
    void cancel();

    /**
     * Tells whether we send linearizable RPC instead of regular RPC.
     */
    const bool linearizabilityOn;

  PROTECTED:
    template <typename RpcRequest>
    void fillLinearizabilityHeader(RpcRequest* reqHdr);

    bool waitInternal(Dispatch* dispatch, uint64_t abortTime = ~0UL);

    /**
     * If the linearizability feature is on, we save the rpcId assigned in
     * #fillLinearizabilityHeader function.
     */
    uint64_t assignedRpcId;

    /**
     * Indicates the completion of linearizable RPC is recorded already.
     * With the flag, we prevent waitInternal() from processing same response
     * multiple times.
     */
    bool responseProcessed;

    DISALLOW_COPY_AND_ASSIGN(LinearizableObjectRpcWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_LINEARIZABLEOBJECTRPCWRAPPER_H
