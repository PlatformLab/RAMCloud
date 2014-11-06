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

#include "RamCloud.h"
#include "ObjectRpcWrapper.h"

#ifndef RAMCLOUD_LINEARIZABLEOBJECTRPCWRAPPER_H
#define RAMCLOUD_LINEARIZABLEOBJECTRPCWRAPPER_H

namespace RAMCloud {

/**
 * LinearizableObjectRpcWrapper packages linearizability features for
 * the client side of RPCs that must be sent to the server that stores
 * a particular object.
 */
class LinearizableObjectRpcWrapper : public ObjectRpcWrapper {
  public:
    explicit LinearizableObjectRpcWrapper(RamCloud* ramcloud, bool linearizable,
            uint64_t tableId, const void* key, uint16_t keyLength,
            uint32_t responseHeaderLength, Buffer* response = NULL);
    explicit LinearizableObjectRpcWrapper(RamCloud* ramcloud, bool linearizable,
            uint64_t tableId, uint64_t keyHash, uint32_t responseHeaderLength,
            Buffer* response = NULL);

    /**
     * Destructor for LinearizableObjectRpcWrapper.
     */
    virtual ~LinearizableObjectRpcWrapper() {}

    /**
     * Tells whether we send linearizable RPC instead of regular RPC.
     */
    const bool linearizabilityOn;

  PROTECTED:
    template <typename RpcRequest>
    void fillLinearizabilityHeader(RpcRequest* reqHdr);

    template <typename RpcResponse>
    void handleLinearizabilityResp(const RpcResponse* respHdr);

    /**
     * If the linearizability feature is on, we save the rpcId assigned in
     * #fillLinearizabilityHeader function.
     */
    uint64_t assignedRpcId;

    DISALLOW_COPY_AND_ASSIGN(LinearizableObjectRpcWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_LINEARIZABLEOBJECTRPCWRAPPER_H
