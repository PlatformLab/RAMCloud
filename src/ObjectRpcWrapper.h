/* Copyright (c) 2012 Stanford University
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
#include "RpcWrapper.h"

#ifndef RAMCLOUD_OBJECTRPCWRAPPER_H
#define RAMCLOUD_OBJECTRPCWRAPPER_H

namespace RAMCloud {
class RamCloud;

/**
 * ObjectRpcWrapper manages the client side of RPCs that must be sent to the
 * server that stores a particular object (more specifically, a particular
 * key hash within a particular table). If that server becomes unavailable,
 * or if it doesn't actually store the desired key hash, then this class will
 * retry the RPC with a different server until it eventually succeeds.  RPCs
 * using this wrapper will never fail to complete, though they may loop
 * forever. This wrapper is used for many of the RPCs in RamCloud.
 */
class ObjectRpcWrapper : public RpcWrapper {
  public:
    explicit ObjectRpcWrapper(Context* context, uint64_t tableId,
            const void* key, uint16_t keyLength, uint32_t responseHeaderLength,
            Buffer* response = NULL);
    explicit ObjectRpcWrapper(Context* context, uint64_t tableId,
            uint64_t keyHash, uint32_t responseHeaderLength,
            Buffer* response = NULL);

    /**
     * Destructor for ObjectRpcWrapper.
     */
    virtual ~ObjectRpcWrapper() {}

  PROTECTED:
    virtual bool checkStatus();
    virtual bool handleTransportError();
    virtual void send();

    /// Overall ramcloud state information. Primarily we access dispatch and
    /// objectFinder.
    Context* context;

    /// Information about an object that determines which server the request
    /// is sent to; we must save this information for use in retries.
    uint64_t tableId;
    uint64_t keyHash;

    DISALLOW_COPY_AND_ASSIGN(ObjectRpcWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTRPCWRAPPER_H
