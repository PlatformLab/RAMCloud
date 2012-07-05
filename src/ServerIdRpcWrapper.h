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

#ifndef RAMCLOUD_SERVERIDRPCWRAPPER_H
#define RAMCLOUD_SERVERIDRPCWRAPPER_H

#include "RpcWrapper.h"

namespace RAMCloud {
class RamCloud;

/**
 * ServerIdRpcWrapper manages the client side of most RPCs that are sent from
 * one RAMCloud server to another, or from the coordinator to a server. The
 * common theme among these RPCs is that they are addressed to a particular
 * server id. This class will retry the RPCs after errors, reopening sessions if
 * needed, until either the RPC completes or the server is marked as crashed.
 * The RPCs will fail with an error if the server crashes.
 */
class ServerIdRpcWrapper : public RpcWrapper {
  public:
    explicit ServerIdRpcWrapper(Context& context, ServerId id,
            uint32_t responseHeaderLength, Buffer* response = NULL);

    /**
     * Destructor for ServerIdRpcWrapper.
     */
    virtual ~ServerIdRpcWrapper() {}

    virtual void wait();

  PROTECTED:
    virtual bool handleTransportError();
    virtual void send();

    /// Shared RAMCloud information.
    Context& context;

    /// Target server.
    ServerId id;

    /// This flag is set to true if we discover that the target server is
    /// no longer up; the wait method in wrapper classes should then return
    /// an error.
    bool serverDown;

    DISALLOW_COPY_AND_ASSIGN(ServerIdRpcWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_SERVERIDRPCWRAPPER_H
