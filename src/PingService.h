/* Copyright (c) 2011 Stanford University
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

#ifndef RAMCLOUD_PINGSERVICE_H
#define RAMCLOUD_PINGSERVICE_H

#include "Service.h"

namespace RAMCloud {

/**
 * This Service is used only for ping requests.  Placing these requests in a
 * separate service allows them to have their own threads so that ping-related
 * requests don't block, even if other parts of the server are overloaded.
 */
class PingService : public Service {
  public:
    PingService() {}
    void dispatch(RpcOpcode opcode, Rpc& rpc);
    virtual int maxThreads() {
        return 5;
    }

  PRIVATE:
    void ping(const PingRpc::Request& reqHdr,
              PingRpc::Response& respHdr,
              Rpc& rpc);
    void proxyPing(const ProxyPingRpc::Request& reqHdr,
              ProxyPingRpc::Response& respHdr,
              Rpc& rpc);
    DISALLOW_COPY_AND_ASSIGN(PingService);
};


} // end RAMCloud

#endif  // RAMCLOUD_PINGSERVICE_H
