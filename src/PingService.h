/* Copyright (c) 2011-2013 Stanford University
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
#include "ServerList.h"

namespace RAMCloud {

/**
 * This Service is used only for ping requests.  Placing these requests in a
 * separate service allows them to have their own threads so that ping-related
 * requests don't block, even if other parts of the server are overloaded.
 */
class PingService : public Service {
  public:
    explicit PingService(Context* context);
    void dispatch(WireFormat::Opcode opcode, Rpc* rpc);
    virtual int maxThreads() {
        return 5;
    }

  PRIVATE:
    void getMetrics(const WireFormat::GetMetrics::Request* reqHdr,
            WireFormat::GetMetrics::Response* respHdr,
            Rpc* rpc);
    void getServerId(const WireFormat::GetServerId::Request* reqHdr,
            WireFormat::GetServerId::Response* respHdr,
            Rpc* rpc);
    void ping(const WireFormat::Ping::Request* reqHdr,
            WireFormat::Ping::Response* respHdr,
            Rpc* rpc);
    void proxyPing(const WireFormat::ProxyPing::Request* reqHdr,
            WireFormat::ProxyPing::Response* respHdr,
            Rpc* rpc);
    void serverControl(const WireFormat::ServerControl::Request* reqHdr,
            WireFormat::ServerControl::Response* respHdr,
            Rpc* rpc);
    void kill(const WireFormat::Kill::Request* reqHdr,
            WireFormat::Kill::Response* respHdr,
            Rpc* rpc);

    /// Shared RAMCloud information.
    Context* context;

    /// If this variable is true, the kill method returns without dying.
    /// This is used during unit tests that verify the communication path
    /// for this call.
    bool ignoreKill;

    DISALLOW_COPY_AND_ASSIGN(PingService);
};


} // end RAMCloud

#endif  // RAMCLOUD_PINGSERVICE_H
