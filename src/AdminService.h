/* Copyright (c) 2011-2016 Stanford University
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

#ifndef RAMCLOUD_ADMINSERVICE_H
#define RAMCLOUD_ADMINSERVICE_H

#include "Service.h"
#include "ServerConfig.h"
#include "ServerList.h"

namespace RAMCloud {

/**
 * This Service supports a variety of requests used for cluster management,
 * such as pings, server controls, and server list management.
 */
class AdminService : public Service {
  public:
    explicit AdminService(Context* context, ServerList* serverList,
            const ServerConfig* serverConfig);
    ~AdminService();
    void dispatch(WireFormat::Opcode opcode, Rpc* rpc);

  PRIVATE:
    void getMetrics(const WireFormat::GetMetrics::Request* reqHdr,
            WireFormat::GetMetrics::Response* respHdr,
            Rpc* rpc);
    void getServerConfig(const WireFormat::GetServerConfig::Request* reqHdr,
                         WireFormat::GetServerConfig::Response* respHdr,
                         Rpc* rpc);
    void getServerId(const WireFormat::GetServerId::Request* reqHdr,
            WireFormat::GetServerId::Response* respHdr,
            Rpc* rpc);
    void kill(const WireFormat::Kill::Request* reqHdr,
            WireFormat::Kill::Response* respHdr,
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
    void updateServerList(const WireFormat::UpdateServerList::Request* reqHdr,
                       WireFormat::UpdateServerList::Response* respHdr,
                       Rpc* rpc);

    /// Shared RAMCloud information.
    Context* context;

    /// ServerList to update in response to Coordinator's RPCs. NULL means
    /// that we will reject requests to update our server list.
    ServerList* serverList;

    /// This server's ServerConfig, which we export to curious parties.
    /// NULL means we'll reject curious parties.
    const ServerConfig* serverConfig;

    /// If this variable is true, the kill method returns without dying.
    /// This is used during unit tests that verify the communication path
    /// for this call.
    bool ignoreKill;

    /// Used during unit tests: if true, the next call to getServerId
    /// will return an invalid id, and it will reset this variable to
    /// false.
    bool returnUnknownId;

    DISALLOW_COPY_AND_ASSIGN(AdminService);
};


} // end RAMCloud

#endif  // RAMCLOUD_ADMINSERVICE_H
