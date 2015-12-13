/* Copyright (c) 2011-2015 Stanford University
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

/**
 * \file
 * This file defines the MembershipService class.
 */

#ifndef RAMCLOUD_MEMBERSHIPSERVICE_H
#define RAMCLOUD_MEMBERSHIPSERVICE_H

#include "ServerConfig.h"
#include "ServerList.h"
#include "Service.h"

namespace RAMCloud {

/**
 * This Service is primarily used to maintain a server's global ServerList
 * object. More specifically, the coordinator issues RPCs to this service
 * indicating when servers enter or leave the system (i.e. when the cluster
 * membership changes). When the server first enlists, or if any updates are
 * lost, the coordinator will push the full list.
 *
 * Lost updates are noticed in one of two ways. First, if the coordinator
 * pushes an update newer than the next expected one, we reply will a request
 * for the full list. Second, to avoid staying out of sync when no updates
 * are being issued, the FailureDetector and PingService propagate the latest
 * server list versions. If we ping a machine that has seen an update we have
 * not, then the FailureDetector will wait a short interval and request that
 * the coordinator resend the list if the lost update still has not been
 * received.
 *
 * Additional functionality includes retrieving the ServerId of the machine
 * running this service (see #ServerId for more information) and advertising
 * the server's configuration.
 */
class MembershipService : public Service {
  public:
    explicit MembershipService(Context* context,
                               ServerList* serverList,
                               const ServerConfig* serverConfig);
    ~MembershipService();
    void dispatch(WireFormat::Opcode opcode, Rpc* rpc);

  PRIVATE:
    void getServerConfig(const WireFormat::GetServerConfig::Request* reqHdr,
                         WireFormat::GetServerConfig::Response* respHdr,
                         Rpc* rpc);
    void updateServerList(const WireFormat::UpdateServerList::Request* reqHdr,
                       WireFormat::UpdateServerList::Response* respHdr,
                       Rpc* rpc);

    /// Shared state.
    Context* context;

    /// ServerList to update in response to Coordinator's RPCs.
    ServerList* serverList;

    /// This server's ServerConfig, which we export to curious parties.
    const ServerConfig* serverConfig;

    DISALLOW_COPY_AND_ASSIGN(MembershipService);
};


} // end RAMCloud

#endif  // RAMCLOUD_MEMBERSHIPSERVICE_H
