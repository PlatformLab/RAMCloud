/* Copyright (c) 2011-2012 Stanford University
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

#include "ServerList.h"
#include "Service.h"

namespace RAMCloud {

/**
 * This Service is used only to maintain a server's global ServerList object.
 * More specifically, the coordinator issues RPCs to this service indicating
 * when servers enter or leave the system (i.e. when the cluster membership
 * changes). When the server first enlists, or if any updates are lost, the
 * coordinator will push the full list.
 *
 * Lost updates are noticed in one of two ways. First, if the coordinator
 * pushes an update newer than the next expected one, we reply will a request
 * for the full list. Second, to avoid staying out of sync when not updates
 * are being issued, the FailureDetector and PingService propagate the latest
 * server list versions. If we ping a machine that has seen an update we have
 * not, then the FailureDetector will wait a short interval and request that
 * the coordinator resend the list if the lost update still has not been
 * received.
 */
class MembershipService : public Service {
  public:
    explicit MembershipService(ServerId& ourServerId, ServerList& serverList);
    void dispatch(RpcOpcode opcode, Rpc& rpc);
    virtual int maxThreads() {
        return 1;
    }

  PRIVATE:
    void getServerId(const GetServerIdRpc::Request& reqHdr,
                     GetServerIdRpc::Response& respHdr,
                     Rpc& rpc);
    void setServerList(const SetServerListRpc::Request& reqHdr,
                       SetServerListRpc::Response& respHdr,
                       Rpc& rpc);
    void updateServerList(const UpdateServerListRpc::Request& reqHdr,
                          UpdateServerListRpc::Response& respHdr,
                          Rpc& rpc);

    /// ServerId of this server.
    ServerId& serverId;

    /// ServerList to update in response to Coordinator's RPCs.
    ServerList& serverList;

    DISALLOW_COPY_AND_ASSIGN(MembershipService);
};


} // end RAMCloud

#endif  // RAMCLOUD_MEMBERSHIPSERVICE_H
