/* Copyright (c) 2009-2010 Stanford University
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

#ifndef RAMCLOUD_COORDINATORSERVER_H
#define RAMCLOUD_COORDINATORSERVER_H

#include "ServerList.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "Metrics.h"
#include "Rpc.h"
#include "Server.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Serves RPCs for the cluster coordinator.
 */
class CoordinatorServer : public Server {
  public:
    CoordinatorServer()
        : nextServerId(generateRandom())
        , serverList()
    {}
    virtual ~CoordinatorServer() {}
    void run();
    void dispatch(RpcType type,
                  Transport::ServerRpc& rpc,
                  Responder& responder);

  private:
    void enlistServer(const EnlistServerRpc::Request& reqHdr,
                      EnlistServerRpc::Response& respHdr,
                      Transport::ServerRpc& rpc);

    void getServerList(const GetServerListRpc::Request& reqHdr,
                      GetServerListRpc::Response& respHdr,
                      Transport::ServerRpc& rpc);

    uint64_t nextServerId;
    ProtoBuf::ServerList serverList;

    friend class CoordinatorTest;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorServer);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVER_H
