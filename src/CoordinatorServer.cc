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

#include "CoordinatorServer.h"

namespace RAMCloud {

void
CoordinatorServer::run()
{
    while (true)
        handleRpc<CoordinatorServer>();
}

void
CoordinatorServer::dispatch(RpcType type, Transport::ServerRpc& rpc)
{
    switch (type) {
        case EnlistServerRpc::type:
            callHandler<EnlistServerRpc, CoordinatorServer,
                        &CoordinatorServer::enlistServer>(rpc);
            break;
        case PingRpc::type:
            callHandler<PingRpc, Server, &Server::ping>(rpc);
            break;
        default:
            throw UnimplementedRequestError();
    }
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::enlistServer(const EnlistServerRpc::Request& reqHdr,
                                EnlistServerRpc::Response& respHdr,
                                Transport::ServerRpc& rpc)
{
    uint64_t serverId = nextServerId++;
    // TODO(ongaro): add entry to server list
    // TODO(ongaro): if first server, call youOwn and give it ownership of
    // table 0...errr...deadlock
    LOG(DEBUG, "Server enlisted with id %lu", serverId);
    respHdr.serverId = serverId;
}


} // namespace RAMCloud
