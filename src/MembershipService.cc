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
 * This file implements the MembershipService class. It is responsible for
 * maintaining the global ServerList object describing other servers in the
 * system.
 */

#include "Common.h"
#include "MembershipService.h"
#include "ProtoBuf.h"
#include "ServerId.h"
#include "ServerList.pb.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Construct a new MembershipService object. There should really only be one
 * per server.
 */
MembershipService::MembershipService(ServerId& ourServerId,
                                     ServerList& serverList)
    : serverId(ourServerId),
      serverList(serverList)
{
    // The coordinator will push the server list to us once we've
    // enlisted.
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
MembershipService::dispatch(WireFormat::Opcode opcode, Rpc& rpc)
{
    switch (opcode) {
    case WireFormat::GetServerId::opcode:
        callHandler<WireFormat::GetServerId, MembershipService,
            &MembershipService::getServerId>(rpc);
        break;
    case WireFormat::SetServerList::opcode:
        callHandler<WireFormat::SetServerList, MembershipService,
            &MembershipService::setServerList>(rpc);
        break;
    case WireFormat::UpdateServerList::opcode:
        callHandler<WireFormat::UpdateServerList, MembershipService,
            &MembershipService::updateServerList>(rpc);
        break;
    default:
        throw UnimplementedRequestError(HERE);
    }
}

/**
 * Top-level service method to handle the REPLACE_SERVER_LIST request.
 *
 * \copydetails Service::ping
 */
void
MembershipService::getServerId(const WireFormat::GetServerId::Request& reqHdr,
                              WireFormat::GetServerId::Response& respHdr,
                              Rpc& rpc)
{
    // The serverId should be set by enlisting before any RPCs are dispatched
    // to this handler.
    assert(serverId.isValid());

    respHdr.serverId = *serverId;
}

/**
 * Top-level service method to handle the REPLACE_SERVER_LIST request.
 *
 * \copydetails Service::ping
 */
void
MembershipService::setServerList(
    const WireFormat::SetServerList::Request& reqHdr,
    WireFormat::SetServerList::Response& respHdr,
    Rpc& rpc)
{
    ProtoBuf::ServerList list;
    ProtoBuf::parseFromRequest(rpc.requestPayload, sizeof(reqHdr),
                               reqHdr.serverListLength, list);

    serverList.applyFullList(list);
}

/**
 * Top-level service method to handle the UPDATE_SERVER_LIST request.
 *
 * \copydetails Service::ping
 */
void
MembershipService::updateServerList(
        const WireFormat::UpdateServerList::Request& reqHdr,
        WireFormat::UpdateServerList::Response& respHdr,
        Rpc& rpc)
{
    ProtoBuf::ServerList update;
    ProtoBuf::parseFromRequest(rpc.requestPayload, sizeof(reqHdr),
                               reqHdr.serverListLength, update);

    LOG(NOTICE, "Got server list update (version number %lu)",
        update.version_number());

    respHdr.lostUpdates = serverList.applyUpdate(update);
}

} // namespace RAMCloud
