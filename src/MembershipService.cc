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
 * This file implements the MembershipService class. It is responsible for
 * maintaining the global ServerList object describing other servers in the
 * system.
 */

#include "Common.h"
#include "MembershipService.h"
#include "ProtoBuf.h"
#include "ServerId.h"
#include "ServerConfig.pb.h"
#include "ServerList.pb.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Construct a new MembershipService object. There should really only be one
 * per server.
 */
MembershipService::MembershipService(Context* context,
                                     ServerList* serverList,
                                     const ServerConfig* serverConfig)
    : context(context)
    , serverList(serverList)
    , serverConfig(serverConfig)
{
    context->services[WireFormat::MEMBERSHIP_SERVICE] = this;
}

MembershipService::~MembershipService()
{
    context->services[WireFormat::MEMBERSHIP_SERVICE] = NULL;
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
MembershipService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    switch (opcode) {
    case WireFormat::GetServerConfig::opcode:
        callHandler<WireFormat::GetServerConfig, MembershipService,
            &MembershipService::getServerConfig>(rpc);
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
 * Top-level service method to handle the GET_SERVER_CONFIG request.
 *
 * Yes, this is out of place in the membership service, but it needs to be
 * handled by a service that will always be present and it seems silly to
 * introduce one for a single RPC. If there are others, perhaps we will
 * want a generic server information service.
 *
 * \copydetails Service::ping
 */
void
MembershipService::getServerConfig(
    const WireFormat::GetServerConfig::Request* reqHdr,
    WireFormat::GetServerConfig::Response* respHdr,
    Rpc* rpc)
{
    ProtoBuf::ServerConfig serverConfigBuf;
    serverConfig->serialize(serverConfigBuf);
    respHdr->serverConfigLength = ProtoBuf::serializeToResponse(
        rpc->replyPayload, &serverConfigBuf);
}

/**
 * Top-level service method to handle the UPDATE_SERVER_LIST request.
 *
 * \copydetails Service::ping
 */
void
MembershipService::updateServerList(
    const WireFormat::UpdateServerList::Request* reqHdr,
    WireFormat::UpdateServerList::Response* respHdr,
    Rpc* rpc)
{
    uint32_t reqOffset = sizeof32(*reqHdr);
    uint32_t reqLen = rpc->requestPayload->size();

    // Repeatedly apply the server lists in the RPC while we haven't reached
    // the end of the RPC.
    while (reqOffset < reqLen) {
        ProtoBuf::ServerList list;
        auto* part = rpc->requestPayload->getOffset<
                    WireFormat::UpdateServerList::Request::Part>(reqOffset);
        reqOffset += sizeof32(*part);

        // Bounds check on rpc size.
        if (part == NULL || reqOffset + part->serverListLength > reqLen) {
            LOG(WARNING, "A partial UpdateServerList request is detected. "
                    "Perhaps limit the number of ProtoBufs the Coordinator"
                    "ServerList can batch into one rpc.");
            break;
        }


        // Check passed, parse server list and apply.
        ProtoBuf::parseFromRequest(rpc->requestPayload, reqOffset,
                                   part->serverListLength, &list);
        reqOffset += part->serverListLength;
        respHdr->currentVersion = serverList->applyServerList(list);
    }
}

} // namespace RAMCloud
