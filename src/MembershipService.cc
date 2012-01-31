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

/**
 * \file
 * This file implements the MembershipService class. It is responsible for
 * maintaining the global ServerList object describing other servers in the
 * system.
 */

#include <unordered_set>

#include "Common.h"
#include "CoordinatorClient.h"
#include "MembershipService.h"
#include "ProtoBuf.h"
#include "Rpc.h"
#include "ServerId.h"
#include "ServerList.pb.h"
#include "ServiceLocator.h"
#include "ShortMacros.h"

// Feel free to change this to DEBUG if the individual addition and removal
// messages get to be too annoying (will certainly be the case in large
// clusters).
#define __DEBUG NOTICE

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
MembershipService::dispatch(RpcOpcode opcode, Rpc& rpc)
{
    switch (opcode) {
    case GetServerIdRpc::opcode:
        callHandler<GetServerIdRpc, MembershipService,
            &MembershipService::getServerId>(rpc);
        break;
    case SetServerListRpc::opcode:
        callHandler<SetServerListRpc, MembershipService,
            &MembershipService::setServerList>(rpc);
        break;
    case UpdateServerListRpc::opcode:
        callHandler<UpdateServerListRpc, MembershipService,
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
MembershipService::getServerId(const GetServerIdRpc::Request& reqHdr,
                              GetServerIdRpc::Response& respHdr,
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
MembershipService::setServerList(const SetServerListRpc::Request& reqHdr,
                                 SetServerListRpc::Response& respHdr,
                                 Rpc& rpc)
{
    ProtoBuf::ServerList list;
    ProtoBuf::parseFromRequest(rpc.requestPayload, sizeof(reqHdr),
                               reqHdr.serverListLength, list);

    LOG(NOTICE, "Got complete list of servers containing %d entries (version "
        "number %lu)", list.server_size(), list.version_number());

    // Build a temporary map of currently live servers so that we can
    // efficiently evict dead servers from the list.
    std::unordered_set<uint64_t> liveServers;
    for (int i = 0; i < list.server_size(); i++)
        liveServers.insert(list.server(i).server_id());

    // Remove dead ServerIds.
    for (uint32_t i = 0; i < serverList.size(); i++) {
        ServerId id = serverList[i];
        if (id.isValid() && !contains(liveServers, *id)) {
            LOG(__DEBUG, "  Removing server id %lu (locator \"%s\")",
                *id, serverList.getLocator(id).c_str());
            serverList.remove(id);
        }
    }

    // Add new ones.
    for (int i = 0; i < list.server_size(); i++) {
        const auto& server = list.server(i);
        ServerId id(server.server_id());
        if (!serverList.contains(id)) {
            const string& locator = server.service_locator();
            ServiceMask services =
                ServiceMask::deserialize(server.service_mask());
            uint32_t readMBytesPerSec = server.backup_read_mbytes_per_sec();
            LOG(__DEBUG, "  Adding server id %lu (locator \"%s\") "
                         "with services %s and %u MB/s storage",
                *id, locator.c_str(), services.toString().c_str(),
                readMBytesPerSec);
            serverList.add(id, locator, services, readMBytesPerSec);
        }
    }

    serverList.setVersion(list.version_number());
}

/**
 * Top-level service method to handle the UPDATE_SERVER_LIST request.
 *
 * \copydetails Service::ping
 */
void
MembershipService::updateServerList(const UpdateServerListRpc::Request& reqHdr,
                                    UpdateServerListRpc::Response& respHdr,
                                    Rpc& rpc)
{
    ProtoBuf::ServerList update;
    ProtoBuf::parseFromRequest(rpc.requestPayload, sizeof(reqHdr),
                               reqHdr.serverListLength, update);

    respHdr.lostUpdates = false;

    // If this isn't the next expected update, request that the entire list
    // be pushed again.
    if (update.version_number() != (serverList.getVersion() + 1)) {
        LOG(NOTICE, "Update generation number is %lu, but last seen was %lu. "
            "Something was lost! Grabbing complete list again!",
            update.version_number(), serverList.getVersion());
        respHdr.lostUpdates = true;
        return;
    }

    LOG(__DEBUG, "Got server list update (version number %lu)",
        update.version_number());

    // Process the update.
    for (int i = 0; i < update.server_size(); i++) {
        const auto& server = update.server(i);
        ServerId id(server.server_id());
        if (server.is_in_cluster()) {
            const string& locator = server.service_locator();
            ServiceMask services =
                ServiceMask::deserialize(server.service_mask());
            uint32_t readMBytesPerSec = server.backup_read_mbytes_per_sec();
            LOG(__DEBUG, "  Adding server id %lu (locator \"%s\") "
                         "with services %s and %u MB/s storage",
                *id, locator.c_str(), services.toString().c_str(),
                readMBytesPerSec);
            serverList.add(id, locator, services, readMBytesPerSec);
        } else {
            if (!serverList.contains(id)) {
                LOG(ERROR, "  Cannot remove server id %lu: The server is "
                    "not in our list, despite list version numbers matching "
                    "(%lu). Something is screwed up! Requesting the entire "
                    "list again.", *id, update.version_number());
                respHdr.lostUpdates = true;
                return;
            }

            LOG(__DEBUG, "  Removing server id %lu (locator \"%s\")",
                *id, serverList.getLocator(id).c_str());
            serverList.remove(id);
        }
    }

    serverList.setVersion(update.version_number());
}

} // namespace RAMCloud
