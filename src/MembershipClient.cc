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
 * This file implements the MembershipClient class, used to initiate RPCs to
 * instances of the MembershipService.
 */

#include "Common.h"
#include "MembershipClient.h"
#include "ProtoBuf.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Instruct the cluster membership service for the specified server to replace
 * its idea of cluster membership with the complete list given.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param serverId
 *      Identifies the server to which this update should be sent.
 * \param list
 *      The complete server list representing all cluster membership.
 *
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
void
MembershipClient::UpdateServerList(Context* context, ServerId serverId,
        ProtoBuf::ServerList* list)
{
    UpdateServerListRpc rpc(context, serverId, list);
    return rpc.wait();
}

/**
 * Constructor for UpdateServerListRpc: initiates an RPC in the same way as
 * #MembershipClient::UpdateServerListRpc, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param serverId
 *      Identifies the server to which this update should be sent.
 * \param list
 *      The complete server list representing all cluster membership.
 */
UpdateServerListRpc::UpdateServerListRpc(Context* context, ServerId serverId,
        ProtoBuf::ServerList* list)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::UpdateServerList::Response))
{
    WireFormat::UpdateServerList::Request* reqHdr(
            allocHeader<WireFormat::UpdateServerList>(serverId));
    reqHdr->serverListLength = serializeToRequest(&request, list);
    send();
}
}  // namespace RAMCloud
