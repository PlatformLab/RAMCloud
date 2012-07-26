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
 * Given a Session, obtain the ServerId for the server behind that session.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param session
 *      Connection to a RAMCloud server.
 *
 * \return
 *       The RAMCloud identifier for the server  associated with #session.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
ServerId
MembershipClient::getServerId(Context& context, Transport::SessionRef session)
{
    GetServerIdRpc2 rpc(context, session);
    return rpc.wait();
}

/**
 * Constructor for GetServerIdRpc2: initiates an RPC in the same way as
 * #MembershipClient::getServerId, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param session
 *      Connection to a RAMCloud server.
 */
GetServerIdRpc2::GetServerIdRpc2(Context& context,
        Transport::SessionRef session)
    : RpcWrapper(sizeof(WireFormat::GetServerId::Response))
    , context(context)
{
    this->session = session;
    allocHeader<WireFormat::GetServerId>();
    send();
}

/**
 * Wait for a getServerId RPC to complete, and return the same
 * results as #MembershipClient::getServerId.
 *
 * \return
 *       The RAMCloud identifier for the server associated with the
 *       session specified in the constructor.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
ServerId
GetServerIdRpc2::wait()
{
    waitInternal(*context.dispatch);
    if (getState() != RpcState::FINISHED) {
        throw TransportException(HERE);
    }
    const WireFormat::GetServerId::Response& respHdr(
            getResponseHeader<WireFormat::GetServerId>());
    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);
    return ServerId(respHdr.serverId);
}

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
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */ 
void
MembershipClient::setServerList(Context& context, ServerId serverId,
        ProtoBuf::ServerList& list)
{
    SetServerListRpc2 rpc(context, serverId, list);
    return rpc.wait();
}

/**
 * Constructor for SetServerListRpc2: initiates an RPC in the same way as
 * #MembershipClient::setServerList, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param serverId
 *      Identifies the server to which this update should be sent.
 * \param list
 *      The complete server list representing all cluster membership.
 */
SetServerListRpc2::SetServerListRpc2(Context& context, ServerId serverId,
        ProtoBuf::ServerList& list)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::SetServerList::Response))
{
    WireFormat::SetServerList::Request& reqHdr(
            allocHeader<WireFormat::SetServerList>());
    reqHdr.serverListLength = serializeToRequest(request, list);
    send();
}

/**
 * Notify a server that other servers have entered or left the cluster.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param serverId
 *      Identifies the server to which this update should be sent.
 * \param changes
 *      Information about changes to the list of servers in the cluster.
 *
 * \return
 *      Returns true if the server successfully applied the update, otherwise
 *      returns false if it could not. Failure is due to the version number of
 *      the update not matching what was expected (i.e. the server lost an
 *      update at some point).
 *
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */ 
bool
MembershipClient::updateServerList(Context& context, ServerId serverId,
        ProtoBuf::ServerList& changes)
{
    UpdateServerListRpc2 rpc(context, serverId, changes);
    return rpc.wait();
}

/**
 * Constructor for UpdateServerListRpc2: initiates an RPC in the same way as
 * #MembershipClient::updateServerList, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param serverId
 *      Identifies the server to which this update should be sent.
 * \param changes
 *      Information about changes to the list of servers in the cluster.
 */
UpdateServerListRpc2::UpdateServerListRpc2(Context& context, ServerId serverId,
        ProtoBuf::ServerList& changes)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::UpdateServerList::Response))
{
    WireFormat::UpdateServerList::Request& reqHdr(
            allocHeader<WireFormat::UpdateServerList>());
    reqHdr.serverListLength = serializeToRequest(request, changes);
    send();
}

/**
 * Wait for an updateServerList RPC to complete, and throw exceptions
 * for any errors.
 *
 * \return
 *      Returns true if the server successfully applied the update, otherwise
 *      returns false if it could not. Failure is due to the version number of
 *      the update not matching what was expected (i.e. the server lost an
 *      update at some point).
 *
 * \throw ServerDoesntExistException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
bool
UpdateServerListRpc2::wait()
{
    waitAndCheckErrors();
    const WireFormat::UpdateServerList::Response& respHdr(
            getResponseHeader<WireFormat::UpdateServerList>());
    return respHdr.lostUpdates == 0;
}

}  // namespace RAMCloud
