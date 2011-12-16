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
 * \param serviceLocator
 *      Identifies the server to which this update should be sent.
 *
 * \param list
 *      The complete server list representing all cluster membership.
 */ 
void
MembershipClient::setServerList(const char* serviceLocator,
                                ProtoBuf::ServerList& list)
{
    // Fill in the request.
    Buffer req, resp;
    SetServerListRpc::Request& reqHdr(allocHeader<SetServerListRpc>(req));
    Transport::SessionRef session =
            Context::get().transportManager->getSession(serviceLocator);
    reqHdr.serverListLength = serializeToRequest(req, list);
    sendRecv<SetServerListRpc>(session, req, resp);
    checkStatus(HERE);
}

/**
 * Issue a cluster membership update to the specified server.
 *
 * XXX- This should be asynchronous. There's no reason for the Coordinator
 *      to wait.
 *
 * \param serviceLocator
 *      Identifies the server to which this update should be sent.
 *
 * \param update
 *      The update to be sent.
 *
 * \return
 *      Returns true if the server successfully applied the update, otherwise
 *      returns false if it could not. Failure is due to the version number of
 *      the update not matching what was expected (i.e. the server lost an
 *      update at some point).
 */
bool
MembershipClient::updateServerList(const char* serviceLocator,
                                   ProtoBuf::ServerList& update)
{
    // Fill in the request.
    Buffer req, resp;
    UpdateServerListRpc::Request& reqHdr(allocHeader<UpdateServerListRpc>(req));
    Transport::SessionRef session =
            Context::get().transportManager->getSession(serviceLocator);
    reqHdr.serverListLength = serializeToRequest(req, update);
    const UpdateServerListRpc::Response& respHdr(
        sendRecv<UpdateServerListRpc>(session, req, resp));
    checkStatus(HERE);
    return respHdr.lostUpdates == 0;
}

}  // namespace RAMCloud
