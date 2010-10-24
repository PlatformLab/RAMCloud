/* Copyright (c) 2010 Stanford University
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

#include "Coordinator.h"
#include "ProtoBuf.h"

namespace RAMCloud {

/**
 * Servers call this when they come online to beg for work.
 * \return
 *      A server ID guaranteed never to have been used before.
 */
uint64_t
Coordinator::enlistServer(ServerType serverType, string localServiceLocator)
{
    while (true) {
        try {
            Buffer req;
            Buffer resp;
            EnlistServerRpc::Request& reqHdr(
                allocHeader<EnlistServerRpc>(req));
            reqHdr.serverType = serverType;
            reqHdr.serviceLocatorLength = localServiceLocator.length() + 1;
            strncpy(new(&req, APPEND) char[reqHdr.serviceLocatorLength],
                    localServiceLocator.c_str(),
                    reqHdr.serviceLocatorLength);
            const EnlistServerRpc::Response& respHdr(
                sendRecv<EnlistServerRpc>(session, req, resp));
            checkStatus();
            return respHdr.serverId;
        } catch (TransportException& e) {
            LOG(NOTICE,
                "TransportException trying to talk to coordinator: %s",
                e.message.c_str());
            LOG(NOTICE, "retrying");
        }
    }
}

/**
 * List all live servers.
 * Masters call and cache this periodically to find backups.
 */
void
Coordinator::getServerList(ProtoBuf::ServerList& serverList)
{
    Buffer req;
    Buffer resp;
    allocHeader<GetServerListRpc>(req);
    const GetServerListRpc::Response& respHdr(
        sendRecv<GetServerListRpc>(session, req, resp));
    checkStatus();
    ProtoBuf::parseFromResponse(resp, sizeof(respHdr),
                                respHdr.serverListLength, serverList);
}

} // namespace RAMCloud
