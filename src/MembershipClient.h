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
 * This file defines the MembershipClient class.
 */

#ifndef RAMCLOUD_MEMBERSHIPCLIENT_H
#define RAMCLOUD_MEMBERSHIPCLIENT_H

#include "Client.h"
#include "ServerId.h"
#include "ServerIdRpcWrapper.h"
#include "ServerList.pb.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * Encapsulates the state of a MembershipClient::setServerList
 * request, allowing it to execute asynchronously.
 */
class SetServerListRpc2 : public ServerIdRpcWrapper {
    public:
    SetServerListRpc2(Context& context, ServerId serverId,
            ProtoBuf::ServerList& list);
    ~SetServerListRpc2() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SetServerListRpc2);
};

/**
 * Encapsulates the state of a MembershipClient::updateServerList
 * request, allowing it to execute asynchronously.
 */
class UpdateServerListRpc2 : public ServerIdRpcWrapper {
    public:
    UpdateServerListRpc2(Context& context, ServerId serverId,
            ProtoBuf::ServerList& update);
    ~UpdateServerListRpc2() {}
    bool wait();

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(UpdateServerListRpc2);
};

/**
 * This class implements the client-side interface to the membership service,
 * which runs on each RAMCloud server. The coordinator uses this interface to
 * push cluster membership updates so that servers have an up-to-date view of
 * all other servers in the cluster and receive failure notifications that may
 * require some action.
 *
 * In addition, Servers may use this interface to obtain the ServerId of another
 * server. This can be used to ensure that the proper server is being talked to,
 * since ServiceLocators can be ambiguous (restarted servers may listen on the
 * exact same network address that a previous one might have).
 *
 * See #MembershipService for more information.
 */
class MembershipClient : public Client {
  public:
    static void setServerList(Context& context, ServerId serverId,
            ProtoBuf::ServerList& list);
    static bool updateServerList(Context& context, ServerId serverId,
            ProtoBuf::ServerList& changes);

    explicit MembershipClient(Context& context) : context(context) {}
    ServerId getServerId(Transport::SessionRef session);

  private:
    /// Shared RAMCloud information.
    Context& context;

    DISALLOW_COPY_AND_ASSIGN(MembershipClient);
};
} // namespace RAMCloud

#endif // RAMCLOUD_MEMBERSHIPCLIENT_H
