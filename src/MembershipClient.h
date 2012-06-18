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
#include "ServerList.pb.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * This class implements the client-side interface to the ping service.
 */
class MembershipClient : public Client {
  public:
    explicit MembershipClient(Context& context) : context(context) {}
    ServerId getServerId(Transport::SessionRef session);
    void setServerList(const char* serviceLocator,
                       ProtoBuf::ServerList& list);
    bool updateServerList(const char* serviceLocator,
                          ProtoBuf::ServerList& update);

  private:
    /// Shared RAMCloud information.
    Context& context;

    DISALLOW_COPY_AND_ASSIGN(MembershipClient);
};
} // namespace RAMCloud

#endif // RAMCLOUD_MEMBERSHIPCLIENT_H
