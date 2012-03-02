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

#include "TestUtil.h"
#include "BindTransport.h"
#include "MembershipClient.h"
#include "MembershipService.h"
#include "ServerId.h"
#include "ServerList.h"
#include "ServerList.pb.h"
#include "ServerListBuilder.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

class MembershipServiceTest : public ::testing::Test {
  public:
    ServerId serverId;
    ServerList serverList;
    MembershipService service;
    BindTransport transport;
    TransportManager::MockRegistrar mockRegistrar;
    MembershipClient client;

    MembershipServiceTest()
        : serverId()
        , serverList()
        , service(serverId, serverList)
        , transport()
        , mockRegistrar(transport)
        , client()
    {
        transport.addService(service, "mock:host=member", MEMBERSHIP_SERVICE);
    }

    DISALLOW_COPY_AND_ASSIGN(MembershipServiceTest);
};

TEST_F(MembershipServiceTest, getServerId) {
    serverId = ServerId(523, 234);
    EXPECT_EQ(ServerId(523, 234), client.getServerId(
        Context::get().transportManager->getSession("mock:host=member")));
}

}  // namespace RAMCloud
