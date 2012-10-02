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

#include "TestUtil.h"
#include "BindTransport.h"
#include "CoordinatorServerList.h"
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
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    MembershipService service;
    BindTransport transport;
    TransportManager::MockRegistrar mockRegistrar;

    MembershipServiceTest()
        : context()
        , serverId(99, 2)
        , serverList(&context)
        , serverConfig(ServerConfig::forTesting())
        , service(serverId, serverList, serverConfig)
        , transport(&context)
        , mockRegistrar(&context, transport)
    {
        transport.addService(service, "mock:host=member",
                             WireFormat::MEMBERSHIP_SERVICE);
        serverList.testingAdd({serverId, "mock:host=member",
                               {WireFormat::PING_SERVICE}, 100,
                              ServerStatus::UP});
    }

    DISALLOW_COPY_AND_ASSIGN(MembershipServiceTest);
};

TEST_F(MembershipServiceTest, getServerId) {
    serverId = ServerId(523, 234);
    EXPECT_EQ(ServerId(523, 234), MembershipClient::getServerId(&context,
        context.transportManager->getSession("mock:host=member")));
}

TEST_F(MembershipServiceTest, updateServerList) {
    // Create a temporary coordinator server list (with its own context)
    // to use as a source for update information.
    Context context2;
    CoordinatorServerList source(&context2);
    ServerId id1 = source.generateUniqueId();
    source.add(id1, "mock:host=55", {WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 100);
    ServerId id2 = source.generateUniqueId();
    source.add(id2, "mock:host=56", {WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 100);
    ServerId id3 = source.generateUniqueId();
    source.add(id3, "mock:host=57", {WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 100);
    ProtoBuf::ServerList fullList;
    source.serialize(fullList);

    MembershipClient::UpdateServerList(&context, serverId, &fullList);
    EXPECT_STREQ("mock:host=55", serverList.getLocator(id1));
    EXPECT_STREQ("mock:host=56", serverList.getLocator(id2));
    EXPECT_STREQ("mock:host=57", serverList.getLocator(id3));
}

}  // namespace RAMCloud
