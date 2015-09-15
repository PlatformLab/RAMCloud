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

#include "TestUtil.h"
#include "BindTransport.h"
#include "CoordinatorServerList.h"
#include "CoordinatorService.h"
#include "CoordinatorUpdateManager.h"
#include "MembershipService.h"
#include "MockExternalStorage.h"
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
    std::mutex mutex;
    MockExternalStorage storage;
    TestLog::Enable logEnabler;

    MembershipServiceTest()
        : context()
        , serverId(99, 2)
        , serverList(&context)
        , serverConfig(ServerConfig::forTesting())
        , service(&context, &serverList, &serverConfig)
        , transport(&context)
        , mockRegistrar(&context, transport)
        , mutex()
        , storage(true)
        , logEnabler()
    {
        context.externalStorage  = &storage;
        transport.registerServer(&context, "mock:host=member");
        serverList.testingAdd({serverId, "mock:host=member",
                               {WireFormat::PING_SERVICE}, 100,
                              ServerStatus::UP});
    }

    typedef std::unique_lock<std::mutex> Lock;
    DISALLOW_COPY_AND_ASSIGN(MembershipServiceTest);
};

TEST_F(MembershipServiceTest, updateServerList_single) {
    Lock lock(mutex); // Lock used to trick internal calls
    // Create a temporary coordinator server list (with its own context)
    // to use as a source for update information.
    Context context2;
    context2.externalStorage = &storage;
    CoordinatorServerList source(&context2);
    source.haltUpdater();
    CoordinatorService coordinatorService(&context2, 1000, true);
    ServerId id1 = source.enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 0, 100, "mock:host=55");
    ServerId id2 = source.enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 0, 100, "mock:host=56");
    ServerId id3 = source.enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 0, 100, "mock:host=57");
    ProtoBuf::ServerList fullList;
    source.serialize(&fullList, {WireFormat::MASTER_SERVICE,
            WireFormat::BACKUP_SERVICE});

    CoordinatorServerList::UpdateServerListRpc
        rpc(&context, serverId, &fullList);
    rpc.send();
    rpc.waitAndCheckErrors();
    EXPECT_STREQ("mock:host=55", serverList.getLocator(id1).c_str());
    EXPECT_STREQ("mock:host=56", serverList.getLocator(id2).c_str());
    EXPECT_STREQ("mock:host=57", serverList.getLocator(id3).c_str());
    const WireFormat::UpdateServerList::Response* respHdr(
            rpc.getResponseHeader<WireFormat::UpdateServerList>());
    EXPECT_EQ(3lu, respHdr->currentVersion);
}

TEST_F(MembershipServiceTest, updateServerList_multi) {
    Lock lock(mutex); // Lock used to trick internal calls
    // Create a temporary coordinator server list (with its own context)
    // to use as a source for update information.
    Context context2;
    context2.externalStorage = &storage;
    ProtoBuf::ServerList fullList, update2, update3;
    CoordinatorServerList source(&context2);
    CoordinatorService coordinatorService(&context2, 1000, true);
    source.haltUpdater();

    // Full List v1
    ServerId id1 = source.enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 0, 100, "mock:host=55");
    source.serialize(&fullList, {WireFormat::MASTER_SERVICE,
            WireFormat::BACKUP_SERVICE});
    // Update v2
    ServerId id2 = source.enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 0, 100, "mock:host=56");
    EXPECT_EQ(2U, source.updates.size());
    update2 = source.updates.back().incremental;
    // Update v3
    ServerId id3 = source.enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::PING_SERVICE}, 0, 100, "mock:host=57");
    update3 = source.updates.back().incremental;


    CoordinatorServerList::UpdateServerListRpc
        rpc(&context, serverId, &fullList);
    rpc.appendServerList(&update2);
    rpc.appendServerList(&update3);
    rpc.send();
    rpc.waitAndCheckErrors();
    EXPECT_STREQ("mock:host=55", serverList.getLocator(id1).c_str());
    EXPECT_STREQ("mock:host=56", serverList.getLocator(id2).c_str());
    EXPECT_STREQ("mock:host=57", serverList.getLocator(id3).c_str());
    const WireFormat::UpdateServerList::Response* respHdr(
            rpc.getResponseHeader<WireFormat::UpdateServerList>());
    EXPECT_EQ(3lu, respHdr->currentVersion);
}

}  // namespace RAMCloud
