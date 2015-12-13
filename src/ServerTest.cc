/* Copyright (c) 2012-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for
 * any purpose with or without fee is hereby granted, provided that
 * the above copyright notice and this permission notice appear in all
 * copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
 * AUTHORS BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
 * OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "BindTransport.h"
#include "MockCluster.h"
#include "ProtoBuf.h"
#include "Server.h"
#include "TransportManager.h"
#include "WorkerManager.h"

namespace RAMCloud {

class ServerTest: public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    ServerConfig config;
    Tub<Server> server;

    ServerTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , config(ServerConfig::forTesting())
        , server()
    {
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE,
                           WireFormat::MEMBERSHIP_SERVICE,
                           WireFormat::PING_SERVICE};
        config.coordinatorLocator = cluster.coordinatorLocator;
        config.localLocator = "mock:host=server0";
        server.construct(&context, &config);
    }

    ~ServerTest() {
        cluster.syncCoordinatorServerList();
    }

    DISALLOW_COPY_AND_ASSIGN(ServerTest);
};

TEST_F(ServerTest, startForTesting) {
    server->startForTesting(cluster.transport);
    cluster.syncCoordinatorServerList();
    PingClient::ping(&context, server->serverId, ServerId());
}

// run is too much of a pain to and not that interesting.

TEST_F(ServerTest, createAndRegisterServices) {
    server->createAndRegisterServices();
    EXPECT_TRUE(server->master);
    EXPECT_TRUE(server->backup);
    EXPECT_TRUE(server->membership);
    EXPECT_TRUE(server->ping);
    EXPECT_EQ(server->master.get(),
        context.services[WireFormat::MASTER_SERVICE]);
    EXPECT_EQ(server->backup.get(),
        context.services[WireFormat::BACKUP_SERVICE]);
    EXPECT_EQ(server->membership.get(),
        context.services[WireFormat::MEMBERSHIP_SERVICE]);
    EXPECT_EQ(server->ping.get(),
        context.services[WireFormat::PING_SERVICE]);
}

TEST_F(ServerTest, enlist) {
    server->createAndRegisterServices();
    cluster.transport.registerServer(&context, config.localLocator);
    TestLog::Enable _("serverCrashed", "enlistServer", NULL);
    server->enlist({128, 0});
    EXPECT_EQ(
        "enlistServer: Enlisting server at mock:host=server0 "
        "(server id 1.0) supporting services: MASTER_SERVICE, "
        "BACKUP_SERVICE, PING_SERVICE, MEMBERSHIP_SERVICE | "
        "enlistServer: Backup at id 1.0 has 100 MB/s read",
         TestLog::get());
    ASSERT_TRUE(server->master->serverId.isValid());
    EXPECT_TRUE(server->backup->serverId.isValid());

    ProtoBuf::ServerList serverList;
    CoordinatorClient::getServerList(&context, &serverList);
    EXPECT_EQ(1, serverList.server_size());
    auto mask = config.services.serialize();
    EXPECT_EQ(config.localLocator, serverList.server(0).service_locator());
    EXPECT_EQ(mask, serverList.server(0).services());
}

} // namespace RAMCloud
