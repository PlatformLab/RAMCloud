/* Copyright (c) 2012 Stanford University
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
#include "ServiceManager.h"
#include "TransportManager.h"

namespace RAMCloud {

class ServerTest: public ::testing::Test {
  public:
    MockCluster cluster;
    ServerConfig config;
    Tub<Server> server;
    PingClient ping;

    ServerTest()
        : cluster()
        , config(ServerConfig::forTesting())
        , server()
        , ping()
    {
        config.services = {MASTER_SERVICE, BACKUP_SERVICE, MEMBERSHIP_SERVICE,
                           PING_SERVICE};
        config.coordinatorLocator = cluster.coordinatorLocator;
        config.localLocator = "mock:host=server0";
        server.construct(config);
    }

    DISALLOW_COPY_AND_ASSIGN(ServerTest);
};

TEST_F(ServerTest, startForTesting) {
    EXPECT_THROW(ping.ping(config.localLocator.c_str(), 0, 100 * 1000),
                 TransportException);
    server->startForTesting(cluster.transport);
    ping.ping(config.localLocator.c_str(), 0, 100 * 1000);
}

// run is too much of a pain to and not that interesting.

TEST_F(ServerTest, createAndRegisterServices) {
    // Use testing config, but register with the real ServiceManager instead
    // of the BindTransport and see if things get set up properly.
    server->createAndRegisterServices(NULL);
    EXPECT_TRUE(server->coordinator);
    EXPECT_TRUE(server->master);
    EXPECT_TRUE(server->backup);
    EXPECT_TRUE(server->membership);
    EXPECT_TRUE(server->ping);
    const auto& services = Context::get().serviceManager->services;
    ASSERT_TRUE(services[MASTER_SERVICE]);
    EXPECT_EQ(server->master.get(), &services[MASTER_SERVICE]->service);
    ASSERT_TRUE(services[BACKUP_SERVICE]);
    EXPECT_EQ(server->backup.get(), &services[BACKUP_SERVICE]->service);
    ASSERT_TRUE(services[MEMBERSHIP_SERVICE]);
    EXPECT_EQ(server->membership.get(), &services[MEMBERSHIP_SERVICE]->service);
    ASSERT_TRUE(services[PING_SERVICE]);
    EXPECT_EQ(server->ping.get(), &services[PING_SERVICE]->service);
}

TEST_F(ServerTest, enlist) {
    server->createAndRegisterServices(&cluster.transport);
    server->enlist();
    ASSERT_TRUE(server->master->serverId.isValid());
    EXPECT_TRUE(server->backup->serverId.isValid());

    ProtoBuf::ServerList serverList;
    server->coordinator->getServerList(serverList);
    EXPECT_EQ(1, serverList.server_size());
    auto mask = config.services.serialize();
    EXPECT_EQ(config.localLocator, serverList.server(0).service_locator());
    EXPECT_EQ(mask, serverList.server(0).service_mask());
}

} // namespace RAMCloud
