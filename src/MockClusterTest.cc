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
#include "MockCluster.h"
#include "ProtoBuf.h"

namespace RAMCloud {

class MockClusterTest : public ::testing::Test {
  public:
    Tub<MockCluster> cluster;
    ServerConfig config;

    MockClusterTest()
        : cluster()
        , config(ServerConfig::forTesting())
    {
        cluster.construct();
    }

    DISALLOW_COPY_AND_ASSIGN(MockClusterTest);
};

TEST_F(MockClusterTest, constructor) {
    ProtoBuf::ServerList serverList;
    cluster->getCoordinatorClient()->getServerList(serverList);
    EXPECT_EQ(0, serverList.server_size());
}

static bool destructorFilter(string s) { return s == "~MockCluster"; }

TEST_F(MockClusterTest, destructor) {
    config.services = {BACKUP_SERVICE};
    cluster->addServer(config);
    config.services = {MASTER_SERVICE};
    cluster->addServer(config);
    TestLog::Enable _(&destructorFilter);
    cluster.destroy();
    EXPECT_EQ("~MockCluster: MASTER_SERVICE | "
              "~MockCluster: BACKUP_SERVICE",
              TestLog::get());
}

TEST_F(MockClusterTest, addServer) {
    config.services = {BACKUP_SERVICE};
    Server* server = cluster->addServer(config);
    EXPECT_EQ(server->config.localLocator, "mock:host=server0");
    EXPECT_FALSE(server->master);
    EXPECT_TRUE(server->backup);
    EXPECT_FALSE(server->membership);
    EXPECT_FALSE(server->ping);
    EXPECT_EQ(1u, cluster->servers.size());
    cluster->get<BackupClient>(server)->openSegment({99, 0}, 100);
    server = cluster->addServer(config);
    EXPECT_EQ(server->config.localLocator, "mock:host=server1");
}

TEST_F(MockClusterTest, get) {
    config.services = {BACKUP_SERVICE};
    cluster->get<BackupClient>(
        cluster->addServer(config))->openSegment({99, 0}, 100);
}

// Don't delete this.  Occasionally useful for checking unit test perf.
#if 0
// Benchmark times to create various mock server configuarions.
// Notice that gtest *does* include the fixture construction time
// in the time it reports for the test case run times.
static const int its = 100;

TEST_F(MockClusterTest, benchCoordinatorOnly) {
}

TEST_F(MockClusterTest, benchBackupCreate) {
    config.services = {BACKUP_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchMasterCreate) {
    config.services = {MASTER_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchTinyMasterCreate) {
    config.master.logBytes = config.backup.segmentSize;
    config.master.hashTableBytes = 1 * 1024;
    config.services = {MASTER_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchMembershipCreate) {
    config.services = {MEMBERSHIP_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchPingCreate) {
    config.services = {PING_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchFullCreate) {
    config.services = {MASTER_SERVICE, BACKUP_SERVICE,
                       MEMBERSHIP_SERVICE, PING_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}
#endif

} // namespace RAMCloud
