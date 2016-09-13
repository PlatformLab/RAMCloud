/* Copyright (c) 2012-2016 Stanford University
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
    Context context;
    Tub<MockCluster> cluster;
    ServerConfig config;

    MockClusterTest()
        : context()
        , cluster()
        , config(ServerConfig::forTesting())
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
        cluster.construct(&context);
    }

    DISALLOW_COPY_AND_ASSIGN(MockClusterTest);
};

TEST_F(MockClusterTest, constructor) {
    ProtoBuf::ServerList serverList;
    CoordinatorClient::getServerList(&context, &serverList);
    EXPECT_EQ(0, serverList.server_size());
}

static bool destructorFilter(string s) { return s == "~MockCluster"; }

TEST_F(MockClusterTest, destructor) {
    config.services = {WireFormat::BACKUP_SERVICE};
    cluster->addServer(config);
    config.services = {WireFormat::MASTER_SERVICE};
    cluster->addServer(config);
    TestLog::Enable _(&destructorFilter);
    cluster.destroy();
    EXPECT_EQ("~MockCluster: MASTER_SERVICE | "
              "~MockCluster: BACKUP_SERVICE",
              TestLog::get());
}

TEST_F(MockClusterTest, addServer) {
    config.services = {WireFormat::BACKUP_SERVICE};
    Server* server = cluster->addServer(config);
    server->backup.get()->testingSkipCallerIdCheck = true;
    cluster->coordinatorContext.coordinatorServerList->sync();
    EXPECT_EQ(server->config.localLocator, "mock:host=server0");
    EXPECT_FALSE(server->master);
    EXPECT_TRUE(server->backup);
    EXPECT_FALSE(server->adminService);
    EXPECT_EQ(1u, cluster->servers.size());
    Segment segment;
    BackupClient::writeSegment(&context, server->serverId, ServerId(),
                               100, 0, &segment, 0, 0, {}, true, false, false);
    server = cluster->addServer(config);
    EXPECT_EQ(server->config.localLocator, "mock:host=server1");
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
    config.services = {WireFormat::BACKUP_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchMasterCreate) {
    config.services = {WireFormat::MASTER_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchTinyMasterCreate) {
    config.master.logBytes = config.segmentSize;
    config.master.hashTableBytes = 1 * 1024;
    config.services = {MASTER_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchAdminCreate) {
    config.services = {WireFormat::ADMIN_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}

TEST_F(MockClusterTest, benchFullCreate) {
    config.services = {WireFormat::MASTER_SERVICE,
                       WireFormat::BACKUP_SERVICE,
                       WireFormat::ADMIN_SERVICE};
    for (int i = 0; i < its; ++i)
        cluster->addServer(config);
}
#endif

} // namespace RAMCloud
