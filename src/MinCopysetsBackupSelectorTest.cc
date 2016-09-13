/* Copyright (c) 2009-2016 Stanford University
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

#define _GLIBCXX_USE_SCHED_YIELD
#include <thread>
#undef _GLIBCXX_USE_SCHED_YIELD

#include "TestUtil.h"
#include "Common.h"
#include "MinCopysetsBackupSelector.h"
#include "MockCluster.h"
#include "ServiceMask.h"
#include "ShortMacros.h"

namespace RAMCloud {

struct MinCopysetsBackupSelectorTest : public ::testing::Test {
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    MinCopysetsBackupSelector* selector;
    ServerList* sl;
    std::vector<ServerId> ids;

    MinCopysetsBackupSelectorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , selector()
        , sl()
        , ids()
    {
        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::ADMIN_SERVICE};
        config.master.numReplicas = 3u;
        config.master.useMinCopysets = true;
        Server* server = cluster.addServer(config);
        selector = static_cast<MinCopysetsBackupSelector*>(
            server->master->objectManager.replicaManager.backupSelector.get());
        sl = static_cast<ServerList*>(selector->tracker.parent);
        for (uint32_t i = 1; i < 13; i++) {
            uint64_t rep_id = (i + 1) / 3;
            sl->testingAdd({{i, 0}, "mock:host=backup1",
                           {WireFormat::BACKUP_SERVICE},
                           100, ServerStatus::UP, rep_id});
            ids.push_back(ServerId(i , 0));
        }
    }

    DISALLOW_COPY_AND_ASSIGN(MinCopysetsBackupSelectorTest);
};


TEST_F(MinCopysetsBackupSelectorTest, selectSecondary) {
    ServerId id = selector->selectSecondary(0, NULL);
    EXPECT_EQ(ServerId(), id);
    const ServerId conflicts_1[] = { ids[0] };
    id = selector->selectSecondary(1, &conflicts_1[0]);
    // Should return an invalid server because the replicationId of the first
    // node is 0.
    EXPECT_EQ(ServerId(), id);
    const ServerId conflicts_2[] = { ids[11] };
    id = selector->selectSecondary(1, &conflicts_2[0]);
    // Should return an invalid server because there are only 2 servers with a
    // replicationId of 4, while numReplicas is set to 3.
    EXPECT_EQ(ServerId(), id);
    const ServerId conflicts_3[] = { ids[1], ids[3] };
    id = selector->selectSecondary(2, &conflicts_3[0]);
    // Should return the third node in the replication group.
    EXPECT_EQ(ids[2], id);
    const ServerId conflicts_4[] = { ids[4], ids[5], ids[7], ids[8] };
    id = selector->selectSecondary(4, &conflicts_4[0]);
    // Should return the third node in the replication group.
    EXPECT_EQ(ids[6], id);
}

TEST_F(MinCopysetsBackupSelectorTest, getReplicationGroupServer) {
    selector->applyTrackerChanges();
    const ServerId conflicts_1[] = { ids[1], ids[2], ids[3] };
    EXPECT_EQ(ServerId(), selector->getReplicationGroupServer(
        3, &conflicts_1[0], 1u));
    const ServerId conflicts_2[] = { ids[1], ids[2] };
    EXPECT_EQ(ids[3], selector->getReplicationGroupServer(
        2, &conflicts_2[0], 1u));
    const ServerId conflicts_3[] = { ids[1], ids[3] };
    EXPECT_EQ(ids[2], selector->getReplicationGroupServer(
        2, &conflicts_3[0], 1u));
    const ServerId conflicts_4[] = { ids[2], ids[3] };
    EXPECT_EQ(ids[1], selector->getReplicationGroupServer(
        2, &conflicts_4[0], 1u));
}

} // namespace RAMCloud
