/* Copyright (c) 2009-2019 Stanford University
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
#include "MockCluster.h"
#include "PlusOneBackupSelector.h"
#include "ServiceMask.h"
#include "ShortMacros.h"

namespace RAMCloud {

struct PlusOneBackupSelectorTest : public ::testing::Test {
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    PlusOneBackupSelector* selector;

    PlusOneBackupSelectorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , selector()
    {
        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::ADMIN_SERVICE};
        config.master.numReplicas = 1u;
        config.master.usePlusOneBackup = true;
        Server* server = cluster.addServer(config);
        selector = static_cast<PlusOneBackupSelector*>(
            server->master->objectManager.replicaManager.backupSelector.get());
    }

    void addDifferentHosts(std::vector<ServerId>& ids) {
        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::BACKUP_SERVICE,
                           WireFormat::ADMIN_SERVICE};
        for (uint32_t i = 1; i < 10; i++) {
            config.backup.mockSpeed = i * 10;
            config.localLocator = format("mock:host=backup%u", i);
            ids.push_back(cluster.addServer(config)->serverId);
        }
    }
    DISALLOW_COPY_AND_ASSIGN(PlusOneBackupSelectorTest);
};

TEST_F(PlusOneBackupSelectorTest, selectSecondary) {
    std::vector<ServerId> ids;
    addDifferentHosts(ids);

    ServerId id = selector->selectSecondary(0, NULL);
    EXPECT_EQ(ServerId(2, 0), id);

    const ServerId conflicts[] = { ids[0] };

    id = selector->selectSecondary(1, &conflicts[0]);
    EXPECT_EQ(ServerId(3, 0), id);
}

TEST_F(PlusOneBackupSelectorTest, selectSecondary_logThrottling) {
    // First problem: generate a log message.
    TestLog::reset();
    ServerId id = selector->selectSecondary(0, NULL);
    EXPECT_EQ(ServerId(), id);
    EXPECT_EQ("selectSecondary: PlusOneBackupSelector could not find a "
              "suitable server in 1 attempts; may need to wait for additional "
              "servers to enlist",
            TestLog::get());
    EXPECT_FALSE(selector->okToLogNextProblem);

    // Recurring problem: no new message.
    TestLog::reset();
    id = selector->selectSecondary(0, NULL);
    EXPECT_EQ("", TestLog::get());

    // Successful completion: messages reenabled.
    std::vector<ServerId> ids;
    addDifferentHosts(ids);
    id = selector->selectSecondary(0, NULL);
    EXPECT_EQ(ServerId(2, 0), id);
    EXPECT_TRUE(selector->okToLogNextProblem);
}

} // namespace RAMCloud
