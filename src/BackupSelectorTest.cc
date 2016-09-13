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
#include "MockCluster.h"
#include "ServiceMask.h"
#include "ShortMacros.h"

namespace RAMCloud {

struct BackupSelectorTest : public ::testing::Test {
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    BackupSelector* selector;

    BackupSelectorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , selector()
    {
        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::ADMIN_SERVICE};
        Server* server = cluster.addServer(config);
        ObjectManager* objectManager = &server->master->objectManager;
        selector = objectManager->replicaManager.backupSelector.get();
    }

    char lastChar(const string& s) {
        return *(s.end() - 1);
    }

    void addEqualHosts(std::vector<ServerId>& ids) {
        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::BACKUP_SERVICE,
                           WireFormat::ADMIN_SERVICE};
        config.backup.mockSpeed = 100;
        for (uint32_t i = 1; i < 10; i++) {
            config.localLocator = format("mock:host=backup%u", i);
            ids.push_back(cluster.addServer(config)->serverId);
        }
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
    DISALLOW_COPY_AND_ASSIGN(BackupSelectorTest);
};

TEST_F(BackupSelectorTest, backupStats_getExpectedReadMs) {
    // Check against 32-bit artithmetic overflow.
    BackupStats stats;
    stats.primaryReplicaCount = 10;
    stats.expectedReadMBytesPerSec = 100;
    EXPECT_EQ(880u, stats.getExpectedReadMs());
    stats.primaryReplicaCount = 11;
    stats.expectedReadMBytesPerSec = 100;
    EXPECT_EQ(960u, stats.getExpectedReadMs());
}

struct BackgroundEnlistBackup {
    explicit BackgroundEnlistBackup(Context* context)
        : context(context) {}

    void operator()() {
        // Try to get selectPrimary to block on the empty server list.
        std::this_thread::yield();
        usleep(1 * 1000);
        // See if enlisting a server unblocks the call.
        CoordinatorClient::enlistServer(context, 0, {},
                                        {WireFormat::BACKUP_SERVICE},
                                        "mock:host=backup10", 10);
    }

    Context* context;

    DISALLOW_COPY_AND_ASSIGN(BackgroundEnlistBackup);
};

TEST_F(BackupSelectorTest, selectPrimaryNoHosts) {
    ServerId id = selector->selectPrimary(0, NULL);
    EXPECT_EQ(ServerId(), id);
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqual) {
    MockRandom _(1);
    // getRandomServerIdWithServer(BACKUP_SERVICE) returns backups in order
    // that they enlisted above.
    std::vector<ServerId> ids;
    addEqualHosts(ids);

    ServerId backup = selector->selectPrimary(0, NULL); // use backup1 / id 2
    EXPECT_EQ(ids[0], backup);
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqualOneConstraint) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addEqualHosts(ids);

    ServerId backup =
        selector->selectPrimary(1, &ids[0]); // must skip backup1, use backup2
    EXPECT_EQ(ids[1], backup);
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqualThreeConstraints) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addEqualHosts(ids);

    ServerId backup =
        selector->selectPrimary(3, &ids[0]); // must skip backup1/2/3, use 4
    EXPECT_EQ(ids[3], backup);
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqualNonContiguousConstraints) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addEqualHosts(ids);

    std::vector<ServerId> conflicts = { ids[0], ids[1], ids[3] };
    ServerId backup =
        selector->selectPrimary(3, &conflicts[0]); // skip backup1/2/4, use 3
    EXPECT_EQ(ids[2], backup);
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeeds) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(ids);

    ServerId backup = selector->selectPrimary(0, NULL);
    EXPECT_EQ(ids[4], backup); // Of the 5 inspected the last is the fastest.
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeedsOneConstraint) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(ids);

    ServerId backup = selector->selectPrimary(1, &ids[4]);
    EXPECT_EQ(ids[5], backup); // 5 inspected, but must retry on the last
                               // because it's marked as a conflict, so
                               // the sixth server gets selected.
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeedsFourConstraints) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(ids);

    ServerId backup = selector->selectPrimary(4, &ids[4]);
    EXPECT_EQ(ids[8], backup); // First 4 are slow, next 4 conflict,
                               // choose the ninth.
}

TEST_F(BackupSelectorTest, selectPrimaryEvenPrimaryPlacement) {
    std::vector<ServerId> ids;
    addEqualHosts(ids);

    uint32_t primaryCounts[9] = {};
    for (uint32_t i = 0; i < 900; ++i) {
        ServerId primary = selector->selectPrimary(0, NULL);
        ++primaryCounts[lastChar(selector->tracker.getLocator(primary)) - '1'];
    }
    EXPECT_LT(*std::max_element(primaryCounts, primaryCounts + 9) -
              *std::min_element(primaryCounts, primaryCounts + 9),
              5U); // range < 5 won't fail often
}

TEST_F(BackupSelectorTest, selectSecondary) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(ids);

    ServerId id = selector->selectSecondary(0, NULL);
    EXPECT_EQ(ServerId(2, 0), id);

    const ServerId conflicts[] = { ids[1] };

    id = selector->selectSecondary(1, &conflicts[0]);
    EXPECT_EQ(ServerId(4, 0), id);
}

TEST_F(BackupSelectorTest, selectSecondary_logThrottling) {
    // First problem: generate a log message.
    TestLog::reset();
    ServerId id = selector->selectSecondary(0, NULL);
    EXPECT_EQ(ServerId(), id);
    EXPECT_EQ("selectSecondary: BackupSelector could not find a suitable "
            "server in 100 attempts; may need to wait for additional "
            "servers to enlist",
            TestLog::get());
    EXPECT_FALSE(selector->okToLogNextProblem);

    // Recurring problem: no new message.
    TestLog::reset();
    id = selector->selectSecondary(0, NULL);
    EXPECT_EQ("", TestLog::get());

    // Successful completion: messages reenabled.
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(ids);
    id = selector->selectSecondary(0, NULL);
    EXPECT_EQ(ServerId(2, 0), id);
    EXPECT_TRUE(selector->okToLogNextProblem);
}

TEST_F(BackupSelectorTest, signalFreedPrimary) {
    MockRandom _(1);
    // getRandomServerIdWithServer(BACKUP_SERVICE) returns backups in order
    // that they enlisted above.
    std::vector<ServerId> ids;
    addEqualHosts(ids);

    ServerId backup = selector->selectPrimary(0, NULL); // use backup1 / id 2
    EXPECT_EQ(ids[0], backup);

    BackupStats *stats = selector->tracker[backup];
    stats->primaryReplicaCount = 10;
    EXPECT_EQ(10u, stats->primaryReplicaCount);
    selector->signalFreedPrimary(backup);
    EXPECT_EQ(9u, stats->primaryReplicaCount);
}

#if 0
// This test should run forever, hence why it is commented out.
// Occasionally, when self-doubt mounts, it is worth running, though.
TEST_F(BackupSelectorTest, selectSecondaryConflictsWithAll) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(ids);

    ServerId id = selector->selectSecondary(uint32_t(ids.size()), &ids[0]);
    EXPECT_EQ(ServerId(0, 0), id);
}
#endif

TEST_F(BackupSelectorTest, applyTrackerChanges) {
    ServerList* sl = static_cast<ServerList*>(selector->tracker.parent);
    sl->testingAdd({{1, 0}, "mock:host=backup1", {WireFormat::BACKUP_SERVICE},
                   100, ServerStatus::UP});
    sl->testingAdd({{2, 0}, "mock:host=backup2", {WireFormat::BACKUP_SERVICE},
                   100, ServerStatus::UP});
    selector->applyTrackerChanges();
    EXPECT_EQ(3u, selector->tracker.size());
    EXPECT_EQ(2u, selector->replicationIdMap.size());
    EXPECT_EQ(2u, selector->replicationIdMap.count(0u));
    sl->testingAdd({{2, 0}, "mock:host=backup2", {WireFormat::BACKUP_SERVICE},
                   100, ServerStatus::UP, 1});
    selector->applyTrackerChanges();
    EXPECT_EQ(2u, selector->replicationIdMap.size());
    EXPECT_EQ(1u, selector->replicationIdMap.count(0u));
    EXPECT_EQ(1u, selector->replicationIdMap.count(1u));
    sl->testingRemove({1, 0});
    selector->applyTrackerChanges();
    EXPECT_EQ(1u, selector->replicationIdMap.size());
    EXPECT_EQ(0u, selector->replicationIdMap.count(0u));
    EXPECT_EQ(1u, selector->replicationIdMap.count(1u));
}

TEST_F(BackupSelectorTest, applyTrackerChanges_removeWithoutAdd) {
    // All we care about for this test is that it doesn't crash.
    ServerDetails entry;
    entry.serverId = ServerId(2, 0);
    entry.status = ServerStatus::CRASHED;
    selector->tracker.enqueueChange(entry, ServerChangeEvent::SERVER_CRASHED);
    entry.status = ServerStatus::REMOVE;
    selector->tracker.enqueueChange(entry, ServerChangeEvent::SERVER_REMOVED);
    selector->applyTrackerChanges();
    EXPECT_EQ(1u, selector->replicationIdMap.size());
}

TEST_F(BackupSelectorTest, conflict) {
    ServerId backup(1, 0);
    const ServerId conflictingId(backup);
    const ServerId nonConflictingId(*conflictingId + 1);
    EXPECT_FALSE(selector->conflict(backup, nonConflictingId));
    EXPECT_TRUE(selector->conflict(backup, conflictingId));
}

TEST_F(BackupSelectorTest, conflictWithAny) {
    ServerId w;
    ServerId x(1);
    ServerId y(2);
    ServerId z(3);
    const ServerId existing[] = {
        ServerId(1, 0),
        ServerId(2, 0),
        ServerId(3, 0)
    };
    EXPECT_FALSE(selector->conflictWithAny(w, 0, NULL));
    EXPECT_FALSE(selector->conflictWithAny(w, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(x, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(y, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(z, 3, existing));
}

TEST_F(BackupSelectorTest, conflictWithAny_allowLocalBackup_false) {
    ServerId ownId = *selector->serverId;
    EXPECT_TRUE(selector->conflictWithAny(ownId, 0, NULL));
}

TEST_F(BackupSelectorTest, conflictWithAny_allowLocalBackup_true) {
    // Reset BackupSelector with allowLocalBackup = true
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::MASTER_SERVICE,
                       WireFormat::ADMIN_SERVICE};
    config.master.allowLocalBackup = true;
    Server* server = cluster.addServer(config);
    ObjectManager* objectManager = &server->master->objectManager;
    selector = objectManager->replicaManager.backupSelector.get();

    ServerId ownId = *selector->serverId;
    ServerId w;
    ServerId x(1);
    ServerId y(2);
    ServerId z(3);
    const ServerId existing[] = {
        ServerId(1, 0),
        ServerId(2, 0),
        ServerId(3, 0)
    };
    EXPECT_FALSE(selector->conflictWithAny(ownId, 0, NULL));
    EXPECT_FALSE(selector->conflictWithAny(w, 0, NULL));
    EXPECT_FALSE(selector->conflictWithAny(w, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(x, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(y, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(z, 3, existing));
}

} // namespace RAMCloud
