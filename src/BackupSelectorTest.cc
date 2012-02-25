/* Copyright (c) 2009-2011 Stanford University
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
    MockCluster cluster;
    CoordinatorClient* coordinator;
    BackupSelector* selector;

    BackupSelectorTest()
        : cluster()
        , coordinator()
        , selector()
    {
        Context::get().logger->setLogLevels(SILENT_LOG_LEVEL);

        coordinator = cluster.getCoordinatorClient();

        ServerConfig config = ServerConfig::forTesting();
        config.services = {MASTER_SERVICE, MEMBERSHIP_SERVICE};
        Server* server = cluster.addServer(config);
        selector = &server->master->replicaManager.backupSelector;
    }

    char lastChar(const string& s) {
        return *(s.end() - 1);
    }

    void addEqualHosts(CoordinatorClient* coordinator,
                       std::vector<ServerId>& ids) {
        ServiceMask b{BACKUP_SERVICE};
        auto& c = coordinator;
        ids.push_back(c->enlistServer({}, b, "mock:host=backup1", 100));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup2", 100));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup3", 100));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup4", 100));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup5", 100));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup6", 100));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup7", 100));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup8", 100));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup9", 100));
    }

    void addDifferentHosts(CoordinatorClient* coordinator,
                           std::vector<ServerId>& ids) {
        ServiceMask b{BACKUP_SERVICE};
        auto& c = coordinator;
        ids.push_back(c->enlistServer({}, b, "mock:host=backup1", 10));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup2", 20));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup3", 30));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup4", 40));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup5", 50));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup6", 60));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup7", 70));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup8", 80));
        ids.push_back(c->enlistServer({}, b, "mock:host=backup9", 90));
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
    explicit BackgroundEnlistBackup(Context* context,
                                    CoordinatorClient* coordinator)
        : context(context), coordinator(coordinator) {}

    void operator()() {
        Context::Guard _(*context);
        // Try to get selectPrimary to block on the empty server list.
        std::this_thread::yield();
        usleep(1 * 1000);
        // See if enlisting a server unblocks the call.
        coordinator->enlistServer({}, {BACKUP_SERVICE},
                                  "mock:host=backup10", 10);
    }

    Context* context;
    CoordinatorClient* const coordinator;

    DISALLOW_COPY_AND_ASSIGN(BackgroundEnlistBackup);
};

TEST_F(BackupSelectorTest, selectPrimaryNoHosts) {
    // Check to make sure the server waits on server list updates from the
    // coordinator.
    BackgroundEnlistBackup enlist{&Context::get(), coordinator};
    std::thread enlistThread{std::ref(enlist)};
    ServerId id = selector->selectPrimary(0, NULL);
    EXPECT_EQ(ServerId(2, 0), id);
    enlistThread.join();
}

TEST_F(BackupSelectorTest, selectPrimaryAllConflict) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    BackgroundEnlistBackup enlist{&Context::get(), coordinator};
    std::thread enlistThread{std::ref(enlist)};

    ServerId id = selector->selectPrimary(9, &ids[0]);
    EXPECT_EQ(ServerId(11, 0), id);
    enlistThread.join();
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqual) {
    MockRandom _(1);
    // getRandomServerIdWithServer(BACKUP_SERVICE) returns backups in order
    // that they enlisted above.
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    ServerId backup = selector->selectPrimary(0, NULL); // use backup1 / id 2
    EXPECT_EQ(ids[0], backup);
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqualOneConstraint) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    ServerId backup =
        selector->selectPrimary(1, &ids[0]); // must skip backup1, use backup2
    EXPECT_EQ(ids[1], backup);
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqualThreeConstraints) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    ServerId backup =
        selector->selectPrimary(3, &ids[0]); // must skip backup1/2/3, use 4
    EXPECT_EQ(ids[3], backup);
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqualNonContiguousConstraints) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    std::vector<ServerId> conflicts = { ids[0], ids[1], ids[3] };
    ServerId backup =
        selector->selectPrimary(3, &conflicts[0]); // skip backup1/2/4, use 3
    EXPECT_EQ(ids[2], backup);
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeeds) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(coordinator, ids);

    ServerId backup = selector->selectPrimary(0, NULL);
    EXPECT_EQ(ids[4], backup); // Of the 5 inspected the last is the fastest.
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeedsOneConstraint) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(coordinator, ids);

    ServerId backup = selector->selectPrimary(1, &ids[4]);
    EXPECT_EQ(ids[5], backup); // 5 inspected, but must retry on the last
                               // because it's marked as a conflict, so
                               // the sixth server gets selected.
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeedsFourConstraints) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(coordinator, ids);

    ServerId backup = selector->selectPrimary(4, &ids[4]);
    EXPECT_EQ(ids[8], backup); // First 4 are slow, next 4 conflict,
                               // choose the ninth.
}

TEST_F(BackupSelectorTest, selectPrimaryEvenPrimaryPlacement) {
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

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
    addDifferentHosts(coordinator, ids);

    ServerId id = selector->selectSecondary(0, NULL);
    EXPECT_EQ(ServerId(2, 0), id);

    const ServerId conflicts[] = { ids[1] };

    id = selector->selectSecondary(1, &conflicts[0]);
    EXPECT_EQ(ServerId(4, 0), id);
}

#if 0
// This test should run forever, hence why it is commented out.
// Occasionally, when self-doubt mounts, it is worth running, though.
TEST_F(BackupSelectorTest, selectSecondaryConflictsWithAll) {
    MockRandom _(1);
    std::vector<ServerId> ids;
    addDifferentHosts(coordinator, ids);

    ServerId id = selector->selectSecondary(uint32_t(ids.size()), &ids[0]);
    EXPECT_EQ(ServerId(0, 0), id);
}
#endif

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

} // namespace RAMCloud
