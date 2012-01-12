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

#include "TestUtil.h"
#include "Common.h"
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "BackupClient.h"
#include "BindTransport.h"
#include "ServiceMask.h"
#include "ShortMacros.h"

namespace RAMCloud {

struct BackupSelectorTest : public ::testing::Test {
    Tub<BindTransport> transport;
    Tub<TransportManager::MockRegistrar> mockRegistrar;
    Tub<CoordinatorService> coordinatorService;
    Tub<CoordinatorClient> coordinator;
    Tub<BackupSelector> selector;

    BackupSelectorTest()
        : transport()
        , mockRegistrar()
        , coordinatorService()
        , coordinator()
        , selector()
    {
        transport.construct();
        mockRegistrar.construct(*transport);

        coordinatorService.construct();
        transport->addService(*coordinatorService,
                              "mock:host=coordinator", COORDINATOR_SERVICE);

        coordinator.construct("mock:host=coordinator");
        selector.construct(coordinator.get());
    }

    char lastChar(const string& s) {
        return *(s.end() - 1);
    }

    string condenseBackups(uint32_t numBackups,
                           BackupSelector::Backup* backups[]) {
        string r;
        for (uint32_t i = 0; i < numBackups; ++i) {
            if (backups[i] == NULL)
                r.push_back('x');
            else
                r.push_back(lastChar(backups[i]->service_locator()));
        }
        return r;
    }

    string randomRound() {
        BackupSelector::Backup* randomBackups[] = {
            selector->getRandomHost(),
            selector->getRandomHost(),
            selector->getRandomHost(),
        };
        return condenseBackups(3, randomBackups);
    }

    void addEqualHosts(Tub<CoordinatorClient>& coordinator,
                       std::vector<ServerId>& ids) {
        ServiceMask b{BACKUP_SERVICE};
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup1", 100));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup2", 100));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup3", 100));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup4", 100));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup5", 100));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup6", 100));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup7", 100));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup8", 100));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup9", 100));
    }

    void addDifferentHosts(Tub<CoordinatorClient>& coordinator,
                           std::vector<ServerId>& ids) {
        ServiceMask b{BACKUP_SERVICE};
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup1", 10));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup2", 20));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup3", 30));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup4", 40));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup5", 50));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup6", 60));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup7", 70));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup8", 80));
        ids.push_back(coordinator->enlistServer(b, "mock:host=backup9", 90));
    }
};

TEST_F(BackupSelectorTest, selectPrimaryNoHosts) {
    selector->updateHostListThrower.tillThrow = 10;
    EXPECT_THROW(selector->selectPrimary(0, NULL),
                 TestingException);
}

TEST_F(BackupSelectorTest, selectPrimaryAllConflict) {
    MockRandom _(1); // getRandomHost will return: 2 4 6 8 5 1 3 7 9
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    selector->updateHostListThrower.tillThrow = 10;
    EXPECT_THROW(selector->selectPrimary(9, &ids[0]), TestingException);
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqual) {
    MockRandom _(1); // getRandomHost will return: 2 4 6 8 5 1 3 7 9
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    BackupSelector::Backup* backup = selector->selectPrimary(0, NULL); // use 2
    EXPECT_EQ(*ids[1], backup->server_id());
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqualOneConstraint) {
    MockRandom _(1); // getRandomHost will return: 2 4 6 8 5 1 3 7 9
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    BackupSelector::Backup* backup =
        selector->selectPrimary(1, &ids[1]); // must skip 2, use 4
    EXPECT_EQ(*ids[3], backup->server_id());
}

TEST_F(BackupSelectorTest, selectPrimaryAllEqualThreeConstraints) {
    MockRandom _(1); // getRandomHost will return: 2 4 6 8 5 1 3 7 9
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    BackupSelector::Backup* backup =
        selector->selectPrimary(3, &ids[1]); // must skip 2, 4, use 6
    EXPECT_EQ(*ids[5], backup->server_id());
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeeds) {
    MockRandom _(1); // getRandomHost will return: 2 4 6 8 5 1 3 7 9
    std::vector<ServerId> ids;
    addDifferentHosts(coordinator, ids);

    BackupSelector::Backup* backup = selector->selectPrimary(0, NULL);
    EXPECT_EQ(*ids[7], backup->server_id());
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeedsOneConstraint) {
    MockRandom _(1); // getRandomHost will return: 2 4 6 8 5 1 3 7 9
    std::vector<ServerId> ids;
    addDifferentHosts(coordinator, ids);

    BackupSelector::Backup* backup = selector->selectPrimary(1, &ids[7]);
    EXPECT_EQ(*ids[5], backup->server_id());
}

TEST_F(BackupSelectorTest, selectPrimaryDifferentSpeedsFourConstraints) {
    MockRandom _(1); // getRandomHost will return: 2 4 6 8 5 1 3 7 9
    std::vector<ServerId> ids;
    addDifferentHosts(coordinator, ids);

    BackupSelector::Backup* backup = selector->selectPrimary(4, &ids[4]);
    EXPECT_EQ(*ids[8], backup->server_id());
}

TEST_F(BackupSelectorTest, selectPrimaryEvenPrimaryPlacement) {
    std::vector<ServerId> ids;
    addEqualHosts(coordinator, ids);

    uint32_t primaryCounts[9] = {};
    for (uint32_t i = 0; i < 900; ++i) {
        BackupSelector::Backup* primary = selector->selectPrimary(0, NULL);
        ++primaryCounts[lastChar(primary->service_locator()) - '1'];
    }
    EXPECT_LT(*std::max_element(primaryCounts, primaryCounts + 9) -
              *std::min_element(primaryCounts, primaryCounts + 9),
              5U); // range < 5 won't fail often
}

TEST_F(BackupSelectorTest, selectSecondary) {
    selector->updateHostListThrower.tillThrow = 10;
    EXPECT_THROW(selector->selectSecondary(0, NULL), TestingException);

    coordinator->enlistServer({BACKUP_SERVICE}, "mock:host=backup1");
    selector->updateHostListFromCoordinator();
    EXPECT_EQ(selector->hosts.mutable_server(0),
              selector->selectSecondary(0, NULL));

    const ServerId conflicts[] = {
        ServerId(selector->hosts.server(0).server_id())
    };
    selector->updateHostListThrower.tillThrow = 10;
    EXPECT_THROW(selector->selectSecondary(1, conflicts), TestingException);
}

TEST_F(BackupSelectorTest, getRandomHost) {
    coordinator->enlistServer({BACKUP_SERVICE}, "mock:host=backup1");
    coordinator->enlistServer({BACKUP_SERVICE}, "mock:host=backup2");
    coordinator->enlistServer({BACKUP_SERVICE}, "mock:host=backup3");
    selector->updateHostListFromCoordinator();
    MockRandom _(1);
    EXPECT_EQ("213", randomRound());
    EXPECT_EQ("132", randomRound());
    EXPECT_EQ("312", randomRound());
}

TEST_F(BackupSelectorTest, conflict) {
    BackupSelector::Backup backup;
    const ServerId conflictingId(backup.server_id());
    const ServerId nonConflictingId(*conflictingId + 1);
    EXPECT_FALSE(selector->conflict(&backup, nonConflictingId));
    EXPECT_TRUE(selector->conflict(&backup, conflictingId));
}

TEST_F(BackupSelectorTest, conflictWithAny) {
    BackupSelector::Backup w, x, y, z;
    x.set_server_id(1);
    y.set_server_id(2);
    z.set_server_id(3);
    const ServerId existing[] = {
        ServerId(1, 0),
        ServerId(2, 0),
        ServerId(3, 0)
    };
    EXPECT_FALSE(selector->conflictWithAny(&w, 0, NULL));
    EXPECT_FALSE(selector->conflictWithAny(&w, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(&x, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(&y, 3, existing));
    EXPECT_TRUE(selector->conflictWithAny(&z, 3, existing));
}

TEST_F(BackupSelectorTest, updateHostListFromCoordinator) {
    selector->updateHostListFromCoordinator();
    EXPECT_EQ(0, selector->hosts.server_size());
    EXPECT_EQ(0U, selector->hostsOrder.size());
    EXPECT_EQ(0U, selector->numUsedHosts);

    coordinator->enlistServer({BACKUP_SERVICE}, "mock:host=backup1");
    coordinator->enlistServer({BACKUP_SERVICE}, "mock:host=backup2");
    coordinator->enlistServer({BACKUP_SERVICE}, "mock:host=backup3");
    selector->updateHostListFromCoordinator();
    EXPECT_EQ(3, selector->hosts.server_size());
    EXPECT_EQ(3u, selector->hostsOrder.size());
    EXPECT_EQ(0U, selector->hostsOrder[0]);
    EXPECT_EQ(1U, selector->hostsOrder[1]);
    EXPECT_EQ(2U, selector->hostsOrder[2]);
    EXPECT_EQ(0U, selector->numUsedHosts);
}

} // namespace RAMCloud
