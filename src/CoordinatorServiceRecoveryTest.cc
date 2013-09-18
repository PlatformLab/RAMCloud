/* Copyright (c) 2010-2012 Stanford University
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

#include "CoordinatorServiceRecovery.h"
#include "MockCluster.h"

namespace RAMCloud {

class CoordinatorServiceRecoveryTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    CoordinatorServiceRecovery* coordRecovery;
    LogCabinHelper* logCabinHelper;

    CoordinatorServiceRecoveryTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , coordRecovery()
        , logCabinHelper()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        coordRecovery = &cluster.coordinator.get()->coordinatorRecovery;
        logCabinHelper = coordRecovery->service.logCabinHelper.get();
    }

    ~CoordinatorServiceRecoveryTest() {
        // Finish all pending ServerList updates before destroying cluster.
        cluster.syncCoordinatorServerList();
    }

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServiceRecoveryTest);
};

namespace {
bool replayFilter(string s) {
    return s == "replay";
}
}

// This test seems somewhat stupid since it is testing a simple dispatch.
// But why not? We could remove it at some point.
TEST_F(CoordinatorServiceRecoveryTest, replay_basic) {
    ProtoBuf::ServerInformation serverInfo;
    serverInfo.set_entry_type("ServerUp");
    serverInfo.set_server_id(ServerId(1, 0).getId());
    serverInfo.set_service_mask(
            ServiceMask({WireFormat::MASTER_SERVICE}).serialize());
    serverInfo.set_read_speed(0);
    serverInfo.set_service_locator("mock:host=master");
    logCabinHelper->appendProtoBuf(
                coordRecovery->service.expectedEntryId, serverInfo);

    ProtoBuf::ServerCrashInfo serverCrashed;
    serverCrashed.set_entry_type("ServerCrashed");
    serverCrashed.set_server_id(ServerId(1, 0).getId());
    logCabinHelper->appendProtoBuf(
                coordRecovery->service.expectedEntryId, serverCrashed);

    ProtoBuf::ServerUpdate serverUpdate;
    serverUpdate.set_entry_type("ServerUpdate");
    serverUpdate.set_server_id(ServerId(1, 0).getId());
    logCabinHelper->appendProtoBuf(
                coordRecovery->service.expectedEntryId, serverUpdate);

    ProtoBuf::ServerReplicationUpdate serverReplicationUpdate;
    serverReplicationUpdate.set_entry_type("ServerReplicationUpdate");
    serverReplicationUpdate.set_server_id(ServerId(1, 0).getId());
    serverReplicationUpdate.set_replication_id(10lu);
    logCabinHelper->appendProtoBuf(
                coordRecovery->service.expectedEntryId,
                serverReplicationUpdate);

    TestLog::Enable _(replayFilter);
    coordRecovery->replay(true);
    EXPECT_EQ("replay: Entry Id: 0, Entry Type: ServerUp\n | "
              "replay: Entry Id: 1, Entry Type: ServerCrashed\n | "
              "replay: Entry Id: 2, Entry Type: ServerUpdate\n | "
              "replay: Entry Id: 3, Entry Type: ServerReplicationUpdate\n",
              TestLog::get());
}

}  // namespace RAMCloud
