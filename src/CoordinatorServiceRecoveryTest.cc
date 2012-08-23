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
    Context context;
    MockCluster cluster;
    CoordinatorServiceRecovery* coordRecovery;
    LogCabinHelper* logCabinHelper;

    CoordinatorServiceRecoveryTest()
        : context()
        , cluster(context)
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

TEST_F(CoordinatorServiceRecoveryTest, replay_basic) {
    ProtoBuf::ServerInformation serverInfo;
    serverInfo.set_entry_type("ServerEnlisting");
    serverInfo.set_server_id(ServerId(1, 0).getId());
    serverInfo.set_service_mask(
            ServiceMask({WireFormat::MASTER_SERVICE}).serialize());
    serverInfo.set_read_speed(0);
    serverInfo.set_write_speed(0);
    serverInfo.set_service_locator("mock:host=master");
    logCabinHelper->appendProtoBuf(serverInfo);

    serverInfo.set_entry_type("ServerEnlisted");
    logCabinHelper->appendProtoBuf(serverInfo);

    ProtoBuf::ServerUpdate serverUpdate;
    serverUpdate.set_entry_type("ServerUpdate");
    serverUpdate.set_server_id(ServerId(1, 0).getId());
    serverUpdate.set_min_open_segment_id(10);
    logCabinHelper->appendProtoBuf(serverUpdate);

    ProtoBuf::StateServerDown stateServerDown;
    stateServerDown.set_entry_type("StateServerDown");
    stateServerDown.set_server_id(ServerId(1, 0).getId());
    logCabinHelper->appendProtoBuf(stateServerDown);

    TestLog::Enable _(replayFilter);
    coordRecovery->replay(true);
    EXPECT_EQ("replay: 0 - ServerEnlisting\n | "
              "replay: 1 - ServerEnlisted\n | "
              "replay: 2 - ServerUpdate\n | "
              "replay: 3 - StateServerDown\n",
              TestLog::get());
}

TEST_F(CoordinatorServiceRecoveryTest, replay_error) {
    ProtoBuf::EntryType dummyEntry;
    dummyEntry.set_entry_type("UnrecognizedEntryType");
    logCabinHelper->appendProtoBuf(dummyEntry);

    TestLog::Enable _(replayFilter);
    EXPECT_THROW(coordRecovery->replay(),
                 CoordinatorServiceRecovery::UnexpectedEntryTypeException);
    EXPECT_EQ("replay: 0 - UnrecognizedEntryType\n",
              TestLog::get());
}

}  // namespace RAMCloud
