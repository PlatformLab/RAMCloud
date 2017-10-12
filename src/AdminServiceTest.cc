/* Copyright (c) 2011-2016 Stanford University
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

#include<fstream>
#include "TestUtil.h"
#include "Common.h"
#include "AdminClient.h"
#include "AdminService.h"
#include "BindTransport.h"
#include "CacheTrace.h"
#include "CoordinatorClient.h"
#include "CoordinatorServerList.h"
#include "CoordinatorService.h"
#include "FailSession.h"
#include "Key.h"
#include "MasterService.h"
#include "MockExternalStorage.h"
#include "RamCloud.h"
#include "RawMetrics.h"
#include "ServerList.h"
#include "ServerMetrics.h"
#include "Tablets.pb.h"
#include "TimeTrace.h"
#include "TransportManager.h"
#include "Tub.h"

// Note: this file tests both AdminService.cc and AdminClient.cc.

namespace RAMCloud {

// This class is designed to substitute class TebletMapFetcher
// in ObjectFinder so that the client can locate the proper server
// that owns a table without the need of going through the
// coordinator. We use this class, since we don't have cluster in
// this test.
class MockTableConfigFetcher : public ObjectFinder::TableConfigFetcher {
  public:

    /**
    * Constructor.
    *
    * \param tableId
    *      tableId that the server holds
    * \param locator
    *      stringLocator of the server that holds the table
    */
    explicit MockTableConfigFetcher(string locator, uint64_t tableId)
        : locator(locator)
        , tableId(tableId)
    {}
    bool tryGetTableConfig(
            uint64_t tableId,
            std::map<TabletKey, TabletWithLocator>* tableMap,
            std::multimap< std::pair<uint64_t, uint8_t>,
                                     IndexletWithLocator>* tableIndexMap) {

        tableMap->clear();
        Tablet rawEntry({tableId, 0, uint64_t(~0), ServerId(),
                    Tablet::NORMAL, LogPosition()});
        TabletWithLocator entry(rawEntry, locator);

        TabletKey key {entry.tablet.tableId, entry.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key, entry));
        return true;
    }
    string locator;
    uint64_t tableId;

};


class AdminServiceTest : public ::testing::Test {
  public:
    Context context;
    ServerList serverList;
    ServerConfig serverConfig;
    BindTransport transport;
    TransportManager::MockRegistrar mockRegistrar;
    AdminService adminService;
    ServerId serverId;
    Tub<RamCloud> ramcloud;
    TestLog::Enable logSilencer;
    Tub<ServerConfig> masterConfig;
    Tub<MasterService> masterService;
    std::mutex mutex;
    MockExternalStorage storage;

    AdminServiceTest()
        : context()
        , serverList(&context)
        , serverConfig(ServerConfig::forTesting())
        , transport(&context)
        , mockRegistrar(&context, transport)
        , adminService(&context, &serverList, &serverConfig)
        , serverId(1, 3)
        , ramcloud()
        , logSilencer()
        , masterConfig()
        , masterService()
        , mutex()
        , storage(true)
    {
        transport.registerServer(&context, "mock:host=ping");
        serverList.testingAdd({serverId, "mock:host=ping",
                               {WireFormat::ADMIN_SERVICE}, 100,
                               ServerStatus::UP});
        ramcloud.construct(&context, "mock:host=coordinator");
    }

    void
    addMasterService() {
        masterConfig = ServerConfig::forTesting();
        masterConfig->services = {WireFormat::MASTER_SERVICE,
                                  WireFormat::ADMIN_SERVICE};
        masterService.construct(adminService.context, masterConfig.get());
        masterService->setServerId(serverId);
    }

    void constructUnimplementedServerControlRpc(
            Buffer* reqBuf,
            WireFormat::ServerControl::Request* reqHdr,
            WireFormat::ServerControl::ServerControlType type,
            bool appendKey) {
        // Defaults
        reqHdr->common.opcode = WireFormat::SERVER_CONTROL;
        reqHdr->common.service = WireFormat::ADMIN_SERVICE;
        reqHdr->type = type;
        reqHdr->controlOp = WireFormat::ControlOp(0);

        switch (type) {
            case WireFormat::ServerControl::OBJECT:
            {
                reqHdr->tableId = 1;
                reqHdr->keyLength = 1;
                if (appendKey) reqBuf->appendCopy("0", 1);
                break;
            }
            case WireFormat::ServerControl::INDEX:
            {
                reqHdr->tableId = 1;
                reqHdr->indexId = 2;
                reqHdr->keyLength = 1;
                if (appendKey) reqBuf->appendCopy("A", 1);
                break;
            }
            default:
                break;
        }
    }

    typedef std::unique_lock<std::mutex> Lock;
    DISALLOW_COPY_AND_ASSIGN(AdminServiceTest);
};

TEST_F(AdminServiceTest, getServerId) {
    adminService.setServerId(ServerId(3, 5));
    Transport::SessionRef session =
            context.transportManager->openSession("mock:host=ping");
    ServerId id = AdminClient::getServerId(&context, session);
    EXPECT_EQ("3.5", id.toString());
}

TEST_F(AdminServiceTest, getServerId_transportError) {
    string exceptionMessage("no exception");
    try {
        AdminClient::getServerId(&context, FailSession::get());
    } catch (TransportException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("getServerId RPC failed", exceptionMessage);
}

TEST_F(AdminServiceTest, ping_basics) {
    AdminClient::ping(&context, serverId);
    EXPECT_EQ("", TestLog::get());
    TestLog::reset();
    AdminClient::ping(&context, serverId, serverId);
    EXPECT_EQ("ping: Received ping request from server 1.3", TestLog::get());
    TestLog::reset();
    EXPECT_THROW(AdminClient::ping(&context, serverId, ServerId(99)),
            CallerNotInClusterException);
    EXPECT_EQ("ping: Received ping request from server 99.0 | "
            "ping: Received ping from server not in cluster: 99.0",
            TestLog::get());
}

TEST_F(AdminServiceTest, ping_wait_timeout) {
    ServerId serverId2(2, 3);
    MockTransport mockTransport(&context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    serverList.testingAdd({serverId2, "mock2:", {WireFormat::ADMIN_SERVICE},
                           100, ServerStatus::UP});
    PingRpc rpc(&context, serverId2);
    uint64_t start = Cycles::rdtsc();
    EXPECT_FALSE(rpc.wait(1000000));
    EXPECT_EQ("wait: timeout", TestLog::get());
    double elapsedMicros = 1e06* Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsedMicros, 1000.0);
    EXPECT_LE(elapsedMicros, 2000.0);
}

// Helper function that runs in a separate thread for the following test.
static void pingThread(PingRpc* rpc, bool* result) {
    *result = rpc->wait(100000000);
}

TEST_F(AdminServiceTest, ping_wait_serverGoesAway) {
    ServerId serverId2(2, 3);
    MockTransport mockTransport(&context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    serverList.testingAdd({serverId2, "mock2:", {WireFormat::ADMIN_SERVICE},
                           100, ServerStatus::UP});
    bool result = 0;
    PingRpc rpc(&context, serverId2);
    std::thread thread(pingThread, &rpc, &result);
    usleep(100);
    EXPECT_EQ(0LU, result);

    // Delete the server, then fail the ping RPC so that there is a retry
    // that discovers that targetId is gone.
    serverList.testingRemove(serverId2);
    mockTransport.lastNotifier->failed();

    // Give the other thread a chance to finish.
    for (int i = 0; (result == 0) && (i < 1000); i++) {
        usleep(100);
    }

    EXPECT_FALSE(result);
    EXPECT_EQ("wait: server doesn't exist", TestLog::get());
    thread.join();
}

TEST_F(AdminServiceTest, ping_wait_exception) {
    PingRpc rpc(&context, serverId, ServerId(99));
    EXPECT_THROW(rpc.wait(100000), CallerNotInClusterException);
}

TEST_F(AdminServiceTest, ping_wait_success) {
    PingRpc rpc(&context, serverId);
    EXPECT_TRUE(rpc.wait(100000));
}

TEST_F(AdminServiceTest, proxyPing_basics) {
    uint64_t ns = AdminClient::proxyPing(&context, serverId, serverId, 100000);
    EXPECT_NE(-1U, ns);
    EXPECT_LT(10U, ns);
}
TEST_F(AdminServiceTest, proxyPing_timeout) {
    // Test the situation where the target times out.
    ServerId targetId(2, 3);
    MockTransport mockTransport(&context);
    context.transportManager->registerMock(&mockTransport, "mock2");
    serverList.testingAdd({targetId, "mock2:", {WireFormat::ADMIN_SERVICE}, 100,
                           ServerStatus::UP});
    uint64_t start = Cycles::rdtsc();
    EXPECT_EQ(0xffffffffffffffffU,
              AdminClient::proxyPing(&context, serverId, targetId, 1000000));
    double elapsedMicros = 1e06* Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsedMicros, 1000.0);
    EXPECT_LE(elapsedMicros, 2000.0);
}

TEST_F(AdminServiceTest, serverControl_ObjectServerControl_Basic) {
    // Everything works EXPECT STATUS_UNIMPLEMENTED_REQUEST
    AdminServiceTest::addMasterService();
    context.getMasterService()->tabletManager.addTablet(
            1, 0, ~0UL, TabletManager::NORMAL);

    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::OBJECT, true);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNIMPLEMENTED_REQUEST, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_ObjectServerControl_NoService) {
    // NO Master Service EXPECT STATUS_UNKNOWN_TABLET
    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::OBJECT, true);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNKNOWN_TABLET, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_ObjectServerControl_BadKey) {
    // Missing key EXPECT STATUS_REQUEST_FORMAT_ERROR
    AdminServiceTest::addMasterService();

    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::OBJECT, false);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_REQUEST_FORMAT_ERROR, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_ObjectServerControl_NoTablet) {
    // Object Not Found EXPECT STATUS_UNKNOWN_TABLET
    AdminServiceTest::addMasterService();

    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::OBJECT, true);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNKNOWN_TABLET, respHdr->common.status);

    context.getMasterService()->tabletManager.addTablet(
            1, 0, ~0UL, TabletManager::NORMAL);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNIMPLEMENTED_REQUEST, respHdr->common.status);

    context.getMasterService()->tabletManager.changeState(
            1, 0, ~0UL, TabletManager::NORMAL, TabletManager::NOT_READY);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNKNOWN_TABLET, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_IndexServerControl_Basic) {
    // Everything works EXPECT STATUS_UNIMPLEMENTED_REQUEST
    AdminServiceTest::addMasterService();
    context.getMasterService()->tabletManager.addTablet(
            9, 0, ~0UL, TabletManager::NORMAL);
    context.getMasterService()->indexletManager.addIndexlet(
            1, 2, 9, "A", 1, "B", 1);

    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::INDEX, true);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNIMPLEMENTED_REQUEST, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_IndexServerControl_NoService) {
    // NO Master Service EXPECT STATUS_UNKNOWN_INDEXLET
    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::INDEX, true);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNKNOWN_INDEXLET, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_IndexServerControl_BadKey) {
    // Missing key EXPECT STATUS_REQUEST_FORMAT_ERROR
        AdminServiceTest::addMasterService();
    context.getMasterService()->tabletManager.addTablet(
            9, 0, ~0UL, TabletManager::NORMAL);
    context.getMasterService()->indexletManager.addIndexlet(
            1, 2, 9, "A", 1, "B", 1);

    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::INDEX, false);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_REQUEST_FORMAT_ERROR, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_IndexServerControl_NoIndexlet) {
    // Index Not Found EXPECT STATUS_UNKNOWN_INDEXLET}
    AdminServiceTest::addMasterService();
    context.getMasterService()->tabletManager.addTablet(
            9, 0, ~0UL, TabletManager::NORMAL);

    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::INDEX, true);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNKNOWN_INDEXLET, respHdr->common.status);

    context.getMasterService()->indexletManager.addIndexlet(
            1, 2, 9, "A", 1, "B", 1);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNIMPLEMENTED_REQUEST, respHdr->common.status);

    context.getMasterService()->indexletManager.deleteIndexlet(1, 2, "A",
            1, "B", 1);

    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNKNOWN_INDEXLET, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_ServerControl) {
    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    // ServerControl performs no check EXPECT STATUS_UNIMPLEMENTED_REQUEST
    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::SERVER_ID, true);
    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_UNIMPLEMENTED_REQUEST, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_Invalid) {
    Buffer reqBuf;
    Buffer respBuf;
    Service::Rpc rpc(NULL, &reqBuf, &respBuf);   // Fake RPC with no worker.
    WireFormat::ServerControl::Request* reqHdr =
                reqBuf.emplaceAppend<WireFormat::ServerControl::Request>();
    WireFormat::ServerControl::Response* respHdr =
                respBuf.emplaceAppend<WireFormat::ServerControl::Response>();

    // Invalid RPC type EXPECT STATUS_REQUEST_FORMAT_ERROR
    AdminServiceTest::constructUnimplementedServerControlRpc(
            &reqBuf, reqHdr, WireFormat::ServerControl::INVALID, true);
    adminService.serverControl(reqHdr, respHdr, &rpc);
    EXPECT_EQ(STATUS_REQUEST_FORMAT_ERROR, respHdr->common.status);
}

TEST_F(AdminServiceTest, serverControl_DipatchProfilerBasics) {
    Buffer output;
    uint64_t totalElements = 50000000;

    // Testing basics of operation START_DISPATCH_PROFILER.
    ASSERT_FALSE(adminService.context->dispatch->profilerFlag);
    AdminClient::serverControl(&context, serverId,
                              WireFormat::START_DISPATCH_PROFILER,
                              &totalElements, sizeof32(totalElements), &output);
    ASSERT_TRUE(adminService.context->dispatch->profilerFlag);
    ASSERT_EQ(totalElements,
                adminService.context->dispatch->totalElements);

    // Testing basics of operation STOP_DISPATCH_PROFILER.
    AdminClient::serverControl(&context, serverId,
                              WireFormat::STOP_DISPATCH_PROFILER, " ", 1,
                              &output);
    ASSERT_FALSE(adminService.context->dispatch->profilerFlag);

    // Testing basics of operation STOP_DISPATCH_PROFILER.
    AdminClient::serverControl(&context, serverId,
                              WireFormat::DUMP_DISPATCH_PROFILE,
                              "pollingTimesTestFile.txt", 25, &output);
    std::ifstream stream("pollingTimesTestFile.txt");
    EXPECT_FALSE(stream.fail());
    stream.close();
    remove("pollingTimesTestFile.txt");

    // Testing unimplemented ControlOp.
    EXPECT_THROW(AdminClient::serverControl(&context, serverId,
                                           WireFormat::ControlOp(0),
                                           "File.txt", 9, &output)
                 , UnimplementedRequestError);
}

TEST_F(AdminServiceTest, serverControl_DispatchProfilerExceptions) {
    Buffer output;
    uint32_t totalElements = 10000000;

    // Testing MessageTooShortError for ControlOp
    // WireFormat::START_DISPATCH_PROFILER
    EXPECT_THROW(AdminClient::serverControl(&context, serverId,
                            WireFormat::START_DISPATCH_PROFILER,
                            &totalElements, sizeof32(totalElements),
                            &output)
                , MessageTooShortError);

    // Testing RequestFormatError for ControlOp
    // WireFormat::DUMP_DISPATCH_PROFILER
    EXPECT_THROW(AdminClient::serverControl(&context, serverId,
                            WireFormat::DUMP_DISPATCH_PROFILE,
                            "pollingTimesTestFile.txt", 24, &output)
                , RequestFormatError);
    EXPECT_THROW(AdminClient::serverControl(&context, serverId,
                            WireFormat::DUMP_DISPATCH_PROFILE,
                            "FolderNotExisting/File.txt", 27, &output)
                , RequestFormatError);
}

TEST_F(AdminServiceTest, serverControl_getTimeTrace) {
    Buffer output;

    TimeTrace::reset();
    TimeTrace::record("sample");
    AdminClient::serverControl(&context, serverId, WireFormat::GET_TIME_TRACE,
            "abc", 3, &output);
    EXPECT_EQ("     0.0 ns (+   0.0 ns): sample",
            TestUtil::toString(&output));
}

TEST_F(AdminServiceTest, serverControl_logTimeTrace) {
    Buffer output;

    Cycles::mockTscValue = 100;
    Cycles::mockCyclesPerSec = 2000000000;
    TimeTrace::reset();
    TimeTrace::record("sample");
    AdminClient::serverControl(&context, serverId, WireFormat::LOG_TIME_TRACE,
                "abc", 3, &output);
    EXPECT_EQ("printInternal: Starting TSC 100, cyclesPerSec 2000000000 | "
            "printInternal:      0.0 ns (+   0.0 ns): sample",
            TestLog::get());
    Cycles::mockTscValue = 0;
    Cycles::mockCyclesPerSec = 0;
}

TEST_F(AdminServiceTest, serverControl_getCacheTrace) {
    Buffer output;

    Util::mockPmcValue = 1;
    context.cacheTrace->record("sample");
    AdminClient::serverControl(&context, serverId, WireFormat::GET_CACHE_TRACE,
            "abc", 3, &output);
    EXPECT_EQ("0 misses (+0 misses): sample",
            TestUtil::toString(&output));
    Util::mockPmcValue = 0;
}

TEST_F(AdminServiceTest, serverControl_logCacheTrace) {
    Buffer output;

    Util::mockPmcValue = 1;
    context.cacheTrace->record("sample");
    AdminClient::serverControl(&context, serverId, WireFormat::LOG_CACHE_TRACE,
                "abc", 3, &output);
    EXPECT_EQ("printInternal: 0 misses (+0 misses): sample",
            TestLog::get());
    Util::mockPmcValue = 0;
}

TEST_F(AdminServiceTest, serverControl_addLogMessage) {
    AdminClient::logMessage(&context, serverId, ERROR,
            "Test string to write to log %d, %s%c", 42, "extra string", '!');

    EXPECT_EQ("serverControl: Test string to write to log 42, extra string!",
            TestLog::get());
}

TEST_F(AdminServiceTest, serverControl_addLogMessage_bad) {
    Buffer output;
    const char* testStr = "Test string to write to log";

    // making bad serverControls by not using AdminClient::logMessage function

    // empty message
    EXPECT_THROW(AdminClient::serverControl(&context, serverId,
                WireFormat::LOG_MESSAGE, testStr, 0, &output), ClientException);

    // message with only 2 bytes (need at least sizeof(LogLevel) = 4 bytes)
    EXPECT_THROW(AdminClient::serverControl(&context, serverId,
                WireFormat::LOG_MESSAGE, testStr, 2, &output), ClientException);

    // message with no log level
    EXPECT_THROW(AdminClient::serverControl(&context, serverId,
                WireFormat::LOG_MESSAGE, testStr, (uint32_t) strlen(testStr),
                &output), ClientException);
}

TEST_F(AdminServiceTest, serverControl_resetMetrics) {
    Buffer output;

    TimeTrace::record("sample");
    AdminClient::serverControl(&context, serverId, WireFormat::RESET_METRICS);
    AdminClient::serverControl(&context, serverId, WireFormat::GET_TIME_TRACE,
            "abc", 3, &output);
    EXPECT_EQ("No time trace events to print", TestUtil::toString(&output));
}

TEST_F(AdminServiceTest, updateServerList_noServerList) {
    WireFormat::UpdateServerList::Response response;
    response.common.status = STATUS_OK;
    AdminService admin2(&context, NULL, NULL);
    admin2.updateServerList(NULL, &response, NULL);
    EXPECT_STREQ("invalid RPC request type",
            statusToString(response.common.status));
}

TEST_F(AdminServiceTest, updateServerList_single) {
    Lock lock(mutex); // Lock used to trick internal calls
    // Create a temporary coordinator server list (with its own context)
    // to use as a source for update information.
    Context context2;
    context2.externalStorage = &storage;
    CoordinatorService coordinatorService(&context2, 1000, true);
    CoordinatorServerList* source(context2.coordinatorServerList);
    source->haltUpdater();
    ServerId id1 = source->enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::ADMIN_SERVICE}, 0, 100, "mock:host=55");
    ServerId id2 = source->enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::ADMIN_SERVICE}, 0, 100, "mock:host=56");
    ServerId id3 = source->enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::ADMIN_SERVICE}, 0, 100, "mock:host=57");
    ProtoBuf::ServerList fullList;
    source->serialize(&fullList, {WireFormat::MASTER_SERVICE,
            WireFormat::BACKUP_SERVICE});

    CoordinatorServerList::UpdateServerListRpc
        rpc(&context, serverId, &fullList);
    rpc.send();
    rpc.waitAndCheckErrors();
    EXPECT_STREQ("mock:host=55", serverList.getLocator(id1).c_str());
    EXPECT_STREQ("mock:host=56", serverList.getLocator(id2).c_str());
    EXPECT_STREQ("mock:host=57", serverList.getLocator(id3).c_str());
    const WireFormat::UpdateServerList::Response* respHdr(
            rpc.getResponseHeader<WireFormat::UpdateServerList>());
    EXPECT_EQ(3lu, respHdr->currentVersion);
}

TEST_F(AdminServiceTest, updateServerList_multi) {
    Lock lock(mutex); // Lock used to trick internal calls
    // Create a temporary coordinator server list (with its own context)
    // to use as a source for update information.
    Context context2;
    context2.externalStorage = &storage;
    ProtoBuf::ServerList fullList, update2, update3;
    CoordinatorService coordinatorService(&context2, 1000, true);
    CoordinatorServerList* source(context2.coordinatorServerList);
    source->haltUpdater();

    // Full List v1
    ServerId id1 = source->enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::ADMIN_SERVICE}, 0, 100, "mock:host=55");
    source->serialize(&fullList, {WireFormat::MASTER_SERVICE,
            WireFormat::BACKUP_SERVICE});
    // Update v2
    ServerId id2 = source->enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::ADMIN_SERVICE}, 0, 100, "mock:host=56");
    EXPECT_EQ(2U, source->updates.size());
    update2 = source->updates.back().incremental;
    // Update v3
    ServerId id3 = source->enlistServer({WireFormat::MASTER_SERVICE,
            WireFormat::ADMIN_SERVICE}, 0, 100, "mock:host=57");
    update3 = source->updates.back().incremental;


    CoordinatorServerList::UpdateServerListRpc
        rpc(&context, serverId, &fullList);
    rpc.appendServerList(&update2);
    rpc.appendServerList(&update3);
    rpc.send();
    rpc.waitAndCheckErrors();
    EXPECT_STREQ("mock:host=55", serverList.getLocator(id1).c_str());
    EXPECT_STREQ("mock:host=56", serverList.getLocator(id2).c_str());
    EXPECT_STREQ("mock:host=57", serverList.getLocator(id3).c_str());
    const WireFormat::UpdateServerList::Response* respHdr(
            rpc.getResponseHeader<WireFormat::UpdateServerList>());
    EXPECT_EQ(3lu, respHdr->currentVersion);
}

} // namespace RAMCloud
