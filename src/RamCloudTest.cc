/* Copyright (c) 2011 Stanford University
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
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "MasterService.h"
#include "TransportManager.h"
#include "BindTransport.h"
#include "RamCloud.h"

namespace RAMCloud {

class RamCloudTest : public ::testing::Test {
  public:
    BindTransport transport;
    CoordinatorService coordinatorService;
    CoordinatorClient* coordinatorClient1;
    CoordinatorClient* coordinatorClient2;
    ServerConfig masterConfig1;
    ServerConfig masterConfig2;
    MasterService* master1;
    MasterService* master2;
    RamCloud* ramcloud;

  public:
    RamCloudTest()
        : transport()
        , coordinatorService()
        , coordinatorClient1(NULL)
        , coordinatorClient2(NULL)
        , masterConfig1()
        , masterConfig2()
        , master1(NULL)
        , master2(NULL)
        , ramcloud(NULL)
    {
        masterConfig1.coordinatorLocator = "mock:host=coordinatorService";
        masterConfig1.localLocator = "mock:host=master1";
        MasterService::sizeLogAndHashTable("64", "8", &masterConfig1);
        masterConfig2.coordinatorLocator = "mock:host=coordinatorService";
        masterConfig2.localLocator = "mock:host=master2";
        MasterService::sizeLogAndHashTable("64", "8", &masterConfig2);

        transportManager.registerMock(&transport);
        transport.addService(coordinatorService,
                              "mock:host=coordinatorService");

        coordinatorClient1 = new CoordinatorClient(
                             "mock:host=coordinatorService");
        master1 = new MasterService(masterConfig1, coordinatorClient1, 0);
        transport.addService(*master1, "mock:host=master1");
        master1->serverId.construct(
            coordinatorClient1->enlistServer(MASTER,
                                             masterConfig1.localLocator));

        coordinatorClient2 = new CoordinatorClient(
                             "mock:host=coordinatorService");
        master2 = new MasterService(masterConfig2, coordinatorClient2, 0);
        transport.addService(*master2, "mock:host=master2");
        master2->serverId.construct(
            coordinatorClient2->enlistServer(MASTER,
                                             masterConfig2.localLocator));

        ramcloud = new RamCloud("mock:host=coordinatorService");
        ramcloud->createTable("table1");
        ramcloud->createTable("table2");
        TestLog::enable();
    }

    ~RamCloudTest()
    {
        TestLog::disable();
        delete ramcloud;
        delete master1;
        delete master2;
        delete coordinatorClient1;
        delete coordinatorClient2;
        transportManager.unregisterMock();
    }

    DISALLOW_COPY_AND_ASSIGN(RamCloudTest);
};

TEST_F(RamCloudTest, multiRead) {
    // Create objects to be read later
    uint32_t tableId1 = ramcloud->openTable("table1");
    uint64_t version1;
    ramcloud->create(tableId1, "firstVal", 8, &version1, false);

    uint32_t tableId2 = ramcloud->openTable("table2");
    uint64_t version2;
    ramcloud->create(tableId2, "secondVal", 9, &version2, false);
    uint64_t version3;
    ramcloud->create(tableId2, "thirdVal", 8, &version3, false);

    // Create requests and read
    MasterClient::ReadObject* requests[3];

    Tub<Buffer> readValue1;
    MasterClient::ReadObject request1(tableId1, 0, &readValue1);
    request1.status = STATUS_RETRY;
    requests[0] = &request1;

    Tub<Buffer> readValue2;
    MasterClient::ReadObject request2(tableId2, 0, &readValue2);
    request2.status = STATUS_RETRY;
    requests[1] = &request2;

    Tub<Buffer> readValue3;
    MasterClient::ReadObject request3(tableId2, 1, &readValue3);
    request3.status = STATUS_RETRY;
    requests[2] = &request3;

    ramcloud->multiRead(requests, 3);

    EXPECT_STREQ("STATUS_OK", statusToSymbol(request1.status));
    EXPECT_EQ(1U, request1.version);
    EXPECT_EQ("firstVal", toString(readValue1.get()));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(request2.status));
    EXPECT_EQ(1U, request2.version);
    EXPECT_EQ("secondVal", toString(readValue2.get()));
    EXPECT_STREQ("STATUS_OK", statusToSymbol(request3.status));
    EXPECT_EQ(2U, request3.version);
    EXPECT_EQ("thirdVal", toString(readValue3.get()));
}

}  // namespace RAMCloud
