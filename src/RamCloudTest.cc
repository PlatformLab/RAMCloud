/* Copyright (c) 2010 Stanford University
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
#include "BackupManager.h"
#include "CoordinatorClient.h"
#include "CoordinatorServer.h"
#include "MasterServer.h"
#include "MockTransport.h"
#include "TransportManager.h"
#include "BindTransport.h"
#include "Recovery.h"
#include "RamCloud.h"

namespace RAMCloud {

class RamCloudTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(RamCloudTest);
    CPPUNIT_TEST(test_multiRead);
    CPPUNIT_TEST(test_multiRead_badTable);
    CPPUNIT_TEST(test_multiRead_noSuchObject);
    CPPUNIT_TEST_SUITE_END();

    BindTransport* transport;
    CoordinatorServer* coordinatorServer;
    CoordinatorClient* coordinatorClient1;
    CoordinatorClient* coordinatorClient2;
    ServerConfig masterConfig1;
    ServerConfig masterConfig2;
    MasterServer* master1;
    MasterServer* master2;
    RamCloud* ramcloud;

  public:
    RamCloudTest()
        : transport(NULL)
        , coordinatorServer(NULL)
        , coordinatorClient1(NULL)
        , coordinatorClient2(NULL)
        , masterConfig1()
        , masterConfig2()
        , master1(NULL)
        , master2(NULL)
        , ramcloud(NULL)
    {
        masterConfig1.coordinatorLocator = "mock:host=coordinatorServer";
        masterConfig1.localLocator = "mock:host=master1";
        MasterServer::sizeLogAndHashTable("64", "8", &masterConfig1);
        masterConfig2.coordinatorLocator = "mock:host=coordinatorServer";
        masterConfig2.localLocator = "mock:host=master2";
        MasterServer::sizeLogAndHashTable("64", "8", &masterConfig2);
    }

    void setUp() {
        transport = new BindTransport();
        transportManager.registerMock(transport);
        coordinatorServer = new CoordinatorServer();
        transport->addServer(*coordinatorServer, "mock:host=coordinatorServer");

        coordinatorClient1 = new CoordinatorClient(
                             "mock:host=coordinatorServer");
        master1 = new MasterServer(masterConfig1, coordinatorClient1, 0);
        transport->addServer(*master1, "mock:host=master1");
        master1->serverId.construct(
            coordinatorClient1->enlistServer(MASTER,
                                             masterConfig1.localLocator));

        coordinatorClient2 = new CoordinatorClient(
                             "mock:host=coordinatorServer");
        master2 = new MasterServer(masterConfig2, coordinatorClient2, 0);
        transport->addServer(*master2, "mock:host=master2");
        master2->serverId.construct(
            coordinatorClient2->enlistServer(MASTER,
                                             masterConfig2.localLocator));

        ramcloud = new RamCloud("mock:host=coordinatorServer");
        ramcloud->createTable("table1");
        ramcloud->createTable("table2");
        TestLog::enable();
    }

    void tearDown() {
        TestLog::disable();
        delete ramcloud;
        delete master1;
        delete master2;
        delete coordinatorClient1;
        delete coordinatorClient2;
        delete coordinatorServer;
        transportManager.unregisterMock();
        delete transport;
    }

    /*
    * Testing multiRead such that multiple Objects are read from
    * multiple masters.
    */
    void test_multiRead() {
        // Create objects to be read later
        uint32_t tableId1 = ramcloud->openTable("table1");
        uint64_t version1;
        ramcloud->create(tableId1, "firstVal", 8, &version1, false);

        uint32_t tableId2 = ramcloud->openTable("table2");
        uint64_t version2;
        ramcloud->create(tableId2, "secondVal", 9, &version2, false);
        uint64_t version3;
        ramcloud->create(tableId2, "thirdVal", 8, &version3, false);
        uint64_t version4;
        ramcloud->create(tableId2, "forthVal", 8, &version4, false);

        // Create requests and read
        MasterClient::ReadObject requests[4];

        Tub<Buffer> readValue1;
        uint64_t readVersion1;
        Status readStatus1;
        MasterClient::ReadObject request1(tableId1, 0, &readValue1,
                                          &readVersion1, &readStatus1);
        Tub<Buffer> readValue2;
        uint64_t readVersion2;
        Status readStatus2;
        MasterClient::ReadObject request2(tableId2, 0, &readValue2,
                                          &readVersion2, &readStatus2);
        Tub<Buffer> readValue3;
        uint64_t readVersion3;
        Status readStatus3;
        MasterClient::ReadObject request3(tableId2, 1, &readValue3,
                                          &readVersion3, &readStatus3);
        Tub<Buffer> readValue4;
        uint64_t readVersion4;
        Status readStatus4;
        MasterClient::ReadObject request4(tableId2, 2, &readValue4,
                                          &readVersion4, &readStatus4);

        requests[0] = request1;
        requests[1] = request2;
        requests[2] = request3;
        requests[3] = request4;

        ramcloud->multiRead(requests, 4);

        CPPUNIT_ASSERT_EQUAL(0, readStatus1);
        if (readValue1) {
            CPPUNIT_ASSERT_EQUAL(1, readVersion1);
            CPPUNIT_ASSERT_EQUAL("firstVal", toString(readValue1.get()));
        }
        CPPUNIT_ASSERT_EQUAL(0, readStatus2);
        if (readValue2){
            CPPUNIT_ASSERT_EQUAL(1, readVersion2);
            CPPUNIT_ASSERT_EQUAL("secondVal", toString(readValue2.get()));
        }
        CPPUNIT_ASSERT_EQUAL(0, readStatus3);
        if (readValue3){
            CPPUNIT_ASSERT_EQUAL(2, readVersion3);
            CPPUNIT_ASSERT_EQUAL("thirdVal", toString(readValue3.get()));
        }
        CPPUNIT_ASSERT_EQUAL(0, readStatus4);
        if (readValue4){
            CPPUNIT_ASSERT_EQUAL(3, readVersion4);
            CPPUNIT_ASSERT_EQUAL("forthVal", toString(readValue4.get()));
        }
    }

    void test_multiRead_badTable() {
        // Create one table and one value, for control of multiRead
        uint32_t tableId1 = ramcloud->openTable("table1");
        uint64_t version1;
        ramcloud->create(tableId1, "firstVal", 8, &version1, false);

        // Create requests and read
        MasterClient::ReadObject requests[2];

        Tub<Buffer> readValue1;
        uint64_t readVersion1;
        Status readStatus1;
        MasterClient::ReadObject request1(tableId1, 0, &readValue1,
                                          &readVersion1, &readStatus1);

        // Ensure this tableId is not the same as that created earlier
        uint32_t tableId2 = tableId1 + 10;
        Tub<Buffer> readValue2;
        uint64_t readVersion2;
        Status readStatus2;
        MasterClient::ReadObject request2(tableId2, 0, &readValue2,
                                          &readVersion2, &readStatus2);

        requests[0] = request1;
        requests[1] = request2;

        ramcloud->multiRead(requests, 2);

        CPPUNIT_ASSERT_EQUAL(0, readStatus1);
        if (readValue1) {
            CPPUNIT_ASSERT_EQUAL(1, readVersion1);
            CPPUNIT_ASSERT_EQUAL("firstVal", toString(readValue1.get()));
        }
        // Status 1 corresponds to STATUS_TABLE_DOESNT_EXIST
        CPPUNIT_ASSERT_EQUAL(1, readStatus2);

    }


    void test_multiRead_noSuchObject() {
        // Create one table and two values, for control of multiRead
        uint32_t tableId1 = ramcloud->openTable("table1");
        uint64_t version1;
        ramcloud->create(tableId1, "firstVal", 8, &version1, false);
        uint64_t version2;
        ramcloud->create(tableId1, "secondVal", 9, &version2, false);

        // Create requests and read
        MasterClient::ReadObject requests[3];

        Tub<Buffer> readValue1;
        uint64_t readVersion1;
        Status readStatus1;
        MasterClient::ReadObject request1(tableId1, 0, &readValue1,
                                          &readVersion1, &readStatus1);

        Tub<Buffer> readValue2;
        uint64_t readVersion2;
        Status readStatus2;
        MasterClient::ReadObject request2(tableId1, 1, &readValue2,
                                          &readVersion2, &readStatus2);

        Tub<Buffer> readValueError;
        uint64_t readVersionError;
        Status readStatusError;
        MasterClient::ReadObject requestError(tableId1, 20, &readValueError,
                                        &readVersionError, &readStatusError);

        requests[0] = request1;
        requests[1] = requestError;
        requests[2] = request2;

        ramcloud->multiRead(requests, 3);

        CPPUNIT_ASSERT_EQUAL(0, readStatus1);
        if (readValue1) {
            CPPUNIT_ASSERT_EQUAL(1, readVersion1);
            CPPUNIT_ASSERT_EQUAL("firstVal", toString(readValue1.get()));
        }
        // Status 2 corresponds to STATUS_OBJECT_DOESNT_EXIST
        CPPUNIT_ASSERT_EQUAL(2, readStatusError);
        CPPUNIT_ASSERT_EQUAL(0, readStatus2);
        if (readValue2) {
            CPPUNIT_ASSERT_EQUAL(2, readVersion2);
            CPPUNIT_ASSERT_EQUAL("secondVal", toString(readValue2.get()));
        }
    }

    DISALLOW_COPY_AND_ASSIGN(RamCloudTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(RamCloudTest);

}  // namespace RAMCloud
