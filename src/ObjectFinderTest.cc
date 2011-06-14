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
#include "BindTransport.h"
#include "CoordinatorClient.h"
#include "CoordinatorServer.h"
#include "ObjectFinder.h"

namespace RAMCloud {
struct Refresher {
    Refresher() : called(0) {}
    void operator()(ProtoBuf::Tablets& tabletMap) {
        ProtoBuf::Tablets_Tablet tablet1;
        tablet1.set_table_id(0);
        tablet1.set_start_object_id(0);
        tablet1.set_end_object_id(~0UL);
        tablet1.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet1.set_service_locator("mock:host=fail");

        ProtoBuf::Tablets_Tablet tablet2;
        tablet2.set_table_id(1);
        tablet2.set_start_object_id(0);
        tablet2.set_end_object_id(~0UL);
        tablet2.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet2.set_service_locator("mock:host=host1");

        ProtoBuf::Tablets_Tablet tablet3;
        tablet3.set_table_id(2);
        tablet3.set_start_object_id(0);
        tablet3.set_end_object_id(~0UL);
        tablet3.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet3.set_service_locator("mock:host=host2");

        tabletMap.clear_tablet();

        switch (++called) {
            case 1:
            case 2:
                tablet2.set_state(ProtoBuf::Tablets_Tablet_State_RECOVERING);
            case 3:
                *tabletMap.add_tablet() = tablet1;
                *tabletMap.add_tablet() = tablet2;
                *tabletMap.add_tablet() = tablet3;
        }
    }
    uint32_t called;
};

class ObjectFinderTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ObjectFinderTest);
    CPPUNIT_TEST(test_lookup);
    CPPUNIT_TEST(test_multiLookup_basics);
    CPPUNIT_TEST(test_multiLookup_badTable);
    CPPUNIT_TEST_SUITE_END();

    BindTransport* transport;
    CoordinatorServer* coordinatorService;
    CoordinatorClient* coordinatorClient;
    Service* host1Service;
    Service* host2Service;
    ObjectFinder* objectFinder;

  public:
    ObjectFinderTest()
        : transport()
        , coordinatorService()
        , coordinatorClient()
        , host1Service()
        , host2Service()
        , objectFinder()
    {}

    void setUp() {
        transport = new BindTransport();
        transportManager.registerMock(transport);
        coordinatorService = new CoordinatorServer();
        transport->addService(*coordinatorService, "mock:host=coordinator");
        coordinatorClient = new CoordinatorClient("mock:host=coordinator");
        host1Service = new Service();
        transport->addService(*host1Service, "mock:host=host1");
        host2Service = new Service();
        transport->addService(*host2Service, "mock:host=host2");
        objectFinder = new ObjectFinder(*coordinatorClient);
    }

    void tearDown() {
        delete objectFinder;
        delete host1Service;
        delete host2Service;
        delete coordinatorClient;
        delete coordinatorService;
        transportManager.unregisterMock();
        delete transport;
    }

    void test_lookup() {
        Refresher refresher;
        objectFinder->refresher = boost::ref(refresher);
        Transport::SessionRef session(objectFinder->lookup(1, 2));
        // first tablet map is empty, throws TableDoesntExistException
        // get a new tablet map
        // find tablet in recovery
        // get a new tablet map
        // find tablet in recovery
        // get a new tablet map
        // find tablet in operation
        CPPUNIT_ASSERT_EQUAL(3, refresher.called);
        CPPUNIT_ASSERT_EQUAL("mock:host=host1",
            static_cast<BindTransport::BindSession*>(session.get())->locator);
    }

    void test_multiLookup_basics() {
        Refresher refresher;
        objectFinder->refresher = boost::ref(refresher);

        MasterClient::ReadObject* requests[3];

        Tub<Buffer> readValue1;
        MasterClient::ReadObject request1(1, 0, &readValue1);
        request1.status = STATUS_RETRY;
        requests[0] = &request1;

        Tub<Buffer> readValue2;
        MasterClient::ReadObject request2(1, 1, &readValue2);
        request2.status = STATUS_RETRY;
        requests[1] = &request2;

        Tub<Buffer> readValue3;
        MasterClient::ReadObject request3(2, 0, &readValue3);
        request3.status = STATUS_RETRY;
        requests[2] = &request3;

        std::vector<ObjectFinder::MasterRequests> requestBins =
                                        objectFinder->multiLookup(requests, 3);

        CPPUNIT_ASSERT_EQUAL("mock:host=host1",
            static_cast<BindTransport::BindSession*>(
            requestBins[0].sessionRef.get())->locator);
        CPPUNIT_ASSERT_EQUAL(1, requestBins[0].requests[0]->tableId);
        CPPUNIT_ASSERT_EQUAL(0, requestBins[0].requests[0]->id);
        CPPUNIT_ASSERT_EQUAL("STATUS_RETRY", statusToSymbol(request1.status));
        CPPUNIT_ASSERT_EQUAL(1, requestBins[0].requests[1]->tableId);
        CPPUNIT_ASSERT_EQUAL(1, requestBins[0].requests[1]->id);
        CPPUNIT_ASSERT_EQUAL("STATUS_RETRY", statusToSymbol(request2.status));

        CPPUNIT_ASSERT_EQUAL("mock:host=host2",
            static_cast<BindTransport::BindSession*>(
            requestBins[1].sessionRef.get())->locator);
        CPPUNIT_ASSERT_EQUAL(2, requestBins[1].requests[0]->tableId);
        CPPUNIT_ASSERT_EQUAL(0, requestBins[1].requests[0]->id);
        CPPUNIT_ASSERT_EQUAL("STATUS_RETRY", statusToSymbol(request3.status));
    }

    void test_multiLookup_badTable() {
        Refresher refresher;
        objectFinder->refresher = boost::ref(refresher);

        MasterClient::ReadObject* requests[2];

        Tub<Buffer> readValue1;
        MasterClient::ReadObject request1(1, 0, &readValue1);
        request1.status = STATUS_RETRY;
        requests[0] = &request1;

        Tub<Buffer> readValueError;
        MasterClient::ReadObject requestError(3, 0, &readValueError);
        requestError.status = STATUS_RETRY;
        requests[1] = &requestError;

        std::vector<ObjectFinder::MasterRequests> requestBins =
                                        objectFinder->multiLookup(requests, 2);

        CPPUNIT_ASSERT_EQUAL("mock:host=host1",
            static_cast<BindTransport::BindSession*>(
            requestBins[0].sessionRef.get())->locator);
        CPPUNIT_ASSERT_EQUAL(1, requestBins[0].requests[0]->tableId);
        CPPUNIT_ASSERT_EQUAL(0, requestBins[0].requests[0]->id);
        CPPUNIT_ASSERT_EQUAL("STATUS_RETRY", statusToSymbol(request1.status));

        CPPUNIT_ASSERT_EQUAL("STATUS_TABLE_DOESNT_EXIST",
                             statusToSymbol(requestError.status));
    }

    DISALLOW_COPY_AND_ASSIGN(ObjectFinderTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ObjectFinderTest);

}  // namespace RAMCloud
