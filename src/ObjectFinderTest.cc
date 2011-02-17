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
        tablet2.set_service_locator("mock:host=null");

        tabletMap.clear_tablet();

        switch (++called) {
            case 1:
            case 2:
                tablet2.set_state(ProtoBuf::Tablets_Tablet_State_RECOVERING);
            case 3:
                *tabletMap.add_tablet() = tablet1;
                *tabletMap.add_tablet() = tablet2;
        }
    }
    uint32_t called;
};

class ObjectFinderTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ObjectFinderTest);
    CPPUNIT_TEST(test_lookup);
    CPPUNIT_TEST_SUITE_END();

    BindTransport* transport;
    CoordinatorServer* coordinatorServer;
    CoordinatorClient* coordinatorClient;
    Server* nullServer;
    ObjectFinder* objectFinder;

  public:
    ObjectFinderTest()
        : transport()
        , coordinatorServer()
        , coordinatorClient()
        , nullServer()
        , objectFinder()
    {}

    void setUp() {
        transport = new BindTransport();
        transportManager.registerMock(transport);
        coordinatorServer = new CoordinatorServer("mock:");
        transport->addServer(*coordinatorServer, "mock:host=coordinator");
        coordinatorClient = new CoordinatorClient("mock:host=coordinator");
        nullServer = new Server();
        transport->addServer(*nullServer, "mock:host=null");
        objectFinder = new ObjectFinder(*coordinatorClient);
    }

    void tearDown() {
        delete objectFinder;
        delete nullServer;
        delete coordinatorClient;
        delete coordinatorServer;
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
        CPPUNIT_ASSERT_EQUAL("mock:host=null",
            static_cast<BindTransport::BindSession*>(session.get())->locator);
    }

    DISALLOW_COPY_AND_ASSIGN(ObjectFinderTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ObjectFinderTest);

}  // namespace RAMCloud
