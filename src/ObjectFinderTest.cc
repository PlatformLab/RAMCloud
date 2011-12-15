/* Copyright (c) 2010-2011 Stanford University
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
#include "MockCluster.h"
#include "ObjectFinder.h"

namespace RAMCloud {
struct Refresher : public ObjectFinder::TabletMapFetcher {
    Refresher() : called(0) {}
    void getTabletMap(ProtoBuf::Tablets& tabletMap) {
        ProtoBuf::Tablets_Tablet tablet1;
        tablet1.set_table_id(0);
        tablet1.set_start_key_hash(0);
        tablet1.set_end_key_hash(~0UL);
        tablet1.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet1.set_service_locator("mock:host=fail");

        ProtoBuf::Tablets_Tablet tablet2;
        tablet2.set_table_id(1);
        tablet2.set_start_key_hash(0);
        tablet2.set_end_key_hash(~0UL);
        tablet2.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet2.set_service_locator("mock:host=server0");

        ProtoBuf::Tablets_Tablet tablet3;
        tablet3.set_table_id(2);
        tablet3.set_start_key_hash(0);
        tablet3.set_end_key_hash(~0UL);
        tablet3.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet3.set_service_locator("mock:host=server1");

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

class ObjectFinderTest : public ::testing::Test {
  public:
    MockCluster cluster;
    Tub<ObjectFinder> objectFinder;
    Refresher* refresher;

    ObjectFinderTest()
        : cluster()
        , objectFinder()
        , refresher()
    {
        objectFinder.construct(*cluster.getCoordinatorClient());

        ServerConfig config = ServerConfig::forTesting();
        config.services = {MASTER_SERVICE};
        cluster.addServer(config);
        cluster.addServer(config);

        refresher = new Refresher();
        objectFinder->tabletMapFetcher.reset(refresher);
    }

    ~ObjectFinderTest() {
        // refresher is deleted by objectFinder
    }

    DISALLOW_COPY_AND_ASSIGN(ObjectFinderTest);
};

TEST_F(ObjectFinderTest, lookup) {
    Transport::SessionRef session(objectFinder->lookup(1, "testKey", 7));
    // first tablet map is empty, throws TableDoesntExistException
    // get a new tablet map
    // find tablet in recovery
    // get a new tablet map
    // find tablet in recovery
    // get a new tablet map
    // find tablet in operation
    EXPECT_EQ(3U, refresher->called);
    EXPECT_EQ("mock:host=server0",
        static_cast<BindTransport::BindSession*>(session.get())->locator);
}

TEST_F(ObjectFinderTest, multiLookup_basics) {
    MasterClient::ReadObject* requests[3];

    Tub<Buffer> readValue1;
    MasterClient::ReadObject request1(1, "0", 1, &readValue1);
    request1.status = STATUS_RETRY;
    requests[0] = &request1;

    Tub<Buffer> readValue2;
    MasterClient::ReadObject request2(1, "1", 1, &readValue2);
    request2.status = STATUS_RETRY;
    requests[1] = &request2;

    Tub<Buffer> readValue3;
    MasterClient::ReadObject request3(2, "0", 1, &readValue3);
    request3.status = STATUS_RETRY;
    requests[2] = &request3;

    std::vector<ObjectFinder::MasterRequests> requestBins =
                                    objectFinder->multiLookup(requests, 3);

    EXPECT_EQ("mock:host=server0",
        static_cast<BindTransport::BindSession*>(
        requestBins[0].sessionRef.get())->locator);
    EXPECT_EQ(1U, requestBins[0].requests[0]->tableId);
    EXPECT_EQ("0", requestBins[0].requests[0]->key);
    EXPECT_STREQ("STATUS_RETRY", statusToSymbol(request1.status));
    EXPECT_EQ(1U, requestBins[0].requests[1]->tableId);
    EXPECT_EQ("1", requestBins[0].requests[1]->key);
    EXPECT_STREQ("STATUS_RETRY", statusToSymbol(request2.status));

    EXPECT_EQ("mock:host=server1",
        static_cast<BindTransport::BindSession*>(
        requestBins[1].sessionRef.get())->locator);
    EXPECT_EQ(2U, requestBins[1].requests[0]->tableId);
    EXPECT_EQ("0", requestBins[1].requests[0]->key);
    EXPECT_STREQ("STATUS_RETRY", statusToSymbol(request3.status));
}

TEST_F(ObjectFinderTest, multiLookup_badTable) {
    TestLog::Enable _;

    MasterClient::ReadObject* requests[1];

    Tub<Buffer> readValueError;
    MasterClient::ReadObject requestError(3, "0", 1, &readValueError);
    requestError.status = STATUS_RETRY;
    requests[0] = &requestError;

    std::vector<ObjectFinder::MasterRequests> requestBins =
                                    objectFinder->multiLookup(requests, 1);

    EXPECT_STREQ("STATUS_TABLE_DOESNT_EXIST",
                            statusToSymbol(requestError.status));
}

}  // namespace RAMCloud
