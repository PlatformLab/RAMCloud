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
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<ObjectFinder> objectFinder;
    Refresher* refresher;

    ObjectFinderTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , objectFinder()
        , refresher()
    {
        objectFinder.construct(&context);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE};
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

TEST_F(ObjectFinderTest, lookup_key) {
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
        static_cast<BindTransport::BindSession*>(session.get())->
                getServiceLocator());
}

TEST_F(ObjectFinderTest, lookup_hash) {
    KeyHash keyHash = Key::getHash(1, "testKey", 7);
    Transport::SessionRef session(objectFinder->lookup(1, keyHash));
    // first tablet map is empty, throws TableDoesntExistException
    // get a new tablet map
    // find tablet in recovery
    // get a new tablet map
    // find tablet in recovery
    // get a new tablet map
    // find tablet in operation
    EXPECT_EQ(3U, refresher->called);
    EXPECT_EQ("mock:host=server0",
        static_cast<BindTransport::BindSession*>(session.get())->
                getServiceLocator());
}

TEST_F(ObjectFinderTest, flushSession) {
    KeyHash keyHash = Key::getHash(1, "testKey", 7);
    objectFinder->lookup(1, keyHash);
    EXPECT_FALSE(context.transportManager->sessionCache.find(
            "mock:host=server0")
            == context.transportManager->sessionCache.end());
    TestLog::reset();
    objectFinder->flushSession(1, keyHash);
    EXPECT_TRUE(context.transportManager->sessionCache.find(
            "mock:host=server0")
            == context.transportManager->sessionCache.end());
    EXPECT_EQ("flushSession: flushing session for mock:host=server0",
            TestLog::get());
    objectFinder->flushSession(99, 0);
}

}  // namespace RAMCloud
