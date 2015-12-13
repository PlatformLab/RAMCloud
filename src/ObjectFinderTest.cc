/* Copyright (c) 2010-2015 Stanford University
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
struct Refresher : public ObjectFinder::TableConfigFetcher {
    Refresher() : called(0) {}
    void getTableConfig(
         uint64_t tableId,
         std::map<TabletKey, TabletWithLocator>* tableMap,
         std::multimap< std::pair<uint64_t, uint8_t>,
                                ObjectFinder::Indexlet>* tableIndexMap) {

        called++;
        Tablet rawTablet2({1, 0, uint64_t(~0), ServerId(),
                                Tablet::NORMAL, LogPosition()});
        TabletWithLocator tablet2(rawTablet2, "mock:host=server1");

        Tablet rawTablet3({2, 0, 1000, ServerId(),
                                Tablet::NORMAL, LogPosition()});
        TabletWithLocator tablet3(rawTablet3, "mock:host=server2");

        Tablet rawTablet4({2, 1000, uint64_t(~0), ServerId(),
                                Tablet::NORMAL, LogPosition()});
        TabletWithLocator tablet4(rawTablet4, "mock:host=server6");

        Tablet rawTablet5({3, 0, 1000, ServerId(),
                                Tablet::NORMAL, LogPosition()});
        TabletWithLocator tablet5(rawTablet5, "mock:host=server3");

        Tablet rawTablet8({3, 10000, uint64_t(~0), ServerId(),
                                Tablet::NORMAL, LogPosition()});
        TabletWithLocator tablet8(rawTablet8, "mock:host=server3");

        Tablet rawTablet6({4, 0, uint64_t(~0), ServerId(),
                                Tablet::NORMAL, LogPosition()});
        TabletWithLocator tablet6(rawTablet6, "mock:host=server4");

        Tablet rawTablet7({5, 13274077256558369931LLU, 18303021482201187663LLU,
                            ServerId(), Tablet::NORMAL, LogPosition()});
        TabletWithLocator tablet7(rawTablet7, "mock:host=server5");

        if (called <= 2) {
            tablet2.tablet.status = Tablet::RECOVERING;
        }

        TabletKey key2 {tablet2.tablet.tableId,
                                            tablet2.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key2, tablet2));

        TabletKey key3 {tablet3.tablet.tableId,
                                    tablet3.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key3, tablet3));

        TabletKey key4 {tablet4.tablet.tableId,
                                    tablet4.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key4, tablet4));

        TabletKey key5 {tablet5.tablet.tableId,
                                    tablet5.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key5, tablet5));

        TabletKey key8 {tablet8.tablet.tableId,
                                    tablet8.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key8, tablet8));

        TabletKey key6 {tablet6.tablet.tableId,
                                    tablet6.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key6, tablet6));

        TabletKey key7 {tablet7.tablet.tableId,
                                    tablet7.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key7, tablet7));


        char* b = new char('b');
        char* l = new char('l');
        char* w = new char('w');

        ObjectFinder::Indexlet indexlet0(reinterpret_cast<void*>(b), 1,
            reinterpret_cast<void*>(l), 1, ServerId(), "mock:host=server0");
        ObjectFinder::Indexlet indexlet1(reinterpret_cast<void*>(l), 1,
            reinterpret_cast<void*>(w), 1, ServerId(), "mock:host=server1");
        ObjectFinder::Indexlet indexlet2(NULL, 0, reinterpret_cast<void*>(l)
                                     , 1, ServerId(), "mock:host=server2");
        ObjectFinder::Indexlet indexlet3(reinterpret_cast<void*>(l), 1, NULL
                                     , 0, ServerId(), "mock:host=server3");

        tableIndexMap->insert(std::make_pair(
                                        std::make_pair(1, 0), indexlet0));
        tableIndexMap->insert(std::make_pair(
                                        std::make_pair(1, 0), indexlet1));
        tableIndexMap->insert(std::make_pair(
                                        std::make_pair(1, 1), indexlet2));
        tableIndexMap->insert(std::make_pair(
                                        std::make_pair(1, 1), indexlet3));

    }
    uint32_t called;
};

class ObjectFinderTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    Tub<ObjectFinder> objectFinder;
    Refresher* refresher;

    ObjectFinderTest()
        : logEnabler()
        , context()
        , objectFinder()
        , refresher()
    {
        context.transportManager->registerMock(NULL);

        objectFinder.construct(&context);
        refresher = new Refresher();
        objectFinder->tableConfigFetcher.reset(refresher);
    }

    ~ObjectFinderTest() {
        // refresher is deleted by objectFinder
    }

    DISALLOW_COPY_AND_ASSIGN(ObjectFinderTest);
};

TEST_F(ObjectFinderTest, flush) {
    // expect nothing to be there before refreshing the coordinator
    EXPECT_EQ(objectFinder->debugString(), "");

    // fetch the tableMap
    EXPECT_THROW(Transport::SessionRef session1(
                    objectFinder->lookup(10, "testKey", 7)),
                 TableDoesntExistException);
    EXPECT_EQ(objectFinder->debugString(),
               "{{tableId : 1, keyHash : 0}, {start_key_hash : 0,"
               " end_key_hash : 18446744073709551615, state : 1}},"
               " {{tableId : 2, keyHash : 0}, {start_key_hash : 0,"
               " end_key_hash : 1000, state : 0}},"
               " {{tableId : 2, keyHash : 1000}, {start_key_hash : 1000,"
               " end_key_hash : 18446744073709551615, state : 0}},"
               " {{tableId : 3, keyHash : 0}, {start_key_hash : 0,"
               " end_key_hash : 1000, state : 0}},"
               " {{tableId : 3, keyHash : 10000}, {start_key_hash : 10000,"
               " end_key_hash : 18446744073709551615, state : 0}},"
               " {{tableId : 4, keyHash : 0}, {start_key_hash : 0,"
               " end_key_hash : 18446744073709551615, state : 0}},"
               " {{tableId : 5, keyHash : 13274077256558369931},"
               " {start_key_hash : 13274077256558369931,"
               " end_key_hash : 18303021482201187663, state : 0}}");

    // flush multiple tables in the middle
    for (uint64_t i = 3; i <= 5; i++) {
        objectFinder->flush(i);
    }
    EXPECT_EQ(objectFinder->debugString(),
                "{{tableId : 1, keyHash : 0}, {start_key_hash : 0,"
                " end_key_hash : 18446744073709551615, state : 1}},"
                " {{tableId : 2, keyHash : 0}, {start_key_hash : 0,"
                " end_key_hash : 1000, state : 0}},"
                " {{tableId : 2, keyHash : 1000}, {start_key_hash : 1000,"
                " end_key_hash : 18446744073709551615, state : 0}}");

    // try to flush a removed table
    objectFinder->flush(5);
    EXPECT_EQ(objectFinder->debugString(),
                "{{tableId : 1, keyHash : 0}, {start_key_hash : 0,"
                " end_key_hash : 18446744073709551615, state : 1}},"
                " {{tableId : 2, keyHash : 0}, {start_key_hash : 0,"
                " end_key_hash : 1000, state : 0}},"
                " {{tableId : 2, keyHash : 1000}, {start_key_hash : 1000,"
                " end_key_hash : 18446744073709551615, state : 0}}");

    // try to flush an entry that never existed.
    objectFinder->flush(10);
    EXPECT_EQ(objectFinder->debugString(),
                "{{tableId : 1, keyHash : 0}, {start_key_hash : 0,"
                " end_key_hash : 18446744073709551615, state : 1}},"
                " {{tableId : 2, keyHash : 0}, {start_key_hash : 0,"
                " end_key_hash : 1000, state : 0}},"
                " {{tableId : 2, keyHash : 1000}, {start_key_hash : 1000,"
                " end_key_hash : 18446744073709551615, state : 0}}");

    // flush a table with multiple tablets. Also the end tablet
    objectFinder->flush(2);
    EXPECT_EQ(objectFinder->debugString(),
                "{{tableId : 1, keyHash : 0}, {start_key_hash : 0,"
                " end_key_hash : 18446744073709551615, state : 1}}");

    //flush the table in the beginning
    objectFinder->flush(1);
    EXPECT_EQ(objectFinder->debugString(), "");
}

TEST_F(ObjectFinderTest, lookup_stringKey) {
    Transport::SessionRef session = objectFinder->lookup(1, "abc", 3);
    ASSERT_TRUE(session != NULL);
    EXPECT_EQ("mock:host=server1", session->getServiceLocator());
}
TEST_F(ObjectFinderTest, lookup_stringKey_noSuchTable) {
    EXPECT_THROW(objectFinder->lookup(99, "abc", 3), TableDoesntExistException);
}

TEST_F(ObjectFinderTest, lookup_keyHash) {
    // Make sure that no session was cached initially.
    EXPECT_TRUE(objectFinder->lookupTablet(1, 9999lu)->session == NULL);

    Transport::SessionRef session = objectFinder->lookup(1, 9999lu);
    ASSERT_TRUE(session != NULL);
    EXPECT_EQ("mock:host=server1", session->getServiceLocator());

    // Make sure that the session was cached.
    EXPECT_EQ(session, objectFinder->lookupTablet(1, 9999lu)->session);
}

TEST_F(ObjectFinderTest, lookup_index_noSuchIndex) {
    Transport::SessionRef session = objectFinder->lookup(2, 99, "abc", 3);
    ASSERT_TRUE(session == NULL);
}
TEST_F(ObjectFinderTest, lookup_index_success) {
    // Make sure that no session was cached initially.
    EXPECT_TRUE(objectFinder->lookupIndexlet(1, 1, "abc", 3)->session
            == NULL);

    Transport::SessionRef session = objectFinder->lookup(1, 1, "abc", 3);
    ASSERT_TRUE(session != NULL);
    EXPECT_EQ("mock:host=server2", session->getServiceLocator());

    // Make sure that the session was cached.
    EXPECT_EQ(session, objectFinder->lookupIndexlet(1, 1, "abc", 3)->session);
}

TEST_F(ObjectFinderTest, lookupIndexlet) {
    char a = 'a';
    char b = 'b';
    char g = 'g';
    char l = 'l';
    char w = 'w';
    char z = 'z';

    // before any of the valid indexlets
    Transport::SessionRef session0(objectFinder->lookup(1, 0,
                                            reinterpret_cast<void*>(&a), 1));
    EXPECT_EQ(Transport::SessionRef(), session0);

    // start of the first indexlet
    Transport::SessionRef session1(objectFinder->lookup(1, 0,
                                            reinterpret_cast<void*>(&b), 1));
    EXPECT_EQ("mock:host=server0",
        static_cast<BindTransport::BindSession*>(session1.get())->
                getServiceLocator());

    // middle of the first indexlet
    Transport::SessionRef session2(objectFinder->lookup(1, 0,
                                            reinterpret_cast<void*>(&g), 1));
    EXPECT_EQ("mock:host=server0",
        static_cast<BindTransport::BindSession*>(session2.get())->
                getServiceLocator());

    // begin of the second indexlet
    Transport::SessionRef session3(objectFinder->lookup(1, 0,
                                            reinterpret_cast<void*>(&l), 1));
    EXPECT_EQ("mock:host=server1",
        static_cast<BindTransport::BindSession*>(session3.get())->
                getServiceLocator());

    // end of the second or last indexlet
    Transport::SessionRef session4(objectFinder->lookup(1, 0,
                                            reinterpret_cast<void*>(&w), 1));
    EXPECT_EQ(Transport::SessionRef(), session4);

    // beyond the last indexlet
    Transport::SessionRef session5(objectFinder->lookup(1, 0,
                                            reinterpret_cast<void*>(&z), 1));
    EXPECT_EQ(Transport::SessionRef(), session5);

    // where first key is NULL
    Transport::SessionRef session6(objectFinder->lookup(1, 1,
                                            reinterpret_cast<void*>(&b), 1));
    EXPECT_EQ("mock:host=server2",
        static_cast<BindTransport::BindSession*>(session6.get())->
                getServiceLocator());

    // where last key is NULL
    Transport::SessionRef session7(objectFinder->lookup(1, 1,
                                            reinterpret_cast<void*>(&z), 1));
    EXPECT_EQ("mock:host=server3",
        static_cast<BindTransport::BindSession*>(session7.get())->
                getServiceLocator());
}

TEST_F(ObjectFinderTest, lookupTablet) {

    // expect nothing to be there before refreshing the coordinator
    EXPECT_EQ(objectFinder->debugString(), "");

    // testing recovery
    EXPECT_EQ(0U, refresher->called);
    Transport::SessionRef session9(objectFinder->lookup(1, "testKey", 7));
    EXPECT_EQ(3U, refresher->called);
    // first tablet map is empty, throws TableDoesntExistException
    // get a new tablet map
    // find tablet in recovery
    // get a new tablet map
    // find tablet in recovery
    // get a new tablet map
    // find tablet in operation
    EXPECT_EQ("mock:host=server1",
        static_cast<BindTransport::BindSession*>(session9.get())->
                getServiceLocator());

    // Looking up non-existant tablet, throws TableDoesntExistException
    EXPECT_THROW(Transport::SessionRef session1(
                    objectFinder->lookup(10, "testKey", 7)),
                TableDoesntExistException);

    // Lookup key before the first tablet
    EXPECT_THROW(Transport::SessionRef session2(
                    objectFinder->lookup(0, "testKey", 7)),
                TableDoesntExistException);

    // looking up key in the first tablet
    Transport::SessionRef session3(objectFinder->lookup(1, "testKey", 7));
    EXPECT_EQ("mock:host=server1",
        static_cast<BindTransport::BindSession*>(session3.get())->
                getServiceLocator());

    // looking up key in the middle tablet
    Transport::SessionRef session4(objectFinder->lookup(2, "testKey", 7));
    EXPECT_EQ("mock:host=server6",
        static_cast<BindTransport::BindSession*>(session4.get())->
                getServiceLocator());

    // looking up key in the middle of two tablets
    // Ensuring tableMap iterates in the correct range for a tableId.
    // ("testKey",7) hashes to 14083349934329982302, which is not present.
    EXPECT_THROW(Transport::SessionRef session5(
                    objectFinder->lookup(5, "bogus", 5)),
                 TableDoesntExistException);

    // looking up key in the last tablet
    Transport::SessionRef session6(objectFinder->lookup(5, "testKey", 7));
    EXPECT_EQ("mock:host=server5",
        static_cast<BindTransport::BindSession*>(session6.get())->
                getServiceLocator());

    // looking up key after the last tablet
    EXPECT_THROW(Transport::SessionRef session7(
                    objectFinder->lookup(5, "testKeyCheck", 12)),
                 TableDoesntExistException);

    // expect to stay consistent with the coordinator
    objectFinder->flush(2);
    Transport::SessionRef session8(objectFinder->lookup(2, "testKey", 7));
    EXPECT_EQ("mock:host=server6",
            static_cast<BindTransport::BindSession*>(session8.get())->
                    getServiceLocator());
}

TEST_F(ObjectFinderTest, flushSession_tablet) {
    KeyHash keyHash = Key::getHash(1, "testKey", 7);
    objectFinder->lookup(1, keyHash);
    EXPECT_FALSE(context.transportManager->sessionCache.find(
            "mock:host=server1")
            == context.transportManager->sessionCache.end());
    EXPECT_TRUE(objectFinder->lookupTablet(1, 9999lu)->session != NULL);

    TestLog::reset();
    objectFinder->flushSession(1, keyHash);
    // Make sure that the session is no longer cached either in ObjectFinder
    // or TransportManager.
    EXPECT_TRUE(objectFinder->lookupTablet(1, keyHash)->session == NULL);
    EXPECT_TRUE(context.transportManager->sessionCache.find(
            "mock:host=server1")
            == context.transportManager->sessionCache.end());
    EXPECT_EQ("flushSession: flushing session for mock:host=server1",
            TestLog::get());
    objectFinder->flushSession(99, 0);
}

TEST_F(ObjectFinderTest, flushSession_index) {
    objectFinder->lookup(1, 1, "abc", 3);
    EXPECT_FALSE(context.transportManager->sessionCache.find(
            "mock:host=server2")
            == context.transportManager->sessionCache.end());
    EXPECT_TRUE(objectFinder->lookupIndexlet(1, 1, "abc", 3)->session != NULL);

    TestLog::reset();
    objectFinder->flushSession(1, 1, "abc", 3);
    // Make sure that the session is no longer cached either in ObjectFinder
    // or TransportManager.
    EXPECT_TRUE(objectFinder->lookupIndexlet(1, 1, "abc", 3)->session == NULL);
    EXPECT_TRUE(context.transportManager->sessionCache.find(
            "mock:host=server2")
            == context.transportManager->sessionCache.end());
    EXPECT_EQ("keyCompare: Comparing keys: abc vs l | "
            "flushSession: flushing session for mock:host=server2 | "
            "keyCompare: Comparing keys: abc vs l",
             TestLog::get());
    objectFinder->flushSession(99, 0);
}

}  // namespace RAMCloud
