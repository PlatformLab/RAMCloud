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

#include "Log.h"
#include "Table.h"
#include "Tablets.pb.h"
#include "TabletProfiler.h"
#include "Will.h"

namespace RAMCloud {

/**
 * Unit tests for Will.
 */
class WillTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(WillTest); // NOLINT

    CPPUNIT_TEST_SUITE(WillTest);
    CPPUNIT_TEST(test_Will_constructor);
    CPPUNIT_TEST(test_Will_debugDump);
    CPPUNIT_TEST(test_Will_addTablet);
    CPPUNIT_TEST(test_Will_addPartition);
    CPPUNIT_TEST(test_Will_endToEnd);
    CPPUNIT_TEST_SUITE_END();

    ProtoBuf::Tablets::Tablet
    createTablet(uint64_t serverId, uint64_t tableId, uint64_t firstKey,
        uint64_t lastKey)
    {
        ProtoBuf::Tablets_Tablet tablet;
        tablet.set_table_id(tableId);
        tablet.set_start_object_id(firstKey);
        tablet.set_end_object_id(lastKey);
        tablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet.set_server_id(serverId);
        return tablet;
    }

    Table*
    createAndAddTablet(ProtoBuf::Tablets &tablets, uint64_t serverId,
                 uint64_t tableId, uint64_t firstKey, uint64_t lastKey)
    {
        ProtoBuf::Tablets_Tablet tablet = createTablet(serverId, tableId,
            firstKey, lastKey);
        Table* table = new Table(tablet.table_id());
        tablet.set_user_data(reinterpret_cast<uint64_t>(table));
        *tablets.add_tablet() = tablet;
        return table;
    }

    void
    destroyTables(ProtoBuf::Tablets &tablets)
    {
        std::set<Table*> tables;
        foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
            tables.insert(reinterpret_cast<Table*>(tablet.user_data()));
        foreach (Table* table, tables)
            delete table;
    }

  public:
    TestLog::Enable* logEnabler;

    WillTest() : logEnabler(NULL) {}

    void
    setUp()
    {
        logEnabler = new TestLog::Enable();
    }

    void
    tearDown()
    {
        delete logEnabler;
    }

    void
    test_Will_constructor()
    {
        ProtoBuf::Tablets tablets;
        Will w(tablets, 1000, 1001);
        CPPUNIT_ASSERT_EQUAL(0, w.currentId);
        CPPUNIT_ASSERT_EQUAL(0, w.currentMaxBytes);
        CPPUNIT_ASSERT_EQUAL(1000, w.maxBytesPerPartition);
        CPPUNIT_ASSERT_EQUAL(1001, w.maxReferantsPerPartition);
        CPPUNIT_ASSERT_EQUAL(0, w.entries.size());
    }

    void
    test_Will_debugDump()
    {
        ProtoBuf::Tablets tablets;
        Table *t = createAndAddTablet(tablets, 0, 0, 0, -1);
        t->profiler.track(0, 5000, LogTime(0, 0));
        t->profiler.track(-1, 1500, LogTime(0, 1));
        Will w(tablets, 5000, 10);
        w.debugDump();
        CPPUNIT_ASSERT_EQUAL("debugDump:                                      "
            "  L A S T    W I L L    A N D    T E S T A M E N T | debugDump: -"
            "-----------------------------------------------------------------"
            "----------------------------------------------------------- | deb"
            "ugDump: Partition             TableId            FirstKey        "
            "     LastKey     MinBytes     MaxBytes   MinReferants   MaxRefera"
            "nts | debugDump: ------------------------------------------------"
            "-----------------------------------------------------------------"
            "------------ | debugDump:         0  0x0000000000000000  0x000000"
            "0000000000  0x00ffffffffffffff          4KB          4KB         "
            "     1              1 | debugDump:         1  0x0000000000000000 "
            " 0x0100000000000000  0xffffffffffffffff          1KB          1KB"
            "              1              1", TestLog::get());
        destroyTables(tablets);
    }

    void
    test_Will_addTablet()
    {
        // it's unclear how to test this, since it's such a simple method.
        // suggestions appreciated.
    }

    void
    test_Will_addPartition()
    {
        ProtoBuf::Tablets emptyTablets;
        Will w(emptyTablets, 5000, 50);

        // add a small first partition
        {
            ProtoBuf::Tablets::Tablet tablet = createTablet(0, 97, 500, 10000);
            Partition p(0, -1, 100, 150, 10, 20);
            w.addPartition(p, tablet);
        }

        CPPUNIT_ASSERT_EQUAL(0, w.entries[0].partitionId);
        CPPUNIT_ASSERT_EQUAL(97, w.entries[0].tableId);
        // ensure that the tablet restricts the key range
        CPPUNIT_ASSERT_EQUAL(500, w.entries[0].firstKey);
        CPPUNIT_ASSERT_EQUAL(10000, w.entries[0].lastKey);
        CPPUNIT_ASSERT_EQUAL(100, w.entries[0].minBytes);
        CPPUNIT_ASSERT_EQUAL(150, w.entries[0].maxBytes);
        CPPUNIT_ASSERT_EQUAL(10, w.entries[0].minReferants);
        CPPUNIT_ASSERT_EQUAL(20, w.entries[0].maxReferants);
        CPPUNIT_ASSERT_EQUAL(150, w.currentMaxBytes);
        CPPUNIT_ASSERT_EQUAL(20, w.currentMaxReferants);
        CPPUNIT_ASSERT_EQUAL(0, w.currentId);

        // add another partition that doesn't go over the max
        {
            ProtoBuf::Tablets::Tablet tablet = createTablet(0, 12, 0, -1);
            Partition p(0, -1, 4000, 4849, 10, 20);
            w.addPartition(p, tablet);
        }

        CPPUNIT_ASSERT_EQUAL(0, w.currentId);

        // now exceed the byte maximum
        {
            ProtoBuf::Tablets::Tablet tablet = createTablet(0, 15, 0, -1);
            Partition p(0, -1, 2, 2, 10, 10);
            w.addPartition(p, tablet);
        }

        CPPUNIT_ASSERT_EQUAL(1, w.currentId);
        CPPUNIT_ASSERT_EQUAL(2, w.currentMaxBytes);
        CPPUNIT_ASSERT_EQUAL(10, w.currentMaxReferants);

        // now exceed the referant maximum
        {
            ProtoBuf::Tablets::Tablet tablet = createTablet(0, 12, 0, -1);
            Partition p(0, -1, 3, 3, 49, 49);
            w.addPartition(p, tablet);
        }

        CPPUNIT_ASSERT_EQUAL(2, w.currentId);
        CPPUNIT_ASSERT_EQUAL(3, w.currentMaxBytes);
        CPPUNIT_ASSERT_EQUAL(49, w.currentMaxReferants);
    }

    // This is an end-to-end test of the Will and TabletProfiler code.
    // Basically, we want to set up a simulated set of table operations
    // followed by a Will calculation and ensure that it matches what we
    // expect.
    void
    test_Will_endToEnd()
    {

    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(WillTest);

} // namespace RAMCloud
