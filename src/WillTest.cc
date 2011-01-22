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

#include <boost/unordered_map.hpp>

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
    CPPUNIT_TEST(test_Will_endToEnd_2);
    CPPUNIT_TEST(test_Will_endToEnd_3);
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
        CPPUNIT_ASSERT_EQUAL(0, w.currentCount);
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
        CPPUNIT_ASSERT_EQUAL(1, w.currentCount);

        // add another partition that doesn't go over the max
        {
            ProtoBuf::Tablets::Tablet tablet = createTablet(0, 12, 0, -1);
            Partition p(0, -1, 4000, 4849, 10, 20);
            w.addPartition(p, tablet);
        }

        CPPUNIT_ASSERT_EQUAL(0, w.currentId);
        CPPUNIT_ASSERT_EQUAL(2, w.currentCount);

        // now exceed the byte maximum
        {
            ProtoBuf::Tablets::Tablet tablet = createTablet(0, 15, 0, -1);
            Partition p(0, -1, 2, 2, 10, 10);
            w.addPartition(p, tablet);
        }

        CPPUNIT_ASSERT_EQUAL(1, w.currentId);
        CPPUNIT_ASSERT_EQUAL(1, w.currentCount);
        CPPUNIT_ASSERT_EQUAL(2, w.currentMaxBytes);
        CPPUNIT_ASSERT_EQUAL(10, w.currentMaxReferants);

        // now exceed the referant maximum
        {
            ProtoBuf::Tablets::Tablet tablet = createTablet(0, 12, 0, -1);
            Partition p(0, -1, 3, 3, 49, 49);
            w.addPartition(p, tablet);
        }

        CPPUNIT_ASSERT_EQUAL(2, w.currentId);
        CPPUNIT_ASSERT_EQUAL(1, w.currentCount);
        CPPUNIT_ASSERT_EQUAL(3, w.currentMaxBytes);
        CPPUNIT_ASSERT_EQUAL(49, w.currentMaxReferants);
    }


////////////////////////////////////////////////////////////////////
// The following is all part of end-to-end testing of the Will and
// TabletProfiler code.
//
// Basically, we want to set up a simulated set of table operations
// followed by a Will calculation and ensure that it matches what we
// expect.
////////////////////////////////////////////////////////////////////

    // A simple class that tracks all "objects" in a "tablet".
    // One can query the exact number of bytes and objects within
    // specific key ranges.
    class TabletOracle {
      public:
        TabletOracle() : objectMap() {}

        void
        addObject(uint64_t key, uint64_t bytes)
        {
            if (objectMap.find(key) != objectMap.end())
                throw Exception(HERE, "don't do that!");
            objectMap[key] = bytes;
        }

        void
        removeObject(uint64_t key)
        {
            if (objectMap.find(key) == objectMap.end())
                throw Exception(HERE, "don't do this, either!");
            objectMap.erase(key);
        }

        uint64_t
        getBytesInRange(uint64_t firstKey, uint64_t lastKey)
        {
            boost::unordered_map<uint64_t, uint64_t>::iterator it =
                objectMap.begin();
            uint64_t bytes = 0;
            while (it != objectMap.end()) {
                if (it->first >= firstKey && it->first <= lastKey)
                    bytes += it->second;
                it++;
            }
            return bytes;
        }

        uint64_t
        getReferantsInRange(uint64_t firstKey, uint64_t lastKey)
        {
            boost::unordered_map<uint64_t, uint64_t>::iterator it =
                objectMap.begin();
            uint64_t objects = 0;
            while (it != objectMap.end()) {
                if (it->first >= firstKey && it->first <= lastKey)
                    objects++;
                it++;
            }
            return objects;
        }

      private:
        boost::unordered_map<uint64_t, uint64_t> objectMap;
    };

    // A wrapper for TabletOracle that takes into account
    // multiple tablets.
    class Oracle {
      public:
        Oracle() : tabletMap() {}

        void
        addObject(uint64_t table, uint64_t key, uint64_t bytes)
        {
            tabletMap[table].addObject(key, bytes);
        }

        void
        removeObject(uint64_t table, uint64_t key)
        {
            tabletMap[table].removeObject(key);
        }

        uint64_t
        getBytesInRange(uint64_t table, uint64_t firstKey, uint64_t lastKey)
        {
            return tabletMap[table].getBytesInRange(firstKey, lastKey);
        }

        uint64_t
        getReferantsInRange(uint64_t table, uint64_t firstKey, uint64_t lastKey)
        {
            return tabletMap[table].getReferantsInRange(firstKey, lastKey);
        }

      private:
        boost::unordered_map<uint64_t, TabletOracle> tabletMap;
    };

    void
    sanityCheckWill(Will& w, Oracle& o)
    {
        uint64_t totalMinBytes = 0;
        uint64_t totalMaxBytes = 0;
        uint64_t totalMinReferants = 0;
        uint64_t totalMaxReferants = 0;
        uint64_t totalRealBytes = 0;
        uint64_t totalRealReferants = 0;
        uint64_t lastId = 0;
        for (unsigned int i = 0; i < w.entries.size(); i++) {
            Will::WillEntry* we = &w.entries[i];

            uint64_t realBytes = o.getBytesInRange(we->tableId,
                we->firstKey, we->lastKey);
            uint64_t realReferants = o.getReferantsInRange(we->tableId,
                we->firstKey, we->lastKey);

#if 0
            fprintf(stderr, "part %lu, tbl %lu, fk 0x%016lx, lk 0x%016lx\n",
                we->partitionId, we->tableId, we->firstKey, we->lastKey);
            fprintf(stderr, "  we->maxBytes: %lu, realBytes: %lu\n\n",
                we->maxBytes, realBytes);
#endif

            if (we->partitionId != lastId) {
                CPPUNIT_ASSERT(totalRealBytes <= totalMaxBytes);
                CPPUNIT_ASSERT(labs(totalMaxBytes - totalRealBytes) <=
                    (int64_t)TabletProfiler::getMaximumByteError());
                CPPUNIT_ASSERT(totalRealReferants <= totalMaxReferants);
                CPPUNIT_ASSERT(labs(totalMaxReferants - totalRealReferants) <=
                    (int64_t)TabletProfiler::getMaximumReferantError());

                totalMinBytes = 0;
                totalMaxBytes = 0;
                totalMinReferants = 0;
                totalMaxReferants = 0;
                totalRealBytes = 0;
                totalRealReferants = 0;
                lastId = we->partitionId;
            }

            CPPUNIT_ASSERT(we->minBytes <= realBytes);
            CPPUNIT_ASSERT(we->maxBytes >= realBytes);
            CPPUNIT_ASSERT(we->minReferants <= realReferants);
            CPPUNIT_ASSERT(we->maxReferants >= realReferants);

            totalMinBytes += we->minBytes;
            totalMaxBytes += we->maxBytes;
            totalMinReferants += we->minReferants;
            totalMaxReferants += we->maxReferants;
            totalRealBytes += realBytes;
            totalRealReferants += realReferants;
        }
        CPPUNIT_ASSERT(totalRealBytes <= totalMaxBytes);
        CPPUNIT_ASSERT(labs(totalMaxBytes - totalRealBytes) <=
            (int64_t)TabletProfiler::getMaximumByteError());
        CPPUNIT_ASSERT(totalRealReferants <= totalMaxReferants);
        CPPUNIT_ASSERT(labs(totalMaxReferants - totalRealReferants) <=
            (int64_t)TabletProfiler::getMaximumReferantError());
    }

    // Simple test that writes 18GB of objects with incrementing
    // keys, computes a will, then deletes 6GB and recomputes.
    void
    test_Will_endToEnd()
    {
        ProtoBuf::Tablets tablets;
        Table* t;
        Oracle o;

        const uint64_t maxBytesPerPartition = 640 * 1024 * 1024;
        const uint64_t maxReferantsPerPartition = 10 * 1000 * 1000;

        // 5GB, auto-incrementing, 256KB objects
        t = createAndAddTablet(tablets, 0, 0, 0, -1);
        for (uint64_t i = 0; i < (5UL * 1024 * 1024) / 256; i++) {
            t->profiler.track(i, 256 * 1024, LogTime(0, i));
            o.addObject(0, i, 256 * 1024);
        }

        // 1GB, auto-incrementing, 64KB objects
        t = createAndAddTablet(tablets, 0, 1, 0, -1);
        for (uint64_t i = 0; i < (1UL * 1024 * 1024) / 64; i++) {
            t->profiler.track(i, 64 * 1024, LogTime(0, i));
            o.addObject(1, i, 64 * 1024);
        }

        // 12GB, auto-incrementing, 512KB objects
        t = createAndAddTablet(tablets, 0, 2, 0, -1);
        for (uint64_t i = 0; i < (12UL * 1024 * 1024) / 512; i++) {
            t->profiler.track(i, 512 * 1024, LogTime(0, i));
            o.addObject(2, i, 512 * 1024);
        }

        {
            Will w(tablets, maxBytesPerPartition, maxReferantsPerPartition);
            sanityCheckWill(w, o);
        }

        // Untrack half of the 12GB ones
        for (uint64_t i = 0; i < (12UL * 1024 * 1024) / 512; i += 2) {
            t->profiler.untrack(i, 512 * 1024, LogTime(0, i));
            o.removeObject(2, i);
        }

        {
            Will w(tablets, maxBytesPerPartition, maxReferantsPerPartition);
            sanityCheckWill(w, o);
        }
    }

    // Try very small objects to test the referant limit, rather
    // than the byte limit.
    void
    test_Will_endToEnd_2()
    {
        ProtoBuf::Tablets tablets;
        Table* t;
        Oracle o;

        const uint64_t maxBytesPerPartition = 640 * 1024 * 1024;
        const uint64_t maxReferantsPerPartition = 100 * 1000;

        // 5GB, auto-incrementing, 10KB objects
        t = createAndAddTablet(tablets, 0, 0, 0, -1);
        for (uint64_t i = 0; i < 200 * 1000; i++) {
            t->profiler.track(i, 1, LogTime(0, i));
            o.addObject(0, i, 1);
        }

        {
            Will w(tablets, maxBytesPerPartition, maxReferantsPerPartition);
            sanityCheckWill(w, o);
        }
    }

    // mix of random keys, sequential keys, and multi-field keys.
    // objects are randomly sized, uniformly from 128bytes to 64KB
    void
    test_Will_endToEnd_3()
    {
        ProtoBuf::Tablets tablets;
        Table* t;
        Oracle o;

        const uint64_t maxBytesPerPartition = 640 * 1024 * 1024;
        const uint64_t maxReferantsPerPartition = 10 * 1000 * 1000;

        // 2GB, random keys, avg. 128KB objects
        t = createAndAddTablet(tablets, 0, 0, 0, -1);
        for (uint64_t i = 0; i < 27307; i++) {
            uint64_t key = generateRandom();
            uint64_t bytes = ((key % 2048) + 1) * 128;
            t->profiler.track(key, bytes, LogTime(0, i));
            o.addObject(0, key, bytes);
        }

        // 2GB, sequential keys, avg. 128KB objects
        t = createAndAddTablet(tablets, 0, 1, 0, -1);
        for (uint64_t i = 0; i < 27307; i++) {
            uint64_t bytes = ((generateRandom() % 2048) + 1) * 128;
            t->profiler.track(i, bytes, LogTime(1, i));
            o.addObject(1, i, bytes);
        }

        // 2GB, multi-field (2) keys, avg. 128KB objects
        t = createAndAddTablet(tablets, 0, 2, 0, -1);
        for (uint64_t i = 0; i < 27307; i++) {
            uint64_t key = i / 2;
            if (i & 1)
                key |= (1UL << 60);
            uint64_t bytes = ((generateRandom() % 2048) + 1) * 128;
            t->profiler.track(key, bytes, LogTime(2, i));
            o.addObject(2, key, bytes);
        }

        {
            Will w(tablets, maxBytesPerPartition, maxReferantsPerPartition);
            sanityCheckWill(w, o);
        }
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(WillTest);

} // namespace RAMCloud
