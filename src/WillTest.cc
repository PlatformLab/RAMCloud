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
class WillTest : public ::testing::Test {
  public:
    TestLog::Enable* logEnabler;

    WillTest() : logEnabler(NULL)
    {
        logEnabler = new TestLog::Enable();
    }

    ~WillTest()
    {
        delete logEnabler;
    }

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

  private:
    DISALLOW_COPY_AND_ASSIGN(WillTest);
};

TEST_F(WillTest, Will_constructor) {
    ProtoBuf::Tablets tablets;
    Will w(tablets, 1000, 1001);
    EXPECT_EQ(0U, w.currentId);
    EXPECT_EQ(0U, w.currentMaxBytes);
    EXPECT_EQ(0U, w.currentCount);
    EXPECT_EQ(1000U, w.maxBytesPerPartition);
    EXPECT_EQ(1001U, w.maxReferentsPerPartition);
    EXPECT_EQ(0U, w.entries.size());
}

TEST_F(WillTest, Will_serialize) {
    ProtoBuf::Tablets tablets;
    Table *t = createAndAddTablet(tablets, 0, 0, 0, -1);
    t->profiler.track(0, 5000, LogTime(0, 0));
    t->profiler.track(-1, 1500, LogTime(0, 1));
    createAndAddTablet(tablets, 0, 23, 0, -1);
    Will w(tablets, 5000, 10);

    ProtoBuf::Tablets will;
    w.serialize(will);
    EXPECT_EQ(3, will.tablet_size());
    EXPECT_EQ(0U, will.tablet(0).table_id());
    EXPECT_EQ(0U, will.tablet(0).start_object_id());
    EXPECT_EQ(0x00ffffffffffffffUL,
        will.tablet(0).end_object_id());
    EXPECT_EQ(ProtoBuf::Tablets_Tablet_State_NORMAL,
        will.tablet(0).state());
    EXPECT_EQ(0U, will.tablet(0).user_data());

    EXPECT_EQ(0U, will.tablet(1).table_id());
    EXPECT_EQ(0x0100000000000000UL,
        will.tablet(1).start_object_id());
    EXPECT_EQ(0xffffffffffffffffUL,
        will.tablet(1).end_object_id());
    EXPECT_EQ(ProtoBuf::Tablets_Tablet_State_NORMAL,
        will.tablet(1).state());
    EXPECT_EQ(1U, will.tablet(1).user_data());

    EXPECT_EQ(23U, will.tablet(2).table_id());
    EXPECT_EQ(1U, will.tablet(2).user_data());
}

TEST_F(WillTest, Will_debugDump) {
    ProtoBuf::Tablets tablets;
    Table *t = createAndAddTablet(tablets, 0, 0, 0, -1);
    t->profiler.track(0, 5000, LogTime(0, 0));
    t->profiler.track(-1, 1500, LogTime(0, 1));
    Will w(tablets, 5000, 10);
    w.debugDump();
    EXPECT_EQ("debugDump:                                      "
        "  L A S T    W I L L    A N D    T E S T A M E N T | debugDump: -"
        "-----------------------------------------------------------------"
        "----------------------------------------------------------- | deb"
        "ugDump: Partition             TableId            FirstKey        "
        "     LastKey     MinBytes     MaxBytes   MinReferents   MaxRefere"
        "nts | debugDump: ------------------------------------------------"
        "-----------------------------------------------------------------"
        "------------ | debugDump:         0  0x0000000000000000  0x000000"
        "0000000000  0x00ffffffffffffff          4KB          4KB         "
        "     1              1 | debugDump:         1  0x0000000000000000 "
        " 0x0100000000000000  0xffffffffffffffff          1KB          1KB"
        "              1              1", TestLog::get());
    destroyTables(tablets);
}

TEST_F(WillTest, Will_addTablet) {
    // it's unclear how to test this, since it's such a simple method.
    // suggestions appreciated.
}

TEST_F(WillTest, Will_addPartition) {
    ProtoBuf::Tablets emptyTablets;
    Will w(emptyTablets, 5000, 50);

    // add a small first partition
    {
        ProtoBuf::Tablets::Tablet tablet = createTablet(0, 97, 500, 10000);
        Partition p(0, -1, 100, 150, 10, 20);
        w.addPartition(p, tablet);
    }

    EXPECT_EQ(0U, w.entries[0].partitionId);
    EXPECT_EQ(97U, w.entries[0].tableId);
    // ensure that the tablet restricts the key range
    EXPECT_EQ(500U, w.entries[0].firstKey);
    EXPECT_EQ(10000U, w.entries[0].lastKey);
    EXPECT_EQ(100U, w.entries[0].minBytes);
    EXPECT_EQ(150U, w.entries[0].maxBytes);
    EXPECT_EQ(10U, w.entries[0].minReferents);
    EXPECT_EQ(20U, w.entries[0].maxReferents);
    EXPECT_EQ(150U, w.currentMaxBytes);
    EXPECT_EQ(20U, w.currentMaxReferents);
    EXPECT_EQ(0U, w.currentId);
    EXPECT_EQ(1U, w.currentCount);

    // add another partition that doesn't go over the max
    {
        ProtoBuf::Tablets::Tablet tablet = createTablet(0, 12, 0, -1);
        Partition p(0, -1, 4000, 4849, 10, 20);
        w.addPartition(p, tablet);
    }

    EXPECT_EQ(0U, w.currentId);
    EXPECT_EQ(2U, w.currentCount);

    // now exceed the byte maximum
    {
        ProtoBuf::Tablets::Tablet tablet = createTablet(0, 15, 0, -1);
        Partition p(0, -1, 2, 2, 10, 10);
        w.addPartition(p, tablet);
    }

    EXPECT_EQ(1U, w.currentId);
    EXPECT_EQ(1U, w.currentCount);
    EXPECT_EQ(2U, w.currentMaxBytes);
    EXPECT_EQ(10U, w.currentMaxReferents);

    // now exceed the referant maximum
    {
        ProtoBuf::Tablets::Tablet tablet = createTablet(0, 12, 0, -1);
        Partition p(0, -1, 3, 3, 49, 49);
        w.addPartition(p, tablet);
    }

    EXPECT_EQ(2U, w.currentId);
    EXPECT_EQ(1U, w.currentCount);
    EXPECT_EQ(3U, w.currentMaxBytes);
    EXPECT_EQ(49U, w.currentMaxReferents);
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
    getReferentsInRange(uint64_t firstKey, uint64_t lastKey)
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
    getReferentsInRange(uint64_t table, uint64_t firstKey, uint64_t lastKey)
    {
        return tabletMap[table].getReferentsInRange(firstKey, lastKey);
    }

    private:
    boost::unordered_map<uint64_t, TabletOracle> tabletMap;
};

void
sanityCheckWill(Will& w, Oracle& o)
{
    uint64_t totalMinBytes = 0;
    uint64_t totalMaxBytes = 0;
    uint64_t totalMinReferents = 0;
    uint64_t totalMaxReferents = 0;
    uint64_t totalRealBytes = 0;
    uint64_t totalRealReferents = 0;
    uint64_t lastId = 0;
    for (unsigned int i = 0; i < w.entries.size(); i++) {
        Will::WillEntry* we = &w.entries[i];

        uint64_t realBytes = o.getBytesInRange(we->tableId,
            we->firstKey, we->lastKey);
        uint64_t realReferents = o.getReferentsInRange(we->tableId,
            we->firstKey, we->lastKey);

#if 0
        fprintf(stderr, "part %lu, tbl %lu, fk 0x%016lx, lk 0x%016lx\n",
            we->partitionId, we->tableId, we->firstKey, we->lastKey);
        fprintf(stderr, "  we->maxBytes: %lu, realBytes: %lu\n\n",
            we->maxBytes, realBytes);
#endif

        if (we->partitionId != lastId) {
            EXPECT_TRUE(totalRealBytes <= totalMaxBytes);
            EXPECT_TRUE(labs(totalMaxBytes - totalRealBytes) <=
                (int64_t)TabletProfiler::getMaximumByteError());
            EXPECT_TRUE(totalRealReferents <= totalMaxReferents);
            EXPECT_TRUE(labs(totalMaxReferents - totalRealReferents) <=
                (int64_t)TabletProfiler::getMaximumReferentError());

            totalMinBytes = 0;
            totalMaxBytes = 0;
            totalMinReferents = 0;
            totalMaxReferents = 0;
            totalRealBytes = 0;
            totalRealReferents = 0;
            lastId = we->partitionId;
        }

        EXPECT_TRUE(we->minBytes <= realBytes);
        EXPECT_TRUE(we->maxBytes >= realBytes);
        EXPECT_TRUE(we->minReferents <= realReferents);
        EXPECT_TRUE(we->maxReferents >= realReferents);

        totalMinBytes += we->minBytes;
        totalMaxBytes += we->maxBytes;
        totalMinReferents += we->minReferents;
        totalMaxReferents += we->maxReferents;
        totalRealBytes += realBytes;
        totalRealReferents += realReferents;
    }
    EXPECT_TRUE(totalRealBytes <= totalMaxBytes);
    EXPECT_TRUE(labs(totalMaxBytes - totalRealBytes) <=
        (int64_t)TabletProfiler::getMaximumByteError());
    EXPECT_TRUE(totalRealReferents <= totalMaxReferents);
    EXPECT_TRUE(labs(totalMaxReferents - totalRealReferents) <=
        (int64_t)TabletProfiler::getMaximumReferentError());
}

// Simple test that writes 18GB of objects with incrementing
// keys, computes a will, then deletes 6GB and recomputes.
TEST_F(WillTest, Will_endToEnd) {
    ProtoBuf::Tablets tablets;
    Table* t;
    Oracle o;

    const uint64_t maxBytesPerPartition = 640 * 1024 * 1024;
    const uint64_t maxReferentsPerPartition = 10 * 1000 * 1000;

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
        Will w(tablets, maxBytesPerPartition, maxReferentsPerPartition);
        sanityCheckWill(w, o);
    }

    // Untrack half of the 12GB ones
    for (uint64_t i = 0; i < (12UL * 1024 * 1024) / 512; i += 2) {
        t->profiler.untrack(i, 512 * 1024, LogTime(0, i));
        o.removeObject(2, i);
    }

    {
        Will w(tablets, maxBytesPerPartition, maxReferentsPerPartition);
        sanityCheckWill(w, o);
    }
}

// Try very small objects to test the referant limit, rather
// than the byte limit.
TEST_F(WillTest, Will_endToEnd_2) {
    ProtoBuf::Tablets tablets;
    Table* t;
    Oracle o;

    const uint64_t maxBytesPerPartition = 640 * 1024 * 1024;
    const uint64_t maxReferentsPerPartition = 100 * 1000;

    // 5GB, auto-incrementing, 10KB objects
    t = createAndAddTablet(tablets, 0, 0, 0, -1);
    for (uint64_t i = 0; i < 200 * 1000; i++) {
        t->profiler.track(i, 1, LogTime(0, i));
        o.addObject(0, i, 1);
    }

    {
        Will w(tablets, maxBytesPerPartition, maxReferentsPerPartition);
        sanityCheckWill(w, o);
    }
}

// mix of random keys, sequential keys, and multi-field keys.
// objects are randomly sized, uniformly from 128bytes to 64KB
TEST_F(WillTest, Will_endToEnd_3) {
    ProtoBuf::Tablets tablets;
    Table* t;
    Oracle o;

    const uint64_t maxBytesPerPartition = 640 * 1024 * 1024;
    const uint64_t maxReferentsPerPartition = 10 * 1000 * 1000;

    // 2GB, random keys, avg. 128KB objects
    t = createAndAddTablet(tablets, 0, 0, 0, -1);
    for (uint64_t i = 0; i < 27307; i++) {
        uint64_t key = generateRandom();
        uint64_t bytes = ((key % 2048) + 1) * 128;
        t->profiler.track(key,
                            downCast<uint32_t>(bytes),
                            LogTime(0, i));
        o.addObject(0, key, bytes);
    }

    // 2GB, sequential keys, avg. 128KB objects
    t = createAndAddTablet(tablets, 0, 1, 0, -1);
    for (uint64_t i = 0; i < 27307; i++) {
        uint64_t bytes = ((generateRandom() % 2048) + 1) * 128;
        t->profiler.track(i,
                            downCast<uint32_t>(bytes),
                            LogTime(1, i));
        o.addObject(1, i, bytes);
    }

    // 2GB, multi-field (2) keys, avg. 128KB objects
    t = createAndAddTablet(tablets, 0, 2, 0, -1);
    for (uint64_t i = 0; i < 27307; i++) {
        uint64_t key = i / 2;
        if (i & 1)
            key |= (1UL << 60);
        uint64_t bytes = ((generateRandom() % 2048) + 1) * 128;
        t->profiler.track(key,
                            downCast<uint32_t>(bytes),
                            LogTime(2, i));
        o.addObject(2, key, bytes);
    }

    {
        Will w(tablets, maxBytesPerPartition, maxReferentsPerPartition);
        sanityCheckWill(w, o);
    }
}

} // namespace RAMCloud
