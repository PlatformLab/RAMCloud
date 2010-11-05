/* Copyright (c) 2009-2010 Stanford University
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

/*
 * NB: This file makes use of the ObjectMap typedef, which is just
 *     a HashTable templated on the Object type. Don't be confused
 *     when you don't see any "HashTable<...>" types lying around.
 */

#include "TestUtil.h"

#include "HashTable.h"
#include "Object.h"

namespace RAMCloud {

/**
 * Unit tests for HashTable::PerfDistribution.
 */
class HashTablePerfDistributionTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(HashTablePerfDistributionTest); // NOLINT

    CPPUNIT_TEST_SUITE(HashTablePerfDistributionTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_storeSample);
    CPPUNIT_TEST_SUITE_END();

  public:
    HashTablePerfDistributionTest() {}

    void test_constructor()
    {
        RAMCloud::ObjectMap::PerfDistribution d;
        CPPUNIT_ASSERT_EQUAL(~0UL, d.min);
        CPPUNIT_ASSERT_EQUAL(0UL, d.max);
        CPPUNIT_ASSERT_EQUAL(0UL, d.binOverflows);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[0]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[1]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[2]);
    }

    void test_storeSample()
    {
        ObjectMap::PerfDistribution d;

        // You can't use CPPUNIT_ASSERT_EQUAL here because it tries to take a
        // reference to BIN_WIDTH. See 10.4.6.2 Member Constants of The C++
        // Programming Language by Bjarne Stroustrup for more about static
        // constant integers.
        CPPUNIT_ASSERT(10 == ObjectMap::PerfDistribution::BIN_WIDTH);

        d.storeSample(3);
        CPPUNIT_ASSERT_EQUAL(3UL, d.min);
        CPPUNIT_ASSERT_EQUAL(3UL, d.max);
        CPPUNIT_ASSERT_EQUAL(0UL, d.binOverflows);
        CPPUNIT_ASSERT_EQUAL(1UL, d.bins[0]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[1]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[2]);

        d.storeSample(3);
        d.storeSample(d.NBINS * d.BIN_WIDTH + 40);
        d.storeSample(12);
        d.storeSample(78);

        CPPUNIT_ASSERT_EQUAL(3UL, d.min);
        CPPUNIT_ASSERT_EQUAL(d.NBINS * d.BIN_WIDTH + 40, d.max);
        CPPUNIT_ASSERT_EQUAL(1UL, d.binOverflows);
        CPPUNIT_ASSERT_EQUAL(2UL, d.bins[0]);
        CPPUNIT_ASSERT_EQUAL(1UL, d.bins[1]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[2]);
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(HashTablePerfDistributionTest);


/**
 * Unit tests for HashTable::Entry.
 */
class HashTableEntryTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(HashTableEntryTest); // NOLINT

    CPPUNIT_TEST_SUITE(HashTableEntryTest);
    CPPUNIT_TEST(test_size);
    CPPUNIT_TEST(test_pack);
    CPPUNIT_TEST(test_clear);
    CPPUNIT_TEST(test_setReferant);
    CPPUNIT_TEST(test_setChainPointer);
    CPPUNIT_TEST(test_isAvailable);
    CPPUNIT_TEST(test_getReferant);
    CPPUNIT_TEST(test_getChainPointer);
    CPPUNIT_TEST(test_hashMatches);
    CPPUNIT_TEST_SUITE_END();

    /**
     * Return whether fields make it through #HashTable::Entry::pack() and
     * #HashTable::Entry::unpack() successfully.
     * \param hash
     *      See #HashTable::Entry::pack().
     * \param chain
     *      See #HashTable::Entry::pack().
     * \param ptr
     *      See #HashTable::Entry::pack().
     * \return
     *      Whether the fields out of #HashTable::Entry::unpack() are the same.
     */
    static bool
    packable(uint64_t hash, bool chain, uint64_t ptr)
    {
        ObjectMap::Entry e;

        ObjectMap::Entry::UnpackedEntry in;
        ObjectMap::Entry::UnpackedEntry out;

        in.hash = hash;
        in.chain = chain;
        in.ptr = ptr;

        e.pack(in.hash, in.chain, in.ptr);
        out = e.unpack();

        return (in.hash == out.hash &&
                in.chain == out.chain &&
                in.ptr == out.ptr);
    }

  public:
    HashTableEntryTest() {}

    void test_size()
    {
        CPPUNIT_ASSERT(8 == sizeof(ObjectMap::Entry));
    }

    void test_pack() // also tests unpack
    {
        CPPUNIT_ASSERT(packable(0x0000UL, false, 0x000000000000UL));
        CPPUNIT_ASSERT(packable(0xffffUL, true,  0x7fffffffffffUL));
        CPPUNIT_ASSERT(packable(0xffffUL, false, 0x7fffffffffffUL));
        CPPUNIT_ASSERT(packable(0xa257UL, false, 0x3cdeadbeef98UL));
    }

    // No tests for test_unpack, since test_pack tested it.

    void test_clear()
    {
        ObjectMap::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        e.clear();
        ObjectMap::Entry::UnpackedEntry out;
        out = e.unpack();
        CPPUNIT_ASSERT_EQUAL(0UL, out.hash);
        CPPUNIT_ASSERT_EQUAL(false, out.chain);
        CPPUNIT_ASSERT_EQUAL(0UL, out.ptr);
    }

    void test_setReferant()
    {
        ObjectMap::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        e.setReferant(0xaaaaUL,
                        reinterpret_cast<const Object*>(0x7fffffffffffUL));
        ObjectMap::Entry::UnpackedEntry out;
        out = e.unpack();
        CPPUNIT_ASSERT_EQUAL(0xaaaaUL, out.hash);
        CPPUNIT_ASSERT_EQUAL(false, out.chain);
        CPPUNIT_ASSERT_EQUAL(0x7fffffffffffUL, out.ptr);
    }

    void test_setChainPointer()
    {
        ObjectMap::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        {
            ObjectMap::CacheLine *cl;
            cl = reinterpret_cast<ObjectMap::CacheLine*>(
                0x7fffffffffffUL);
            e.setChainPointer(cl);
        }
        ObjectMap::Entry::UnpackedEntry out;
        out = e.unpack();
        CPPUNIT_ASSERT_EQUAL(0UL, out.hash);
        CPPUNIT_ASSERT_EQUAL(true, out.chain);
        CPPUNIT_ASSERT_EQUAL(0x7fffffffffffUL, out.ptr);
    }

    void test_isAvailable()
    {
        ObjectMap::Entry e;
        e.clear();
        CPPUNIT_ASSERT(e.isAvailable());
        e.setChainPointer(reinterpret_cast<ObjectMap::CacheLine*>(
            0x1UL));
        CPPUNIT_ASSERT(!e.isAvailable());
        e.setReferant(0UL, reinterpret_cast<const Object*>(0x1UL));
        CPPUNIT_ASSERT(!e.isAvailable());
        e.clear();
        CPPUNIT_ASSERT(e.isAvailable());
    }

    void test_getReferant()
    {
        ObjectMap::Entry e;
        const Object *o = reinterpret_cast<const Object*>(0x7fffffffffffUL);
        e.setReferant(0xaaaaUL, o);
        CPPUNIT_ASSERT_EQUAL(o, e.getReferant());
    }

    void test_getChainPointer()
    {
        ObjectMap::CacheLine *cl;
        cl = reinterpret_cast<ObjectMap::CacheLine*>(0x7fffffffffffUL);
        ObjectMap::Entry e;
        e.setChainPointer(cl);
        CPPUNIT_ASSERT_EQUAL(cl, e.getChainPointer());
        e.clear();
        CPPUNIT_ASSERT(NULL == e.getChainPointer());
        e.setReferant(0UL, reinterpret_cast<const Object*>(0x1UL));
        CPPUNIT_ASSERT(NULL == e.getChainPointer());
    }

    void test_hashMatches()
    {
        ObjectMap::Entry e;
        e.clear();
        CPPUNIT_ASSERT(!e.hashMatches(0UL));
        e.setChainPointer(reinterpret_cast<ObjectMap::CacheLine*>(
            0x1UL));
        CPPUNIT_ASSERT(!e.hashMatches(0UL));
        e.setReferant(0UL, reinterpret_cast<const Object*>(0x1UL));
        CPPUNIT_ASSERT(e.hashMatches(0UL));
        CPPUNIT_ASSERT(!e.hashMatches(0xbeefUL));
        e.setReferant(0xbeefUL, reinterpret_cast<const Object*>(0x1UL));
        CPPUNIT_ASSERT(!e.hashMatches(0UL));
        CPPUNIT_ASSERT(e.hashMatches(0xbeefUL));
        CPPUNIT_ASSERT(!e.hashMatches(0xfeedUL));
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(HashTableEntryTest);

/**
 * Unit tests for HashTable.
 */
class HashTableTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(HashTableTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_constructor_truncate);
    CPPUNIT_TEST(test_destructor);
    CPPUNIT_TEST(test_simple);
    CPPUNIT_TEST(test_multiTable);
    CPPUNIT_TEST(test_hash);
    CPPUNIT_TEST(test_findBucket);
    CPPUNIT_TEST(test_lookupEntry_notFound);
    CPPUNIT_TEST(test_lookupEntry_cacheLine0Entry0);
    CPPUNIT_TEST(test_lookupEntry_cacheLine0Entry7);
    CPPUNIT_TEST(test_lookupEntry_cacheLine2Entry0);
    CPPUNIT_TEST(test_lookupEntry_hashCollision);
    CPPUNIT_TEST(test_lookup);
    CPPUNIT_TEST(test_remove);
    CPPUNIT_TEST(test_replace_normal);
    CPPUNIT_TEST(test_replace_cacheLine0Entry0);
    CPPUNIT_TEST(test_replace_cacheLine0Entry7);
    CPPUNIT_TEST(test_replace_cacheLine2Entry0);
    CPPUNIT_TEST(test_replace_cacheLineFull);
    CPPUNIT_TEST(test_forEach);
    CPPUNIT_TEST_SUITE_END();
    DISALLOW_COPY_AND_ASSIGN(HashTableTest); //NOLINT

    // convenient abbreviation
#define seven (ObjectMap::ENTRIES_PER_CACHE_LINE - 1)

    /**
     * Insert an array of values into a single-bucket hash table.
     * \param[in] ht
     *      A hash table with a single bucket.
     * \param[in] values
     *      An array of values to add to the bucket (in order). These need not
     *      be initialized and will be set counting up from 0.
     * \param[in] tableId
     *      The table ID to use for each object inserted.
     * \param[in] numEnt
     *      The number of values in \a values.
     * \param[in] cacheLines
     *      An array of cache lines to back the bucket with. 
     * \param[in] numCacheLines
     *      The number of cache lines in \a cacheLines.
     */
    void insertArray(ObjectMap *ht, Object *values, uint64_t tableId,
                     uint64_t numEnt, ObjectMap::CacheLine *cacheLines,
                     uint64_t numCacheLines)
    {
        ObjectMap::CacheLine *cl;

        // clear out all the cache lines
        cl = &cacheLines[0];
        while (cl < &cacheLines[numCacheLines]) {
            for (uint64_t j = 0; j < ht->entriesPerCacheLine(); j++)
                cl->entries[j].clear();
            cl++;
        }

        // chain all the cache lines
        cl = &cacheLines[0];
        while (cl < &cacheLines[numCacheLines - 1]) {
            cl->entries[seven].setChainPointer(cl + 1);
            cl++;
        }

        // fill in the "log" entries
        for (uint64_t i = 0; i < numEnt; i++) {
            values[i].table = tableId;
            values[i].id = i;

            uint64_t littleHash;
            (void) ht->findBucket(0, i, &littleHash);

            ObjectMap::Entry *entry;
            if (0 < i && i == numEnt - 1 && i % seven == 0)
                entry = &cacheLines[i / seven - 1].entries[seven];
            else
                entry = &cacheLines[i / seven].entries[i % seven];
            entry->setReferant(littleHash, &values[i]);
        }

        ht->buckets = cacheLines;
    }

    /**
     * Automatically sets the HashTable::buckets to \c NULL when it goes out of
     * scope.
     */
    class AutoTearDown {
      public:
        explicit AutoTearDown(ObjectMap *ht) : ht(ht) {}
        ~AutoTearDown() { ht->buckets = NULL; }
      private:
        ObjectMap *ht;
        DISALLOW_COPY_AND_ASSIGN(AutoTearDown);
    };

    /**
     * Common setup code for the lookupEntry and insert tests.
     * This mostly declares variables on the stack, so it's a macro.
     * \li \a numEnt is set to \a _numEnt
     * \li \a numCacheLines is the number of cache lines used to hold the
     * entries.
     * \li \a ht is a hash table of one bucket.
     * \li \a values is an array of \a numEnt objects.
     * \li \a cacheLines is an array of \a numCacheLines cache lines referring
     * to the objects in \a values. These cache lines make up \a ht's bucket.
     * \param _tableId
     *      The table id to use for all objects placed in the hashtable.
     * \param _numEnt
     *      The number of entries to place in the hashtable.
     */
#define SETUP(_tableId, _numEnt)  \
    uint64_t tableId = _tableId; \
    uint64_t numEnt = _numEnt; \
    uint64_t numCacheLines; \
    numCacheLines = ((numEnt + ObjectMap::ENTRIES_PER_CACHE_LINE - 2) /\
                              (ObjectMap::ENTRIES_PER_CACHE_LINE - 1));\
    if (numCacheLines == 0) \
        numCacheLines = 1; \
    ObjectMap ht(1); \
    Object values[numEnt]; \
    ObjectMap::CacheLine cacheLines[numCacheLines]; \
    insertArray(&ht, values, tableId, numEnt, cacheLines, numCacheLines); \
    AutoTearDown _atd(&ht)

    /**
     * Create an Object with no data on the stack.
     * \param n
     *      The variable name for the object.
     * \param t
     *      The table ID for the object.
     * \param k
     *      The object ID for the object.
     */
#define DECL_OBJECT(n, t, k) \
    Object n(sizeof(Object)); \
    n.table = (t); \
    n.id = (k)

#define NULL_OBJECT (static_cast<const Object*>(NULL))

    /**
     * Find an entry in a single-bucket hash table by position.
     * \param[in] ht
     *      A hash table with a single bucket.
     * \param[in] x
     *      The number of the cache line in the chain, starting from 0.
     * \param[in] y
     *      The number of the entry in the cache line, starting from 0.
     * \return
     *      The entry at \a x and \a y in the only bucket of \a ht.
     */
    ObjectMap::Entry& entryAt(ObjectMap *ht, uint64_t x,
        uint64_t y)
    {
        ObjectMap::CacheLine *cl = &ht->buckets[0];
        while (x > 0) {
            cl = cl->entries[seven].getChainPointer();
            x--;
        }
        return cl->entries[y];
    }

    /**
     * Ensure an entry in a single-bucket hash table contains a given pointer.
     * \param[in] ht
     *      A hash table with a single bucket.
     * \param[in] x
     *      The number of the cache line in the chain, starting from 0.
     * \param[in] y
     *      The number of the entry in the cache line, starting from 0.
     * \param[in] ptr
     *      The pointer that we expect to find at the given position.
     */
    void assertEntryIs(ObjectMap *ht, uint64_t x, uint64_t y,
        const Object *ptr)
    {
        uint64_t littleHash;
        (void) ht->findBucket(0, ptr->id, &littleHash);
        ObjectMap::Entry& entry = entryAt(ht, x, y);
        CPPUNIT_ASSERT(entry.hashMatches(littleHash));
        CPPUNIT_ASSERT_EQUAL(ptr, entry.getReferant());
    }

    ObjectMap::Entry *findBucketAndLookupEntry(ObjectMap *ht,
                                               uint64_t tableId,
                                               uint64_t objectId)
    {
        uint64_t secondaryHash;
        ObjectMap::CacheLine *bucket;
        bucket = ht->findBucket(0, objectId, &secondaryHash);
        return ht->lookupEntry(bucket, secondaryHash, tableId, objectId);
    }

  public:

    HashTableTest()
    {
    }

    void test_constructor()
    {
        char buf[sizeof(ObjectMap) + 1024];
        memset(buf, 0xca, sizeof(buf));
        ObjectMap *ht = new(buf) ObjectMap(16);
        for (uint32_t i = 0; i < 16; i++) {
            for (uint32_t j = 0; j < ht->entriesPerCacheLine(); j++)
                CPPUNIT_ASSERT(ht->buckets[i].entries[j].isAvailable());
        }
    }

    void test_constructor_truncate()
    {
        // This is effectively testing nearestPowerOfTwo.
        CPPUNIT_ASSERT_EQUAL(1UL, ObjectMap(1).numBuckets);
        CPPUNIT_ASSERT_EQUAL(2UL, ObjectMap(2).numBuckets);
        CPPUNIT_ASSERT_EQUAL(2UL, ObjectMap(3).numBuckets);
        CPPUNIT_ASSERT_EQUAL(4UL, ObjectMap(4).numBuckets);
        CPPUNIT_ASSERT_EQUAL(4UL, ObjectMap(5).numBuckets);
        CPPUNIT_ASSERT_EQUAL(4UL, ObjectMap(6).numBuckets);
        CPPUNIT_ASSERT_EQUAL(4UL, ObjectMap(7).numBuckets);
        CPPUNIT_ASSERT_EQUAL(8UL, ObjectMap(8).numBuckets);
    }

    void test_destructor()
    {
        char buf[sizeof(ObjectMap) + 1024];
        ObjectMap *ht = new(buf) ObjectMap(16);
        ht->~HashTable();
        CPPUNIT_ASSERT(ht->buckets == NULL);
        ht->~HashTable();
        CPPUNIT_ASSERT(ht->buckets == NULL);
    }

    void test_simple()
    {
        ObjectMap ht(1024);

        DECL_OBJECT(a, 0, 0);
        DECL_OBJECT(b, 0, 10);

        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 0));
        ht.replace(0, 0, &a);
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&a), ht.lookup(0, 0));
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 10));
        ht.replace(0, 10, &b);
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&b), ht.lookup(0, 10));
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&a), ht.lookup(0, 0));
    }

    void test_multiTable()
    {
        ObjectMap ht(1024);

        DECL_OBJECT(a, 0, 0);
        DECL_OBJECT(b, 1, 0);
        DECL_OBJECT(c, 0, 1);

        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 0));
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(1, 0));
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 1));

        ht.replace(0, 0, &a);
        ht.replace(1, 0, &b);
        ht.replace(0, 1, &c);

        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&a), ht.lookup(0, 0));
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&b), ht.lookup(1, 0));
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&c), ht.lookup(0, 1));
    }

    /**
     * Ensure that #RAMCloud::HashTable::hash() generates hashes using the full
     * range of bits.
     */
    void test_hash()
    {
        uint64_t observedBits = 0UL;
        srand(1);
        for (uint32_t i = 0; i < 50; i++) {
            uint64_t input = generateRandom();
            observedBits |= ObjectMap::hash(input);
        }
        CPPUNIT_ASSERT_EQUAL(~0UL, observedBits);
    }

    void test_findBucket()
    {
        ObjectMap ht(1024);
        ObjectMap::CacheLine *bucket;
        uint64_t hashValue;
        uint64_t secondaryHash;
        bucket = ht.findBucket(0, 4327, &secondaryHash);
        hashValue = ObjectMap::hash(0) ^ ObjectMap::hash(4327);
        CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(bucket - ht.buckets),
                             (hashValue & 0x0000ffffffffffffffffUL) % 1024);
        CPPUNIT_ASSERT_EQUAL(secondaryHash, hashValue >> 48);
    }

    /**
     * Test #RAMCloud::HashTable::lookupEntry() when the object ID is not
     * found.
     */
    void test_lookupEntry_notFound()
    {
        {
            SETUP(0, 0);
            CPPUNIT_ASSERT_EQUAL(static_cast<ObjectMap::Entry*>(NULL),
                                 findBucketAndLookupEntry(&ht, 0, numEnt + 1));
            CPPUNIT_ASSERT_EQUAL(1UL, ht.getPerfCounters().lookupEntryCalls);
            CPPUNIT_ASSERT(ht.getPerfCounters().lookupEntryCycles > 0);
            CPPUNIT_ASSERT(ht.getPerfCounters().lookupEntryDist.max > 0);
        }
        {
            SETUP(0, ObjectMap::ENTRIES_PER_CACHE_LINE * 5);
            CPPUNIT_ASSERT_EQUAL(static_cast<ObjectMap::Entry*>(NULL),
                                 findBucketAndLookupEntry(&ht, 0, numEnt + 1));
            CPPUNIT_ASSERT_EQUAL(5UL,
                        ht.getPerfCounters().lookupEntryChainsFollowed);
        }
    }

    /**
     * Test #RAMCloud::HashTable::lookupEntry() when the object ID is found in
     * the first entry of the first cache line.
     */
    void test_lookupEntry_cacheLine0Entry0()
    {
        SETUP(0, 1);
        CPPUNIT_ASSERT_EQUAL(&entryAt(&ht, 0, 0),
                             findBucketAndLookupEntry(&ht, 0, 0));
    }

    /**
     * Test #RAMCloud::HashTable::lookupEntry() when the object ID is found in
     * the last entry of the first cache line.
     */
    void test_lookupEntry_cacheLine0Entry7()
    {
        SETUP(0, ObjectMap::ENTRIES_PER_CACHE_LINE);
        CPPUNIT_ASSERT_EQUAL(&entryAt(&ht, 0, seven),
                             findBucketAndLookupEntry(&ht, 0, seven));
    }

    /**
     * Test #RAMCloud::HashTable::lookupEntry() when the object ID is found in
     * the first entry of the third cache line.
     */
    void test_lookupEntry_cacheLine2Entry0()
    {
        SETUP(0, ObjectMap::ENTRIES_PER_CACHE_LINE * 5);

        // with 8 entries per cache line:
        // cl0: [ k00, k01, k02, k03, k04, k05, k06, cl1 ]
        // cl1: [ k07, k09, k09, k10, k11, k12, k13, cl2 ]
        // cl2: [ k14, k15, k16, k17, k18, k19, k20, cl3 ]
        // ...

        CPPUNIT_ASSERT_EQUAL(&entryAt(&ht, 2, 0),
                             findBucketAndLookupEntry(&ht, 0, seven * 2));
    }

    /**
     * Test #RAMCloud::HashTable::lookupEntry() when there is a hash collision
     * with another Entry.
     */
    void test_lookupEntry_hashCollision()
    {
        SETUP(0, 1);
        CPPUNIT_ASSERT_EQUAL(&entryAt(&ht, 0, 0),
                             findBucketAndLookupEntry(&ht, 0, 0));
        CPPUNIT_ASSERT(ht.getPerfCounters().lookupEntryDist.max > 0);
        values[0].id = 0x43324890UL;
        CPPUNIT_ASSERT_EQUAL(static_cast<ObjectMap::Entry*>(NULL),
                             findBucketAndLookupEntry(&ht, 0, 0));
        CPPUNIT_ASSERT_EQUAL(1UL,
                             ht.getPerfCounters().lookupEntryHashCollisions);
    }

    void test_lookup()
    {
        ObjectMap ht(1);
        DECL_OBJECT(v, 0, 83UL);
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 83UL));
        ht.replace(0, 83UL, &v);
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&v), ht.lookup(0, 83UL));
    }

    void test_remove()
    {
        ObjectMap ht(1);
        CPPUNIT_ASSERT(!ht.remove(0, 83UL));
        DECL_OBJECT(v, 0, 83UL);
        ht.replace(0, 83UL, &v);
        CPPUNIT_ASSERT(ht.remove(0, 83UL));
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 83UL));
        CPPUNIT_ASSERT(!ht.remove(0, 83UL));
    }

    void test_replace_normal()
    {
        ObjectMap ht(1);
        DECL_OBJECT(v, 0, 83UL);
        DECL_OBJECT(w, 0, 83UL);
        CPPUNIT_ASSERT(!ht.replace(0, 83UL, &v));
        CPPUNIT_ASSERT_EQUAL(1UL, ht.getPerfCounters().replaceCalls);
        CPPUNIT_ASSERT(ht.getPerfCounters().replaceCycles > 0);
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&v), ht.lookup(0, 83UL));
        CPPUNIT_ASSERT(ht.replace(0, 83UL, &v));
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&v), ht.lookup(0, 83UL));
        CPPUNIT_ASSERT(ht.replace(0, 83UL, &w));
        CPPUNIT_ASSERT_EQUAL(const_cast<const Object*>(&w), ht.lookup(0, 83UL));
    }

    /**
     * Test #RAMCloud::HashTable::replace() when the object ID is new and the
     * first entry of the first cache line is available.
     */
    void test_replace_cacheLine0Entry0()
    {
        SETUP(0, 0);
        DECL_OBJECT(v, 0, 83UL);
        ht.replace(0, 83UL, &v);
        assertEntryIs(&ht, 0, 0, &v);
    }

    /**
     * Test #RAMCloud::HashTable::replace() when the object ID is new and the
     * last entry of the first cache line is available.
     */
    void test_replace_cacheLine0Entry7()
    {
        SETUP(0, ObjectMap::ENTRIES_PER_CACHE_LINE - 1);
        DECL_OBJECT(v, 0, 83UL);
        ht.replace(0, 83UL, &v);
        assertEntryIs(&ht, 0, seven, &v);
    }

    /**
     * Test #RAMCloud::HashTable::replace() when the object ID is new and the
     * first entry of the third cache line is available. The third cache line
     * is already chained onto the second.
     */
    void test_replace_cacheLine2Entry0()
    {
        SETUP(0, ObjectMap::ENTRIES_PER_CACHE_LINE * 2);
        cacheLines[2].entries[0].clear();
        cacheLines[2].entries[1].clear();
        DECL_OBJECT(v, 0, 83UL);
        ht.replace(0, 83UL, &v);
        assertEntryIs(&ht, 2, 0, &v);
        CPPUNIT_ASSERT_EQUAL(2UL, ht.getPerfCounters().insertChainsFollowed);
    }

    /**
     * Test #RAMCloud::HashTable::replace() when the object ID is new and the
     * first and only cache line is full. The second cache line needs to be
     * allocated.
     */
    void test_replace_cacheLineFull()
    {
        SETUP(0, ObjectMap::ENTRIES_PER_CACHE_LINE);
        DECL_OBJECT(v, 0, 83UL);
        ht.replace(0, 83UL, &v);
        CPPUNIT_ASSERT(entryAt(&ht, 0, seven).getChainPointer() != NULL);
        CPPUNIT_ASSERT(entryAt(&ht, 0, seven).getChainPointer() !=
                       &cacheLines[1]);
        assertEntryIs(&ht, 1, 0, &values[seven]);
        assertEntryIs(&ht, 1, 1, &v);
    }

    struct ForEachTestStruct {
        uint64_t key1, key2, count;
    };

    /**
     * Callback used by test_forEach().
     */ 
    static void
    test_forEach_callback(const ForEachTestStruct *p, void *cookie)
    {
        CPPUNIT_ASSERT_EQUAL(cookie, reinterpret_cast<void *>(57));
        const_cast<ForEachTestStruct *>(p)->count++;
    }

    /**
     * Simple test for #RAMCloud::HashTable::forEach(), ensuring that it
     * properly traverses multiple buckets and chained cachelines.
     */
    void test_forEach()
    {
        HashTable<ForEachTestStruct, &ForEachTestStruct::key1,
            &ForEachTestStruct::key2> ht(2);
        ForEachTestStruct checkoff[256];
        memset(checkoff, 0, sizeof(checkoff));

        for (uint32_t i = 0; i < sizeof(checkoff) / sizeof(checkoff[0]); i++) {
            checkoff[i].key1 = 0;
            checkoff[i].key2 = i;
            ht.replace(0, i, &checkoff[i]);
        }

        uint64_t t = ht.forEach(test_forEach_callback,
            reinterpret_cast<void *>(57));
        CPPUNIT_ASSERT_EQUAL(sizeof(checkoff) / sizeof(checkoff[0]), t);

        for (uint32_t i = 0; i < sizeof(checkoff) / sizeof(checkoff[0]); i++)
            CPPUNIT_ASSERT_EQUAL(1, checkoff[i].count);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(HashTableTest);

} // namespace RAMCloud
