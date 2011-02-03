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

#include "TestUtil.h"

#include "HashTable.h"

namespace RAMCloud {

class TestObject {
  public:
    TestObject() : _key1(0), _key2(0) {}
    TestObject(uint64_t key1, uint64_t key2) : _key1(key1), _key2(key2) {}
    uint64_t key1() const { return _key1; }
    uint64_t key2() const { return _key2; }
    uint64_t _key1, _key2;
};

typedef HashTable<TestObject*> TestObjectMap;

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
        RAMCloud::TestObjectMap::PerfDistribution d;
        CPPUNIT_ASSERT_EQUAL(~0UL, d.min);
        CPPUNIT_ASSERT_EQUAL(0UL, d.max);
        CPPUNIT_ASSERT_EQUAL(0UL, d.binOverflows);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[0]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[1]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[2]);
    }

    void test_storeSample()
    {
        TestObjectMap::PerfDistribution d;

        // You can't use CPPUNIT_ASSERT_EQUAL here because it tries to take a
        // reference to BIN_WIDTH. See 10.4.6.2 Member Constants of The C++
        // Programming Language by Bjarne Stroustrup for more about static
        // constant integers.
        CPPUNIT_ASSERT(10 == TestObjectMap::PerfDistribution::BIN_WIDTH);

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
    CPPUNIT_TEST(test_trivial_clear);
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
     * \param type
     *      See #HashTable::Entry::pack().
     * \param typeBits
     *      See #HashTable::Entry::pack().
     * \return
     *      Whether the fields out of #HashTable::Entry::unpack() are the same.
     */
    static bool
    packable(uint64_t hash, bool chain, uint64_t ptr, uint8_t type,
        uint32_t typeBits)
    {
        TestObjectMap::Entry e;

        TestObjectMap::Entry::UnpackedEntry in;
        TestObjectMap::Entry::UnpackedEntry out;

        in.hash = hash;
        in.chain = chain;
        in.type = type;
        in.ptr = ptr;

        e.pack(in.hash, in.chain, in.ptr, type, typeBits);
        out = e.unpack(typeBits);

        return (in.hash == out.hash &&
                in.chain == out.chain &&
                in.type == out.type &&
                in.ptr == out.ptr);
    }

  public:
    HashTableEntryTest() {}

    void test_size()
    {
        CPPUNIT_ASSERT(8 == sizeof(TestObjectMap::Entry));
    }

    void test_pack() // also tests unpack
    {
        // first without any type bits used
        CPPUNIT_ASSERT(packable(0x0000UL, false, 0x000000000000UL, 0, 0));
        CPPUNIT_ASSERT(packable(0xffffUL, true,  0x7fffffffffffUL, 0, 0));
        CPPUNIT_ASSERT(packable(0xffffUL, false, 0x7fffffffffffUL, 0, 0));
        CPPUNIT_ASSERT(packable(0xa257UL, false, 0x3cdeadbeef98UL, 0, 0));

        // now test that type bits work as planned
        CPPUNIT_ASSERT(packable(0x0000UL, false, 0x000000000000UL, 1, 1));
        CPPUNIT_ASSERT(packable(0xffffUL, true,  0x0fffffffffffUL, 2, 3));
        CPPUNIT_ASSERT(packable(0xffffUL, false, 0x007fffffffffUL, 5, 8));
        CPPUNIT_ASSERT(packable(0xa257UL, false, 0x0f37ab6fbbe6UL, 0, 2));

        // and now test the exception cases of pack()
        TestObjectMap::Entry e;
        CPPUNIT_ASSERT_THROW(e.pack(0, 0, 0, 4, 2), Exception);
        CPPUNIT_ASSERT_NO_THROW(e.pack(0, 0, 0, 3, 2));
        CPPUNIT_ASSERT_THROW(e.pack(0, 0, 0, 1,
            TestObjectMap::MAX_TYPEBITS + 1), Exception);
        CPPUNIT_ASSERT_NO_THROW(e.pack(0, 0, 0, 1,
            TestObjectMap::MAX_TYPEBITS));
        CPPUNIT_ASSERT_THROW(e.pack(0, false, 0x7fffffffffffUL, 0, 1),
            Exception);     // stack addresses won't fit! ugh.
    }

    // No tests for test_unpack, since test_pack tested it.

    void test_clear()
    {
        TestObjectMap::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        e.clear();
        TestObjectMap::Entry::UnpackedEntry out;
        out = e.unpack(0);
        CPPUNIT_ASSERT_EQUAL(0UL, out.hash);
        CPPUNIT_ASSERT_EQUAL(false, out.chain);
        CPPUNIT_ASSERT_EQUAL(0UL, out.ptr);
    }

    void test_trivial_clear()
    {
        TestObjectMap::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        e.clear();
        TestObjectMap::Entry f;
        f.value = 0xdeadbeefdeadbeefUL;
        f.pack(0, false, 0);
        CPPUNIT_ASSERT_EQUAL(e.value, f.value);
    }

    void test_setReferant()
    {
        TestObjectMap::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        e.setReferant(0xaaaaUL, reinterpret_cast<TestObject*>(
            0x7fffffffffffUL), 0, 0);
        TestObjectMap::Entry::UnpackedEntry out;
        out = e.unpack(0);
        CPPUNIT_ASSERT_EQUAL(0xaaaaUL, out.hash);
        CPPUNIT_ASSERT_EQUAL(false, out.chain);
        CPPUNIT_ASSERT_EQUAL(0x7fffffffffffUL, out.ptr);
        CPPUNIT_ASSERT_EQUAL(0, out.type);

        e.setReferant(0xaaaaUL, reinterpret_cast<TestObject*>(
            0x3fffffffffffUL), 1, 1);
        out = e.unpack(1);
        CPPUNIT_ASSERT_EQUAL(0xaaaaUL, out.hash);
        CPPUNIT_ASSERT_EQUAL(false, out.chain);
        CPPUNIT_ASSERT_EQUAL(0x3fffffffffffUL, out.ptr);
        CPPUNIT_ASSERT_EQUAL(1, out.type);
    }

    void test_setChainPointer()
    {
        TestObjectMap::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        {
            TestObjectMap::CacheLine *cl;
            cl = reinterpret_cast<TestObjectMap::CacheLine*>(
                0x7fffffffffffUL);
            e.setChainPointer(cl);
        }
        TestObjectMap::Entry::UnpackedEntry out;
        out = e.unpack(0);
        CPPUNIT_ASSERT_EQUAL(0UL, out.hash);
        CPPUNIT_ASSERT_EQUAL(true, out.chain);
        CPPUNIT_ASSERT_EQUAL(0x7fffffffffffUL, out.ptr);
    }

    void test_isAvailable()
    {
        TestObjectMap::Entry e;
        e.clear();
        CPPUNIT_ASSERT(e.isAvailable(0));
        e.setChainPointer(reinterpret_cast<TestObjectMap::CacheLine*>(
            0x1UL));
        CPPUNIT_ASSERT(!e.isAvailable(0));
        e.setReferant(0UL, reinterpret_cast<TestObject*>(0x1UL), 0, 0);
        CPPUNIT_ASSERT(!e.isAvailable(0));
        e.clear();
        CPPUNIT_ASSERT(e.isAvailable(0));
    }

    void test_getReferant()
    {
        TestObjectMap::Entry e;
        TestObject *o =
            reinterpret_cast<TestObject*>(0x7fffffffffffUL);
        e.setReferant(0xaaaaUL, o, 0, 0);
        CPPUNIT_ASSERT_EQUAL(o, e.getReferant(0, NULL));

        uint8_t type;
        o = reinterpret_cast<TestObject*>(0x3fffffffffffUL);
        e.setReferant(0xaaaaUL, o, 1, 1);
        CPPUNIT_ASSERT_EQUAL(o, e.getReferant(1, &type));
        CPPUNIT_ASSERT_EQUAL(1, type);
    }

    void test_getChainPointer()
    {
        TestObjectMap::CacheLine *cl;
        cl = reinterpret_cast<TestObjectMap::CacheLine*>(0x7fffffffffffUL);
        TestObjectMap::Entry e;
        e.setChainPointer(cl);
        CPPUNIT_ASSERT_EQUAL(cl, e.getChainPointer(0));
        e.clear();
        CPPUNIT_ASSERT(NULL == e.getChainPointer(0));
        e.setReferant(0UL, reinterpret_cast<TestObject*>(0x1UL), 0, 0);
        CPPUNIT_ASSERT(NULL == e.getChainPointer(0));
    }

    void test_hashMatches()
    {
        TestObjectMap::Entry e;
        e.clear();
        CPPUNIT_ASSERT(!e.hashMatches(0UL, 0));
        e.setChainPointer(reinterpret_cast<TestObjectMap::CacheLine*>(
            0x1UL));
        CPPUNIT_ASSERT(!e.hashMatches(0UL, 0));
        e.setReferant(0UL, reinterpret_cast<TestObject*>(0x1UL), 0, 0);
        CPPUNIT_ASSERT(e.hashMatches(0UL, 0));
        CPPUNIT_ASSERT(!e.hashMatches(0xbeefUL, 0));
        e.setReferant(0xbeefUL,
            reinterpret_cast<TestObject*>(0x1UL), 0, 0);
        CPPUNIT_ASSERT(!e.hashMatches(0UL, 0));
        CPPUNIT_ASSERT(e.hashMatches(0xbeefUL, 0));
        CPPUNIT_ASSERT(!e.hashMatches(0xfeedUL, 0));
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
    CPPUNIT_TEST(test_constructor_typeBits);
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
#define seven (TestObjectMap::ENTRIES_PER_CACHE_LINE - 1)

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
    void insertArray(TestObjectMap *ht, TestObject *values, uint64_t tableId,
                     uint64_t numEnt,
                     LargeBlockOfMemory<TestObjectMap::CacheLine> *cacheLines,
                     uint64_t numCacheLines)
    {
        TestObjectMap::CacheLine *cl;

        // chain all the cache lines
        cl = &cacheLines->get()[0];
        while (cl < &cacheLines->get()[numCacheLines - 1]) {
            cl->entries[seven].setChainPointer(cl + 1);
            cl++;
        }

        // fill in the "log" entries
        for (uint64_t i = 0; i < numEnt; i++) {
            values[i]._key1 = tableId;
            values[i]._key2 = i;

            uint64_t littleHash;
            (void) ht->findBucket(0, i, &littleHash);

            TestObjectMap::Entry *entry;
            if (0 < i && i == numEnt - 1 && i % seven == 0)
                entry = &cacheLines->get()[i / seven - 1].entries[seven];
            else
                entry = &cacheLines->get()[i / seven].entries[i % seven];
            entry->setReferant(littleHash, &values[i], 0, 0);
        }

        ht->buckets.swap(*cacheLines);
    }

    /**
     * Common setup code for the lookupEntry and insert tests.
     * This mostly declares variables on the stack, so it's a macro.
     * \li \a numEnt is set to \a _numEnt
     * \li \a numCacheLines is the number of cache lines used to hold the
     * entries.
     * \li \a ht is a hash table of one bucket.
     * \li \a values is an array of \a numEnt objects.
     * \param _tableId
     *      The table id to use for all objects placed in the hashtable.
     * \param _numEnt
     *      The number of entries to place in the hashtable.
     */
#define SETUP(_tableId, _numEnt)  \
    uint64_t tableId = _tableId; \
    uint64_t numEnt = _numEnt; \
    uint64_t numCacheLines; \
    numCacheLines = ((numEnt + TestObjectMap::ENTRIES_PER_CACHE_LINE - 2) /\
                              (TestObjectMap::ENTRIES_PER_CACHE_LINE - 1));\
    if (numCacheLines == 0) \
        numCacheLines = 1; \
    TestObjectMap ht(1); \
    TestObject values[numEnt]; \
    LargeBlockOfMemory<TestObjectMap::CacheLine> _cacheLines( \
                        numCacheLines * sizeof(TestObjectMap::CacheLine)); \
    insertArray(&ht, values, tableId, numEnt, &_cacheLines, numCacheLines); \

#define NULL_OBJECT (static_cast<TestObject*>(NULL))

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
    TestObjectMap::Entry& entryAt(TestObjectMap *ht, uint64_t x,
        uint64_t y)
    {
        TestObjectMap::CacheLine *cl = &ht->buckets.get()[0];
        while (x > 0) {
            cl = cl->entries[seven].getChainPointer(0);
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
    void assertEntryIs(TestObjectMap *ht, uint64_t x, uint64_t y,
        TestObject *ptr)
    {
        uint64_t littleHash;
        (void) ht->findBucket(0, ptr->key2(), &littleHash);
        TestObjectMap::Entry& entry = entryAt(ht, x, y);
        CPPUNIT_ASSERT(entry.hashMatches(littleHash, 0));
        CPPUNIT_ASSERT_EQUAL(ptr, entry.getReferant(0, NULL));
    }

    TestObjectMap::Entry *findBucketAndLookupEntry(TestObjectMap *ht,
                                               uint64_t tableId,
                                               uint64_t objectId)
    {
        uint64_t secondaryHash;
        TestObjectMap::CacheLine *bucket;
        bucket = ht->findBucket(0, objectId, &secondaryHash);
        return ht->lookupEntry(bucket, secondaryHash, tableId, objectId);
    }

  public:

    HashTableTest()
    {
    }

    void test_constructor()
    {
        TestObjectMap ht(16);
        for (uint32_t i = 0; i < 16; i++) {
            for (uint32_t j = 0; j < ht.entriesPerCacheLine(); j++)
                CPPUNIT_ASSERT(ht.buckets.get()[i].entries[j].isAvailable(0));
        }
    }

    void test_constructor_truncate()
    {
        // This is effectively testing nearestPowerOfTwo.
        CPPUNIT_ASSERT_EQUAL(1UL, TestObjectMap(1).numBuckets);
        CPPUNIT_ASSERT_EQUAL(2UL, TestObjectMap(2).numBuckets);
        CPPUNIT_ASSERT_EQUAL(2UL, TestObjectMap(3).numBuckets);
        CPPUNIT_ASSERT_EQUAL(4UL, TestObjectMap(4).numBuckets);
        CPPUNIT_ASSERT_EQUAL(4UL, TestObjectMap(5).numBuckets);
        CPPUNIT_ASSERT_EQUAL(4UL, TestObjectMap(6).numBuckets);
        CPPUNIT_ASSERT_EQUAL(4UL, TestObjectMap(7).numBuckets);
        CPPUNIT_ASSERT_EQUAL(8UL, TestObjectMap(8).numBuckets);
    }

    void test_constructor_typeBits()
    {
        // Test was we handle numTypes and calculate typeBits correctly
        CPPUNIT_ASSERT_EQUAL(1, TestObjectMap(1).numTypes);
        CPPUNIT_ASSERT_EQUAL(0, TestObjectMap(1).typeBits);

        CPPUNIT_ASSERT_THROW(TestObjectMap(1, 0), Exception);
        CPPUNIT_ASSERT_NO_THROW(TestObjectMap(1, TestObjectMap::MAX_NUMTYPES));
        CPPUNIT_ASSERT_THROW(TestObjectMap(1, TestObjectMap::MAX_NUMTYPES + 1),
            Exception);

        CPPUNIT_ASSERT_EQUAL(0, TestObjectMap(1, 1).typeBits);
        CPPUNIT_ASSERT_EQUAL(1, TestObjectMap(1, 2).typeBits);
        CPPUNIT_ASSERT_EQUAL(2, TestObjectMap(1, 3).typeBits);
        CPPUNIT_ASSERT_EQUAL(2, TestObjectMap(1, 4).typeBits);
        CPPUNIT_ASSERT_EQUAL(3, TestObjectMap(1, 5).typeBits);
        CPPUNIT_ASSERT_EQUAL(3, TestObjectMap(1, 6).typeBits);
        CPPUNIT_ASSERT_EQUAL(3, TestObjectMap(1, 7).typeBits);
        CPPUNIT_ASSERT_EQUAL(3, TestObjectMap(1, 8).typeBits);
        CPPUNIT_ASSERT_EQUAL(4, TestObjectMap(1, 9).typeBits);
        int8_t maxTypeBits = TestObjectMap::MAX_TYPEBITS;
        CPPUNIT_ASSERT_EQUAL(maxTypeBits,
            TestObjectMap(1, TestObjectMap::MAX_NUMTYPES).typeBits);
    }

    void test_destructor()
    {
    }

    void test_simple()
    {
        TestObjectMap ht(1024);

        TestObject a(0, 0);
        TestObject b(0, 10);

        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 0));
        ht.replace(&a);
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(&a),
            ht.lookup(0, 0));
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 10));
        ht.replace(&b);
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(&b),
            ht.lookup(0, 10));
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(&a),
            ht.lookup(0, 0));
    }

    void test_multiTable()
    {
        TestObjectMap ht(1024);

        TestObject a(0, 0);
        TestObject b(1, 0);
        TestObject c(0, 1);

        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 0));
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(1, 0));
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 1));

        ht.replace(&a);
        ht.replace(&b);
        ht.replace(&c);

        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(&a),
            ht.lookup(0, 0));
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(&b),
            ht.lookup(1, 0));
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(&c),
            ht.lookup(0, 1));
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
            observedBits |= TestObjectMap::hash(input);
        }
        CPPUNIT_ASSERT_EQUAL(~0UL, observedBits);
    }

    void test_findBucket()
    {
        TestObjectMap ht(1024);
        TestObjectMap::CacheLine *bucket;
        uint64_t hashValue;
        uint64_t secondaryHash;
        bucket = ht.findBucket(0, 4327, &secondaryHash);
        hashValue = TestObjectMap::hash(0) ^ TestObjectMap::hash(4327);
        CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(bucket - ht.buckets.get()),
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
            CPPUNIT_ASSERT_EQUAL(static_cast<TestObjectMap::Entry*>(NULL),
                                 findBucketAndLookupEntry(&ht, 0, 1));
            CPPUNIT_ASSERT_EQUAL(1UL, ht.getPerfCounters().lookupEntryCalls);
            CPPUNIT_ASSERT(ht.getPerfCounters().lookupEntryCycles > 0);
            CPPUNIT_ASSERT(ht.getPerfCounters().lookupEntryDist.max > 0);
        }
        {
            SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE * 5);
            CPPUNIT_ASSERT_EQUAL(static_cast<TestObjectMap::Entry*>(NULL),
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
        SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE);
        CPPUNIT_ASSERT_EQUAL(&entryAt(&ht, 0, seven),
                             findBucketAndLookupEntry(&ht, 0, seven));
    }

    /**
     * Test #RAMCloud::HashTable::lookupEntry() when the object ID is found in
     * the first entry of the third cache line.
     */
    void test_lookupEntry_cacheLine2Entry0()
    {
        SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE * 5);

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
        values[0]._key2 = 0x43324890UL;
        CPPUNIT_ASSERT_EQUAL(static_cast<TestObjectMap::Entry*>(NULL),
                             findBucketAndLookupEntry(&ht, 0, 0));
        CPPUNIT_ASSERT_EQUAL(1UL,
                             ht.getPerfCounters().lookupEntryHashCollisions);
    }

    void test_lookup()
    {
        uint8_t type;
        TestObjectMap ht(1, 2);
        TestObject *v = new TestObject(0, 83UL);
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 83UL));
        ht.replace(v, 1);
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(v),
            ht.lookup(0, 83UL, &type));
        CPPUNIT_ASSERT_EQUAL(1, type);

        delete v;
    }

    void test_remove()
    {
        TestObject * ptr;
        uint8_t type;
        TestObjectMap ht(1, 2);
        CPPUNIT_ASSERT(!ht.remove(0, 83UL));
        TestObject *v = new TestObject(0, 83UL);
        ht.replace(v, 1);
        CPPUNIT_ASSERT(ht.remove(0, 83UL, &ptr, &type));
        CPPUNIT_ASSERT_EQUAL(v, ptr);
        CPPUNIT_ASSERT_EQUAL(1, type);
        CPPUNIT_ASSERT_EQUAL(NULL_OBJECT, ht.lookup(0, 83UL));
        CPPUNIT_ASSERT(!ht.remove(0, 83UL));
        delete v;
    }

    void test_replace_normal()
    {
        uint8_t type;
        TestObject* replaced;
        TestObjectMap ht(1, 2);
        TestObject *v = new TestObject(0, 83UL);
        TestObject *w = new TestObject(0, 83UL);
        CPPUNIT_ASSERT(!ht.replace(v));
        CPPUNIT_ASSERT_EQUAL(1UL, ht.getPerfCounters().replaceCalls);
        CPPUNIT_ASSERT(ht.getPerfCounters().replaceCycles > 0);
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(v),
            ht.lookup(0, 83UL));
        CPPUNIT_ASSERT(ht.replace(v, 1, NULL, &type));
        CPPUNIT_ASSERT_EQUAL(0, type);
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(v),
            ht.lookup(0, 83UL));
        CPPUNIT_ASSERT(ht.replace(w, 0, &replaced, &type));
        CPPUNIT_ASSERT_EQUAL(v, replaced);
        CPPUNIT_ASSERT_EQUAL(1, type);
        CPPUNIT_ASSERT_EQUAL(const_cast<TestObject*>(w),
            ht.lookup(0, 83UL));
        delete v;
        delete w;
    }

    /**
     * Test #RAMCloud::HashTable::replace() when the object ID is new and the
     * first entry of the first cache line is available.
     */
    void test_replace_cacheLine0Entry0()
    {
        SETUP(0, 0);
        TestObject v(0, 83UL);
        ht.replace(&v);
        assertEntryIs(&ht, 0, 0, &v);
    }

    /**
     * Test #RAMCloud::HashTable::replace() when the object ID is new and the
     * last entry of the first cache line is available.
     */
    void test_replace_cacheLine0Entry7()
    {
        SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE - 1);
        TestObject v(0, 83UL);
        ht.replace(&v);
        assertEntryIs(&ht, 0, seven, &v);
    }

    /**
     * Test #RAMCloud::HashTable::replace() when the object ID is new and the
     * first entry of the third cache line is available. The third cache line
     * is already chained onto the second.
     */
    void test_replace_cacheLine2Entry0()
    {
        SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE * 2);
        ht.buckets.get()[2].entries[0].clear();
        ht.buckets.get()[2].entries[1].clear();
        TestObject v(0, 83UL);
        ht.replace(&v);
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
        SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE);
        TestObject v(0, 83UL);
        ht.replace(&v);
        CPPUNIT_ASSERT(entryAt(&ht, 0, seven).getChainPointer(0) != NULL);
        CPPUNIT_ASSERT(entryAt(&ht, 0, seven).getChainPointer(0) !=
                       &ht.buckets.get()[1]);
        assertEntryIs(&ht, 1, 0, &values[seven]);
        assertEntryIs(&ht, 1, 1, &v);
    }

    struct ForEachTestStruct {
        ForEachTestStruct() : _key1(0), _key2(0), count(0) {}
        uint64_t key1() const { return _key1; }
        uint64_t key2() const { return _key2; }
        uint64_t _key1, _key2, count;
    };

    /**
     * Callback used by test_forEach().
     */ 
    static void
    test_forEach_callback(ForEachTestStruct *p, uint8_t type,
        void *cookie)
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
        HashTable<ForEachTestStruct*> ht(2);
        ForEachTestStruct checkoff[256];
        memset(checkoff, 0, sizeof(checkoff));

        for (uint32_t i = 0; i < arrayLength(checkoff); i++) {
            checkoff[i]._key1 = 0;
            checkoff[i]._key2 = i;
            ht.replace(&checkoff[i]);
        }

        uint64_t t = ht.forEach(test_forEach_callback,
            reinterpret_cast<void *>(57));
        CPPUNIT_ASSERT_EQUAL(arrayLength(checkoff), t);

        for (uint32_t i = 0; i < arrayLength(checkoff); i++)
            CPPUNIT_ASSERT_EQUAL(1, checkoff[i].count);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(HashTableTest);

} // namespace RAMCloud
