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
class HashTablePerfDistributionTest : public ::testing::Test {
  public:
    HashTablePerfDistributionTest() {}

    DISALLOW_COPY_AND_ASSIGN(HashTablePerfDistributionTest);
};

TEST_F(HashTablePerfDistributionTest, constructor) {
    RAMCloud::TestObjectMap::PerfDistribution d;
    EXPECT_EQ(~0UL, d.min);
    EXPECT_EQ(0UL, d.max);
    EXPECT_EQ(0UL, d.binOverflows);
    EXPECT_EQ(0UL, d.bins[0]);
    EXPECT_EQ(0UL, d.bins[1]);
    EXPECT_EQ(0UL, d.bins[2]);
}

TEST_F(HashTablePerfDistributionTest, storeSample) {
    TestObjectMap::PerfDistribution d;

    // You can't use EXPECT_TRUE here because it tries to take a
    // reference to BIN_WIDTH. See 10.4.6.2 Member Constants of The C++
    // Programming Language by Bjarne Stroustrup for more about static
    // constant integers.
    EXPECT_TRUE(10 == TestObjectMap::PerfDistribution::BIN_WIDTH);  // NOLINT

    d.storeSample(3);
    EXPECT_EQ(3UL, d.min);
    EXPECT_EQ(3UL, d.max);
    EXPECT_EQ(0UL, d.binOverflows);
    EXPECT_EQ(1UL, d.bins[0]);
    EXPECT_EQ(0UL, d.bins[1]);
    EXPECT_EQ(0UL, d.bins[2]);

    d.storeSample(3);
    d.storeSample(d.NBINS * d.BIN_WIDTH + 40);
    d.storeSample(12);
    d.storeSample(78);

    EXPECT_EQ(3UL, d.min);
    EXPECT_EQ(d.NBINS * d.BIN_WIDTH + 40, d.max);
    EXPECT_EQ(1UL, d.binOverflows);
    EXPECT_EQ(2UL, d.bins[0]);
    EXPECT_EQ(1UL, d.bins[1]);
    EXPECT_EQ(0UL, d.bins[2]);
}


/**
 * Unit tests for HashTable::Entry.
 */
class HashTableEntryTest : public ::testing::Test {
  public:
    HashTableEntryTest() {}

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
        TestObjectMap::Entry e;

        TestObjectMap::Entry::UnpackedEntry in;
        TestObjectMap::Entry::UnpackedEntry out;

        in.hash = hash;
        in.chain = chain;
        in.ptr = ptr;

        e.pack(in.hash, in.chain, in.ptr);
        out = e.unpack();

        return (in.hash == out.hash &&
                in.chain == out.chain &&
                in.ptr == out.ptr);
    }
    DISALLOW_COPY_AND_ASSIGN(HashTableEntryTest);
};

TEST_F(HashTableEntryTest, size) {
    EXPECT_EQ(8U, sizeof(TestObjectMap::Entry));
}

// also tests unpack
TEST_F(HashTableEntryTest, pack) {
    // first without normal cases
    EXPECT_TRUE(packable(0x0000UL, false, 0x000000000000UL));
    EXPECT_TRUE(packable(0xffffUL, true,  0x7fffffffffffUL));
    EXPECT_TRUE(packable(0xffffUL, false, 0x7fffffffffffUL));
    EXPECT_TRUE(packable(0xa257UL, false, 0x3cdeadbeef98UL));

    // and now test the exception cases of pack()
    TestObjectMap::Entry e;
    EXPECT_THROW(e.pack(0, false, 0xffffffffffffUL), Exception);
}

// No tests for test_unpack, since test_pack tested it.

TEST_F(HashTableEntryTest, clear) {
    TestObjectMap::Entry e;
    e.value = 0xdeadbeefdeadbeefUL;
    e.clear();
    TestObjectMap::Entry::UnpackedEntry out;
    out = e.unpack();
    EXPECT_EQ(0UL, out.hash);
    EXPECT_FALSE(out.chain);
    EXPECT_EQ(0UL, out.ptr);
}

TEST_F(HashTableEntryTest, trivial_clear) {
    TestObjectMap::Entry e;
    e.value = 0xdeadbeefdeadbeefUL;
    e.clear();
    TestObjectMap::Entry f;
    f.value = 0xdeadbeefdeadbeefUL;
    f.pack(0, false, 0);
    EXPECT_EQ(e.value, f.value);
}

TEST_F(HashTableEntryTest, setReferent) {
    TestObjectMap::Entry e;
    e.value = 0xdeadbeefdeadbeefUL;
    e.setReferent(0xaaaaUL, reinterpret_cast<TestObject*>(
        0x7fffffffffffUL));
    TestObjectMap::Entry::UnpackedEntry out;
    out = e.unpack();
    EXPECT_EQ(0xaaaaUL, out.hash);
    EXPECT_FALSE(out.chain);
    EXPECT_EQ(0x7fffffffffffUL, out.ptr);
}

TEST_F(HashTableEntryTest, setChainPointer) {
    TestObjectMap::Entry e;
    e.value = 0xdeadbeefdeadbeefUL;
    {
        TestObjectMap::CacheLine *cl;
        cl = reinterpret_cast<TestObjectMap::CacheLine*>(
            0x7fffffffffffUL);
        e.setChainPointer(cl);
    }
    TestObjectMap::Entry::UnpackedEntry out;
    out = e.unpack();
    EXPECT_EQ(0UL, out.hash);
    EXPECT_TRUE(out.chain);
    EXPECT_EQ(0x7fffffffffffUL, out.ptr);
}

TEST_F(HashTableEntryTest, isAvailable) {
    TestObjectMap::Entry e;
    e.clear();
    EXPECT_TRUE(e.isAvailable());
    e.setChainPointer(reinterpret_cast<TestObjectMap::CacheLine*>(
        0x1UL));
    EXPECT_FALSE(e.isAvailable());
    e.setReferent(0UL, reinterpret_cast<TestObject*>(0x1UL));
    EXPECT_FALSE(e.isAvailable());
    e.clear();
    EXPECT_TRUE(e.isAvailable());
}

TEST_F(HashTableEntryTest, getReferent) {
    TestObjectMap::Entry e;
    TestObject *o =
        reinterpret_cast<TestObject*>(0x7fffffffffffUL);
    e.setReferent(0xaaaaUL, o);
    EXPECT_EQ(o, e.getReferent());
}

TEST_F(HashTableEntryTest, getChainPointer) {
    TestObjectMap::CacheLine *cl;
    cl = reinterpret_cast<TestObjectMap::CacheLine*>(0x7fffffffffffUL);
    TestObjectMap::Entry e;
    e.setChainPointer(cl);
    EXPECT_EQ(cl, e.getChainPointer());
    e.clear();
    EXPECT_TRUE(NULL == e.getChainPointer());
    e.setReferent(0UL, reinterpret_cast<TestObject*>(0x1UL));
    EXPECT_TRUE(NULL == e.getChainPointer());
}

TEST_F(HashTableEntryTest, hashMatches) {
    TestObjectMap::Entry e;
    e.clear();
    EXPECT_TRUE(!e.hashMatches(0UL));
    e.setChainPointer(reinterpret_cast<TestObjectMap::CacheLine*>(
        0x1UL));
    EXPECT_TRUE(!e.hashMatches(0UL));
    e.setReferent(0UL, reinterpret_cast<TestObject*>(0x1UL));
    EXPECT_TRUE(e.hashMatches(0UL));
    EXPECT_TRUE(!e.hashMatches(0xbeefUL));
    e.setReferent(0xbeefUL, reinterpret_cast<TestObject*>(0x1UL));
    EXPECT_TRUE(!e.hashMatches(0UL));
    EXPECT_TRUE(e.hashMatches(0xbeefUL));
    EXPECT_TRUE(!e.hashMatches(0xfeedUL));
}

/**
 * Unit tests for HashTable.
 */
class HashTableTest : public ::testing::Test {
  public:
    HashTableTest() { }

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
            entry->setReferent(littleHash, &values[i]);
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
    void assertEntryIs(TestObjectMap *ht, uint64_t x, uint64_t y,
        TestObject *ptr)
    {
        uint64_t littleHash;
        (void) ht->findBucket(0, ptr->key2(), &littleHash);
        TestObjectMap::Entry& entry = entryAt(ht, x, y);
        EXPECT_TRUE(entry.hashMatches(littleHash));
        EXPECT_EQ(ptr, entry.getReferent());
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

    DISALLOW_COPY_AND_ASSIGN(HashTableTest);
};

TEST_F(HashTableTest, constructor) {
    TestObjectMap ht(16);
    for (uint32_t i = 0; i < 16; i++) {
        for (uint32_t j = 0; j < ht.entriesPerCacheLine(); j++)
            EXPECT_TRUE(ht.buckets.get()[i].entries[j].isAvailable());
    }
}

TEST_F(HashTableTest, constructor_truncate) {
    // This is effectively testing nearestPowerOfTwo.
    EXPECT_EQ(1UL, TestObjectMap(1).numBuckets);
    EXPECT_EQ(2UL, TestObjectMap(2).numBuckets);
    EXPECT_EQ(2UL, TestObjectMap(3).numBuckets);
    EXPECT_EQ(4UL, TestObjectMap(4).numBuckets);
    EXPECT_EQ(4UL, TestObjectMap(5).numBuckets);
    EXPECT_EQ(4UL, TestObjectMap(6).numBuckets);
    EXPECT_EQ(4UL, TestObjectMap(7).numBuckets);
    EXPECT_EQ(8UL, TestObjectMap(8).numBuckets);
}

TEST_F(HashTableTest, destructor) {
}

TEST_F(HashTableTest, simple) {
    TestObjectMap ht(1024);

    TestObject a(0, 0);
    TestObject b(0, 10);

    EXPECT_EQ(NULL_OBJECT, ht.lookup(0, 0));
    ht.replace(&a);
    EXPECT_EQ(const_cast<TestObject*>(&a),
        ht.lookup(0, 0));
    EXPECT_EQ(NULL_OBJECT, ht.lookup(0, 10));
    ht.replace(&b);
    EXPECT_EQ(const_cast<TestObject*>(&b),
        ht.lookup(0, 10));
    EXPECT_EQ(const_cast<TestObject*>(&a),
        ht.lookup(0, 0));
}

TEST_F(HashTableTest, multiTable) {
    TestObjectMap ht(1024);

    TestObject a(0, 0);
    TestObject b(1, 0);
    TestObject c(0, 1);

    EXPECT_EQ(NULL_OBJECT, ht.lookup(0, 0));
    EXPECT_EQ(NULL_OBJECT, ht.lookup(1, 0));
    EXPECT_EQ(NULL_OBJECT, ht.lookup(0, 1));

    ht.replace(&a);
    ht.replace(&b);
    ht.replace(&c);

    EXPECT_EQ(const_cast<TestObject*>(&a),
        ht.lookup(0, 0));
    EXPECT_EQ(const_cast<TestObject*>(&b),
        ht.lookup(1, 0));
    EXPECT_EQ(const_cast<TestObject*>(&c),
        ht.lookup(0, 1));
}

/**
    * Ensure that #RAMCloud::HashTable::hash() generates hashes using the full
    * range of bits.
    */
TEST_F(HashTableTest, hash) {
    uint64_t observedBits = 0UL;
    srand(1);
    for (uint32_t i = 0; i < 50; i++) {
        uint64_t input = generateRandom();
        observedBits |= TestObjectMap::hash(input);
    }
    EXPECT_EQ(~0UL, observedBits);
}

TEST_F(HashTableTest, findBucket) {
    TestObjectMap ht(1024);
    TestObjectMap::CacheLine *bucket;
    uint64_t hashValue;
    uint64_t secondaryHash;
    bucket = ht.findBucket(0, 4327, &secondaryHash);
    hashValue = TestObjectMap::hash(0) ^ TestObjectMap::hash(4327);
    EXPECT_EQ(static_cast<uint64_t>(bucket - ht.buckets.get()),
                            (hashValue & 0x0000ffffffffffffffffUL) % 1024);
    EXPECT_EQ(secondaryHash, hashValue >> 48);
}

/**
    * Test #RAMCloud::HashTable::lookupEntry() when the object ID is not
    * found.
    */
TEST_F(HashTableTest, lookupEntry_notFound) {
    {
        SETUP(0, 0);
        EXPECT_EQ(static_cast<TestObjectMap::Entry*>(NULL),
                                findBucketAndLookupEntry(&ht, 0, 1));
        EXPECT_EQ(1UL, ht.getPerfCounters().lookupEntryCalls);
        EXPECT_LT(0U, ht.getPerfCounters().lookupEntryCycles);
        EXPECT_LT(0U, ht.getPerfCounters().lookupEntryDist.max);
    }
    {
        SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE * 5);
        EXPECT_EQ(static_cast<TestObjectMap::Entry*>(NULL),
                  findBucketAndLookupEntry(&ht, 0, numEnt + 1));
        EXPECT_EQ(5UL, ht.getPerfCounters().lookupEntryChainsFollowed);
    }
}

/**
    * Test #RAMCloud::HashTable::lookupEntry() when the object ID is found in
    * the first entry of the first cache line.
    */
TEST_F(HashTableTest, lookupEntry_cacheLine0Entry0) {
    SETUP(0, 1);
    EXPECT_EQ(&entryAt(&ht, 0, 0),
              findBucketAndLookupEntry(&ht, 0, 0));
}

/**
    * Test #RAMCloud::HashTable::lookupEntry() when the object ID is found in
    * the last entry of the first cache line.
    */
TEST_F(HashTableTest, lookupEntry_cacheLine0Entry7) {
    SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE);
    EXPECT_EQ(&entryAt(&ht, 0, seven),
              findBucketAndLookupEntry(&ht, 0, seven));
}

/**
    * Test #RAMCloud::HashTable::lookupEntry() when the object ID is found in
    * the first entry of the third cache line.
    */
TEST_F(HashTableTest, lookupEntry_cacheLine2Entry0) {
    SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE * 5);

    // with 8 entries per cache line:
    // cl0: [ k00, k01, k02, k03, k04, k05, k06, cl1 ]
    // cl1: [ k07, k09, k09, k10, k11, k12, k13, cl2 ]
    // cl2: [ k14, k15, k16, k17, k18, k19, k20, cl3 ]
    // ...

    EXPECT_EQ(&entryAt(&ht, 2, 0),
              findBucketAndLookupEntry(&ht, 0, seven * 2));
}

/**
    * Test #RAMCloud::HashTable::lookupEntry() when there is a hash collision
    * with another Entry.
    */
TEST_F(HashTableTest, lookupEntry_hashCollision) {
    SETUP(0, 1);
    EXPECT_EQ(&entryAt(&ht, 0, 0),
                            findBucketAndLookupEntry(&ht, 0, 0));
    EXPECT_LT(0U, ht.getPerfCounters().lookupEntryDist.max);
    values[0]._key2 = 0x43324890UL;
    EXPECT_EQ(static_cast<TestObjectMap::Entry*>(NULL),
              findBucketAndLookupEntry(&ht, 0, 0));
    EXPECT_EQ(1UL, ht.getPerfCounters().lookupEntryHashCollisions);
}

TEST_F(HashTableTest, lookup) {
    TestObjectMap ht(1);
    TestObject *v = new TestObject(0, 83UL);
    EXPECT_EQ(NULL_OBJECT, ht.lookup(0, 83UL));
    ht.replace(v);
    EXPECT_EQ(const_cast<TestObject*>(v),
        ht.lookup(0, 83UL));

    delete v;
}

TEST_F(HashTableTest, remove) {
    TestObject * ptr;
    TestObjectMap ht(1);
    EXPECT_TRUE(!ht.remove(0, 83UL));
    TestObject *v = new TestObject(0, 83UL);
    ht.replace(v);
    EXPECT_TRUE(ht.remove(0, 83UL, &ptr));
    EXPECT_EQ(v, ptr);
    EXPECT_EQ(NULL_OBJECT, ht.lookup(0, 83UL));
    EXPECT_TRUE(!ht.remove(0, 83UL));
    delete v;
}

TEST_F(HashTableTest, replace_normal) {
    TestObject* replaced;
    TestObjectMap ht(1);
    TestObject *v = new TestObject(0, 83UL);
    TestObject *w = new TestObject(0, 83UL);
    EXPECT_TRUE(!ht.replace(v));
    EXPECT_EQ(1UL, ht.getPerfCounters().replaceCalls);
    EXPECT_LT(0U, ht.getPerfCounters().replaceCycles);
    EXPECT_EQ(const_cast<TestObject*>(v),
        ht.lookup(0, 83UL));
    EXPECT_TRUE(ht.replace(v));
    EXPECT_EQ(const_cast<TestObject*>(v),
        ht.lookup(0, 83UL));
    EXPECT_TRUE(ht.replace(w, &replaced));
    EXPECT_EQ(v, replaced);
    EXPECT_EQ(const_cast<TestObject*>(w),
        ht.lookup(0, 83UL));
    delete v;
    delete w;
}

/**
    * Test #RAMCloud::HashTable::replace() when the object ID is new and the
    * first entry of the first cache line is available.
    */
TEST_F(HashTableTest, replace_cacheLine0Entry0) {
    SETUP(0, 0);
    TestObject v(0, 83UL);
    ht.replace(&v);
    assertEntryIs(&ht, 0, 0, &v);
}

/**
    * Test #RAMCloud::HashTable::replace() when the object ID is new and the
    * last entry of the first cache line is available.
    */
TEST_F(HashTableTest, replace_cacheLine0Entry7) {
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
TEST_F(HashTableTest, replace_cacheLine2Entry0) {
    SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE * 2);
    ht.buckets.get()[2].entries[0].clear();
    ht.buckets.get()[2].entries[1].clear();
    TestObject v(0, 83UL);
    ht.replace(&v);
    assertEntryIs(&ht, 2, 0, &v);
    EXPECT_EQ(2UL, ht.getPerfCounters().insertChainsFollowed);
}

/**
    * Test #RAMCloud::HashTable::replace() when the object ID is new and the
    * first and only cache line is full. The second cache line needs to be
    * allocated.
    */
TEST_F(HashTableTest, replace_cacheLineFull) {
    SETUP(0, TestObjectMap::ENTRIES_PER_CACHE_LINE);
    TestObject v(0, 83UL);
    ht.replace(&v);
    EXPECT_TRUE(entryAt(&ht, 0, seven).getChainPointer() != NULL);
    EXPECT_TRUE(entryAt(&ht, 0, seven).getChainPointer() !=
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
test_forEach_callback(ForEachTestStruct *p, void *cookie)
{
    EXPECT_EQ(cookie, reinterpret_cast<void *>(57));
    const_cast<ForEachTestStruct *>(p)->count++;
}

/**
    * Simple test for #RAMCloud::HashTable::forEach(), ensuring that it
    * properly traverses multiple buckets and chained cachelines.
    */
TEST_F(HashTableTest, forEach) {
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
    EXPECT_EQ(arrayLength(checkoff), t);

    for (uint32_t i = 0; i < arrayLength(checkoff); i++)
        EXPECT_EQ(1U, checkoff[i].count);
}

} // namespace RAMCloud
