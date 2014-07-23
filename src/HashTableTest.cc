/* Copyright (c) 2009-2014 Stanford University
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
    TestObject()
        : tableId(0),
          stringKeyPtr(NULL),
          stringKeyLength(0),
          count(0)
    {
    }

    TestObject(uint64_t tableId, string stringKey)
        : tableId(tableId),
          stringKeyPtr(strdup(stringKey.c_str())),
          stringKeyLength(downCast<uint16_t>(stringKey.length())),
          count(0)
    {
    }

    ~TestObject()
    {
        if (stringKeyPtr != NULL)
            free(stringKeyPtr);
    }

    void
    setKey(string stringKey)
    {
        if (stringKeyPtr != NULL)
            free(stringKeyPtr);

        stringKeyPtr = strdup(stringKey.c_str());
        stringKeyLength = downCast<uint16_t>(stringKey.length());
    }

    uint64_t
    u64Address()
    {
        return reinterpret_cast<uint64_t>(this);
    }

    uint64_t tableId;
    void* stringKeyPtr;
    uint16_t stringKeyLength;
    uint64_t count;             // Used in forEach callback tests

    DISALLOW_COPY_AND_ASSIGN(TestObject);   // NOLINT
} __attribute__((aligned(64)));

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
        HashTable::Entry e;

        HashTable::Entry::UnpackedEntry in;
        HashTable::Entry::UnpackedEntry out;

        in.hash = hash;
        in.chain = chain;
        in.ptr = ptr;

        e.pack(in.hash, in.chain, in.ptr);
        e.unpack(out);

        return (in.hash == out.hash &&
                in.chain == out.chain &&
                in.ptr == out.ptr);
    }
    DISALLOW_COPY_AND_ASSIGN(HashTableEntryTest);
};

TEST_F(HashTableEntryTest, size) {
    EXPECT_EQ(8U, sizeof(HashTable::Entry));
}

// also tests unpack
TEST_F(HashTableEntryTest, pack) {
    // first without normal cases
    EXPECT_TRUE(packable(0x0000UL, false, 0x000000000000UL));
    EXPECT_TRUE(packable(0xffffUL, true,  0x7fffffffffffUL));
    EXPECT_TRUE(packable(0xffffUL, false, 0x7fffffffffffUL));
    EXPECT_TRUE(packable(0xa257UL, false, 0x3cdeadbeef98UL));

    // and now test the exception cases of pack()
    HashTable::Entry e;
    EXPECT_THROW(e.pack(0, false, 0xffffffffffffUL), Exception);
}

// No tests for test_unpack, since test_pack tested it.

TEST_F(HashTableEntryTest, clear) {
    HashTable::Entry e;
    e.value = 0xdeadbeefdeadbeefUL;
    e.clear();
    HashTable::Entry::UnpackedEntry out;
    e.unpack(out);
    EXPECT_EQ(0UL, out.hash);
    EXPECT_FALSE(out.chain);
    EXPECT_EQ(0UL, out.ptr);
}

TEST_F(HashTableEntryTest, trivial_clear) {
    HashTable::Entry e;
    e.value = 0xdeadbeefdeadbeefUL;
    e.clear();
    HashTable::Entry f;
    f.value = 0xdeadbeefdeadbeefUL;
    f.pack(0, false, 0);
    EXPECT_EQ(e.value, f.value);
}

TEST_F(HashTableEntryTest, setReference) {
    HashTable::Entry e;
    e.value = 0xdeadbeefdeadbeefUL;
    e.setReference(0xaaaaUL, 0x7fffffffffffUL);
    HashTable::Entry::UnpackedEntry out;
    e.unpack(out);
    EXPECT_EQ(0xaaaaUL, out.hash);
    EXPECT_FALSE(out.chain);
    EXPECT_EQ(0x7fffffffffffUL, out.ptr);
}

TEST_F(HashTableEntryTest, setChainPointer) {
    HashTable::Entry e;
    e.value = 0xdeadbeefdeadbeefUL;
    {
        HashTable::CacheLine *cl;
        cl = reinterpret_cast<HashTable::CacheLine*>(
            0x7fffffffffffUL);
        e.setChainPointer(cl);
    }
    HashTable::Entry::UnpackedEntry out;
    e.unpack(out);
    EXPECT_EQ(0UL, out.hash);
    EXPECT_TRUE(out.chain);
    EXPECT_EQ(0x7fffffffffffUL, out.ptr);
}

TEST_F(HashTableEntryTest, isAvailable) {
    HashTable::Entry e;
    e.clear();
    EXPECT_TRUE(e.isAvailable());
    e.setChainPointer(reinterpret_cast<HashTable::CacheLine*>(
        0x1UL));
    EXPECT_FALSE(e.isAvailable());
    e.setReference(0UL, 0x1UL);
    EXPECT_FALSE(e.isAvailable());
    e.clear();
    EXPECT_TRUE(e.isAvailable());
}

TEST_F(HashTableEntryTest, getReference) {
    HashTable::Entry e;
    TestObject o;
    uint64_t oRef = o.u64Address();
    e.setReference(0xaaaaUL, oRef);
    EXPECT_EQ(oRef, e.getReference());
    EXPECT_EQ(&o, reinterpret_cast<TestObject*>(e.getReference()));
}

TEST_F(HashTableEntryTest, getChainPointer) {
    HashTable::CacheLine *cl;
    cl = reinterpret_cast<HashTable::CacheLine*>(0x7fffffffffffUL);
    HashTable::Entry e;
    e.setChainPointer(cl);
    EXPECT_EQ(cl, e.getChainPointer());
    e.clear();
    EXPECT_TRUE(NULL == e.getChainPointer());
    e.setReference(0UL, 0x1UL);
    EXPECT_TRUE(NULL == e.getChainPointer());
}

TEST_F(HashTableEntryTest, hashMatches) {
    HashTable::Entry e;
    e.clear();
    EXPECT_TRUE(!e.hashMatches(0UL));
    e.setChainPointer(reinterpret_cast<HashTable::CacheLine*>(
        0x1UL));
    EXPECT_TRUE(!e.hashMatches(0UL));
    e.setReference(0UL, 0x1UL);
    EXPECT_TRUE(e.hashMatches(0UL));
    EXPECT_TRUE(!e.hashMatches(0xbeefUL));
    e.setReference(0xbeefUL, 0x1UL);
    EXPECT_TRUE(!e.hashMatches(0UL));
    EXPECT_TRUE(e.hashMatches(0xbeefUL));
    EXPECT_TRUE(!e.hashMatches(0xfeedUL));
}

/**
 * Unit tests for HashTable.
 */
class HashTableTest : public ::testing::Test {
  public:

    uint64_t tableId;
    uint64_t numEnt;
    HashTable ht;
    vector<TestObject*> values;

    HashTableTest()
        : tableId(),
          numEnt(),
          ht(1),
          values()
    {
    }

    ~HashTableTest()
    {
        foreach(TestObject* o, values)
            delete o;
    }

    /**
     * Common setup code for the lookupEntry and insert tests.
     * \param[in] tableId
     *      The table id to use for all objects placed in the hashtable.
     * \param[in] numEnt
     *      The number of entries to place in the hashtable.
     */
    void setup(uint64_t tableId, uint64_t numEnt)
    {
        this->tableId = tableId;
        this->numEnt = numEnt;
        uint64_t numCacheLines =
                ((numEnt + HashTable::ENTRIES_PER_CACHE_LINE - 2) /
                (HashTable::ENTRIES_PER_CACHE_LINE - 1));
        if (numCacheLines == 0)
            numCacheLines = 1;
        LargeBlockOfMemory<HashTable::CacheLine> cacheLines(
                numCacheLines * sizeof(HashTable::CacheLine));
        insertArray(&ht, values, tableId, numEnt, &cacheLines,
                numCacheLines);
    }

    // convenient abbreviation
#define seven (HashTable::ENTRIES_PER_CACHE_LINE - 1)

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
    void insertArray(HashTable *ht, vector<TestObject*>& values,
                     uint64_t tableId, uint64_t numEnt,
                     LargeBlockOfMemory<HashTable::CacheLine> *cacheLines,
                     uint64_t numCacheLines)
    {
        HashTable::CacheLine *cl;

        // chain all the cache lines
        cl = &cacheLines->get()[0];
        while (cl < &cacheLines->get()[numCacheLines - 1]) {
            cl->entries[seven].setChainPointer(cl + 1);
            cl++;
        }

        // wipe any old values
        foreach(TestObject* o, values)
            delete o;
        values.clear();

        // fill in the "log" entries
        for (uint64_t i = 0; i < numEnt; i++) {
            string stringKey = format("%lu", i);
            values.push_back(new TestObject(tableId, stringKey));
            Key key(values[i]->tableId,
                    values[i]->stringKeyPtr,
                    values[i]->stringKeyLength);

            uint64_t littleHash;
            (void) ht->findBucket(key.getHash(), &littleHash);

            HashTable::Entry *entry;
            if (0 < i && i == numEnt - 1 && i % seven == 0)
                entry = &cacheLines->get()[i / seven - 1].entries[seven];
            else
                entry = &cacheLines->get()[i / seven].entries[i % seven];
            uint64_t reference = values[i]->u64Address();
            entry->setReference(littleHash, reference);
        }

        ht->buckets.swap(*cacheLines);
    }

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
    HashTable::Entry& entryAt(HashTable *ht, uint64_t x,
                                    uint64_t y)
    {
        HashTable::CacheLine *cl = &ht->buckets.get()[0];
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
    void assertEntryIs(HashTable *ht, uint64_t x, uint64_t y,
                       TestObject *ptr)
    {
        Key key(ptr->tableId,
                ptr->stringKeyPtr,
                ptr->stringKeyLength);
        uint64_t littleHash;
        (void) ht->findBucket(key.getHash(), &littleHash);

        HashTable::Entry& entry = entryAt(ht, x, y);
        EXPECT_TRUE(entry.hashMatches(littleHash));
        EXPECT_EQ(ptr->u64Address(), entry.getReference());
    }

    HashTable::Entry *findBucketAndLookupEntry(HashTable *ht,
                                               uint64_t tableId,
                                               const void* stringKey,
                                               uint16_t stringKeyLength)
    {
        Key key(tableId, stringKey, stringKeyLength);
        HashTable::Candidates candidates;
        ht->lookup(key.getHash(), candidates);
        while (!candidates.isDone()) {
            TestObject* obj = reinterpret_cast<TestObject*>(
                candidates.getReference());
            Key candidateKey(obj->tableId,
                             obj->stringKeyPtr,
                             obj->stringKeyLength);
            if (key == candidateKey)
                return &candidates.bucket->entries[candidates.index];
            candidates.next();
        }
        return 0;
    }

    /**
     * Look up the given key and return the reference, if found.
     *
     * \param ht
     *      The hash table to perform the lookup on.
     * \param key
     *      The key to look up.
     * \param outRef
     *      If found, the object's reference is returned here.
     * \return
     *      True if found, false if not.
     */
    bool
    lookup(HashTable* ht, Key& key, uint64_t& outRef)
    {
        HashTable::Candidates candidates;
        ht->lookup(key.getHash(), candidates);
        while (!candidates.isDone()) {
            TestObject* obj = reinterpret_cast<TestObject*>(
                candidates.getReference());
            Key candidateKey(obj->tableId,
                             obj->stringKeyPtr,
                             obj->stringKeyLength);
            if (key == candidateKey) {
                outRef = candidates.getReference();
                return true;
            }
            candidates.next();
        }
        return false;
    }

    /**
     * Add the given key to the hash table. If the key is already in the table,
     * replace the reference with the new one.
     *
     * \param ht
     *      The hash table to perform the lookup on.
     * \param key
     *      The key to insert or replace.
     * \param ref 
     *      The reference to insert corresponding to the given key.
     * \return
     *      True if the key was already in the hash table and was replaced.
     *      False if the key did not already exist and was inserted.
     */
    bool
    replace(HashTable* ht, Key& key, uint64_t ref)
    {
        HashTable::Candidates candidates;
        ht->lookup(key.getHash(), candidates);
        while (!candidates.isDone()) {
            TestObject* obj = reinterpret_cast<TestObject*>(
                candidates.getReference());
            Key candidateKey(obj->tableId,
                             obj->stringKeyPtr,
                             obj->stringKeyLength);
            if (key == candidateKey) {
                candidates.setReference(ref);
                return true;
            }
            candidates.next();
        }
        ht->insert(key.getHash(), ref);
        return false;
    }

    DISALLOW_COPY_AND_ASSIGN(HashTableTest);
};

TEST_F(HashTableTest, constructor) {
    HashTable ht(16);
    for (uint32_t i = 0; i < 16; i++) {
        for (uint32_t j = 0; j < ht.entriesPerCacheLine(); j++)
            EXPECT_TRUE(ht.buckets.get()[i].entries[j].isAvailable());
    }
}

TEST_F(HashTableTest, constructor_truncate) {
    // This is effectively testing nearestPowerOfTwo.
    EXPECT_EQ(1UL, HashTable(1).numBuckets);
    EXPECT_EQ(2UL, HashTable(2).numBuckets);
    EXPECT_EQ(2UL, HashTable(3).numBuckets);
    EXPECT_EQ(4UL, HashTable(4).numBuckets);
    EXPECT_EQ(4UL, HashTable(5).numBuckets);
    EXPECT_EQ(4UL, HashTable(6).numBuckets);
    EXPECT_EQ(4UL, HashTable(7).numBuckets);
    EXPECT_EQ(8UL, HashTable(8).numBuckets);
}

TEST_F(HashTableTest, destructor) {
}

TEST_F(HashTableTest, simple) {
    HashTable ht(1024);

    TestObject a(0, "0");
    TestObject b(0, "10");

    Key aKey(a.tableId, a.stringKeyPtr, a.stringKeyLength);
    Key bKey(b.tableId, b.stringKeyPtr, b.stringKeyLength);

    uint64_t aRef = a.u64Address();
    uint64_t bRef = b.u64Address();
    uint64_t outRef;

    EXPECT_FALSE(lookup(&ht, aKey, outRef));
    replace(&ht, aKey, aRef);
    EXPECT_TRUE(lookup(&ht, aKey, outRef));
    EXPECT_EQ(aRef, outRef);

    EXPECT_FALSE(lookup(&ht, bKey, outRef));
    replace(&ht, bKey, bRef);
    EXPECT_TRUE(lookup(&ht, bKey, outRef));
    EXPECT_EQ(bRef, outRef);
}

TEST_F(HashTableTest, multiTable) {
    HashTable ht(1024);

    TestObject a(0, "0");
    TestObject b(1, "0");
    TestObject c(0, "1");

    Key aKey(a.tableId, a.stringKeyPtr, a.stringKeyLength);
    Key bKey(b.tableId, b.stringKeyPtr, b.stringKeyLength);
    Key cKey(c.tableId, c.stringKeyPtr, c.stringKeyLength);

    uint64_t outRef;

    EXPECT_FALSE(lookup(&ht, aKey, outRef));
    EXPECT_FALSE(lookup(&ht, bKey, outRef));
    EXPECT_FALSE(lookup(&ht, cKey, outRef));

    uint64_t aRef = a.u64Address() ;
    uint64_t bRef = b.u64Address();
    uint64_t cRef = c.u64Address();

    replace(&ht, aKey, aRef);
    replace(&ht, bKey, bRef);
    replace(&ht, cKey, cRef);

    EXPECT_TRUE(lookup(&ht, aKey, outRef));
    EXPECT_EQ(aRef, outRef);

    EXPECT_TRUE(lookup(&ht, bKey, outRef));
    EXPECT_EQ(bRef, outRef);

    EXPECT_TRUE(lookup(&ht, cKey, outRef));
    EXPECT_EQ(cRef, outRef);
}

TEST_F(HashTableTest, findBucket) {
    HashTable ht(1024);
    HashTable::CacheLine *bucket;
    uint64_t hashValue;
    uint64_t secondaryHash;

    Key key(0, "4327", 4);
    hashValue = key.getHash();
    bucket = ht.findBucket(hashValue, &secondaryHash);

    uint64_t actualBucketIdx = static_cast<uint64_t>(bucket - ht.buckets.get());
    uint64_t expectedBucketIdx = (hashValue & 0x0000ffffffffffffffffUL) % 1024;
    EXPECT_EQ(actualBucketIdx, expectedBucketIdx);
    EXPECT_EQ(secondaryHash, hashValue >> 48);
}

/**
 * Test #RAMCloud::HashTable::lookupEntry() when the key is not
 * found.
 */
TEST_F(HashTableTest, lookupEntry_notFound) {
    {
        setup(0, 0);
        EXPECT_EQ(static_cast<HashTable::Entry*>(NULL),
                  findBucketAndLookupEntry(&ht, 0, "0", 1));
    }
    {
        setup(0, HashTable::ENTRIES_PER_CACHE_LINE * 5);

        string key = format("%lu", numEnt + 1);

        EXPECT_EQ(static_cast<HashTable::Entry*>(NULL),
                  findBucketAndLookupEntry(&ht, 0, key.c_str(),
                        downCast<uint16_t>(key.length())));
    }
}

/**
 * Test #RAMCloud::HashTable::lookupEntry() when the key is found in
 * the first entry of the first cache line.
 */
TEST_F(HashTableTest, lookupEntry_cacheLine0Entry0) {
    setup(0, 1);
    EXPECT_EQ(&entryAt(&ht, 0, 0),
              findBucketAndLookupEntry(&ht, 0, "0", 1));
}

/**
 * Test #RAMCloud::HashTable::lookupEntry() when the key is found in
 * the last entry of the first cache line.
 */
TEST_F(HashTableTest, lookupEntry_cacheLine0Entry7) {
    setup(0, HashTable::ENTRIES_PER_CACHE_LINE);
    string key = format("%u", HashTable::ENTRIES_PER_CACHE_LINE - 1);

    EXPECT_EQ(&entryAt(&ht, 0, seven),
              findBucketAndLookupEntry(&ht, 0, key.c_str(),
                    downCast<uint16_t>(key.length())));
}

/**
 * Test #RAMCloud::HashTable::lookupEntry() when the key is found in
 * the first entry of the third cache line.
 */
TEST_F(HashTableTest, lookupEntry_cacheLine2Entry0) {
    setup(0, HashTable::ENTRIES_PER_CACHE_LINE * 5);

    // with 8 entries per cache line:
    // cl0: [ k00, k01, k02, k03, k04, k05, k06, cl1 ]
    // cl1: [ k07, k09, k09, k10, k11, k12, k13, cl2 ]
    // cl2: [ k14, k15, k16, k17, k18, k19, k20, cl3 ]
    // ...

    string key =
            format("%u", (HashTable::ENTRIES_PER_CACHE_LINE - 1) * 2);
    EXPECT_EQ(&entryAt(&ht, 2, 0),
              findBucketAndLookupEntry(&ht, 0, key.c_str(),
                    downCast<uint16_t>(key.length())));
}

/**
 * Test #RAMCloud::HashTable::lookupEntry() when there is a hash collision
 * with another Entry.
 */
TEST_F(HashTableTest, lookupEntry_hashCollision) {
    setup(0, 1);
    EXPECT_EQ(&entryAt(&ht, 0, 0),
              findBucketAndLookupEntry(&ht, 0, "0", 1));
    values[0]->setKey("randomKeyValue");
    EXPECT_EQ(static_cast<HashTable::Entry*>(NULL),
              findBucketAndLookupEntry(&ht, 0, "0", 1));
}

TEST_F(HashTableTest, lookup) {
    HashTable ht(1);
    TestObject *v = new TestObject(0, "0");
    Key vKey(v->tableId, v->stringKeyPtr, v->stringKeyLength);

    uint64_t outRef;
    EXPECT_FALSE(lookup(&ht, vKey, outRef));

    uint64_t vRef = v->u64Address();
    replace(&ht, vKey, vRef);
    EXPECT_TRUE(lookup(&ht, vKey, outRef));
    EXPECT_EQ(outRef, vRef);

    delete v;
}

#if 0
TEST_F(HashTableTest, remove) {
    HashTable ht(1);

    Key key(0, "0", 1);
    EXPECT_FALSE(ht.remove(key));

    TestObject *v = new TestObject(0, "0");
    uint64_t vRef = v->u64Address();

    replace(&ht, key, vRef);
    EXPECT_TRUE(ht.remove(key));

    uint64_t outRef = 0;
    EXPECT_FALSE(lookup(&ht, key, outRef));
    EXPECT_FALSE(ht.remove(key));

    delete v;
}

TEST_F(HashTableTest, replace_normal) {
    HashTable ht(1);

    TestObject *v = new TestObject(0, "0");
    TestObject *w = new TestObject(0, "0");

    uint64_t vRef = v->u64Address();
    uint64_t wRef = w->u64Address();

    // key is identical for both
    Key key(v->tableId, v->stringKeyPtr, v->stringKeyLength);

    EXPECT_FALSE(ht.replace(key.getHash(), vRef));

    uint64_t outRef;

    EXPECT_TRUE(lookup(&ht, key, outRef));
    EXPECT_EQ(vRef, outRef);

    EXPECT_TRUE(ht.replace(key.getHash(), vRef));
    EXPECT_TRUE(lookup(&ht, key, outRef));
    EXPECT_EQ(vRef, outRef);

    EXPECT_TRUE(ht.replace(key.getHash(), wRef));
    EXPECT_TRUE(lookup(&ht, key, outRef));
    EXPECT_EQ(wRef, outRef);

    delete v;
    delete w;
}

/**
 * Test #RAMCloud::HashTable::replace() when the key is new and the
 * first entry of the first cache line is available.
 */
TEST_F(HashTableTest, replace_cacheLine0Entry0) {
    setup(0, 0);
    TestObject v(0, "newKey");
    Key vKey(v.tableId, v.stringKeyPtr, v.stringKeyLength);
    uint64_t vRef = v.u64Address();
    ht.replace(vKey.getHash(), vRef);
    assertEntryIs(&ht, 0, 0, &v);
}

/**
 * Test #RAMCloud::HashTable::replace() when the key is new and the
 * last entry of the first cache line is available.
 */
TEST_F(HashTableTest, replace_cacheLine0Entry7) {
    setup(0, HashTable::ENTRIES_PER_CACHE_LINE - 1);
    TestObject v(0, "newKey");
    Key vKey(v.tableId, v.stringKeyPtr, v.stringKeyLength);
    uint64_t vRef = v.u64Address();
    ht.replace(vKey.getHash(), vRef);
    assertEntryIs(&ht, 0, seven, &v);
}

/**
 * Test #RAMCloud::HashTable::replace() when the key is new and the
 * first entry of the third cache line is available. The third cache line
 * is already chained onto the second.
 */
TEST_F(HashTableTest, replace_cacheLine2Entry0) {
    setup(0, HashTable::ENTRIES_PER_CACHE_LINE * 2);
    ht.buckets.get()[2].entries[0].clear();
    ht.buckets.get()[2].entries[1].clear();
    TestObject v(0, "newKey");
    Key vKey(v.tableId, v.stringKeyPtr, v.stringKeyLength);
    uint64_t vRef = v.u64Address();
    ht.replace(vKey.getHash(), vRef);
    assertEntryIs(&ht, 2, 0, &v);
}

/**
 * Test #RAMCloud::HashTable::replace() when the key is new and the
 * first and only cache line is full. The second cache line needs to be
 * allocated.
 */
TEST_F(HashTableTest, replace_cacheLineFull) {
    setup(0, HashTable::ENTRIES_PER_CACHE_LINE);
    TestObject v(0,  "newKey");
    Key vKey(v.tableId, v.stringKeyPtr, v.stringKeyLength);
    uint64_t vRef = v.u64Address();
    ht.replace(vKey.getHash(), vRef);
    EXPECT_TRUE(entryAt(&ht, 0, seven).getChainPointer() != NULL);
    EXPECT_TRUE(entryAt(&ht, 0, seven).getChainPointer() !=
                &ht.buckets.get()[1]);
    assertEntryIs(&ht, 1, 0, values[seven]);
    assertEntryIs(&ht, 1, 1, &v);
}
#endif

/**
 * Callback used by test_forEach().
 */ 
static void
test_forEach_callback(uint64_t ref, void *cookie)
{
    EXPECT_EQ(cookie, reinterpret_cast<void *>(57));
    reinterpret_cast<TestObject*>(ref)->count++;
}

/**
 * Simple test for #RAMCloud::HashTable::forEach(), ensuring that it
 * properly traverses multiple buckets and chained cachelines.
 */
TEST_F(HashTableTest, forEach) {
    HashTable ht(2);
    uint32_t arrayLen = 256;
    TestObject* checkoff = new TestObject[arrayLen];

    for (uint32_t i = 0; i < arrayLen; i++) {
        string stringKey = format("%u", i);
        checkoff[i].setKey(stringKey);
        Key key(checkoff[i].tableId,
                checkoff[i].stringKeyPtr,
                checkoff[i].stringKeyLength);
        uint64_t ref = checkoff[i].u64Address();
        replace(&ht, key, ref);
    }

    uint64_t t = ht.forEach(test_forEach_callback,
        reinterpret_cast<void *>(57));
    EXPECT_EQ(arrayLen, t);

    for (uint32_t i = 0; i < arrayLen; i++)
        EXPECT_EQ(1U, checkoff[i].count);
}

} // namespace RAMCloud
