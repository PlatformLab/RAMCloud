/* Copyright (c) 2009 Stanford University
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

// RAMCloud pragma [GCCWARN=5]
// RAMCloud pragma [CPPLINT=0]

#include <Common.h>

#include <Hashtable.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

/**
 * Unit tests for Hashtable::Entry.
 */
class HashtableEntryTest : public CppUnit::TestFixture {

    //Hashtable::Entry entries[10];

    DISALLOW_COPY_AND_ASSIGN(HashtableEntryTest); // NOLINT

    CPPUNIT_TEST_SUITE(HashtableEntryTest);
    CPPUNIT_TEST(test_size);
    CPPUNIT_TEST(test_pack);
    CPPUNIT_TEST(test_clear);
    CPPUNIT_TEST(test_setLogPointer);
    CPPUNIT_TEST(test_setChainPointer);
    CPPUNIT_TEST(test_isAvailable);
    CPPUNIT_TEST(test_getLogPointer);
    CPPUNIT_TEST(test_getChainPointer);
    CPPUNIT_TEST(test_hashMatches);
    CPPUNIT_TEST(test_isChainLink);
    CPPUNIT_TEST_SUITE_END();

    static bool
    packable(uint64_t hash, bool chain, uint64_t ptr)
    {
        Hashtable::Entry e;

        Hashtable::Entry::UnpackedEntry in;
        Hashtable::Entry::UnpackedEntry out;

        in.hash = hash;
        in.chain = chain;
        in.ptr = reinterpret_cast<void*>(ptr);

        e.pack(in.hash, in.chain, in.ptr);
        out = e.unpack();

        return (in.hash == out.hash &&
                in.chain == out.chain &&
                in.ptr == out.ptr);
    }

  public:
    HashtableEntryTest() {}

    void test_size()
    {
        CPPUNIT_ASSERT(8 == sizeof(Hashtable::Entry));
    }

    void test_pack()
    {
        CPPUNIT_ASSERT(packable(0x0000UL, false, 0x000000000000UL));
        CPPUNIT_ASSERT(packable(0xffffUL, true,  0x7fffffffffffUL));
        CPPUNIT_ASSERT(packable(0xffffUL, false, 0x7fffffffffffUL));
        CPPUNIT_ASSERT(packable(0xa257UL, false, 0x3cdeadbeef98UL));
    }

    void test_clear()
    {
        Hashtable::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        e.clear();
        Hashtable::Entry::UnpackedEntry out;
        out = e.unpack();
        CPPUNIT_ASSERT_EQUAL(0UL, out.hash);
        CPPUNIT_ASSERT_EQUAL(false, out.chain);
        CPPUNIT_ASSERT_EQUAL((void*) NULL, out.ptr);
    }

    void test_setLogPointer()
    {
        Hashtable::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        e.setLogPointer(0xaaaaUL, (void*) 0x7fffffffffffUL);
        Hashtable::Entry::UnpackedEntry out;
        out = e.unpack();
        CPPUNIT_ASSERT_EQUAL(0xaaaaUL, out.hash);
        CPPUNIT_ASSERT_EQUAL(false, out.chain);
        CPPUNIT_ASSERT_EQUAL((void*) 0x7fffffffffffUL, out.ptr);
    }

    void test_setChainPointer()
    {
        Hashtable::Entry e;
        e.value = 0xdeadbeefdeadbeefUL;
        e.setChainPointer((Hashtable::cacheline*) 0x7fffffffffffUL);
        Hashtable::Entry::UnpackedEntry out;
        out = e.unpack();
        CPPUNIT_ASSERT_EQUAL(0UL, out.hash);
        CPPUNIT_ASSERT_EQUAL(true, out.chain);
        CPPUNIT_ASSERT_EQUAL((void*) 0x7fffffffffffUL, out.ptr);
    }

    void test_isAvailable()
    {
        Hashtable::Entry e;
        e.clear();
        CPPUNIT_ASSERT(e.isAvailable());
        e.setChainPointer((Hashtable::cacheline*) 0x1UL);
        CPPUNIT_ASSERT(!e.isAvailable());
        e.setLogPointer(0UL, (void*) 0x1UL);
        CPPUNIT_ASSERT(!e.isAvailable());
        e.clear();
        CPPUNIT_ASSERT(e.isAvailable());
    }

    void test_getLogPointer()
    {
        Hashtable::Entry e;
        e.setLogPointer(0xaaaaUL, (void*) 0x7fffffffffffUL);
        CPPUNIT_ASSERT_EQUAL((void*) 0x7fffffffffffUL, e.getLogPointer());
    }

    void test_getChainPointer()
    {
        Hashtable::Entry e;
        e.setChainPointer((Hashtable::cacheline*) 0x7fffffffffffUL);
        CPPUNIT_ASSERT_EQUAL((Hashtable::cacheline*) 0x7fffffffffffUL,
                             e.getChainPointer());
    }

    void test_hashMatches()
    {
        Hashtable::Entry e;
        e.clear();
        CPPUNIT_ASSERT(!e.hashMatches(0UL));
        e.setChainPointer((Hashtable::cacheline*) 0x1UL);
        CPPUNIT_ASSERT(!e.hashMatches(0UL));
        e.setLogPointer(0UL, (void*) 0x1UL);
        CPPUNIT_ASSERT(e.hashMatches(0UL));
        CPPUNIT_ASSERT(!e.hashMatches(0xbeefUL));
        e.setLogPointer(0xbeefUL, (void*) 0x1UL);
        CPPUNIT_ASSERT(!e.hashMatches(0UL));
        CPPUNIT_ASSERT(e.hashMatches(0xbeefUL));
        CPPUNIT_ASSERT(!e.hashMatches(0xfeedUL));
    }

    void test_isChainLink()
    {
        Hashtable::Entry e;
        e.clear();
        CPPUNIT_ASSERT(!e.isChainLink());
        e.setChainPointer((Hashtable::cacheline*) 0x1UL);
        CPPUNIT_ASSERT(e.isChainLink());
        e.setLogPointer(0UL, (void*) 0x1UL);
        CPPUNIT_ASSERT(!e.isChainLink());
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(HashtableEntryTest);

/**
 * Unit tests for Hashtable::Entry.
 */
class HashtablePerfDistributionTest : public CppUnit::TestFixture {

    //Hashtable::Entry entries[10];

    DISALLOW_COPY_AND_ASSIGN(HashtablePerfDistributionTest); // NOLINT

    CPPUNIT_TEST_SUITE(HashtablePerfDistributionTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_storeSample);
    CPPUNIT_TEST_SUITE_END();

  public:
    HashtablePerfDistributionTest() {}

    void test_constructor()
    {
        RAMCloud::Hashtable::PerfDistribution d;
        CPPUNIT_ASSERT_EQUAL(~0UL, d.min);
        CPPUNIT_ASSERT_EQUAL(0UL, d.max);
        CPPUNIT_ASSERT_EQUAL(0UL, d.binOverflows);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[0]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[1]);
        CPPUNIT_ASSERT_EQUAL(0UL, d.bins[2]);
    }

    void test_storeSample()
    {
        Hashtable::PerfDistribution d;

        // You can't use CPPUNIT_ASSERT_EQUAL here because it tries to take a
        // reference to BIN_WIDTH. See 10.4.6.2 Member Constants of The C++
        // Programming Language by Bjarne Stroustrup for more about static
        // constant integers.
        CPPUNIT_ASSERT(10 == Hashtable::PerfDistribution::BIN_WIDTH);

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
CPPUNIT_TEST_SUITE_REGISTRATION(HashtablePerfDistributionTest);

class HashtableTest : public CppUnit::TestFixture {
  public:
    void setUp();
    void tearDown();
    void TestSizes();
    void TestSimple();
    void TestMain();
  private:
    CPPUNIT_TEST_SUITE(HashtableTest);
    CPPUNIT_TEST(TestSizes);
    CPPUNIT_TEST(TestSimple);
    CPPUNIT_TEST(TestMain);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::Hashtable *ht;
};
CPPUNIT_TEST_SUITE_REGISTRATION(HashtableTest);

#define NLINES 1024
void
HashtableTest::setUp()
{
    ht = new RAMCloud::Hashtable(NLINES);
}

void
HashtableTest::tearDown()
{
    delete ht;
}

void
HashtableTest::TestSizes()
{
    // We're specifically aiming to fit in a cache line.
    CPPUNIT_ASSERT(8 == sizeof(Hashtable::Entry));
    CPPUNIT_ASSERT(8 * Hashtable::ENTRIES_PER_CACHE_LINE ==
                   sizeof(Hashtable::cacheline));
}

void
HashtableTest::TestSimple()
{
    RAMCloud::Hashtable ht(1024);

    uint64_t a = 0;
    uint64_t b = 10;
    uint64_t c = 11;

    CPPUNIT_ASSERT(ht.Lookup(0) == NULL);
    ht.Insert(0, &a);
    CPPUNIT_ASSERT(ht.Lookup(0) == &a);
    CPPUNIT_ASSERT(ht.Lookup(10) == NULL);
    ht.Insert(10, &b);
    CPPUNIT_ASSERT(ht.Lookup(10) == &b);
    CPPUNIT_ASSERT(ht.Lookup(0) == &a);
}

void
HashtableTest::TestMain()
{
    uint64_t i;
    uint64_t nkeys = NLINES * 4;
    uint64_t nlines = NLINES;

    printf("cache line size: %d\n", sizeof(RAMCloud::Hashtable::cacheline));
    printf("load factor: %.03f\n", (double)nkeys / ((double)nlines * 8));

    for (i = 0; i < nkeys; i++) {
        uint64_t *p = (uint64_t *) xmalloc(sizeof(*p));
        *p = i;
        ht->Insert(*p, p);
    }

    uint64_t b = rdtsc();
    for (i = 0; i < nkeys; i++) {
        uint64_t *p = (uint64_t *) ht->Lookup(i);
        if (p == NULL || *p != i) {
            printf("ERROR: p == NULL || *p != key\n");
            CPPUNIT_ASSERT(false);
        }
    }
    printf("lookup avg: %llu\n", (rdtsc() - b) / nkeys);

    const Hashtable::PerfCounters & pc = ht->getPerfCounters();

    printf("insert: %llu avg ticks, %llu / %lu multi-cacheline accesses\n",
         pc.insertCycles / nkeys, pc.insertChainsFollowed, nkeys);
    printf("lookup: %llu avg ticks, %llu / %lu multi-cacheline accesses, "
           "%llu minikey false positives\n",
           pc.lookupKeyPtrCycles / nkeys, pc.lookupKeyPtrChainsFollowed,
           nkeys, pc.lookupKeyPtrHashCollisions);
    printf("lookup: %llu min ticks\n", pc.lookupKeyPtrDist.min);
    printf("lookup: %llu max ticks\n", pc.lookupKeyPtrDist.max);

    int *histogram = (int *) xmalloc(sizeof(int) * nlines);
    memset(histogram, 0, sizeof(int) * nlines);

    // TODO maybe add these as friend functions for testing the hashtable
    // class
    /*
    for (i = 0; i < nlines; i++) {
        struct cacheline *cl = &table[i];

        int depth = 1;
        while (ISCHAIN(cl, 7)) {
            depth++;
            cl = (struct cacheline *)GETCHAINPTR(cl, 7);
        }
        histogram[depth]++;
    }

    printf("chaining histogram:\n");
    for (i = 0; i < table_lines; i++)
        if (histogram[i] != 0)
            printf("%5d: %.4f%%\n", i, (double)histogram[i] / table_lines * 100.0);

    printf("lookup cycle histogram:\n");
    for (i = 0; i < NBUCKETS; i++) {
        if (buckets[i] != 0 && (double)buckets[i] / nkeys * 100.0 >= 0.0001) {
            //printf("%5d to %5d ticks: %.4f%%\n", i * 10, (i + 1) * 10 - 1, (double)buckets[i] / nkeys * 100.0);
            int pct = round((double)buckets[i] / nkeys * 100.0);
            if (pct > 0) {
                printf("%5d to %5d ticks: (%6.2f%%) ", i * 10, (i + 1) * 10 - 1, (double)buckets[i] / nkeys * 100.0);
                int j;
                for (j = 0; j < pct; j++)
                    printf("*");
                printf("\n");
            }
        }
    }

    printf("lookup cycle CDF:\n");
    double total = 0;
    for (i = 0; i < NBUCKETS; i++) {
        if (buckets[i] == 0)
            continue;
        total += (double)buckets[i] / nkeys * 100.0;
        printf(" <= %5d ticks: %6.2f%%\n", (i + 1) * 10 - 1, total);
        if (total >= 99.99)
            break;
    }
    */
}

} // namespace RAMCloud
