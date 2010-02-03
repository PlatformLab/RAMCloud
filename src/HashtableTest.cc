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

#include <config.h>

#include <shared/common.h>
#include <Hashtable.h>

#include <cppunit/extensions/HelperMacros.h>

#include <string>
#include <cstring>
#include <malloc.h>

class HashtableTest : public CppUnit::TestFixture {
  public:
    void setUp();
    void tearDown();
    void TestSimple();
    void TestMain();
  private:
    CPPUNIT_TEST_SUITE(HashtableTest);
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

    printf("cache line size: %d\n", sizeof(RAMCloud::cacheline));
    printf("load factor: %.03f\n", (double)nkeys / ((double)nlines * 8));

    for (i = 0; i < nkeys; i++) {
        uint64_t *p = (uint64_t *) malloc(sizeof(*p));
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

    printf("insert: %llu avg ticks, %llu / %lu multi-cacheline accesses\n",
           ht->GetInsertCount() / nkeys, ht->GetInsertChainTraversals(), nkeys);
    printf("lookup: %llu avg ticks, %llu / %lu multi-cacheline accesses, "
           "%llu minikey false positives\n",
           ht->GetLookupCount() / nkeys, ht->GetLookupChainTraversals(),
           nkeys, ht->GetLookupFalsePositives());
    printf("lookup: %llu min ticks\n", ht->GetMinTicks());
    printf("lookup: %llu max ticks\n", ht->GetMaxTicks());

    int *histogram = (int *) malloc(sizeof(int) * nlines);
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

