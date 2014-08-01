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

/**
 * \file
 * A performance benchmark for the HashTable.
 */

#include <math.h>

#include "Common.h"
#include "Context.h"
#include "Cycles.h"
#include "HashTable.h"
#include "LargeBlockOfMemory.h"
#include "Memory.h"
#include "OptionParser.h"

namespace RAMCloud {
namespace {

class TestObject {
  public:
    // We don't care about tables or string keys, so we'll assume table 0
    // and let our keys be 64-bit integers.
    explicit TestObject(uint64_t key)
        : key(key)
    {
    }

    uint64_t key;
} __attribute__((aligned(64)));

} // anonymous namespace

void
hashTableBenchmark(uint64_t nkeys, uint64_t nlines)
{
    uint64_t i;
    HashTable ht(nlines);
    LargeBlockOfMemory<TestObject> block(nkeys * sizeof(TestObject));
    TestObject* values = block.get();
    assert(nlines == ht.numBuckets);

    printf("hash table keys: %lu\n", nkeys);
    printf("hash table lines: %lu\n", nlines);
    printf("cache line size: %d\n", ht.bytesPerCacheLine());
    printf("load factor: %.03f\n", static_cast<double>(nkeys) /
           (static_cast<double>(nlines) * ht.entriesPerCacheLine()));

    printf("populating table...");
    fflush(stdout);
    for (i = 0; i < nkeys; i++) {
        Key key(0, &i, sizeof(i));
        values[i] = TestObject(i);
        uint64_t reference(reinterpret_cast<uint64_t>(&values[i]));
        ht.insert(key.getHash(), reference);
    }
    printf("done!\n");

    printf("Starting replaces in 3 seconds (get your measurements ready!)\n");
    sleep(3);
    printf("running replace measurements...");
    fflush(stdout);

    // don't use a CycleCounter, as we may want to run without PERF_COUNTERS
    uint64_t replaceCycles = Cycles::rdtsc();
    HashTable::Candidates c;
    for (i = 0; i < nkeys; i++) {
        Key key(0, &i, sizeof(i));
        uint64_t reference(reinterpret_cast<uint64_t>(&values[i]));

        bool success = false;
        ht.lookup(key.getHash(), c);
        while (!c.isDone()) {
            TestObject* candidateObject =
                reinterpret_cast<TestObject*>(c.getReference());
            Key candidateKey(0,
                             &candidateObject->key,
                             sizeof(candidateObject->key));
            if (candidateKey == key) {
                c.setReference(reference);
                success = true;
                break;
            }
            c.next();
        }
        assert(success);
    }
    i = Cycles::rdtsc() - replaceCycles;
    printf("done!\n");

    values = NULL;

    printf("== replace() took %.3f s ==\n", Cycles::toSeconds(i));

    printf("    external avg: %lu ticks, %lu nsec\n",
           i / nkeys, Cycles::toNanoseconds(i / nkeys));

    printf("Starting lookups in 3 seconds (get your measurements ready!)\n");
    sleep(3);
    printf("running lookup measurements...");
    fflush(stdout);

    // don't use a CycleCounter, as we may want to run without PERF_COUNTERS
    uint64_t lookupCycles = Cycles::rdtsc();
    for (i = 0; i < nkeys; i++) {
        Key key(0, &i, sizeof(i));
        uint64_t reference = 0;
        bool success = false;

        ht.lookup(key.getHash(), c);
        while (!c.isDone()) {
            reference = c.getReference();
            TestObject* candidateObject =
                reinterpret_cast<TestObject*>(reference);
            Key candidateKey(0,
                             &candidateObject->key,
                             sizeof(candidateObject->key));
            if (candidateKey == key) {
                success = true;
                break;
            }
            c.next();
        }
        assert(success);
        assert(reinterpret_cast<TestObject*>(reference)->key == i);
    }
    i = Cycles::rdtsc() - lookupCycles;
    printf("done!\n");

    printf("== lookup() took %.3f s ==\n", Cycles::toSeconds(i));

    printf("    external avg: %lu ticks, %lu nsec\n", i / nkeys,
        Cycles::toNanoseconds(i / nkeys));

    uint64_t *histogram = static_cast<uint64_t *>(
        Memory::xmalloc(HERE, nlines * sizeof(histogram[0])));
    memset(histogram, 0, sizeof(nlines * sizeof(histogram[0])));

    for (i = 0; i < nlines; i++) {
        HashTable::CacheLine *cl;
        HashTable::Entry *entry;

        int depth = 1;
        cl = &ht.buckets.get()[i];
        entry = &cl->entries[ht.entriesPerCacheLine() - 1];
        while ((cl = entry->getChainPointer()) != NULL) {
            depth++;
            entry = &cl->entries[ht.entriesPerCacheLine() - 1];
        }
        histogram[depth]++;
    }

    // TODO(ongaro) Dump raw data instead for standard tools or scripts to use.

    printf("chaining histogram:\n");
    for (i = 0; i < nlines; i++) {
        if (histogram[i] != 0) {
            double percent = static_cast<double>(histogram[i]) * 100.0 /
                             static_cast<double>(nlines);
            printf("%5lu: %.4f%%\n", i, percent);
        }
    }

    free(histogram);
    histogram = NULL;
}

} // namespace RAMCloud

int
main(int argc, char **argv)
{
    using namespace RAMCloud;

    Context context(true);

    uint64_t hashTableMegs, numberOfKeys;
    double loadFactor;

    OptionsDescription benchmarkOptions("HashTableBenchmark");
    benchmarkOptions.add_options()
        ("HashTableMegs,h",
         ProgramOptions::value<uint64_t>(&hashTableMegs)->
            default_value(1),
         "Megabytes of memory allocated to the HashTable")
        ("LoadFactor,f",
         ProgramOptions::value<double>(&loadFactor)->
            default_value(0.50),
         "Load factor desired (automatically calculate the number of keys)")
        ("NumberOfKeys,n",
         ProgramOptions::value<uint64_t>(&numberOfKeys)->
            default_value(0),
         "Number of keys to insert into the HashTable (overrides LoadFactor)");

    OptionParser optionParser(benchmarkOptions, argc, argv);

    uint64_t numberOfCachelines = (hashTableMegs * 1024 * 1024) /
        HashTable::bytesPerCacheLine();
    // HashTable will round to a power of two to avoid divides.
    numberOfCachelines = BitOps::powerOfTwoLessOrEqual(numberOfCachelines);

    // If the user specified a load factor, auto-calculate the number of
    // keys based on the number of cachelines.
    if (numberOfKeys == 0) {
        uint64_t totalEntries = numberOfCachelines *
            HashTable::entriesPerCacheLine();
        numberOfKeys = static_cast<uint64_t>(loadFactor *
                          static_cast<double>(totalEntries));
    }

    hashTableBenchmark(numberOfKeys, numberOfCachelines);
    return 0;
}
