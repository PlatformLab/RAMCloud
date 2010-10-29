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

/**
 * \file
 * A performance benchmark for the HashTable.
 */

#include <math.h>

#include "Common.h"
#include "BenchUtil.h"
#include "HashTable.h"
#include "OptionParser.h"

namespace RAMCloud {

void
hashTableBenchmark(uint64_t nkeys, uint64_t nlines)
{
    uint64_t i;
    HashTable ht(nlines);
    Object **values = static_cast<Object**>(xmalloc(nkeys * sizeof(values[0])));

    printf("hash table keys: %lu\n", nkeys);
    printf("hash table lines: %lu\n", nlines);
    printf("cache line size: %d\n", sizeof(HashTable::CacheLine));
    printf("load factor: %.03f\n", static_cast<double>(nkeys) /
           (static_cast<double>(nlines) * HashTable::ENTRIES_PER_CACHE_LINE));

    printf("populating table...");
    fflush(stdout);
    for (i = 0; i < nkeys; i++) {
        values[i] = new Object(sizeof(Object));
        values[i]->table = 0;
        values[i]->id = i;
        ht.replace(0, i, values[i]);

        // Just here in case.
        //   NB: This alters our PerfDistribution bin counts,
        //       so be sure to adjust for it later on!
        assert(ht.lookup(0, i) == values[i]);
    }
    printf("done!\n");

    free(values);
    values = NULL;

    CycleCounter lookupCycles;
    for (i = 0; i < nkeys; i++) {
        const Object *p = ht.lookup(0, i);
        assert(p != NULL);
    }
    i = lookupCycles.stop();
    printf("lookup avg: %llu ticks, %llu nsec\n", i / nkeys,
        cyclesToNanoseconds(i / nkeys));

    const HashTable::PerfCounters & pc = ht.getPerfCounters();

    printf("replace: %llu avg ticks, %llu nsec, %llu / %lu multi-cacheline "
           "accesses\n",
           pc.replaceCycles / nkeys,
           cyclesToNanoseconds(pc.replaceCycles / nkeys),
           pc.insertChainsFollowed, nkeys);
    printf("lookup: %llu avg ticks, %llu nsec, %llu / %lu multi-cacheline "
           "accesses, %llu minikey false positives\n",
           pc.lookupEntryCycles / nkeys,
           cyclesToNanoseconds(pc.lookupEntryCycles / nkeys),
           pc.lookupEntryChainsFollowed, nkeys, pc.lookupEntryHashCollisions);
    printf("lookup: %llu min ticks, %llu nsec\n",
           pc.lookupEntryDist.min,
           cyclesToNanoseconds(pc.lookupEntryDist.min));
    printf("lookup: %llu max ticks, %llu nsec\n",
           pc.lookupEntryDist.max,
           cyclesToNanoseconds(pc.lookupEntryDist.max));

    uint64_t *histogram = static_cast<uint64_t *>(
        xmalloc(nlines * sizeof(histogram[0])));
    memset(histogram, 0, sizeof(nlines * sizeof(histogram[0])));

    for (i = 0; i < nlines; i++) {
        HashTable::CacheLine *cl;
        HashTable::Entry *entry;

        int depth = 1;
        cl = &ht.buckets[i];
        entry = &cl->entries[HashTable::ENTRIES_PER_CACHE_LINE - 1];
        while ((cl = entry->getChainPointer()) != NULL) {
            depth++;
            entry = &cl->entries[HashTable::ENTRIES_PER_CACHE_LINE - 1];
        }
        histogram[depth]++;
    }

    // TODO(ongaro) Dump raw data instead for standard tools or scripts to use.

    printf("chaining histogram:\n");
    for (i = 0; i < nlines; i++) {
        if (histogram[i] != 0) {
            double percent = static_cast<double>(histogram[i]) * 100.0 /
                             static_cast<double>(nlines);
            printf("%5d: %.4f%%\n", i, percent);
        }
    }

    free(histogram);
    histogram = NULL;

    const HashTable::PerfDistribution & lcd = pc.lookupEntryDist;

    printf("lookup cycle histogram:\n");
    for (i = 0; i < lcd.NBINS; i++) {
        if (lcd.bins[i] == 0)
           continue;
        double percent = static_cast<double>(lcd.bins[i]) * 100.0 /
                         static_cast<double>(nkeys * 2);
        if (percent < 0.5)
            continue;
        printf("%5d to %5d ticks / %5d to %5d nsec: (%6.2f%%) ",
               i * lcd.BIN_WIDTH, (i + 1) * lcd.BIN_WIDTH - 1,
               cyclesToNanoseconds(i * lcd.BIN_WIDTH),
               cyclesToNanoseconds((i + 1) * lcd.BIN_WIDTH - 1),
               percent);
        int j;
        for (j = 0; j < static_cast<int>(round(percent)); j++)
            printf("*");
        printf("\n");
    }

    printf("lookup cycle CDF:\n");
    double total = 0;
    for (i = 0; i < lcd.NBINS; i++) {
        if (lcd.bins[i] == 0)
            continue;
        double percent = static_cast<double>(lcd.bins[i]) * 100.0 /
                         static_cast<double>(nkeys * 2);
        total += percent;
        printf(" <= %5d ticks (%d nsec): %6.2f%%\n",
               (i + 1) * lcd.BIN_WIDTH - 1,
               cyclesToNanoseconds((i + 1) * lcd.BIN_WIDTH - 1), total);
        if (total >= 99.99)
            break;
    }
}

} // namespace RAMCloud

int
main(int argc, char **argv)
{
    uint64_t hashTableMegs, numberOfKeys;
    double loadFactor;

    RAMCloud::OptionsDescription benchmarkOptions("HashTableBenchmark");
    benchmarkOptions.add_options()
        ("HashTableMegs,h",
         RAMCloud::ProgramOptions::value<uint64_t>(&hashTableMegs)->
            default_value(1),
         "Megabytes of memory allocated to the HashTable")
        ("LoadFactor,f",
         RAMCloud::ProgramOptions::value<double>(&loadFactor)->
            default_value(0.50),
         "Load factor desired (automatically calculate the number of keys)")
        ("NumberOfKeys,n",
         RAMCloud::ProgramOptions::value<uint64_t>(&numberOfKeys)->
            default_value(0),
         "Number of keys to insert into the HashTable (overrides LoadFactor)");

    RAMCloud::OptionParser optionParser(benchmarkOptions, argc, argv);

    uint64_t numberOfCachelines = (hashTableMegs * 1024 * 1024) /
        RAMCloud::HashTable::bytesPerCacheLine();

    // If the user specified a load factor, auto-calculate the number of
    // keys based on the number of cachelines.
    if (numberOfKeys == 0) {
        uint64_t totalEntries = numberOfCachelines *
            RAMCloud::HashTable::entriesPerCacheLine();
        numberOfKeys = loadFactor * totalEntries;
    }

    RAMCloud::hashTableBenchmark(numberOfKeys, numberOfCachelines);
    return 0;
}
