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
#include "Object.h"
#include "OptionParser.h"

namespace RAMCloud {

void
hashTableBenchmark(uint64_t nkeys, uint64_t nlines)
{
    uint64_t i;
    ObjectMap ht(nlines);
    Object **values = static_cast<Object**>(xmalloc(nkeys * sizeof(values[0])));

    printf("hash table keys: %lu\n", nkeys);
    printf("hash table lines: %lu\n", nlines);
    printf("cache line size: %d\n", ht.bytesPerCacheLine());
    printf("load factor: %.03f\n", static_cast<double>(nkeys) /
           (static_cast<double>(nlines) * ht.entriesPerCacheLine()));

    printf("populating table...");
    fflush(stdout);
    for (i = 0; i < nkeys; i++) {
        values[i] = new Object(sizeof(Object));
        values[i]->table = 0;
        values[i]->id = i;
        ht.replace(0, i, values[i]);

        // Here just in case.
        //   NB: This alters our PerfDistribution bin counts,
        //       so be sure to reset them below!
        assert(ht.lookup(0, i) == values[i]);
    }
    printf("done!\n");

    // replace/lookup affects the PerfDistribution, so reset for replace
    // benchmarks
    ht.resetPerfCounters();

    printf("running replace measurements...");
    fflush(stdout);

    // don't use a CycleCounter, as we may want to run without PERF_COUNTERS
    uint64_t replaceCycles = rdtsc();
    for (i = 0; i < nkeys; i++)
        ht.replace(0, i, values[i]);
    i = rdtsc() - replaceCycles;
    printf("done!\n");

    free(values);
    values = NULL;

    const ObjectMap::PerfCounters & pc = ht.getPerfCounters();

    printf("== replace() ==\n");

    printf("    external avg: %lu ticks, %lu nsec\n",
           i / nkeys, cyclesToNanoseconds(i / nkeys));

    printf("    internal avg: %lu ticks, %lu nsec\n",
           pc.replaceCycles / nkeys,
           cyclesToNanoseconds(pc.replaceCycles / nkeys));

    printf("    multi-cacheline accesses: %lu / %lu\n",
           pc.insertChainsFollowed, nkeys);

    // replace affects the PerfDistribution, so reset for lookup benchmarks
    ht.resetPerfCounters();

    printf("running lookup measurements...");
    fflush(stdout);

    // don't use a CycleCounter, as we may want to run without PERF_COUNTERS
    uint64_t lookupCycles = rdtsc();
    for (i = 0; i < nkeys; i++) {
        const Object *p = ht.lookup(0, i);
        assert(p != NULL);
    }
    i = rdtsc() - lookupCycles;
    printf("done!\n");

    printf("== lookup() ==\n");

    printf("    external avg: %lu ticks, %lu nsec\n", i / nkeys,
        cyclesToNanoseconds(i / nkeys));

    printf("    internal avg: %lu ticks, %lu nsec\n",
           pc.lookupEntryCycles / nkeys,
           cyclesToNanoseconds(pc.lookupEntryCycles / nkeys));

    printf("    multi-cacheline accesses: %lu / %lu\n",
           pc.lookupEntryChainsFollowed, nkeys);

    printf("    minikey false positives: %lu\n", pc.lookupEntryHashCollisions);

    printf("    min ticks: %lu, %lu nsec\n",
           pc.lookupEntryDist.min,
           cyclesToNanoseconds(pc.lookupEntryDist.min));

    printf("    max ticks: %lu, %lu nsec\n",
           pc.lookupEntryDist.max,
           cyclesToNanoseconds(pc.lookupEntryDist.max));

    uint64_t *histogram = static_cast<uint64_t *>(
        xmalloc(nlines * sizeof(histogram[0])));
    memset(histogram, 0, sizeof(nlines * sizeof(histogram[0])));

    for (i = 0; i < nlines; i++) {
        ObjectMap::CacheLine *cl;
        ObjectMap::Entry *entry;

        int depth = 1;
        cl = &ht.buckets[i];
        entry = &cl->entries[ht.entriesPerCacheLine() - 1];
        while ((cl = entry->getChainPointer(0)) != NULL) {
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

    const ObjectMap::PerfDistribution & lcd = pc.lookupEntryDist;

    printf("lookup cycle histogram:\n");
    for (i = 0; i < lcd.NBINS; i++) {
        if (lcd.bins[i] == 0)
           continue;
        double percent = static_cast<double>(lcd.bins[i]) * 100.0 /
                         static_cast<double>(nkeys);
        if (percent < 0.5)
            continue;
        printf("%5lu to %5lu ticks / %5lu to %5lu nsec: (%6.2f%%) ",
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
                         static_cast<double>(nkeys);
        total += percent;
        printf(" <= %5lu ticks (%lu nsec): %6.2f%%\n",
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
    using namespace RAMCloud;

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
        ObjectMap::bytesPerCacheLine();

    // If the user specified a load factor, auto-calculate the number of
    // keys based on the number of cachelines.
    if (numberOfKeys == 0) {
        uint64_t totalEntries = numberOfCachelines *
            ObjectMap::entriesPerCacheLine();
        numberOfKeys = loadFactor * totalEntries;
    }

    hashTableBenchmark(numberOfKeys, numberOfCachelines);
    return 0;
}
