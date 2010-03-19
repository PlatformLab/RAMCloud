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

#include <Common.h>

#include <HashTable.h>

#include <math.h>

namespace RAMCloud {

void
hashTableBenchmark(uint64_t nkeys, uint64_t nlines)
{
    uint64_t i;
    HashTable ht(nlines);
    Object values[nkeys];

    printf("cache line size: %d\n", sizeof(HashTable::CacheLine));
    printf("load factor: %.03f\n", static_cast<double>(nkeys) /
           (static_cast<double>(nlines) * HashTable::ENTRIES_PER_CACHE_LINE));

    for (i = 0; i < nkeys; i++) {
        values[i].key = i;
        ht.insert(i, &values[i]);
    }

    uint64_t b = rdtsc();
    for (i = 0; i < nkeys; i++) {
        const Object *p = ht.lookup(i);
        assert(static_cast<uint64_t>(p - values) == i);
    }
    printf("lookup avg: %llu\n", (rdtsc() - b) / nkeys);

    const HashTable::PerfCounters & pc = ht.getPerfCounters();

    printf("insert: %llu avg ticks, %llu / %lu multi-cacheline accesses\n",
         pc.insertCycles / nkeys, pc.insertChainsFollowed, nkeys);
    printf("lookup: %llu avg ticks, %llu / %lu multi-cacheline accesses, "
           "%llu minikey false positives\n",
           pc.lookupEntryCycles / nkeys, pc.lookupEntryChainsFollowed,
           nkeys, pc.lookupEntryHashCollisions);
    printf("lookup: %llu min ticks\n", pc.lookupEntryDist.min);
    printf("lookup: %llu max ticks\n", pc.lookupEntryDist.max);

    int histogram[nlines];
    memset(histogram, 0, sizeof(histogram));

    for (i = 0; i < nlines; i++) {
        HashTable::CacheLine *cl;
        HashTable::Entry *entry;

        int depth = 1;
        cl = &ht.buckets[i];
        entry = &cl->entries[HashTable::ENTRIES_PER_CACHE_LINE - 1];
        while (entry->isChainLink()) {
            depth++;
            cl = entry->getChainPointer();
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

    const HashTable::PerfDistribution & lcd = pc.lookupEntryDist;

    printf("lookup cycle histogram:\n");
    for (i = 0; i < lcd.NBINS; i++) {
        if (lcd.bins[i] == 0)
           continue;
        double percent = static_cast<double>(lcd.bins[i]) * 100.0 /
                         static_cast<double>(nkeys);
        if (percent < 0.5)
            continue;
        printf("%5d to %5d ticks: (%6.2f%%) ", i * lcd.BIN_WIDTH,
                                               (i + 1) * lcd.BIN_WIDTH - 1,
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
        printf(" <= %5d ticks: %6.2f%%\n", (i + 1) * lcd.BIN_WIDTH - 1, total);
        if (total >= 99.99)
            break;
    }
}

} // namespace RAMCloud

int
main()
{
    RAMCloud::hashTableBenchmark(1024 * 4, 1024);
    return 0;
}
