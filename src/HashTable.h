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
 * Header file for HashTable.
 */

#ifndef RAMCLOUD_HASHTABLE_H
#define RAMCLOUD_HASHTABLE_H

#include <Common.h>
#include <Object.h>

namespace RAMCloud {

/**
 * A map from object IDs to Object addresses.
 *
 * This is used in resolving most object-level %RAMCloud requests. For example,
 * to read and write a %RAMCloud object, this lets you find the location of the
 * the object.
 *
 * Currently, the HashTable class assumes it is scoped to a specific %RAMCloud
 * table, so it does not concern itself with table IDs.
 *
 * This code is not thread-safe.
 *
 * \section impl Implementation Details
 *
 * The HashTable is an array of #buckets, indexed by the hash of the object ID.
 * Each bucket consists of one or more chained \link CacheLine
 * CacheLines\endlink, the first of which lives inline in the array of buckets.
 * Each cache line consists of several hash table \link Entry Entries\endlink
 * in no particular order.
 *
 * If there are too many hash table entries to fit in the bucket's first cache
 * line, additional overflow cache lines are allocated (outside of the array of
 * buckets). In this case, the last hash table entry in each of the
 * non-terminal cache lines has a pointer to the next cache line instead of a
 * pointer to an Object.
 */
class HashTable {

  public:

    /**
     * Keeps track of statistics for a density distribution of frequencies.
     * See #HashTable::PerfCounters::lookupEntryDist for an example, where it
     * is used to keep track of the distribution of the number of cycles a
     * method takes.
     *
     * See HashTableBenchmark.cc for code to generate a histogram from this.
     *
     * TODO(ongaro): Generalize and move to a utils file.
     */
    struct PerfDistribution {
#if PERF_COUNTERS

        /**
         * The number of bins in which to categorize samples.
         * See #bins
         */
        static const uint64_t NBINS = 5000;

        /**
         * The number of distinct integer values that are recorded in each bin.
         * See #bins.
         */
        static const uint64_t BIN_WIDTH = 10;

        /**
         * The frequencies of samples that fall into each bin.
         * The first bin will have the number of samples between 0 (inclusive)
         * and BIN_WIDTH (exclusive), the second between BIN_WIDTH and
         * BIN_WIDTH * 2, etc.
         */
        uint64_t bins[NBINS];

        /**
         * The frequency of samples that exceeded the highest bin.
         * This is equivalent to the sum of the values in all bins beyond the
         * end of the \a bins array.
         */
        uint64_t binOverflows;

        /**
         * The minimum sample encountered.
         * This will be <tt>~OUL</tt> if no samples were stored.
         */
        uint64_t min;

        /**
         * The maximum sample.
         * This will be <tt>OUL</tt> if no samples were stored.
         */
        uint64_t max;

        PerfDistribution();
        void storeSample(uint64_t value);
#define PERF_DIST_STORE_SAMPLE(d, v) (d).storeSample(v)
#else
#define PERF_DIST_STORE_SAMPLE(d, v) (void) 0
#endif
    };

    /**
     * Performance counters for the HashTable.
     */
    struct PerfCounters {
#if PERF_COUNTERS

        /**
         * The number of #replace() operations.
         */
        uint64_t replaceCalls;

        /**
         * The number of #lookupEntry() operations.
         */
        uint64_t lookupEntryCalls;

        /**
         * The total number of CPU cycles spent across all #replace()
         * operations.
         */
        uint64_t replaceCycles;

        /**
         * The total number of CPU cycles spent across all #lookupEntry()
         * operations.
         */
        uint64_t lookupEntryCycles;

        /**
         * The total number of times a chain pointer was followed to another
         * CacheLine while trying to insert a new entry within #replace().
         */
        uint64_t insertChainsFollowed;

        /**
         * The total number of times a chain pointer was followed to another
         * CacheLine across all #lookupEntry() operations.
         */
        uint64_t lookupEntryChainsFollowed;

        /**
         * The total number of times there was an Entry collision across
         * all #lookupEntry() operations. This is when the buckets collide for
         * an object ID, and the extra disambiguation bits inside the Entry
         * collide, but the Object itself reveals that the entry does not
         * correspond to the given object ID.
         */
        uint64_t lookupEntryHashCollisions;

        /**
         * The distribution of CPU cycles spent for #lookupEntry() operations.
         */
        PerfDistribution lookupEntryDist;

        PerfCounters();
#endif
    };

    explicit HashTable(uint64_t nlines);
    ~HashTable();
    const Object *lookup(uint64_t objectId);
    bool remove(uint64_t objectId);
    bool replace(uint64_t objectId, const Object *ptr);

    /**
     * Return a read-only view of the hash table's performance counters.
     * \return
     *      See above.
     */
    const PerfCounters& getPerfCounters() const {
        return this->perfCounters;
    }

  private:

    // forward declarations
    class Entry;
    struct CacheLine;

    static uint64_t nearestPowerOfTwo(uint64_t n);
    void *mallocAligned(uint64_t len) const;
    void freeAligned(void *p) const;
    static uint64_t hash(uint64_t key);
    CacheLine *findBucket(uint64_t objectId, uint64_t *secondaryHash) const;
    Entry *lookupEntry(CacheLine *bucket, uint64_t secondaryHash,
                       uint64_t objectId);

    /**
     * A hash table entry.
     *
     * Hash table entries live on \link CacheLine CacheLines\endlink.
     *
     * A normal hash table entry (see #setObject(), #getObject(), and
     * #hashMatches()) consists of secondary bits from the #hash() function on
     * the object ID to disambiguate most bucket collisions and the address of
     * the Object. In this case, its chain bit will not be set and its pointer
     * will not be \c NULL.
     *
     * A chaining hash table entry (see #setChainPointer(), #getChainPointer())
     * instead consists of a pointer to another cache line where additional
     * entries can be found. In this case, its chain bit will be set.
     *
     * A hash table entry can also be unused (see #clear() and #isAvailable()).
     * In this case, its pointer will be set to \c NULL.
     */
    class Entry {

      public:
        void clear();
        void setObject(uint64_t hash, const Object *object);
        void setChainPointer(CacheLine *ptr);
        bool isAvailable() const;
        const Object *getObject() const;
        CacheLine *getChainPointer() const;
        bool hashMatches(uint64_t hash) const;

      private:
        /**
         * The packed value stored in the entry.
         *
         * The exact bits are, from MSB to LSB:
         * \li 16 bits for the secondary hash
         * \li 1 bit for whether the pointer is a chain
         * \li 47 bits for the pointer
         *
         * The main reason why it's not a struct with bit fields is that we'll
         * probably want to use atomic operations to set it eventually.
         *
         * Because the exact format is subject to change, you should always set
         * this using #pack() and access its contained fields using #unpack().
         */
        uint64_t value;

        void pack(uint64_t hash, bool chain, uint64_t ptr);

        /**
         * This is the return type of #unpack().
         * See the parameters of #pack() for an explanation.
         */
        struct UnpackedEntry {
            uint64_t hash;
            bool chain;
            uint64_t ptr;
        };

        UnpackedEntry unpack() const;

        friend class HashTableEntryTest;
    };
    static_assert(sizeof(Entry) == 8);

    /**
     * The number of bytes per cache line in this machine.
     */
    static const uint32_t BYTES_PER_CACHE_LINE = 64;

    /**
     * The number of hash table \link Entry Entries\endlink in a CacheLine.
     */
    static const uint32_t ENTRIES_PER_CACHE_LINE = (BYTES_PER_CACHE_LINE /
                                                    sizeof(Entry));
    static_assert(BYTES_PER_CACHE_LINE % sizeof(Entry) == 0);


    /**
     * A linked list of cache lines composes a bucket within the HashTable.
     *
     * Each cache line is composed of several hash table \link Entry
     * Entries\endlink, the last of which may be a link to another CacheLine.
     * See HashTable for more info.
     *
     * A CacheLine is meant to fit on a single L2 cache line on the CPU.
     * Different processors may require tweaking ENTRIES_PER_CACHE_LINE to
     * achieve this.
     */
    struct CacheLine {
        /**
         * See CacheLine.
         */
        Entry entries[ENTRIES_PER_CACHE_LINE];
    };
    static_assert(sizeof(CacheLine) == sizeof(Entry) * ENTRIES_PER_CACHE_LINE);

    /**
     * The array of buckets.
     * See HashTable.
     */
    CacheLine *buckets;

    /**
     * The number of buckets allocated to the table.
     */
    const uint64_t numBuckets;

    /**
     * Whether to allocate memory using #xmalloc_aligned_hugetlb() instead of
     * #xmemalign().
     */
    const bool useHugeTlb;

    /**
     * The performance counters for the HashTable.
     * See #getPerfCounters().
     */
    PerfCounters perfCounters;

    friend class HashTableEntryTest;
    friend class HashTableTest;
    friend void hashTableBenchmark(uint64_t nkeys, uint64_t nlines);
    DISALLOW_COPY_AND_ASSIGN(HashTable);
};


} // namespace RAMCloud

#endif
