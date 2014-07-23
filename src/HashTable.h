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

#ifndef RAMCLOUD_HASHTABLE_H
#define RAMCLOUD_HASHTABLE_H

#include "Common.h"
#include "BitOps.h"
#include "CycleCounter.h"
#include "LargeBlockOfMemory.h"
#include "Memory.h"
#include "MurmurHash3.h"
#include "Key.h"

namespace RAMCloud {

/**
 * A map from Key objects to 47-bit "references". These references are just
 * opaque values that could be anything from direct pointers, to indexes into
 * some other structure, or even tiny bits of data themselves. The only
 * requirements are that 1) they fit within the lower 47 bits of a uint64_t,
 * and 2) the value 0 is never used.
 *
 * This class is used, for instance, in resolving most object-level %RAMCloud
 * requests. I.e., to read and write a %RAMCloud object, this lets you find the
 * location of the the object in the log.
 *
 * This code is not thread-safe.
 *
 * \section impl Implementation Details
 *
 * The HashTable is an array of #buckets, indexed by the hash of the two
 * uint64_t keys. Each bucket consists of one or more chained \link CacheLine
 * CacheLines\endlink, the first of which lives inline in the array of buckets.
 * Each cache line consists of several hash table \link Entry Entries\endlink
 * in no particular order.
 *
 * If there are too many hash table entries to fit in the bucket's first cache
 * line, additional overflow cache lines are allocated (outside of the array of
 * buckets). In this case, the last hash table entry in each of the
 * non-terminal cache lines has a pointer to the next cache line instead of a
 * log reference.
 */
class HashTable {
  PRIVATE:
    // Forward declaration.
    struct CacheLine;

    /**
     * A hash table entry.
     *
     * Hash table entries live on \link CacheLine CacheLines\endlink.
     *
     * A normal hash table entry (see #setReference(), #getReference(), and
     * #hashMatches()) consists of secondary bits extracted from the result of
     * #Key::gethash() to disambiguate most bucket collisions and the log
     * reference. In this case, its chain bit will not be set and its reference
     * will be valid.
     *
     * A chaining hash table entry (see #setChainPointer(), #getChainPointer())
     * instead consists of a pointer to another cache line where additional
     * entries can be found. In this case, its chain bit will be set.
     *
     * A hash table entry can also be unused (see #clear() and #isAvailable()).
     * In this case, its reference will be invalid.
     */
    class Entry {
      public:
        void clear();
        void setReference(uint64_t hash, uint64_t reference);
        void setChainPointer(CacheLine *ptr);
        bool isAvailable() const;
        uint64_t getReference() const;
        CacheLine* getChainPointer() const;
        bool hashMatches(uint64_t hash) const;

      PRIVATE:
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
         * This is filled in by #unpack().
         * See the parameters of #pack() for an explanation.
         */
        struct UnpackedEntry {
            uint64_t hash;
            bool chain;
            uint64_t ptr;
        };

        void unpack(UnpackedEntry& ue) const;
    };
    static_assert(sizeof(Entry) == 8, "HashTable::Entry is not 8 bytes");

    /**
     * The number of bytes per cache line in this machine.
     */
    static const uint32_t BYTES_PER_CACHE_LINE = 64;

    /**
     * The number of hash table Entry objects in a CacheLine. This directly
     * corresponds to the number of elements (references) each cacheline
     * may contain.
     */
    static const uint32_t ENTRIES_PER_CACHE_LINE = (BYTES_PER_CACHE_LINE /
                                                    sizeof(Entry));
    static_assert(BYTES_PER_CACHE_LINE % sizeof(Entry) == 0,
                  "BYTES_PER_CACHE_LINE not a multiple of sizeof(Entry)");


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
    static_assert(sizeof(CacheLine) == sizeof(Entry) * ENTRIES_PER_CACHE_LINE,
                  "HashTable entries don't fit evenly into a cacheline");

  public:
    /**
     * This class is essentially an iterator for potential matches found during
     * a lookup operation. This exists because the HashTable::lookup() method
     * does not know how to extract and compare keys from elements being stored
     * in the table. Instead, each lookup returns an instance of this object,
     * which the caller can use to loop over any potential matches and do their
     * own key equality comparison.
     *
     * Since the HashTable attempts to avoid extraneous comparisons by keeping a
     * small hash in each reference it stores, in nearly all circumstances this
     * class will iterate over zero or one candidates. However, the caller must
     * be able to deal with arbitrarily many in the case of hash collisions.
     *
     * The main reason for turning the HashTable inside-out like this is (as
     * usual) efficiency. If the HashTable were to do compare keys internally,
     * it would need not allocate a Buffer and fill it in from the Log to do
     * the comparison. Then it'd throw that work away as it pops down the stack
     * to return, after which the caller would have to allocate another Buffer
     * and fill it in to return the same object data.
     */
    class Candidates {
      public:
        Candidates();
        uint64_t getReference();
        void setReference(uint64_t reference);
        void remove();
        void next();
        bool isDone();

      PRIVATE:
        void init(CacheLine* cl, uint64_t secondaryHash);

        /// Pointer to the hash table bucket we're currently iterating over.
        CacheLine* bucket;

        /// Index into bucket we're currently iterating over.
        uint32_t index;

        /// This iterator only returns references to entries that share this
        /// secondaryHash. All others cannot possibly be matches. This helps
        /// to reduce the number of candidates whose keys are extracted from
        /// the log and compared.
        uint64_t secondaryHash;

        friend class HashTable;
    };

    explicit HashTable(uint64_t numBuckets);
    ~HashTable();
    void lookup(KeyHash keyHash, Candidates& candidates);
    void insert(KeyHash keyHash, uint64_t reference);
    uint64_t forEachInBucket(void (*callback)(uint64_t, void *),
                             void *cookie,
                             uint64_t bucket);
    uint64_t forEach(void (*callback)(uint64_t, void *), void *cookie);
    void prefetchBucket(KeyHash keyHash);
    static uint32_t bytesPerCacheLine();
    static uint32_t entriesPerCacheLine();
    uint64_t getNumBuckets() const;
    static uint64_t findBucketIndex(uint64_t numBuckets,
                                    KeyHash keyHash,
                                    uint64_t *secondaryHash);

  PRIVATE:

    // forward declarations
    class Entry;
    struct CacheLine;

    CacheLine * findBucket(KeyHash keyHash, uint64_t *secondaryHash);

    /**
     * The number of buckets allocated to the table.
     */
    const uint64_t numBuckets;

    /**
     * The array of buckets.
     * See HashTable.
     */
    LargeBlockOfMemory<CacheLine> buckets;

    friend void hashTableBenchmark(uint64_t nkeys, uint64_t nlines);
    DISALLOW_COPY_AND_ASSIGN(HashTable);
};


} // namespace RAMCloud

#endif
