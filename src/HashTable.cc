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
 * Implementation for HashTable.
 */

#include <HashTable.h>

#include <Common.h>
#include <ugly_memory_stuff.h>

#include <stdint.h>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>

namespace RAMCloud {

#if PERF_COUNTERS
// HashTable::PerfDistribution


/**
 * Constructor for HashTable::PerfDistribution.
 */
HashTable::PerfDistribution::PerfDistribution()
    : bins(NULL), binOverflows(0), min(~0UL), max(0UL)
{
    bins = new uint64_t[NBINS];
    memset(bins, 0, sizeof(bins));
}

HashTable::PerfDistribution::~PerfDistribution()
{
    delete[] bins;
    bins = NULL;
}

/**
 * Record a sampled value by updating the distribution statistics.
 * \param[in] value
 *      The value sampled.
 */
void
HashTable::PerfDistribution::storeSample(uint64_t value)
{
    if (value / BIN_WIDTH < NBINS)
        bins[value / BIN_WIDTH]++;
    else
        binOverflows++;

    if (value < min)
        min = value;
    if (value > max)
        max = value;
}


// HashTable::PerfCounters


/**
 * Constructor for HashTable::PerfCounters.
 */
HashTable::PerfCounters::PerfCounters()
    : replaceCalls(0), lookupEntryCalls(0), replaceCycles(0),
    lookupEntryCycles(0), insertChainsFollowed(0),
    lookupEntryChainsFollowed(0), lookupEntryHashCollisions(0),
    lookupEntryDist()
{
}
#endif

// HashTable::Entry


/**
 * Replace this hash table entry.
 * \param[in] hash
 *      The secondary hash bits (16 bits) computed from the object ID.
 *      Irrelevant if \a chain is true.
 * \param[in] chain
 *      Whether \a ptr is a chain pointer as opposed to an Object pointer.
 * \param[in] ptr
 *      The chain pointer to the next cache line or the Object pointer
 *      (determined by \a chain).
 */
void
HashTable::Entry::pack(uint64_t hash, bool chain, uint64_t ptr)
{
    if (ptr == 0)
        assert(hash == 0 && !chain);

    uint64_t c = chain ? 1 : 0;
    assert((hash & ~(0x000000000000ffffUL)) == 0);
    assert((ptr  & ~(0x00007fffffffffffUL)) == 0);
    this->value = ((hash << 48)  | (c << 47) | ptr);
}

/**
 * Read the contents of this hash table entry.
 * \return
 *      The extracted values. See UnpackedEntry.
 */
HashTable::Entry::UnpackedEntry HashTable::Entry::unpack() const
{
    UnpackedEntry ue;
    ue.hash  = (this->value >> 48) & 0x000000000000ffffUL;
    ue.chain = (this->value >> 47) & 0x0000000000000001UL;
    ue.ptr   = this->value         & 0x00007fffffffffffUL;
    return ue;
}

/**
 * Reinitialize a hash table entry as unused.
 */
void
HashTable::Entry::clear()
{
    pack(0, false, 0);
}

/**
 * Reinitialize a regular hash table entry.
 * \param[in] hash
 *      The secondary hash bits computed from the object ID (16 bits).
 * \param[in] object
 *      The address of the Object. Must not be \c NULL.
 */
void
HashTable::Entry::setObject(uint64_t hash, const Object *object)
{
    assert(object != NULL);
    pack(hash, false, reinterpret_cast<uint64_t>(object));
}

/**
 * Reinitialize a hash table entry as a chain link.
 * \param[in] ptr
 *      The pointer to the next cache line. Must not be \c NULL.
 */
void
HashTable::Entry::setChainPointer(CacheLine *ptr)
{
    assert(ptr != NULL);
    pack(0, true, reinterpret_cast<uint64_t>(ptr));
}

/**
 * Return whether a hash table entry is unused.
 * \return
 *      See above.
 */
bool
HashTable::Entry::isAvailable() const
{
    UnpackedEntry ue = unpack();
    return (ue.ptr == 0);
}

/**
 * Extract the Object address from a hash table entry.
 * The caller must first verify that the hash table entry indeed stores an
 * object address with #hashMatches().
 * \return
 *      The address of the Object stored.
 */
const Object *
HashTable::Entry::getObject() const
{
    UnpackedEntry ue = unpack();
    assert(!ue.chain && ue.ptr != 0);
    return reinterpret_cast<const Object*>(ue.ptr);
}

/**
 * Extract the chain pointer to another cache line.
 * \return
 *      The chain pointer to another cache line. If this entry does not store a
 *      chain pointer, returns \c NULL instead.
 */
HashTable::CacheLine *
HashTable::Entry::getChainPointer() const
{
    UnpackedEntry ue = unpack();
    if (!ue.chain)
        return NULL;
    return reinterpret_cast<CacheLine*>(ue.ptr);
}

/**
 * Check whether the secondary hash bits stored match those given.
 * \param[in] hash
 *      The secondary hash bits computed from the object ID to test (16 bits).
 * \return
 *      Whether the hash table entry holds the address to an Object and the
 *      secondary hash bits for that Object point to \a hash.
 */
bool
HashTable::Entry::hashMatches(uint64_t hash) const
{
    UnpackedEntry ue = unpack();
    return (!ue.chain && ue.ptr != 0 && ue.hash == hash);
}


// HashTable


/**
 * Constructor for HashTable.
 * \param[in] numBuckets
 *      The number of buckets in the new hash table. This should be a power of
 *      two.
 */
HashTable::HashTable(uint64_t numBuckets)
    : buckets(NULL), numBuckets(nearestPowerOfTwo(numBuckets)),
      useHugeTlb(false), perfCounters()
{
    // Allocate space for a new hash table and set its entries to unused.

    uint64_t i, j;

    if (numBuckets != this->numBuckets) {
        fprintf(stderr, "Warning: HashTable truncated to %lu buckets "
                        "(nearest power of two)\n", this->numBuckets);
    }

    size_t bucketsSize = this->numBuckets * sizeof(CacheLine);
    buckets = static_cast<CacheLine *>(mallocAligned(bucketsSize));

    for (i = 0; i < this->numBuckets; i++) {
        for (j = 0; j < ENTRIES_PER_CACHE_LINE; j++)
            buckets[i].entries[j].clear();
    }
}

/**
 * Destructor for HashTable.
 */
HashTable::~HashTable()
{
    // TODO(ongaro): free chained CacheLines that were allocated in insert()

    if (buckets != NULL) {
        freeAligned(buckets);
        buckets = NULL;
    }
}

/**
 * Find the nearest power of 2 that is less than or equal to \a n.
 * \param n
 *      A maximum for the return value.
 * \return
 *      A power of two that is less than or equal to \a n.
 */
uint64_t
HashTable::nearestPowerOfTwo(uint64_t n)
{
    if ((n & (n - 1)) == 0)
        return n;

    for (int i = 63; i >= 0; i--) {
        if ((1UL << i) <= n)
            return (1 << i);
    }
    return 0;
}


/**
 * Allocate an aligned chunk of memory.
 * \param[in] len
 *      The size of the memory chunk to allocate in bytes.
 * \return
 *      A pointer to the newly allocated memory chunk. This is guaranteed to
 *      not be \c NULL.
 */
void *
HashTable::mallocAligned(uint64_t len) const
{
    if (useHugeTlb)
        return xmalloc_aligned_hugetlb(len);
    else
        return xmemalign(sizeof(CacheLine), len);
}

/**
 * Free the chuck of memory returned by #mallocAligned().
 * \param[in] p
 *      A pointer to the memory chunk allocated by #mallocAligned().
 *      Must not be \c NULL.
 */
void
HashTable::freeAligned(void *p) const
{
    if (useHugeTlb) {
        // TODO(ongaro): can't free memory from xmalloc_aligned_hugetlb
    } else {
        free(p);
    }
}

/**
 * A 64-bit to 64-bit hash function.
 * This is a helper to #findBucket().
 */
uint64_t
HashTable::hash(uint64_t key)
{
    // This appears to be hash64shift by Thomas Wang from
    // http://www.concentric.net/~Ttwang/tech/inthash.htm
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
}

/**
 * Find the bucket corresponding to a particular object ID.
 * This also calculates the secondary hash bits used to disambiguate entries in
 * the same bucket.
 * \param[in] objectId
 *      The object ID whose bucket to find.
 * \param[out] secondaryHash
 *      The secondary hash bits (16 bits).
 * \return
 *      The bucket corresponding to the given object ID.
 */
HashTable::CacheLine *
HashTable::findBucket(uint64_t objectId, uint64_t *secondaryHash) const
{
    uint64_t hashValue;
    uint64_t bucketHash;
    hashValue = hash(objectId);
    bucketHash = hashValue & 0x0000ffffffffffffUL;
    *secondaryHash = hashValue >> 48;
    return &buckets[bucketHash & (numBuckets - 1)];
    // This is equivalent to:
    //     &buckets[bucketHash % numBuckets]
    // since numBuckets is a power of two, and this saves about 14 cycles on an
    // Intel Core 2 (see src/misc/modulus.cc).
}

/**
 * Find a hash table entry for a given object ID.
 * This is used in #lookup(), #remove(), and #replace() to find the hash table
 * entry to operate on.
 * \param[in] bucket
 *      The bucket corresponding to \a objectId.
 * \param[in] secondaryHash
 *      Secondary hash bits for \a objectId as returned from #findBucket()
 *      (16 bits).
 * \param[in] objectId
 *      The ID of the object to locate the hash table entry.
 * \return
 *      The pointer to the hash table entry, or \a NULL if there is no such
 *      hash table entry.
 */
HashTable::Entry *
HashTable::lookupEntry(CacheLine *bucket, uint64_t secondaryHash,
                       uint64_t objectId)
{
    CycleCounter cycles(STAT_REF(perfCounters.lookupEntryCycles));
    unsigned int i;

    STAT_INC(perfCounters.lookupEntryCalls);

    CacheLine *cacheLine = bucket;

    while (1) {

        // Try this cache line.
        Entry *candidate = cacheLine->entries;
        for (i = 0; i < ENTRIES_PER_CACHE_LINE; i++, candidate++) {

            if (candidate->hashMatches(secondaryHash)) {
                // The hash within the hash table entry matches, so with high
                // probability this is the pointer we're looking for. To check,
                // we must go to the object.
                if (candidate->getObject()->id == objectId) {
                    PERF_DIST_STORE_SAMPLE(perfCounters.lookupEntryDist,
                                           cycles.stop());
                    return candidate;
                } else {
                    STAT_INC(perfCounters.lookupEntryHashCollisions);
                }
            }
        }

        // Not found in this cache line, see if there's a chain to another
        // cache line.
        cacheLine = cacheLine->entries[ENTRIES_PER_CACHE_LINE - 1].getChainPointer(); // NOLINT
        if (cacheLine == NULL) {
            PERF_DIST_STORE_SAMPLE(perfCounters.lookupEntryDist,
                                   cycles.stop());
            return NULL;
        }
        STAT_INC(perfCounters.lookupEntryChainsFollowed);
    }
}

/**
 * Find the address of an Object.
 * \param[in] objectId
 *      The ID of the object to locate.
 * \return
 *      The address of the Object, or \a NULL if the object doesn't exist.
 */
const Object *
HashTable::lookup(uint64_t objectId)
{
    uint64_t secondaryHash;
    Entry *entry;
    CacheLine *bucket = findBucket(objectId, &secondaryHash);
    entry = lookupEntry(bucket, secondaryHash, objectId);
    if (entry == NULL)
        return NULL;
    return entry->getObject();
}

/**
 * Remove an object ID from the hash table.
 * \param[in] objectId
 *      The ID of the object to remove.
 * \return
 *      Whether the hash table contained the object ID.
 */
bool
HashTable::remove(uint64_t objectId)
{
    uint64_t secondaryHash;
    Entry *entry;
    CacheLine *bucket = findBucket(objectId, &secondaryHash);
    entry = lookupEntry(bucket, secondaryHash, objectId);
    if (entry == NULL)
        return false;
    entry->clear();
    return true;
}

/**
 * Update the location of an object ID in the hash table.
 * This is equivalent to, but faster than, #remove() followed by #replace().
 * \param[in] objectId
 *      The ID of the moved object.
 * \param[in] object
 *      The address of the Object.
 * \retval true
 *      The hash table previously contained \a objectId and its entry has been
 *      updated to reflect the new location of the object.
 * \retval false
 *      The hash table did not previously contain \a objectId. An entry has
 *      been created to reflect the location of the object.
 */
bool
HashTable::replace(uint64_t objectId, const Object *object)
{
    CycleCounter cycles(STAT_REF(perfCounters.replaceCycles));
    uint64_t secondaryHash;
    CacheLine *bucket;
    Entry *entry;
    unsigned int i;

    STAT_INC(perfCounters.replaceCalls);

    bucket = findBucket(objectId, &secondaryHash);
    entry = lookupEntry(bucket, secondaryHash, objectId);
    if (entry != NULL) {
        entry->setObject(secondaryHash, object);
        return true;
    }

    CacheLine *cacheLine = bucket;
    while (1) {
        entry = cacheLine->entries;
        for (i = 0; i < ENTRIES_PER_CACHE_LINE; i++) {
            if (entry->isAvailable()) {
                entry->setObject(secondaryHash, object);
                return false;
            }
            entry++;
        }

        Entry &last = cacheLine->entries[ENTRIES_PER_CACHE_LINE - 1];
        cacheLine = last.getChainPointer();
        if (cacheLine == NULL) {
            // no empty space found, allocate a new cache line
            void *buf = mallocAligned(sizeof(CacheLine));
            cacheLine = static_cast<CacheLine *>(buf);
            cacheLine->entries[0] = last;
            for (i = 1; i < ENTRIES_PER_CACHE_LINE; i++)
                cacheLine->entries[i].clear();
            last.setChainPointer(cacheLine);
        }
        STAT_INC(perfCounters.insertChainsFollowed);
    }
}

} // namespace RAMCloud
