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

// TODO(ongaro): Use meaningful local variable names.

// TODO(ongaro): Use pre-processor macros to turn off stat mechanism.

// TODO(ongaro): Masking is faster than modulus if buckets is a power of 2.


// HashTable::PerfDistribution


/**
 * Constructor for HashTable::PerfDistribution.
 */
HashTable::PerfDistribution::PerfDistribution()
    : binOverflows(0), min(~0UL), max(0UL)
{
    memset(bins, 0, sizeof(bins));
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
    : replaceCycles(0), lookupEntryCycles(0), insertChainsFollowed(0),
    lookupEntryChainsFollowed(0), lookupEntryHashCollisions(0),
    lookupEntryDist()
{
}


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
 * object address with #hashMatches() or #isChainLink().
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
 * The caller should have previously ensured that the hash table entry indeed
 * stores a chain pointer with #isChainLink().
 * \return
 *      The chain pointer to another cache line.
 */
HashTable::CacheLine *
HashTable::Entry::getChainPointer() const
{
    UnpackedEntry ue = unpack();
    assert(ue.chain);
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

/**
 * Check whether this hash table entry has a chain pointer to another cache line.
 * \return
 *      Whether the hash table entry has a chain pointer to another cache line
 *      (as opposed to an object address).
 */
// TODO(ongaro): Remove this, have getChainPointer return NULL instead.
bool
HashTable::Entry::isChainLink() const
{
    UnpackedEntry ue = unpack();
    return ue.chain;
}


// HashTable


/**
 * Constructor for HashTable.
 * \param[in] numBuckets
 *      The number of buckets in the new hash table.
 */
HashTable::HashTable(uint64_t numBuckets)
    : buckets(NULL), numBuckets(numBuckets), useHugeTlb(false), perfCounters()
{
    // Allocate space for a new hash table and set its entries to unused.

    uint64_t i, j;

    size_t bucketsSize = numBuckets * sizeof(CacheLine);
    buckets = static_cast<CacheLine *>(mallocAligned(bucketsSize));

    for (i = 0; i < numBuckets; i++) {
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
 * \param[in] key
 *      The object ID whose bucket to find.
 * \param[out] secondaryHash
 *      The secondary hash bits (16 bits).
 * \return
 *      The bucket corresponding to the given object ID.
 */
HashTable::CacheLine *
HashTable::findBucket(uint64_t key, uint64_t *secondaryHash)
{
    uint64_t hashValue;
    uint64_t bucketHash;
    hashValue = hash(key);
    bucketHash = hashValue & 0x0000ffffffffffffUL;
    *secondaryHash = hashValue >> 48;
    return &buckets[bucketHash % numBuckets];
}

/**
 * Find a hash table entry for a given key.
 * This is used in #lookup(), #remove(), and #replace() to find the hash table
 * entry to operate on.
 * \param[in] bucket
 *      The bucket corresponding to \a key.
 * \param[in] secondaryHash
 *      Secondary hash bits for \a key as returned from #findBucket()
 *      (16 bits).
 * \param[in] key
 *      The ID of the object to locate the hash table entry.
 * \return
 *      The pointer to the hash table entry, or \a NULL if there is no such
 *      hash table entry.
 */
HashTable::Entry *
HashTable::lookupEntry(CacheLine *bucket, uint64_t secondaryHash, uint64_t key)
{
    CycleCounter cycles(&perfCounters.lookupEntryCycles);
    unsigned int i;

    CacheLine *cl = bucket;

    while (1) {

        // Try this cache line.
        Entry *kp = cl->entries;
        for (i = 0; i < ENTRIES_PER_CACHE_LINE; i++, kp++) {

            if (kp->hashMatches(secondaryHash)) {
                // The hash within the hash table entry matches, so with high
                // probability this is the pointer we're looking for. To check,
                // we must go to the object.
                if (kp->getObject()->id == key) {
                    if (PERF_COUNTERS)
                        perfCounters.lookupEntryDist.storeSample(cycles.stop());
                    return kp;
                } else {
                    perfCounters.lookupEntryHashCollisions++;
                }
            }
        }

        // Not found in this cache line, see if there's a chain to another
        // cache line.
        if (!cl->entries[ENTRIES_PER_CACHE_LINE - 1].isChainLink()) {
            if (PERF_COUNTERS)
                perfCounters.lookupEntryDist.storeSample(cycles.stop());
            return NULL;
        }

        cl = cl->entries[ENTRIES_PER_CACHE_LINE - 1].getChainPointer();
        perfCounters.lookupEntryChainsFollowed++;
    }
}

/**
 * Find the address of an Object.
 * \param[in] key
 *      The ID of the object to locate.
 * \return
 *      The address of the Object, or \a NULL if the object doesn't exist.
 */
const Object *
HashTable::lookup(uint64_t key)
{
    uint64_t secondaryHash;
    Entry *kp;
    CacheLine *bucket = findBucket(key, &secondaryHash);
    kp = lookupEntry(bucket, secondaryHash, key);
    if (kp == NULL)
        return NULL;
    return kp->getObject();
}

/**
 * Remove an object ID from the hash table.
 * \param[in] key
 *      The ID of the object to remove.
 * \return
 *      Whether the hash table contained the key.
 */
bool
HashTable::remove(uint64_t key)
{
    uint64_t secondaryHash;
    Entry *kp;
    CacheLine *bucket = findBucket(key, &secondaryHash);
    kp = lookupEntry(bucket, secondaryHash, key);
    if (kp == NULL)
        return false;
    kp->clear();
    return true;
}

/**
 * Update the location of an object ID in the hash table.
 * This is equivalent to, but faster than, #remove() followed by #replace().
 * \param[in] key
 *      The ID of the moved object.
 * \param[in] object
 *      The address of the Object.
 * \retval true
 *      The hash table previously contained key and its entry has been updated
 *      to reflect the new location of the object.
 * \retval false
 *      The hash table did not previously contain key. An entry has been
 *      created to reflect the location of the object.
 */
bool
HashTable::replace(uint64_t key, const Object *object)
{
    CycleCounter cycles(&perfCounters.replaceCycles);
    uint64_t secondaryHash;
    CacheLine *bucket;
    Entry *kp;

    bucket = findBucket(key, &secondaryHash);

    kp = lookupEntry(bucket, secondaryHash, key);
    if (kp != NULL) {
        kp->setObject(secondaryHash, object);
        return true;
    }

    unsigned int i;
    CacheLine *cl = bucket;

    while (1) {
        kp = cl->entries;
        for (i = 0; i < ENTRIES_PER_CACHE_LINE; i++) {
            if (kp->isAvailable()) {
                kp->setObject(secondaryHash, object);
                return false;
            }
            kp++;
        }

        Entry &last = cl->entries[ENTRIES_PER_CACHE_LINE - 1];
        if (last.isChainLink()) {
            cl = last.getChainPointer();
        } else {
            // no empty space found, allocate a new cache line
            cl = static_cast<CacheLine *>(mallocAligned(sizeof(CacheLine)));
            cl->entries[0] = last;
            for (i = 1; i < ENTRIES_PER_CACHE_LINE; i++)
                cl->entries[i].clear();
            last.setChainPointer(cl);
        }
        perfCounters.insertChainsFollowed++;
    }
}

} // namespace RAMCloud
