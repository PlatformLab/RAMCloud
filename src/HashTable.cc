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


// HashTable::PerfDistribution


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


HashTable::PerfCounters::PerfCounters()
    : insertCycles(0), lookupEntryCycles(0), insertChainsFollowed(0),
    lookupEntryChainsFollowed(0), lookupEntryHashCollisions(0),
    lookupEntryDist()
{
}


// HashTable::Entry


/**
 * Replaces this hash table entry.
 * \param[in] hash
 *      The additional hash bits from the object ID.
 * \param[in] chain
 *      Whether \a ptr is a chain pointer as opposed to a Log pointer.
 * \param[in] ptr
 *      The chain pointer to the next cache line or the Log pointer
 *      where the object is located (determined by \a chain).
 */
void
HashTable::Entry::pack(uint64_t hash, bool chain, void *ptr)
{
    if (ptr == NULL)
        assert(hash == 0 && !chain);

    uint64_t c = chain ? 1 : 0;
    uint64_t p = reinterpret_cast<uint64_t>(ptr);
    assert((hash & ~(0x000000000000ffffUL)) == 0);
    assert((p    & ~(0x00007fffffffffffUL)) == 0);
    this->value = ((hash << 48)  | (c << 47) | p);
}

/**
 * Reads the contents of this hash table entry.
 * \return
 *      The extracted values. See UnpackedEntry.
 */
HashTable::Entry::UnpackedEntry HashTable::Entry::unpack() {
    UnpackedEntry ue;
    ue.hash    = (this->value >> 48) & 0x000000000000ffffUL;
    ue.chain   = (this->value >> 47) & 0x0000000000000001UL;
    uint64_t p = this->value         & 0x00007fffffffffffUL;
    ue.ptr   = reinterpret_cast<void*>(p);
    return ue;
}

/**
 * Reinitialize a hash table entry as unused.
 */
void
HashTable::Entry::clear()
{
    pack(0, false, NULL);
}

/**
 * Reinitialize a regular hash table entry.
 * \param[in] hash
 *      The additional hash bits from the object ID.
 * \param[in] ptr
 *      The Log pointer where the object is located. Must not be \c NULL.
 */
void
HashTable::Entry::setLogPointer(uint64_t hash, void *ptr)
{
    assert(ptr != NULL);
    pack(hash, false, ptr);
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
    pack(0, true, ptr);
}

/**
 * \return
 *      Whether a hash table entry is unused.
 */
bool
HashTable::Entry::isAvailable()
{
    UnpackedEntry ue = unpack();
    return (ue.ptr == NULL);
}


/**
 * Extract the Log pointer.
 * The caller must first verify that the hash table entry indeed stores a log
 * pointer with #hashMatches() or #isChainLink().
 * \return
 *      The Log pointer stored in a hash table entry.
 */
void*
HashTable::Entry::getLogPointer()
{
    UnpackedEntry ue = unpack();
    assert(!ue.chain && ue.ptr != NULL);
    return ue.ptr;
}

/**
 * Extract the chain pointer to another cache line.
 * The caller should have previously ensured that the hash table entry indeed
 * stores a chain pointer with #isChainLink().
 * \return
 *      The chain pointer to another cache line.
 */
HashTable::CacheLine*
HashTable::Entry::getChainPointer()
{
    UnpackedEntry ue = unpack();
    assert(ue.chain);
    return static_cast<CacheLine*>(ue.ptr);
}

/**
 * Check whether the additional hash bits stored match those given.
 * \param[in] hash
 *      The additional hash bits from the object ID to test.
 * \return
 *      Whether the hash table entry holds a Log pointer and the additional
 *      hash bits for the object pointed to match \a hash.
 */
bool
HashTable::Entry::hashMatches(uint64_t hash)
{
    UnpackedEntry ue = unpack();
    return (!ue.chain && ue.ptr != NULL && ue.hash == hash);
}

/**
 * Check whether this hash table entry has a chain pointer to another cache line.
 * \return
 *      Whether the hash table entry has a chain pointer to another cache line
 *      (as opposed to a Log pointer to an object).
 */
bool
HashTable::Entry::isChainLink()
{
    UnpackedEntry ue = unpack();
    return ue.chain;
}


// HashTable


/**
 * Allocate an aligned chunk of memory.
 * \param[in] len
 *      The size of the memory chunk to allocate.
 * \return
 *      A pointer to the newly allocated memory chunk. This is guaranteed to
 *      not be \c NULL.
 */
void *
HashTable::mallocAligned(uint64_t len)
{
    return (useHugeTlb) ?
        xmalloc_aligned_hugetlb(len) : xmalloc_aligned_xmalloc(len);
}

/**
 * Take the hashes of an object ID.
 * \param[in] key
 *      The object ID to hash.
 * \param[out] hash
 *      The main hash used to select a bucket.
 * \param[out] mkhash
 *      Additional hash bits used to disambiguate entries in the same bucket.
 */
void
HashTable::hash(uint64_t key, uint64_t *hash, uint16_t *mkhash)
{
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);

    *hash   = key & 0x0000ffffffffffffULL;
    *mkhash = static_cast<uint16_t>(key >> 48);
}

/**
 * \param[in] nlines
 *      The number of buckets in the new hash table.
 */
HashTable::HashTable(uint64_t nlines)
    : buckets(NULL), numBuckets(nlines), useHugeTlb(false), perfCounters()
{
    // Allocate space for a new hash table and set its entries to unused.
    uint64_t i, j;

    buckets = static_cast<CacheLine *>(mallocAligned(numBuckets *
                                                     sizeof(buckets[0])));

    for (i = 0; i < numBuckets; i++) {
        for (j = 0; j < ENTRIES_PER_CACHE_LINE; j++)
            buckets[i].entries[j].clear();
    }
}

/**
 * Find a hash table entry for a given key.
 * This is used in #lookup(), #remove(), and #replace() to find the hash table
 * entry to operate on.
 * \param[in] key
 *      The ID of the object for which to locate the hash table entry.
 * \return
 *      The pointer to the hash table entry, or \a NULL if there is no such
 *      hash table entry.
 */
HashTable::Entry *
HashTable::lookupEntry(uint64_t key)
{
    uint64_t b = rdtsc();
    uint64_t h;
    uint16_t mk;
    unsigned int i;

    // Find the bucket.
    hash(key, &h, &mk);
    CacheLine *cl = &buckets[h % numBuckets];

    while (1) {

        // Try this cache line.
        Entry *kp = cl->entries;
        for (i = 0; i < ENTRIES_PER_CACHE_LINE; i++, kp++) {

            if (kp->hashMatches(mk)) {
                // The hash within the hash table entry matches, so with high
                // probability this is the pointer we're looking for. To check,
                // we assume the object stores its key in the first 64 bits and
                // see if that matches our key.
                uint64_t *obj = static_cast<uint64_t*>(kp->getLogPointer());
                if (*obj == key) {
                    uint64_t diff = rdtsc() - b;
                    perfCounters.lookupEntryCycles += diff;
                    perfCounters.lookupEntryDist.storeSample(diff);
                    return kp;
                } else {
                    perfCounters.lookupEntryHashCollisions++;
                }
            }
        }

        // Not found in this cache line, see if there's a chain to another
        // cache line.
        if (!cl->entries[ENTRIES_PER_CACHE_LINE - 1].isChainLink()) {
            uint64_t diff = rdtsc() - b;
            perfCounters.lookupEntryCycles += diff;
            perfCounters.lookupEntryDist.storeSample(diff);
            return NULL;
        }

        cl = cl->entries[ENTRIES_PER_CACHE_LINE - 1].getChainPointer();
        perfCounters.lookupEntryChainsFollowed++;
    }
}

/**
 * Find the pointer to the Log in memory where the latest version of an object
 * resides.
 * \param[in] key
 *      The ID of the object to locate.
 * \return
 *      The pointer into the Log, or \a NULL if the object doesn't exist.
 */
void *
HashTable::lookup(uint64_t key)
{
    Entry *kp = lookupEntry(key);
    return kp ? kp->getLogPointer() : NULL;
}

/**
 * Remove a key from the hash table.
 * \param[in] key
 *      The ID of the object to remove.
 * \return
 *      Whether the hash table contained the key.
 */
bool
HashTable::remove(uint64_t key) {
    Entry *kp = lookupEntry(key);
    if (!kp)
        return false;
    kp->clear();
    return true;
}

/**
 * Update the object location of a key in the hash table.
 * \param[in] key
 *      The ID of the moved object.
 * \param[in] ptr
 *      The pointer to the Log where the latest version of the object resides.
 * \retval true
 *      The hash table previously contained key and its entry has been updated
 *      to reflect the new location of the object.
 * \retval false
 *      The hash table did not previously contain key. No action has been
 *      taken!
 */
bool
HashTable::replace(uint64_t key, void *ptr) {
    uint64_t h;
    uint16_t mk;
    Entry *kp = lookupEntry(key);
    if (!kp)
        return false;
    hash(key, &h, &mk);
    kp->setLogPointer(mk, ptr);
    return true;
}

/**
 * Add a new key to the hash table.
 * The caller must guarantee that \a key does not exist in the hash table.
 * \param[in] key
 *      The ID of the object that resides at \a ptr.
 * \param[in] ptr
 *      The pointer to the Log where the latest version of the object resides.
 */
void
HashTable::insert(uint64_t key, void *ptr)
{
    uint64_t b = rdtsc();
    uint64_t h;
    uint16_t mk;
    unsigned int i;

    hash(key, &h, &mk);
    CacheLine *cl = &buckets[h % numBuckets];

    while (1) {
        Entry *kp = cl->entries;
        for (i = 0; i < ENTRIES_PER_CACHE_LINE; i++, kp++) {
            if (kp->isAvailable()) {
                kp->setLogPointer(mk, ptr);
                perfCounters.insertCycles += (rdtsc() - b);
                return;
            }
        }

        // no empty space found, allocate a new cache line
        if (!cl->entries[ENTRIES_PER_CACHE_LINE - 1].isChainLink()) {
            CacheLine *ncl =
                static_cast<CacheLine *>(mallocAligned(sizeof(CacheLine)));
            ncl->entries[0] = cl->entries[ENTRIES_PER_CACHE_LINE - 1];
            for (i = 1; i < ENTRIES_PER_CACHE_LINE; i++)
                ncl->entries[i].clear();
            cl->entries[ENTRIES_PER_CACHE_LINE - 1].setChainPointer(ncl);
        }

        cl = cl->entries[ENTRIES_PER_CACHE_LINE - 1].getChainPointer();
        perfCounters.insertChainsFollowed++;
    }
}

} // namespace RAMCloud
