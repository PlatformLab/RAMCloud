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

// RAMCloud pragma [CPPLINT=0]

/**
 * \file
 * Implementation for Hashtable.
 */

#include <Hashtable.h>

#include <Common.h>
#include <ugly_memory_stuff.h>

#include <cstdio>
#include <cstdlib>
#include <stdint.h>
#include <cmath>
#include <cstring>

namespace RAMCloud {

/**
 * The value for an unused hash table entry.
 */
#define UNUSED                  ((uint64_t)0)

/**
 * Extract the Log pointer from a hash table entry.
 * \param[in] x
 *      The value of the hash table entry (\c uint64_t).
 * \return
 *      The Log pointer stored in a hash table entry (\c uint64_t).
 */
#define ADDR(x)                 (x & 0x0000ffffffffffffULL)

/**
 * Extract the additional hash bits stored in the hash table entry.
 * \param[in] x
 *      The value of the hash table entry (\c uint64_t).
 * \return
 *      The additional hash bits (\c uint64_t).
 */
#define GETMINIKEY(x)           (x >> 48)

/**
 * Pack a hash table entry.
 * \param[in] mk
 *      The additional hash bits from the key (\c uint64_t).
 * \param[in] p
 *      The Log pointer where the object is located (\c uint64_t or pointer).
 * \return
 *      The value of the hash table entry (\c uint64_t).
 */
#define MKENTRY(mk, p)          (((uint64_t)mk << 48) | ADDR((uint64_t)p))

/**
 * Check if a hash table entry is a chain pointer to another cache line.
 * \param[in] c
 *      A pointer to the cache line (\c cacheline*).
 * \param[in] x
 *      The offset of the hash table entry in \a c (\c uint32_t). This should
 *      be <tt>ENTRIES_PER_CACHE_LINE - 1</tt>.
 * \return
 *      Whether the hash table entry has a chain pointer to another cache line
 *      (as opposed to a Log pointer to an object; \c uint64_t).
 * \retval zero
 *      It is not a chain pointer.
 * \retval nonzero
 *      It is a chain pointer.
 */
#define ISCHAIN(c,x)            (c->keys[x] &  (0x1ULL << 47))

/**
 * Get the chain pointer to another cache line from a hash table entry.
 * The caller should have previously ensured that #ISCHAIN() on the same
 * arguments is true.
 * \param[in] c
 *      A pointer to the cache line (\c cacheline*).
 * \param[in] x
 *      The offset of the hash table entry in \a c (\c uint32_t). This should
 *      be <tt>ENTRIES_PER_CACHE_LINE - 1</tt>.
 * \return
 *      The chain pointer to another cache line (\c uint64_t).
 */
#define GETCHAINPTR(c,x)        (c->keys[x] & ~(0x1ULL << 47))

/**
 * Pack a chained cache line pointer as a hash table entry.
 * \param[in] x
 *      The pointer to the next cache line (\c uint64_t or pointer).
 * \return
 *      The value of the hash table entry (\c uint64_t).
 */
#define MKCHAINPTR(x)           ((uint64_t)x | (0x1ULL << 47))

/**
 * Test whether a Log pointer is safe to pack in a hash table entry.
 * \param[in] x
 *      The Log pointer (\c uint64_t or pointer).
 * \return
 *      Whether \a x is safe to pack in a hash table entry (\c bool).
 */
#define ISGOODPTR(x)            (((x) & 0xffff800000000000ULL) == 0)


/**
 * Update the distribution statistics for the number of cycles for a
 * #LookupKeyPtr() operation.
 * See #buckets, #oflowbucket, #min_ticks, #max_ticks.
 * \param[in] ticks
 *      The number of cycles used for a #LookupKeyPtr() operation.
 */
void
Hashtable::StoreSample(uint64_t ticks)
{
    if (ticks / 10 < NBUCKETS)
        buckets[ticks / 10]++;
    else
        oflowbucket++;

    if (ticks < min_ticks)
        min_ticks = ticks;
    if (ticks > max_ticks)
        max_ticks = ticks;
}

/**
 * Allocate an aligned chunk of memory.
 * \param[in] len
 *      The size of the memory chunk to allocate.
 * \return
 *      A pointer to the newly allocated memory chunk. This is guaranteed to
 *      not be \c NULL.
 */
void *
Hashtable::MallocAligned(uint64_t len)
{
    return (use_huge_tlb) ?
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
static inline void
hash(uint64_t key, uint64_t *hash, uint16_t *mkhash)
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
Hashtable::Hashtable(uint64_t nlines)
    : table(0), table_lines(nlines), use_huge_tlb(false), ins_total(0),
    lup_total(0), ins_nexts(0), lup_nexts(0), lup_mkfails(0), oflowbucket(0),
    min_ticks(~0), max_ticks(0)
{
    // Allocate space for a new hash table and fill it with UNUSED entries.
    uint64_t i, j;

    table = static_cast<cacheline *>(MallocAligned(table_lines *
                                                   sizeof(table[0])));

    for (i = 0; i < table_lines; i++) {
        for (j = 0; j < ENTRIES_PER_CACHE_LINE; j++)
            table[i].keys[j] = UNUSED;
    }
}

/**
 * Find a hash table entry for a given key.
 * This is used in #Lookup(), #Delete(), and #Replace() to find the hash table
 * entry to operate on.
 * \param[in] key
 *      The ID of the object for which to locate the hash table entry.
 * \return
 *      The pointer to the hash table entry, or \a NULL if there is no such
 *      hash table entry.
 */
uint64_t *
Hashtable::LookupKeyPtr(uint64_t key)
{
    uint64_t b = rdtsc();
    uint64_t h;
    uint16_t mk;
    unsigned int i;

    // Find the bucket.
    hash(key, &h, &mk);
    cacheline *cl = &table[h % table_lines];

    while (1) {

        // Try this cache line.
        uint64_t *kp = cl->keys;
        for (i = 0; i < ENTRIES_PER_CACHE_LINE; i++, kp++) {

            if (*kp != UNUSED && GETMINIKEY(*kp) == mk) {
                // The hash within the hash table entry matches, so with high
                // probability this is the pointer we're looking for. To check,
                // we assume the object stores its key in the first 64 bits and
                // see if that matches our key.
                uint64_t *obj = (uint64_t *)ADDR(*kp);
                if (*obj == key) {
                    uint64_t diff = rdtsc() - b;
                    lup_total += diff;
                    StoreSample(diff);
                    assert(ISGOODPTR((uint64_t)kp));
                    return kp;
                } else {
                    lup_mkfails++;
                }
            }
        }

        // Not found in this cache line, see if there's a chain to another
        // cache line.
        if (!ISCHAIN(cl, ENTRIES_PER_CACHE_LINE - 1)) {
            uint64_t diff = rdtsc() - b;
            lup_total += diff;
            StoreSample(diff);
            return NULL;
        }

        cl = (cacheline *)GETCHAINPTR(cl, ENTRIES_PER_CACHE_LINE - 1);
        lup_nexts++;
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
Hashtable::Lookup(uint64_t key)
{
    uint64_t *kp = LookupKeyPtr(key);
    return kp ? (void *) ADDR(*kp) : NULL;
}

/**
 * Remove a key from the hash table.
 * \param[in] key
 *      The ID of the object to remove.
 * \return
 *      Whether the hash table contained the key.
 */
bool
Hashtable::Delete(uint64_t key) {
    uint64_t *kp = LookupKeyPtr(key);
    if (!kp)
        return false;
    *kp = UNUSED;
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
Hashtable::Replace(uint64_t key, void *ptr) {
    uint64_t h;
    uint16_t mk;
    uint64_t *kp = LookupKeyPtr(key);
    if (!kp)
        return false;
    hash(key, &h, &mk);
    *kp = MKENTRY(mk, ptr);
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
Hashtable::Insert(uint64_t key, void *ptr)
{
    uint64_t b = rdtsc();
    uint64_t h;
    uint16_t mk;
    unsigned int i;

    assert(ISGOODPTR((uint64_t)ptr));

    hash(key, &h, &mk);
    cacheline *cl = &table[h % table_lines];

    while (1) {
        uint64_t *kp = cl->keys;
        for (i = 0; i < ENTRIES_PER_CACHE_LINE; i++, kp++) {
            if (*kp == UNUSED) {
                *kp = MKENTRY(mk, ptr);
                ins_total += (rdtsc() - b);
                return;
            }
        }

        // no empty space found, allocate a new cache line
        if (!ISCHAIN(cl, ENTRIES_PER_CACHE_LINE - 1)) {
            cacheline *ncl =
                static_cast<cacheline *>(MallocAligned(sizeof(cacheline)));
            ncl->keys[0] = cl->keys[ENTRIES_PER_CACHE_LINE - 1];
            for (i = 1; i < ENTRIES_PER_CACHE_LINE; i++)
                ncl->keys[i] = UNUSED;
            cl->keys[ENTRIES_PER_CACHE_LINE - 1] = MKCHAINPTR(ncl);
        }

        uint64_t clp = GETCHAINPTR(cl, ENTRIES_PER_CACHE_LINE - 1);
        cl = reinterpret_cast<cacheline *>(clp);
        ins_nexts++;
    }
}

} // namespace RAMCloud
