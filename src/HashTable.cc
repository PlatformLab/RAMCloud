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

#include "Common.h"
#include "HashTable.h"

namespace RAMCloud {

/**
 * Reinitialize a hash table entry as unused.
 */
void
HashTable::Entry::clear()
{
    // the raw bytes of this Entry must be zero for memset, etc to work
    value = 0;
}

/**
 * Reinitialize a regular hash table entry.
 * \param[in] hash
 *      The secondary hash bits computed from the key (16 bits).
 * \param[in] reference
 *      The Reference to insert. It must be valid.
 */
void
HashTable::Entry::setReference(uint64_t hash, uint64_t reference)
{
    assert(reference != 0);
    pack(hash, false, reference);
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
    UnpackedEntry ue;
    unpack(ue);
    return (ue.ptr == 0);
}

/**
 * Extract the log reference from a hash table entry.
 * The caller must first verify that the hash table entry indeed stores
 * a reference with #hashMatches().
 * \return
 *      47-bit reference referring to the entry being stored in this
 *      Entry.
 */
uint64_t
HashTable::Entry::getReference() const
{
    UnpackedEntry ue;
    unpack(ue);
    assert(!ue.chain && ue.ptr != 0);
    return ue.ptr;
}

/**
 * Extract the chain pointer to another cache line.
 * \return
 *      The chain pointer to another cache line. If this entry does not 
 *      store a chain pointer, returns \c NULL instead.
 */
HashTable::CacheLine*
HashTable::Entry::getChainPointer() const
{
    UnpackedEntry ue;
    unpack(ue);
    if (!ue.chain)
        return NULL;
    return reinterpret_cast<CacheLine*>(ue.ptr);
}

/**
 * Check whether the secondary hash bits stored match those given.
 * \param[in] hash
 *      The secondary hash bits computed from the key to test (16 bits).
 * \return
 *      True if the secondary hash bits stored with this Entry are equal
 *      (indicating a possible match), otherwise false.
 */
bool
HashTable::Entry::hashMatches(uint64_t hash) const
{
    UnpackedEntry ue;
    unpack(ue);
    return (!ue.chain && ue.ptr != 0 && ue.hash == hash);
}

/**
 * Replace this hash table entry.
 * \param[in] hash
 *      The secondary hash bits (16 bits) computed from the key.
 *      Irrelevant if \a chain is true.
 * \param[in] chain
 *      Whether \a ptr is a chain pointer as opposed to a reference.
 * \param[in] ptr
 *      The chain pointer to the next cache line or the reference
 *      (determined by \a chain).
 * \throw Exception
 *      An exception is thrown if the pointer cannot fix in the number
 *      of bits we have.
 */
void
HashTable::Entry::pack(uint64_t hash, bool chain, uint64_t ptr)
{
    if (ptr == 0)
        assert(hash == 0 && !chain);

    if ((ptr >> 47) != 0) {
        throw Exception(HERE, format(
            "The given pointer (0x%016lx) can't fit "
            "in a hash table entry.",
            ptr));
    }

    uint64_t c = chain ? 1 : 0;
    assert((hash >> 16) == 0);
    this->value = ((hash << 48) | (c << 47) | ptr);
}

/**
 * Read the contents of this hash table entry.
 * \param ue
 *      The extracted values are returned here. See UnpackedEntry.
 *
 * Note that this method used to return a UnpackedEntry, but that
 * was about 50 cycles slower than passing one in.
 */
inline void
HashTable::Entry::unpack(UnpackedEntry& ue) const
{
    ue.hash  = (this->value >> 48) & 0x000000000000ffffUL;
    ue.chain = (this->value >> 47) & 0x0000000000000001UL;
    ue.ptr   = this->value         & 0x00007fffffffffffUL;
}

/**
 * Construct an empty set of candidates.
 */
HashTable::Candidates::Candidates()
    : bucket(NULL)
    , index()
    , secondaryHash()
{
}

/**
 * Initialize a candidate object for searching the given bucket in the hash
 * table. This will scan the bucket for the first reference that matches the
 * given secondaryHash.
 */
void
HashTable::Candidates::init(CacheLine* cl, uint64_t secondaryHash)
{
    bucket = cl;
    index = -1;
    this->secondaryHash = secondaryHash;
    next();
}

/**
 * Obtain the reference for the candidate currently pointed to by the
 * iterator. If there is no next candidate, 0 is returned.
 */
uint64_t
HashTable::Candidates::getReference()
{
    if (bucket == NULL)
        return 0;
    return bucket->entries[index].getReference();
}

/**
 * Replace the reference currently pointed to by the iterator. This is
 * used to change what a key refers to in the table.
 */
void
HashTable::Candidates::setReference(uint64_t reference)
{
    if (bucket != NULL)
        bucket->entries[index].setReference(secondaryHash, reference);
}

/**
 * Remove the reference currently pointed to by the iterator. This is
 * used to delete references from the table.
 */
void
HashTable::Candidates::remove()
{
    if (bucket != NULL)
        bucket->entries[index].clear();
}

/**
 * Advance the iterator to the next candidate, if there is one.
 */
void
HashTable::Candidates::next()
{
    while (true) {
        // Not found in the cache line, see if there's a chain to
        // another cache line.
        if (index == ENTRIES_PER_CACHE_LINE && bucket != NULL) {
            Entry* entry = &bucket->entries[ENTRIES_PER_CACHE_LINE - 1];
            bucket = entry->getChainPointer();
            index = -1;
        }

        if (bucket == NULL)
            return;

        // Resume in the current cache line.
        index++;
        Entry* candidate = &bucket->entries[index];
        while (index < ENTRIES_PER_CACHE_LINE) {
            if (candidate->hashMatches(secondaryHash)) {
                // The hash within the hash table entry matches, so with
                // high probability this is the pointer we're looking
                // for. We'll report this index to the user of this
                // class in the next getReference() call so that they
                // can verify the match.
                return;
            }
            candidate++;
            index++;
        }
    }
}

/**
 * If all candidates have been iterated over, return true. Otherwise,
 * return false.
 */
bool
HashTable::Candidates::isDone()
{
    return (bucket == NULL);
}

/**
 * Constructor for HashTable.
 * \param[in] numBuckets
 *      The number of buckets in the new hash table. This should be a power
 *      of two.
 * \throw Exception
 *      An exception is thrown if numBuckets is 0.
 */
HashTable::HashTable(uint64_t numBuckets)
    : numBuckets(BitOps::powerOfTwoLessOrEqual(numBuckets))
    , buckets(this->numBuckets * sizeof(CacheLine))
{
    if (numBuckets != this->numBuckets) {
        RAMCLOUD_LOG(DEBUG,
                     "HashTable truncated to %lu buckets "
                     "(nearest power of two)",
                     this->numBuckets);
    }

    if (numBuckets == 0)
        throw Exception(HERE, "HashTable numBuckets == 0?!");
}

/**
 * Destructor for HashTable.
 */
HashTable::~HashTable()
{
    // TODO(ongaro): free chained CacheLines that were allocated in insert()
}

/**
 * Find possible references to an element in the hash table given a key.
 * This method returns an object that allows the caller to iterate over
 * possible matches, if there are any. It is up to the caller to check
 * whether or not each candidate matches the key they're looking for.
 *
 * \param[in] keyHash
 *      Hash of the key representing the element to look up.
 * \param[out] candidates
 *      A HashTable::Candidates object to initialize so that it can be
 *      used to iterate over all potential matches in order to find the
 *      desired element.
 */
void
HashTable::lookup(KeyHash keyHash, Candidates& candidates)
{
    // Find the bucket using 64 bit hash of the key. Any collisions
    // arising out of this hashing will be detected / resolved by the
    // caller as it examines possible candidates.
    uint64_t secondaryHash;
    CacheLine *bucket = findBucket(keyHash, &secondaryHash);
    candidates.init(bucket, secondaryHash);
}

/**
 * Insert an element corresponding to a given key into the hash table.
 *
 * Note that this method will not ensure that the same key does not already
 * exist (in other words, it will not replace a key). It is up to the caller
 * to ensure that the key was first removed, if necessary.
 *
 * \param[in] keyHash
 *      Hash of the key naming the element to insert.
 * \param[in] reference
 *      Reference to the new element to insert into the hash table.
 */
void
HashTable::insert(KeyHash keyHash, uint64_t reference)
{
    uint64_t secondaryHash;
    int overflowBuckets = 0;
    CacheLine* bucket = findBucket(keyHash, &secondaryHash);
    while (true) {
        Entry* entry = bucket->entries;
        for (size_t i = 0; i < ENTRIES_PER_CACHE_LINE; i++) {
            if (entry->isAvailable()) {
                entry->setReference(secondaryHash, reference);
                return;
            }
            entry++;
        }

        // No free space in the current bucket; see if there is an
        // overflow bucket chained onto this one.
        ++overflowBuckets;
        Entry* last = &bucket->entries[ENTRIES_PER_CACHE_LINE - 1];
        bucket = last->getChainPointer();
        if (bucket == NULL) {
            // no empty space found, allocate a new cache line
            RAMCLOUD_LOG(NOTICE, "Allocating overflow bucket %d for index %lu",
                    overflowBuckets,
                    findBucketIndex(numBuckets, keyHash, &secondaryHash));
            void *buf = Memory::xmemalign(HERE, sizeof(CacheLine),
                                          sizeof(CacheLine));
            bucket = static_cast<CacheLine *>(buf);
            bucket->entries[0] = *last;
            for (size_t i = 1; i < ENTRIES_PER_CACHE_LINE; i++)
                bucket->entries[i].clear();
            last->setChainPointer(bucket);
        }
    }
}

/**
 * Apply the given callback function to each element stored in the
 * specified bucket of the hash table.
 * \param callback
 *      The callback to fire on each element stored in the HashTable.
 * \param cookie
 *      An opaque parameter to pass to the callback function.
 * \param bucket
 *      An index into the HashTable's buckets.  Must be < #numBuckets.
 * \return
 *      The total number of callbacks fired (i.e. the number of elements
 *      in the HashTable).
 */
uint64_t
HashTable::forEachInBucket(void (*callback)(uint64_t, void *),
                           void *cookie,
                           uint64_t bucket)
{
    uint64_t numCalls = 0;
    CacheLine *cl = &buckets.get()[bucket];
    while (1) {
        for (uint32_t j = 0; j < ENTRIES_PER_CACHE_LINE; j++) {
            Entry *e = &cl->entries[j];
            if (!e->isAvailable() && e->getChainPointer() == NULL) {
                callback(e->getReference(), cookie);
                numCalls++;
            }
        }

        Entry *entry = &cl->entries[ENTRIES_PER_CACHE_LINE - 1];
        cl = entry->getChainPointer();
        if (cl == NULL)
            break;
    }
    return numCalls;
}

/**
 * Apply the given callback function to each element stored in the
 * HashTable.
 * \param[in] callback
 *      The callback to fire on each element stored in the HashTable.
 * \param[in] cookie
 *      An opaque parameter to pass to the callback function.
 * \return
 *      The total number of callbacks fired (i.e. the number of elements
 *      in the HashTable).
 */
uint64_t
HashTable::forEach(void (*callback)(uint64_t, void *), void *cookie)
{
    uint64_t numCalls = 0;

    for (uint64_t i = 0; i < numBuckets; i++)
        numCalls += forEachInBucket(callback, cookie, i);

    return numCalls;
}

/**
 * Prefetch the cacheline associated with the given key hash.
 */
void
HashTable::prefetchBucket(KeyHash keyHash)
{
    uint64_t dummy;
    prefetch(findBucket(keyHash, &dummy));
}

/**
 * Return the number of bytes per cache line.
 */
uint32_t
HashTable::bytesPerCacheLine()
{
    return BYTES_PER_CACHE_LINE;
}

/**
 * Return the number of elements (references) each cacheline
 * holds.
 */
uint32_t
HashTable::entriesPerCacheLine()
{
    return ENTRIES_PER_CACHE_LINE;
}

/**
 * Returns the number of buckets allocated to the table.
 */
uint64_t
HashTable::getNumBuckets() const
{
    return numBuckets;
}

/**
 * Find the bucket index corresponding to a particular key.
 * This also calculates the secondary hash bits used to disambiguate entries
 * in the same bucket.
 * \param[in] numBuckets
 *      The number of buckets in the HashTable as reported by
 *      #getNumBuckets().
 * \param[in] keyHash
 *      Hash of the key representing the element we're looking for. 
 * \param[out] secondaryHash
 *      The secondary hash bits (16 bits).
 * \return
 *      The bucket index corresponding to the given referent ID.
 */
uint64_t
HashTable::findBucketIndex(uint64_t numBuckets,
                           KeyHash keyHash,
                           uint64_t *secondaryHash)
{
    uint64_t bucketHash = keyHash & 0x0000ffffffffffffUL;
    *secondaryHash = keyHash >> 48;
    return (bucketHash & (numBuckets - 1));
    // This is equivalent to:
    //     &buckets.get()[bucketHash % numBuckets]
    // since numBuckets is a power of two, and this saves about 14 cycles on
    // an Intel Core 2 (see src/misc/modulus.cc).
}

/**
 * Find the bucket corresponding to a particular key.
 * This also calculates the secondary hash bits used to disambiguate entries
 * in the same bucket.
 * \param[in] keyHash
 *      Hash of key representing the element we're looking for. 
 * \param[out] secondaryHash
 *      The secondary hash bits (16 bits).
 * \return
 *      The bucket corresponding to the given key.
 */
HashTable::CacheLine*
HashTable::findBucket(KeyHash keyHash, uint64_t *secondaryHash) //const
{
    uint64_t bucketIndex = findBucketIndex(numBuckets, keyHash, secondaryHash);
    return &buckets.get()[bucketIndex];
}

} // namespace RAMCloud
