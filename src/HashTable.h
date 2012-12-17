/* Copyright (c) 2009-2012 Stanford University
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
        /**
         * Reinitialize a hash table entry as unused.
         */
        void
        clear()
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
        setReference(uint64_t hash, uint64_t reference)
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
        setChainPointer(CacheLine *ptr)
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
        isAvailable() const
        {
            UnpackedEntry ue = unpack();
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
        getReference() const
        {
            UnpackedEntry ue = unpack();
            assert(!ue.chain && ue.ptr != 0);
            return ue.ptr;
        }

        /**
         * Extract the chain pointer to another cache line.
         * \return
         *      The chain pointer to another cache line. If this entry does not 
         *      store a chain pointer, returns \c NULL instead.
         */
        CacheLine *
        getChainPointer() const
        {
            UnpackedEntry ue = unpack();
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
        hashMatches(uint64_t hash) const
        {
            UnpackedEntry ue = unpack();
            return (!ue.chain && ue.ptr != 0 && ue.hash == hash);
        }

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
        pack(uint64_t hash, bool chain, uint64_t ptr)
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
         * This is the return type of #unpack().
         * See the parameters of #pack() for an explanation.
         */
        struct UnpackedEntry {
            uint64_t hash;
            bool chain;
            uint64_t ptr;
        };

        /**
         * Read the contents of this hash table entry.
         * \return
         *      The extracted values. See UnpackedEntry.
         */
        UnpackedEntry
        unpack() const
        {
            UnpackedEntry ue;
            ue.hash  = (this->value >> 48) & 0x000000000000ffffUL;
            ue.chain = (this->value >> 47) & 0x0000000000000001UL;
            ue.ptr   = this->value         & 0x00007fffffffffffUL;
            return ue;
        }
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
        /**
         * Construct an empty set of candidates.
         */
        Candidates()
            : bucket(NULL)
            , index(0)
            , secondaryHash(0)
        {
        }

        /**
         * Construct a candidate object from a given bucket in the hash table.
         * It will scan the bucket for the first reference that matches the
         * given secondaryHash.
         */
        Candidates(CacheLine* cl, uint64_t secondaryHash)
            : bucket(cl)
            , index(-1)
            , secondaryHash(secondaryHash)
        {
            next();
        }

        /**
         * Obtain the reference for the candidate currently pointed to by the
         * iterator. If there is no next candidate, 0 is returned.
         */
        uint64_t
        getReference()
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
        setReference(uint64_t reference)
        {
            if (bucket != NULL)
                bucket->entries[index].setReference(secondaryHash, reference);
        }

        /**
         * Remove the reference currently pointed to by the iterator. This is
         * used to delete references from the table.
         */
        void
        remove()
        {
            if (bucket != NULL)
                bucket->entries[index].clear();
        }

        /**
         * Advance the iterator to the next candidate, if there is one.
         */
        void
        next()
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
        isDone()
        {
            return (bucket == NULL);
        }

      PRIVATE:
        /// Pointer to the hash table bucket we're currently iterating over.
        CacheLine* bucket;

        /// Index into bucket we're currently iterating over.
        uint32_t index;

        /// This iterator only returns references to entries that share this
        /// secondaryHash. All others cannot possibly be matches. This helps
        /// to reduce the number of candidates whose keys are extracted from
        /// the log and compared.
        uint64_t secondaryHash;
    };

    /**
     * Constructor for HashTable.
     * \param[in] numBuckets
     *      The number of buckets in the new hash table. This should be a power
     *      of two.
     * \throw Exception
     *      An exception is thrown if numBuckets is 0.
     */
    explicit HashTable(uint64_t numBuckets)
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
    ~HashTable()
    {
        // TODO(ongaro): free chained CacheLines that were allocated in insert()
    }

    /**
     * Find possible references to an element in the hash table given a key.
     * This method returns an object that allows the caller to iterate over
     * possible matches, if there are any. It is up to the caller to check
     * whether or not each candidate matches the key they're looking for.
     *
     * \param[in] key
     *      Key representing the element to look up.
     * \return
     *      A HashTable::Candidates object is returned that can be used to
     *      iterate over all potential matches to find the desired element.
     */
    Candidates
    lookup(Key& key)
    {
        // Find the bucket using 64 bit hash of the key. Any collisions
        // arising out of this hashing will be detected / resolved by the
        // caller as it examines possible candidates.
        uint64_t secondaryHash;
        CacheLine *bucket = findBucket(key, &secondaryHash);
        return Candidates(bucket, secondaryHash);
    }

    /**
     * Insert an element corresponding to a given key into the hash table.
     *
     * Note that this method will not ensure that the same key does not already
     * exist (in other words, it will not replace a key). It is up to the caller
     * to ensure that the key was first removed, if necessary.
     *
     * \param[in] key
     *      Key naming the element to insert.
     * \param[in] reference
     *      Reference to the new element to insert into the hash table.
     */
    void
    insert(Key& key, uint64_t reference)
    {
        uint64_t secondaryHash;
        CacheLine* bucket = findBucket(key, &secondaryHash);
        while (true) {
            Entry* entry = bucket->entries;
            for (size_t i = 0; i < ENTRIES_PER_CACHE_LINE; i++) {
                if (entry->isAvailable()) {
                    entry->setReference(secondaryHash, reference);
                    return;
                }
                entry++;
            }

            Entry* last = &bucket->entries[ENTRIES_PER_CACHE_LINE - 1];
            bucket = last->getChainPointer();
            if (bucket == NULL) {
                // no empty space found, allocate a new cache line
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
    forEachInBucket(void (*callback)(uint64_t, void *),
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
    forEach(void (*callback)(uint64_t, void *), void *cookie)
    {
        uint64_t numCalls = 0;

        for (uint64_t i = 0; i < numBuckets; i++)
            numCalls += forEachInBucket(callback, cookie, i);

        return numCalls;
    }

    /**
     * Prefetch the cacheline associated with the given key.
     */
    void
    prefetchBucket(Key& key)
    {
        uint64_t dummy;
        prefetch(findBucket(key, &dummy));
    }

    /**
     * Return the number of bytes per cache line.
     */
    static uint32_t
    bytesPerCacheLine()
    {
        return BYTES_PER_CACHE_LINE;
    }

    /**
     * Return the number of elements (references) each cacheline
     * holds.
     */
    static uint32_t
    entriesPerCacheLine()
    {
        return ENTRIES_PER_CACHE_LINE;
    }

    /**
     * Returns the number of buckets allocated to the table.
     */
    uint64_t
    getNumBuckets() const
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
     * \param[in] key
     *      Key object representing the element we're looking for. 
     * \param[out] secondaryHash
     *      The secondary hash bits (16 bits).
     * \return
     *      The bucket index corresponding to the given referent ID.
     */
    static uint64_t
    findBucketIndex(uint64_t numBuckets, Key& key, uint64_t *secondaryHash)
    {
        uint64_t hashValue = key.getHash();
        uint64_t bucketHash = hashValue & 0x0000ffffffffffffUL;
        *secondaryHash = hashValue >> 48;
        return (bucketHash & (numBuckets - 1));
        // This is equivalent to:
        //     &buckets.get()[bucketHash % numBuckets]
        // since numBuckets is a power of two, and this saves about 14 cycles on
        // an Intel Core 2 (see src/misc/modulus.cc).
    }

  PRIVATE:

    // forward declarations
    class Entry;
    struct CacheLine;

    /**
     * Find the bucket corresponding to a particular key.
     * This also calculates the secondary hash bits used to disambiguate entries
     * in the same bucket.
     * \param[in] key
     *      Key object representing the element we're looking for. 
     * \param[out] secondaryHash
     *      The secondary hash bits (16 bits).
     * \return
     *      The bucket corresponding to the given key.
     */
    CacheLine *
    findBucket(Key& key, uint64_t *secondaryHash) //const
    {
        uint64_t bucketIndex =
                findBucketIndex(numBuckets, key, secondaryHash);
        return &buckets.get()[bucketIndex];
    }

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
