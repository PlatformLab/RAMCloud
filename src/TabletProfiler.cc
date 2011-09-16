/* Copyright (c) 2010 Stanford University
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

#include <math.h>

#include "Log.h"
#include "TabletProfiler.h"

/*
 * The TabletProfiler object efficiently tracks usage of table key spaces and
 * enables computation of partitions that have a bounded error in size and
 * number of referents. A referent is a generic term for whatever the key
 * refers to, be it an object, a tombstone, or something else.
 *
 * The motivation is as follows. In RAMCloud, we attempt to provide locality
 * in the key space (i.e. keys close together should prefer living on the same
 * server). However, for recovery to be efficient, we need to divvy up the key
 * space into contiguous, non-overlapping regions of a desired maximal size.
 * This maximum size dictates how long it takes to recover the partition of a
 * failed master on a new node.
 *
 * The trouble with this approach is that dividing the key space into chunks
 * of keys that represent referents totalling a specific number of bytes is
 * hard, as the access patterns are not predictable and we cannot expect an even
 * distribution within the key space. For instance, users could append only to
 * the beginning of the key space. They could also use random hashes for keys.
 * Or, perhaps key bits are partitioned (the upper 32-bits are a user id, the
 * lower 32-bits are some metadata, etc). They could even use multiple such
 * methods on the same table. Furthermore, referents may also be deleted or
 * overwritten. All of these result in unpredictable densities in the key space,
 * which makes our task more difficult.
 *
 * The TabletProfiler tackles this problem in an approximate way, however it can
 * bound error. That is, if we want partitions of 500 MB, we can guarantee that
 * what we compute is within a reasonable range of 500. Our approach  works as
 * follows. The entire key space is described by a root Subrange, an object that
 * splits the whole 64-bit space into evenly-sized regions and tracks the number
 * of referents and referent bytes used by keys in each region. Each even split
 * of a Subrange is called a Bucket. Buckets maintain statistics about the
 * number of referents and referent bytes within the range they track.
 *
 * When a Bucket gets large (either in number of bytes, or number of referents),
 * it makes sense to take a closer look at that range so that we can more
 * accurately make partitions. We do so by pointing the Bucket to a new child
 * Subrange, which spans the same range as the Bucket. This gives us a higher
 * resolution for a hot key space. Although we don't know where the previous
 * tracked referents fall within each of the new smaller Buckets of the child
 * Subrange, we can track any new key insertions into the range more precisely.
 *
 *
 * These nested Subranges form a tree hierarchy. Each next level down the tree
 * corresponds to smaller and smaller Subranges of the key space. For instance,
 * if each Subrange has 256 Buckets, then the root Subrange's Buckets each
 * describe 1/256th of the whole 64-bit key space. Buckets in Subranges
 * immediately under a root Bucket then describe 1/256th of that 1/256th of the
 * 64-bit key space, or simply 1/2^16th. By setting the number of Buckets per
 * Subrange, we can control how many levels are necessary to zoom in and track
 * very small key ranges, as well as the memory overhead for each allocated
 * level. This presents a trade-off: if each level shaves off fewer bits of the
 * address space, we require more levels and therefore longer traversals.
 * However, more bits per level imply larger Subrange structures.
 *
 * A useful analogy for the structure that these Subranges form may be processor
 * page tables. E.g., each lower level describes a smaller range of addresses,
 * and each higher level can potentially point to larger contiguous pages.
 *
 * When tracking a new referent, statistical updates are reflected in the lowest
 * appropriate Subrange of the tree. Initially, only a root Subrange exists and
 * counts are updated in its Buckets. As the tree grows, the lowest bucket (i.e.
 * the tightest Subrange) corresponding to a given key is updated. This method
 * enables us to dynamically drill down on hot key ranges, all the way to single
 * key granularity, if necessary. Note that the amount of resources employed in
 * tracking is simply proportional to the density of a key range. Futhermore, by
 * deciding to zoom in on a range only after seeing a relatively low threshold
 * number of bytes or referents in that range, we can bound the error introduced
 * in computing partitions. That is, the referents we've tracked at a higher
 * level of the tree exist somewhere in an exponentially larger key space than
 * those tracked lower down. We therefore can't attribute them accurately to
 * fine-grained ranges of the key space. However, if the number of levels in the
 * tree is reasonably small and the maximum size before splitting a Bucket into
 * a new lower Subrange is small, the total error is small.
 *
 * For example, say that all Buckets may track at most 8MB of referents within
 * their range before starting a new child Subrange and each Subrange splits its
 * range into 256 Buckets. We then have at most 8 levels in the tree. To form a
 * new partition, we must draw a dividing line in the key space. Between this
 * new divider and the previous one we know the exact number of bytes and
 * referents tracked in the range except for in at most two leaf Subranges that
 * correspond to the border lines between partitions. In the border Subrange(s),
 * our error is at most the sum of the up to 7 previous tree levels' worth of
 * referents, each of which may fall before or after our dividing lines. In
 * our example, we have at worst 8*7 = 56MB of referents above us in the tree
 * that may come before or after.
 *
 * There are two main parameters of our structure that we care about:
 *      B - The number of bits of key space each Subrange spans.
 *      S - The maximum number of bytes each Bucket should track, before
 *          creating a child Subrange for more accurate accounting.
 *
 * (There is an additional parameter analogous to S for the number of referents
 *  per range, rather than total bytes, but it behaves in essentially the same
 *  way as S.)
 *
 * If each Subrange covers B bits of the key space, then that range is split
 * into 2^B individual Buckets. (Actually, if 64 isn't evenly divisible by B,
 * then the last level may have fewer, but this is a minor detail.) B dictates
 * the overhead involved in allocating each next Subrange. If each Bucket is 16
 * bytes, then each Subrange is approximately B * 16 bytes in size.
 *
 * Using B and S we can compute the worst case overhead that the TabletProfiler
 * structure imposes on the system, as well as the maximum error we can
 * experience in computing partitions. This second aspect is especially
 * important, as it lets us compute the error bound.
 */

namespace RAMCloud {

//////////////////////////////////////////////
// TabletProfiler class
//////////////////////////////////////////////

// Public Methods

/**
 * Create an TabletProfiler object ready to track any keys between
 * 0 and 2^64 - 1.
 *
 * \return
 *      A brand spanking new TabletProfiler.
 */
TabletProfiler::TabletProfiler()
    : root(NULL),
      findHint(NULL, 0),
      lastTracked(LogTime(0, 0)),
      totalTracked(0),
      totalTrackedBytes(0)
{
    root = new Subrange(
        Subrange::BucketHandle(NULL, 0), 0, ~0, LogTime(0, 0));
}

/**
 * Destroy the object, freeing all dynamically allocated resources.
 */
TabletProfiler::~TabletProfiler()
{
    delete root;
}

/**
 * Track a key referent in the TabletProfiler. This updates the internal state
 * appropriately to reflect one newly introduced referent of the given
 * number of bytes. This structure has no way of identifying identical
 * referents, so this must be called only once for each.
 *
 * \param[in] key
 *      The key of the referent to track.
 * \param[in] bytes
 *      The number of bytes consumed by the referent.
 * \param[in] time
 *      The LogTime at which the referent was appended to the Log.
 */
void
TabletProfiler::track(uint64_t key, uint32_t bytes, LogTime time)
{
    assert(time > lastTracked || time == LogTime(0, 0));
    lastTracked = time;

    Subrange::BucketHandle bh = findBucket(key);
    bh.getSubrange()->track(bh, key, bytes, time);
    totalTracked++;
    totalTrackedBytes += bytes;
}

/**
 * Stop tracking a referent, which was previously being tracked via the
 * TabletProfiler::track() method. This must be called at most once per
 * tracked referent.
 *
 * \param[in] key
 *      The key of the referent to track.
 * \param[in] bytes
 *      The number of bytes consumed by the referent.
 * \param[in] time
 *      The LogTime at which the referent was appended to the Log.
 */
void
TabletProfiler::untrack(uint64_t key, uint32_t bytes, LogTime time)
{
    assert(time <= lastTracked);

    Subrange::BucketHandle bh = findBucket(key, &time);
    bool deleted = bh.getSubrange()->untrack(bh, key, bytes, time);
    if (deleted)
        findHint = Subrange::BucketHandle(NULL, 0);

    assert(totalTracked > 0);
    assert(totalTrackedBytes >= bytes);

    totalTracked--;
    totalTrackedBytes -= bytes;
}

/**
 * Obtain an ordered list of sequential partitions that are not expected to
 * greatly exceed the given parameters for number of total bytes and total
 * referents.
 *
 * The returned partitions describe the entire key space.
 *
 * Note that each partition may be exceed the given amounts by a bounded
 * error. Likewise, all partitions except the last may contain a bounded
 * amount less than specified. Finally, the last partition may be
 * arbitrarily small.
 *
 * \param[in] maxPartitionBytes
 *      The desired maximum number of bytes per partition.
 * \param[in] maxPartitionReferents
 *      The desired maximum number of referents per partition.
 * \param[in] residualMaxBytes
 *      Residual byte count to take into account for the first
 *      partition computed. This can be used to obtain a smaller first
 *      partition, which is helpful when combining multiple Tablets'
 *      partitions during the Will computation.
 * \param[in] residualMaxReferents
 *      Same as residualMaxBytes, but for referent counts instead of
 *      byte counts.
 * \return
 *      A PartitionList (vector of Partition structures) that describes the
 *      first and last keys of the calculated partitions.
 */
PartitionList*
TabletProfiler::getPartitions(uint64_t maxPartitionBytes,
    uint64_t maxPartitionReferents, uint64_t residualMaxBytes,
    uint64_t residualMaxReferents)
{
    PartitionList* partitions = new PartitionList();

    // if we have only one partition's worth, then we can give an exact tally
    if ((totalTrackedBytes + residualMaxBytes) < maxPartitionBytes &&
        (totalTracked + residualMaxReferents) < maxPartitionReferents) {
        Partition newPart;
        newPart.minBytes = newPart.maxBytes = totalTrackedBytes;
        newPart.minReferents = newPart.maxReferents = totalTracked;
        newPart.firstKey = 0;
        newPart.lastKey = (uint64_t)-1;
        partitions->push_back(newPart);
    } else {
        PartitionCollector pc(maxPartitionBytes, maxPartitionReferents,
            partitions, residualMaxBytes, residualMaxReferents);
        root->partitionWalk(&pc);
        pc.done();
    }

    return partitions;
}

// Private Methods

/**
 * Look up the Subrange and Bucket corresponding to the given key and,
 * optionally, the given LogTime.
 *
 * This method is an optimisation that wraps around Subrange::findBucket.
 * It merely caches the last lookup and checks that first on the next
 * invocation before doing a full traversal. This significantly speeds
 * up accesses with high key locality, exactly the sort of accesses that
 * can result in a deeply nested structure in the first place.
 *
 * \param[in] key
 *      The key whose smallest Bucket we're looking for.
 * \param[in] time
 *      A pointer to a LogTime to restrict the search to Buckets in
 *      Subranges that are at least as old as the parameter. If
 *      NULL, no restriction occurs.
 * \return
 *      A BucketHandle to the Bucket (and associated Subrange) found.
 */
TabletProfiler::Subrange::BucketHandle
TabletProfiler::findBucket(uint64_t key, LogTime *time)
{
    Subrange* subrange = findHint.getSubrange();
    if (subrange != NULL &&
        (time == NULL || subrange->getCreateTime() <= *time) &&
        (key >= subrange->getFirstKey() && key <= subrange->getLastKey())) {
            findHint = subrange->findBucket(key, time);
    } else {
        findHint = root->findBucket(key, time);
    }

    return findHint;
}

//////////////////////////////////////////////
// TabletProfiler::PartitionCollector class
//////////////////////////////////////////////

/**
 * Construct a PartitionCollector object. This is used when walking the
 * tree to compute the individual partitions.
 *
 * \return
 *      A newly constructed PartitionCollector.
 */
TabletProfiler::PartitionCollector::PartitionCollector(
    uint64_t maxPartitionBytes, uint64_t maxPartitionReferents,
    PartitionList* partitions, uint64_t residualMaxBytes,
    uint64_t residualMaxReferents)
    : partitions(partitions),
      residualMaxBytes(residualMaxBytes),
      residualMaxReferents(residualMaxReferents),
      maxPartitionBytes(maxPartitionBytes),
      maxPartitionReferents(maxPartitionReferents),
      nextFirstKey(0),
      currentFirstKey(0),
      currentKnownBytes(0),
      currentKnownReferents(0),
      previousPossibleBytes(0),
      previousPossibleReferents(0),
      isDone(false)
{
}

/**
 * Update the PartitionCollector by telling it about a key range described
 * by a leaf Bucket in our TabletProfiler. This function expects to be called
 * once for all sequential, non-overlapping key ranges for the entire key
 * space, i.e. all leaf Buckets. It must not be called on internal Bucket,
 * i.e. those that have children.
 *
 * This method tracks the next partition and, if parameters are exceeded,
 * closes the current partition, saves it, and starts a new one. 
 *
 * \param[in] firstKey
 *      The first key of the range for which the given byte and referent counts
 *      apply. At each next invocation of this method firstKey should be equal
 *      to the previous lastKey + 1.
 * \param[in] lastKey
 *      The last key key of the range for which the given byte and referent counts
 *      apply.
 * \param[in] rangeBytes
 *      The number of referent bytes consumed by this (firstKey, lastKey) range.
 * \param[in] rangeReferents 
 *      The number of referent in this (firstKey, lastKey) range.
 * \param[in] possibleBytes
 *      The number of referent bytes that may exist in this range, but we're not
 *      sure.
 * \param[in] possibleReferents
 *      The number of referents that may exist in this range, but we're not sure.
 * \return
 *      true if no Partition was made during this call, else false.
 */
bool
TabletProfiler::PartitionCollector::addRangeLeaf(uint64_t firstKey,
    uint64_t lastKey, uint64_t rangeBytes, uint64_t rangeReferents,
    uint64_t possibleBytes, uint64_t possibleReferents)
{
    bool noPartitionMade = true;

    assert(!isDone);
    assert(nextFirstKey == firstKey);

    currentKnownBytes += rangeBytes;
    currentKnownReferents += rangeReferents;

    uint64_t maxPossibleBytes = currentKnownBytes +
                                possibleBytes +
                                previousPossibleBytes;

    uint64_t maxPossibleReferents = currentKnownReferents +
                                    possibleReferents +
                                    previousPossibleReferents;

    if (maxPossibleBytes + residualMaxBytes >= maxPartitionBytes ||
        maxPossibleReferents + residualMaxReferents >= maxPartitionReferents) {

        pushCurrentTally(lastKey, currentKnownBytes, maxPossibleBytes,
            currentKnownReferents, maxPossibleReferents);
        currentFirstKey = lastKey + 1;

        // Remember by how many bytes and referents we could have been off
        // when drawing this line in the sand. We will need this for the
        // next one.
        //
        // Note that this can be optimised if the Subranges in which we
        // drawn lines to form a Partition share common ancestors. In
        // such cases, we can safely obtain lower estimates. I'm not sure
        // it's worth doing.
        previousPossibleBytes = possibleBytes;
        previousPossibleReferents = possibleReferents;

        noPartitionMade = false;
    }

    nextFirstKey = lastKey + 1;
    return noPartitionMade;
}

/**
 * Add the given byte and reference counts to our current Partition-in-
 * progress. This method will never result in the Partition being split
 * off and a new one started. The reason is that calls to addRangeLeaf()
 * will always split based on a worst-case estimate. If no split occurs,
 * then parent Subranges can use this method to track those values that
 * could not have resulted in a split anyway.
 *
 * Yes, it's a bit confusing, but without drawing pictures I'm not sure
 * how to explain it concisely.
 *
 * \param[in] rangeBytes
 *      The number of bytes to add to our current Partition's count.
 * \param[in] rangeReferents
 *      The number of referents to add to our current Partition's count.
 */
void
TabletProfiler::PartitionCollector::addRangeNonLeaf(uint64_t rangeBytes,
    uint64_t rangeReferents)
{
    currentKnownBytes += rangeBytes;
    currentKnownReferents += rangeReferents;
}

/**
 * Finalise the PartitionCollector after all calls to addRangeLeaf and
 * addRangeNonLeaf have been made. After invocation, the object cannot be
 * altered.
 */
void
TabletProfiler::PartitionCollector::done()
{
    assert(!isDone);
    pushCurrentTally(~0,
        currentKnownBytes,
        currentKnownBytes + previousPossibleBytes,
        currentKnownReferents,
        currentKnownReferents + previousPossibleReferents);

    if (partitions->size() == 0) {
        // empty tablet case
        Partition newPart;
        newPart.firstKey = 0;
        newPart.lastKey = (uint64_t)-1;
        partitions->push_back(newPart);
    }
    isDone = true;
}

// Private Methods

/**
 * Helper method to push the current partition being computed on to
 * the list and resetting state for the next partition.
 *
 * \param[in] lastKey
 *      The last key to be associated with the currently-computed
 *      partition.
 * \param[in] minBytes
 *      The minimum number of bytes that could be in this partition.
 * \param[in] maxBytes
 *      The maximum number of bytes that could be in this partition.
 * \param[in] minReferents
 *      The minimum number of referents that could be in this partition.
 * \param[in] maxReferents
 *      The maximum number of referents that could be in this partition.
 */
void
TabletProfiler::PartitionCollector::pushCurrentTally(uint64_t lastKey,
    uint64_t minBytes, uint64_t maxBytes, uint64_t minReferents,
    uint64_t maxReferents)
{
    assert(!isDone);
    if (currentKnownBytes != 0) {
        assert(currentKnownReferents != 0);

        Partition newPart;
        newPart.firstKey = currentFirstKey;
        newPart.lastKey = lastKey;
        newPart.minBytes = minBytes;
        newPart.maxBytes = maxBytes;
        newPart.minReferents = minReferents;
        newPart.maxReferents = maxReferents;

        partitions->push_back(newPart);
        currentKnownBytes = 0;
        currentKnownReferents = 0;
        residualMaxBytes = 0;
        residualMaxReferents = 0;
    }
}

//////////////////////////////////////////////
// TabletProfiler::Subrange::BucketHandle class
//////////////////////////////////////////////

/**
 * Construct a new BucketHandle, a simple helper object that represents a
 * Bucket and its associated Subrange.
 *
 * \param[in] subrange
 *      A pointer to the Subrange this bucket exists in.
 * \param[in] bucketIndex
 *      The index of this bucket in the given Subrange.
 * \return
 *      A newly constructed BucketHandle.
 */
TabletProfiler::Subrange::BucketHandle::BucketHandle(Subrange *subrange,
    int bucketIndex)
    : subrange(subrange),
      bucketIndex(bucketIndex)
{
}

/**
 * \return
 *      A pointer to the Subrange this BucketHandle represents.
 */
TabletProfiler::Subrange*
TabletProfiler::Subrange::BucketHandle::getSubrange()
{
    return subrange;
}

/**
 * \return
 *      A pointer to the Bucket this BucketHandle represents.
 */
TabletProfiler::Bucket*
TabletProfiler::Subrange::BucketHandle::getBucket()
{
    if (subrange == NULL)
        return NULL;
    return subrange->getBucket(bucketIndex);
}

/**
 * \return
 *      The first key of the range spanned by the Bucket this handle represents.
 */
uint64_t
TabletProfiler::Subrange::BucketHandle::getFirstKey()
{
    return subrange->getBucketFirstKey(*this);
}

/**
 * \return
 *      The last key of the range spanned by the Bucket this handle represents.
 */
uint64_t
TabletProfiler::Subrange::BucketHandle::getLastKey()
{
    return subrange->getBucketLastKey(*this);
}

//////////////////////////////////////////////
// TabletProfiler::Subrange class
//////////////////////////////////////////////

/**
 * Construct a new Subrange object. A Subrange represents a contiguous span
 * of the key space. It divides this span into a number of Buckets, which
 * each maintain their own counts and may have children Subranges if more
 * accurate accounting of that range is required. Subranges form a tree
 * describing the entire key space.
 *
 * \param[in] parent
 *      A BucketHandle representing the parent Bucket. The parent, if it
 *      exists, tracks the same key range, but at a more coarse granularity.
 *      The root Subrange should have an invalid handle, i.e.
 *      BucketHandle(NULL, 0).
 * \param[in] firstKey
 *      The first key of the key space this Subrange is tracking.
 * \param[in] lastKey 
 *      The last key of the key space this Subrange is tracking.
 * \param[in] time
 *      The LogTime at the time of this Subrange's creation. All future calls
 *      to TabletProfiler::track() should be for referents with a LogTime
 *      greater than or equal to this value. 
 * \return
 *      A newly created Subrange object.
 */
TabletProfiler::Subrange::Subrange(BucketHandle parent, uint64_t firstKey,
    uint64_t lastKey, LogTime time)
    : parent(parent),
      bucketWidth(0),
      buckets(NULL),
      numBuckets(1U << BITS_PER_LEVEL),
      firstKey(firstKey),
      lastKey(lastKey),
      totalBytes(0),
      totalReferents(0),
      totalChildren(0),
      createTime(time)
{
    assert(firstKey <= lastKey);

    // calculate each bucket's width in this subrange.
    if (firstKey == 0 && lastKey == ~(uint64_t)0) {
        // special case the full 64-bit space to avoid overflow.
        bucketWidth = (1UL << (64 - BITS_PER_LEVEL));
    } else {
        bucketWidth = (lastKey - firstKey + 1) / numBuckets;

        // if (64 % BITS_PER_LEVEL) != 0, our last level Subranges (which
        // always have a bucketWidth of 1) will have less than 2^BITS_PER_LEVEL
        // buckets.
        if (bucketWidth == 0) {
            numBuckets = downCast<uint32_t>(lastKey - firstKey + 1);
            bucketWidth = 1;
        }
    }

    buckets = static_cast<Bucket*>(xmalloc(sizeof(Bucket) * numBuckets));
    memset(buckets, 0, sizeof(Bucket) * numBuckets);
}

/**
 * Destroy the Subrange, including all child Subranges for constituent Buckets.
 * This recursively frees all memory used at and below this Subrange.
 */
TabletProfiler::Subrange::~Subrange()
{
    for (uint32_t i = 0; i < numBuckets; i++)
        delete buckets[i].child;
    free(buckets);
}

/**
 * Look up the Subrange and Bucket corresponding to the given key and,
 * optionally, the given LogTime.
 *
 * \param[in] key
 *      The key whose smallest Bucket we're looking for.
 * \param[in] time
 *      A pointer to a LogTime to restrict the search to Buckets in
 *      Subranges that are at least as old as the parameter. If
 *      NULL, no restriction occurs.
 * \return
 *      A BucketHandle to the Bucket (and associated Subrange) found.
 */
TabletProfiler::Subrange::BucketHandle
TabletProfiler::Subrange::findBucket(uint64_t key, LogTime *time)
{
    assert(key >= firstKey && key <= lastKey);
    uint64_t idx = (key - firstKey) / bucketWidth;
    Bucket* b = &buckets[idx];
    bool recurse = false;

    // If we're doing a LogTime-restricted search, then we only recurse if the
    // next level's Subrange is as old as or younger than *time.
    //
    // Note that while it's very tempting and natural to use exceptions here to
    // pop off the stack once when we've gone just too far, they're extremely
    // expensive: they appear to slow the whole thing down by about 15x.
    if (b->child != NULL) {
        if (time == NULL || b->child->createTime <= *time)
            recurse = true;
    }

    if (recurse)
        return b->child->findBucket(key, time);

    return BucketHandle(this, downCast<uint32_t>(idx));
}

/**
 * \param[in] bucketIndex
 *      Index of the Bucket to get a pointer to.
 * \return
 *      A pointer to the Bucket corresponding to the given index.
 */
TabletProfiler::Bucket*
TabletProfiler::Subrange::getBucket(uint32_t bucketIndex)
{
    assert(bucketIndex < numBuckets);
    return &buckets[bucketIndex];
}

/**
 * Obtain the first key of the range tracked by the Bucket
 * represented by the given handle. 
 *
 * \param[in] bh
 *      The handle of the Bucket, whose first key we're querying.
 * \return
 *      The first key for the referenced Bucket.
 */
uint64_t
TabletProfiler::Subrange::getBucketFirstKey(BucketHandle bh)
{
    int64_t bucketIndex = bh.getBucket() - bh.getSubrange()->buckets;
    assert(bucketIndex >= 0 && bucketIndex < numBuckets);
    return firstKey + (bucketIndex * bucketWidth);
}

/**
 * Obtain the last key of the range tracked by the Bucket
 * represented by the given handle. 
 *
 * \param[in] bh
 *      The handle of the Bucket, whose first key we're querying.
 * \return
 *      The last key for the referenced Bucket.
 */
uint64_t
TabletProfiler::Subrange::getBucketLastKey(BucketHandle bh)
{
    int64_t bucketIndex = bh.getBucket() - bh.getSubrange()->buckets;
    assert(bucketIndex >= 0 && bucketIndex < numBuckets);
    return firstKey + (bucketIndex * bucketWidth) + bucketWidth - 1;
}

/**
 * Determine if this Subrange is at the bottom of the tree,
 * i.e. that each Bucket covers only 1 key in the key space
 * and therefore cannot have any children.
 *
 * \return
 *      true if this Subrange is at the bottom, else false.
 */
bool
TabletProfiler::Subrange::isBottom()
{
    return (bucketWidth == 1);
}

/**
 * Walk this Subrange, tracking the number of parent bytes and
 * referents before recursing, and calling the addRangeLeaf method on
 * all leaf Buckets and the addRangeNonLeaf method on non-leaf Buckets,
 * none of whose children participated in a Partition being made. This
 * is used to compute the desired partitions of the key space.
 *
 * \param[in] pc
 *      The PartitionCollector to use for this walk.
 * \param[in] parentBytes
 *      The number of bytes in parent Subranges, which may or may
 *      not be in this smaller Subrange. This parameter is used for
 *      internal recursion, and should normally be omitted in preference
 *      of the default value. 
 * \param[in] parentReferents
 *      The number of referents in parent Subranges, which may or may
 *      not be in this smaller Subrange. This parameter is used for
 *      internal recursion, and should normally be omitted in preference
 *      of the default value. 
 * \return
 *      True if no Partition as generated due to this call (or any of its,
 *      recursive calls). False otherwise. This is used to determine when to
 *      add counts to the current Partition being generated, and when those
 *      bytes/referents have already been taken into account when a Partition
 *      was made.
 */
bool
TabletProfiler::Subrange::partitionWalk(PartitionCollector *pc,
    uint64_t parentBytes, uint64_t parentReferents)
{
    bool noPartitionMade = true;
    for (uint32_t i = 0; i < numBuckets; i++) {
        if (buckets[i].child != NULL) {
            bool ret = buckets[i].child->partitionWalk(pc,
                parentBytes + buckets[i].totalBytes,
                parentReferents + buckets[i].totalReferents);

            if (ret) {
                pc->addRangeNonLeaf(buckets[i].totalBytes,
                    buckets[i].totalReferents);
            }

            noPartitionMade = ret && noPartitionMade;
        } else {
            BucketHandle bh(this, i);
            bool ret = pc->addRangeLeaf(bh.getFirstKey(), bh.getLastKey(),
                buckets[i].totalBytes, buckets[i].totalReferents,
                parentBytes, parentReferents);
            noPartitionMade = ret && noPartitionMade;
        }
    }
    return noPartitionMade;
}

/**
 * Add a new object to be tracked by the TabletProfiler. This method will
 * automatically extend the tree into a further Subrange if necessary and
 * appropriate. Note that this method should only be called on leaf Subranges.
 *
 * \param[in] bh
 *      BucketHandle representing the Bucket this key should be tracked in.
 *      This parameter is typically computed by the findBucket() method.
 * \param[in] key
 *      The key of the referent to be tracked.
 * \param[in] bytes
 *      The number of bytes consumed by the referent associated with the key.
 * \param[in] time
 *      The LogTime corresponding to the append of the referent in the Log.
 */
void
TabletProfiler::Subrange::track(BucketHandle bh, uint64_t key,
    uint32_t bytes, LogTime time)
{
    Bucket* b = bh.getBucket();

    assert(this == bh.getSubrange());
    assert(b->child == NULL);

    uint64_t newTotalBytes = b->totalBytes + bytes;
    uint64_t newTotalReferents = b->totalReferents + 1;

    if (!isBottom() && (newTotalBytes   > BUCKET_SPLIT_BYTES ||
                        newTotalReferents > BUCKET_SPLIT_OBJS)) {
        // we've outgrown this bucket. time to add a child so we can drill
        // down on this range.
        b->child = new Subrange(bh, bh.getFirstKey(), bh.getLastKey(), time);

        BucketHandle childBh = b->child->findBucket(key);
        childBh.getSubrange()->track(childBh, key, bytes, time);
        totalChildren++;
    } else {
        b->totalBytes += bytes;
        b->totalReferents++;
        totalBytes += bytes;
        totalReferents++;
    }
}

/**
 * Remove an referent previously tracked by the TabletProfiler (i.e. provided to
 * the track method). This method will automatically merge the Subrange
 * with its parent Bucket if appropriate. If merging does occur, this method
 * releases unneeded resouces and returns true to indicate that the provided
 * BucketHandle no longer references a valid Subrange/Bucket. Note that this
 * method may be called on both internal and leaf Subranges.
 *
 * \param[in] bh
 *      BucketHandle representing the Bucket this key is being tracked in.
 *      This parameter is typically computed by the findBucket() method.
 * \param[in] key
 *      The key of the referent being tracked.
 * \param[in] bytes
 *      The number of bytes consumed by the referent associated with the key.
 * \param[in] time
 *      The LogTime corresponding to the append of the referent in the Log. This
 *      should be the same parameter as passed to the track method for this
 *      referent and is used only for sanity checking and interface consistency.
 * \return
 *      true if removal of this referent resulted in the Bucket referenced by
 *      the BucketHandle bh to be merged with the parent. Otherwise false.
 *      This can be used to detect if the given BucketHandle is no longer
 *      valid after the call.
 */
bool
TabletProfiler::Subrange::untrack(BucketHandle bh, uint64_t key,
    uint32_t bytes, LogTime time)
{
    Bucket* b = bh.getBucket();

    // the following should be strictly true unless this module is buggy
    assert(this == bh.getSubrange());
    assert(createTime <= time);
    if (b->child != NULL)
        assert(b->child->createTime > time);

    // the following could be caused either by our bugs, or the caller's bugs
    // if we're sufficiently confident in this code, perhaps these should be
    // exceptions
    assert(totalBytes >= bytes);
    assert(totalReferents > 0);
    assert(b->totalBytes >= bytes);
    assert(b->totalReferents > 0);

    totalBytes -= bytes;
    totalReferents--;
    b->totalBytes -= bytes;
    b->totalReferents--;

    // now check to see if this subrange should be joined with the parent.
    Bucket* parentBucket = parent.getBucket();
    if (totalChildren == 0 && parentBucket != NULL) {
        uint64_t bytesWithParent = totalBytes + parentBucket->totalBytes;
        uint64_t referentsWithParent =
            totalReferents + parentBucket->totalReferents;

        if (bytesWithParent   <= BUCKET_MERGE_BYTES &&
            referentsWithParent <= BUCKET_MERGE_OBJS) {

            // time to merge this Subrange with the parent Bucket
            parentBucket->child = NULL;
            parentBucket->totalBytes += totalBytes;
            parentBucket->totalReferents += totalReferents;

            Subrange* parentSubrange = parent.getSubrange();
            parentSubrange->totalBytes += totalBytes;
            parentSubrange->totalReferents += totalReferents;
            parentSubrange->totalChildren--;

            // we're done with this range. the caller's pointer is now invalid.
            delete this;
            return true;
        }
    }

    return false;
}

/**
 * \return
 *      The LogTime at which this Subrange was created, as passed to
 *      the constructor.
 */
LogTime
TabletProfiler::Subrange::getCreateTime()
{
    return createTime;
}

/**
 * \return
 *      The first key of this Subrange.
 */
uint64_t
TabletProfiler::Subrange::getFirstKey()
{
    return firstKey;
}

/**
 * \return
 *      The last key of this Subrange.
 */
uint64_t
TabletProfiler::Subrange::getLastKey()
{
    return lastKey;
}

} // namespace
