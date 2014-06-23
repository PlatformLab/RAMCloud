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

#include "Enumeration.h"
#include "Object.h"

namespace RAMCloud {

/**
 * Used internally by enumerateTablet() to pass arguments to
 * enumerateBucket().
 */
struct EnumerateBucketArgs {
    /// The table containing the tablet being enumerated.
    uint64_t tableId;

    /// The smallest key hash value for the tablet requested by the
    /// client, which may differ from the tablet owned by this master.
    uint64_t requestedTabletStartHash;

    /// Log containing the objects we're enumerating.
    Log* log;

    /// Iterator containing information about previous tablet configurations.
    EnumerationIterator* iter;

    /// A vector in which to place the resulting objects.
    std::vector<Log::Reference>* objectReferences;
};

/**
 * Helper function to process an individual entry in a bucket. Filters
 * the entry by the desired table ID and tablet start and end hashes,
 * and by any previous state stored on the iterator stack (besides the
 * top most entry). If the object passes all filters, then its reference
 * is pushed onto a vector so the caller can place the resulting
 * objects into the RPC payload.
 *
 * \param reference
 *      An entry in the HashTable bucket.
 * \param cookie
 *      A pointer to EnumerateBucketArgs. Note: This is a void* rather
 *      than a EnumerateBucketArgs* to conform to the
 *      HashTable::forEachInBucket interface.
 */
static void
enumerateBucket(uint64_t reference, void* cookie)
{
    EnumerateBucketArgs& args = *static_cast<EnumerateBucketArgs*>(cookie);

    LogEntryType type;
    Buffer buffer;
    type = args.log->getEntry(Log::Reference(reference), buffer);

    if (type != LOG_ENTRY_TYPE_OBJ)
        return;

    // Filter objects by table and tablet hash range.
    Key key(type, buffer);
    KeyHash keyHash = key.getHash();
    if (key.getTableId() != args.tableId ||
        keyHash < args.requestedTabletStartHash ||
        args.iter->top().tabletEndHash < keyHash) {
        return;
    }

    // Filter out objects from stale iterator entries. Skip the
    // topmost entry, which refers to the current master's state.
    for (int64_t frameIndex = static_cast<int64_t>(args.iter->size()) - 2;
         frameIndex >= 0; frameIndex--) {
        const EnumerationIterator::Frame& frame =
                args.iter->get(downCast<uint32_t>(frameIndex));
        if (frame.tabletStartHash <= keyHash &&
            keyHash <= frame.tabletEndHash) {

            uint64_t secondaryHash = 0;
            uint64_t bucketIndex = HashTable::findBucketIndex(
                frame.numBuckets, keyHash, &secondaryHash);

            if (bucketIndex < frame.bucketIndex ||
                (bucketIndex == frame.bucketIndex &&
                 keyHash < frame.bucketNextHash)) {
                return;
            }
        }
    }

    // If the last Enumeration hit a large bucket, then we are
    // resuming iteration in the same bucket and need to filter by the
    // previous progress made through the bucket.
    if (keyHash < args.iter->top().bucketNextHash) {
        return;
    }

    args.objectReferences->push_back(Log::Reference(reference));
}

/**
 * Appends objects to a buffer. Each object is a uint32_t size and a complete,
 * serialized Object.
 *
 * \param log
 *      The log containing the objects.
 * \param buffer
 *      The buffer to append to.
 * \param references 
 *      The objects to append.
 * \param maxBytes
 *      The maximum number of bytes to append.
 * \param keysOnly
 *      False means that full objects are returned, containing both keys
 *      and data. True means that the returned objects have
 *      been truncated so that the object data (normally the last
 *      field of the object) is omitted.       
 */
static int64_t
appendObjectsToBuffer(Log& log,
                      Buffer* buffer,
                      std::vector<Log::Reference>& references,
                      uint32_t maxBytes, bool keysOnly)
{
    for (uint32_t index = 0; index < references.size(); index++) {
        Buffer objectBuffer;
        log.getEntry(references[index], objectBuffer);

        Object object(objectBuffer);
        uint32_t length = objectBuffer.size();
        if (keysOnly) {
            uint32_t dataLength = object.getValueLength();
            length -= dataLength;
        }

        if (buffer->size() + sizeof(length) + length > maxBytes) {
            return index;
        }

        buffer->emplaceAppend<uint32_t>(length);
        Buffer::Iterator it(&objectBuffer, 0, length);
        while (!it.isDone()) {
            buffer->append(it.getData(), it.getLength());
            it.next();
        }
    }

    return -1;
}

/**
 * Functor for comparing two objects by key hash value.
 */
class ObjectHashComparator {
  public:
    /**
     * Construct a new key hash comparison functor.
     *
     * \param log
     *      Log containing the objects we're enumerating.
     */
    explicit ObjectHashComparator(Log& log)
        : log(log)
    {
    }

    /**
     * Compares two objects by key hash value.
     *
     * \param firstRef
     *      The first object to compare.
     * \param secondRef
     *      The second object to compare.
     * \return
     *      True if the first hash is less than the second hash, otherwise false.
     */
    bool
    operator()(Log::Reference firstRef, Log::Reference secondRef)
    {
        LogEntryType firstType;
        Buffer firstBuffer;
        firstType = log.getEntry(firstRef, firstBuffer);
        Key firstKey(firstType, firstBuffer);
        KeyHash firstHash = firstKey.getHash();

        LogEntryType secondType;
        Buffer secondBuffer;
        secondType = log.getEntry(secondRef, secondBuffer);
        Key secondKey(secondType, secondBuffer);
        KeyHash secondHash = secondKey.getHash();

        return firstHash < secondHash;
    }

  private:
    /// Log used to look up the objects associated with the references we're
    /// comparing.
    Log& log;
};

/**
 * Initiates Enumeration through the specified tablet. Enumeration may
 * not be complete upon return, call #complete() before reading the
 * values of any output parameters.
 *
 * \param tableId
 *      The table containing the tablet being enumerated.
 * \param keysOnly
 *      False means that full objects are returned, containing both keys
 *      and data. True means that the returned objects have
 *      been truncated so that the object data (normally the last
 *      field of the object) is omitted.
 * \param requestedTabletStartHash
 *      The start hash of the tablet as requested by the client.
 * \param actualTabletStartHash
 *      The start hash of the tablet that actually lives on this server.
 * \param actualTabletEndHash
 *      The end hash of the tablet that actually lives on this server.
 * \param[out] nextTabletStartHash
 *      A place to store the next tabletStartHash to return to client.
 * \param[in,out] iter
 *      The iterator provided by the client. The iterator object will
 *      be modified with state that should be returned to the client.
 * \param log
 *      The log containing the objects referenced in the objectMap.
 * \param objectMap
 *      The hash table of objects living on this server.
 * \param[out] payload
 *      A Buffer to hold the resulting objects.
 * \param maxPayloadBytes
 *      The maximum number of bytes of objects to be returned.
 */
Enumeration::Enumeration(uint64_t tableId,
                         bool keysOnly,
                         uint64_t requestedTabletStartHash,
                         uint64_t actualTabletStartHash,
                         uint64_t actualTabletEndHash,
                         uint64_t* nextTabletStartHash,
                         EnumerationIterator& iter,
                         Log& log,
                         HashTable& objectMap,
                         Buffer& payload, uint32_t maxPayloadBytes)
    : tableId(tableId)
    , keysOnly(keysOnly)
    , requestedTabletStartHash(requestedTabletStartHash)
    , actualTabletStartHash(actualTabletStartHash)
    , actualTabletEndHash(actualTabletEndHash)
    , nextTabletStartHash(nextTabletStartHash)
    , iter(iter)
    , log(log)
    , objectMap(objectMap)
    , payload(payload)
    , maxPayloadBytes(maxPayloadBytes)
{
}

/**
 * Completes an Enumeration. Upon return, the payload buffer will
 * contain objects to be returned to the client (if any are left in
 * the table), iter will be filled with the state to be returned to
 * the client, and nextTabletStartHash will be set to the next tablet
 * for the client to iterate.
 */
void
Enumeration::complete()
{
    // Check iterator state to see if the tablet configuration has
    // changed since the last call to enumerateTablet().
    if (iter.size() == 0 ||
        iter.top().tabletStartHash != actualTabletStartHash ||
        iter.top().tabletEndHash != actualTabletEndHash ||
        iter.top().numBuckets != objectMap.getNumBuckets()) {

        EnumerationIterator::Frame frame(
            actualTabletStartHash, actualTabletEndHash,
            objectMap.getNumBuckets(), 0, 0);
        iter.push(frame);
    }

    uint64_t bucketIndex = iter.top().bucketIndex;
    uint64_t numBuckets = objectMap.getNumBuckets();
    uint32_t bucketStart;
    uint32_t initialPayloadLength = payload.size();
    bool payloadFull = false;
    std::vector<Log::Reference> objectRefs;
    EnumerateBucketArgs args;
    args.tableId = tableId;
    args.requestedTabletStartHash = requestedTabletStartHash;
    args.log = &log;
    args.iter = &iter;
    args.objectReferences = &objectRefs;
    void* cookie = static_cast<void*>(&args);
    for (; bucketIndex < numBuckets && !payloadFull; bucketIndex++) {
        objectRefs.clear();
        bucketStart = payload.size();
        objectMap.forEachInBucket(enumerateBucket, cookie, bucketIndex);
        int64_t overflow = appendObjectsToBuffer(log, &payload, objectRefs,
                                                 maxPayloadBytes, keysOnly);
        payloadFull = overflow >= 0;
    }

    // Clean up if last bucket is incomplete.
    if (payloadFull) {
        payload.truncate(bucketStart);

        // If we failed to enumerate at least one entire bucket, then
        // sort the current bucket and fill the buffer with whatever
        // objects can fit.
        if (iter.top().bucketIndex == bucketIndex) {
            ObjectHashComparator comparator(log);
            std::sort(objectRefs.begin(), objectRefs.end(), comparator);

            int64_t overflow = appendObjectsToBuffer(log, &payload, objectRefs,
                                                     maxPayloadBytes, keysOnly);
            if (overflow >= 0) {
                LogEntryType type;
                Buffer buffer;
                type = log.getEntry(objectRefs[overflow], buffer);
                Key key(type, buffer);
                KeyHash nextHash = key.getHash();
                iter.top().bucketNextHash = nextHash;
            }
        }
    }

    // At end of iteration, bucketIndex points to next (uncovered) bucket.
    iter.top().bucketIndex = bucketIndex;

    // Check end of tablet.
    *nextTabletStartHash = requestedTabletStartHash;
    if (bucketIndex >= numBuckets &&
            payload.size() == initialPayloadLength) {
        while (iter.size() > 0 &&
               iter.top().tabletEndHash <= actualTabletEndHash) {
            iter.pop();
        }

        // Note: If this is the last tablet, nextTabletStartHash will
        // roll around to 0.
        *nextTabletStartHash = actualTabletEndHash + 1;
    }
}

} // namespace RAMCloud
