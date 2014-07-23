/* Copyright (c) 2014 Stanford University
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

#include "IndexLookup.h"

namespace RAMCloud {
/**
 * Constructor for IndexLookup class.
 *
 * Invoking the constructor will initiate the process of fetching
 * the objects specified by the arguments. Other methods such as
 * isReady and getNext may be used to retrieve the objects one at
 * a time.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this class.
 * \param tableId
 *      Id of the table in which lookup is to be done.
 * \param indexId
 *      Id of an index within tableId, which will be used for the lookup.
 * \param firstKey
 *      Starting key for the key range in which keys are to be matched.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      this object.
 * \param firstKeyLength
 *      Length in bytes of the firstKey.
 * \param lastKey
 *      Ending key for the key range in which keys are to be matched.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      this object.
 * \param lastKeyLength
 *      Length in byes of the lastKey.
 * \param maxNumHashes
 *      Maximum number of hashes that the server is allowed to return
 *      in a single rpc.
 * \param flags
 *      Provides additional information to control the range query.
 */
IndexLookup::IndexLookup(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                         const void* firstKey, uint16_t firstKeyLength,
                         const void* lastKey, uint16_t lastKeyLength,
                         uint32_t maxNumHashes, Flags flags)
    : ramcloud(ramcloud)
    , lookupRpc()
    , tableId(tableId)
    , indexId(indexId)
    , firstKey(firstKey)
    , firstKeyLength(firstKeyLength)
    , lastKey(lastKey)
    , lastKeyLength(lastKeyLength)
    , flags(flags)
    , maxNumHashes(maxNumHashes)
    , nextKey(NULL)
    , nextKeyLength(0)
    , nextKeyHash(0)
    , numInserted(0)
    , numRemoved(0)
    , numAssigned(0)
    , curObj()
    , curIdx(RPC_ID_NOT_ASSIGN)
    , finishedLookup(false)
{
    for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
        readRpc[i].status = FREE;
    }

    lookupRpc.resp.reset();
    lookupRpc.status = SENT;
    lookupRpc.rpc.construct(ramcloud, tableId, indexId,
                            firstKey, firstKeyLength,
                            0, lastKey, lastKeyLength,
                            maxNumHashes, &lookupRpc.resp);
}

IndexLookup::~IndexLookup()
{
    free(nextKey);
}


/**
 * This method returns an indication of whether the indexed read
 * has made enough progress that getNext can return immediately
 * without blocking. In addition, this method does most of the
 * real work for indexed reads, so it must be invoked (either
 * directly or indirectly by calling getNext) in order for the
 * read to make progress.
 *
 * \return
 *      True means that getNext will not block if it is invoked;
 *      false means that getNext may block.
 */
bool
IndexLookup::isReady()
{
    // In order to handle multiple RPCs concurrently and to
    // tolerate failures, the implementation of this module
    // is organized around a collection of rules using the
    // DCFT style. Each rule consists of code that makes a
    // small amount of progress in the indexed read, such as
    // starting a new RPC or handling an RPC completion. Each
    // rule also has a predicate (just an "if" test on state
    // variables) that determines when the rule can execute.
    // The predicates enforce a partial order on the rule
    // actions.
    //
    // Rules can potentially fire in many different orders,
    // depending on when RPCs complete and what errors occur;
    // the order in the code below reflects the normal progress
    // to handle a particular returned object.

    if (!finishedLookup) {
        // Rule 1:
        // Handle the completion of a lookIndexKeys RPC.
        if (lookupRpc.status == SENT && lookupRpc.rpc->isReady()) {
            lookupRpc.rpc->wait(&lookupRpc.numHashes, &nextKeyLength,
                                &nextKeyHash);
            lookupRpc.offset = sizeof32(WireFormat::LookupIndexKeys::Response);
            if (nextKey != NULL)
                free(nextKey);
            // Save the "next key" information from this response,
            // which will be used as the starting key for the next
            // lookupIndexKeys request.
            // This malloc and memcpy can be expensive, but it is expected
            // index lookup won't span accross many servers.
            // TODO(zhihao): if we can make sure that lookupIndexKeys
            // construction doesn't clear the response buffer before copying
            // firstKey into request buffer, we can consider removing this
            // malloc & memcpy.
            nextKey = malloc(nextKeyLength);
            uint32_t off = lookupRpc.offset
                           + lookupRpc.numHashes * (uint32_t) sizeof(KeyHash);
            memcpy(nextKey, lookupRpc.resp.getOffset<char>(off),
                   nextKeyLength);
            lookupRpc.status = RESULT_READY;
        }
        /// Rule 2:
        /// If an returned lookupIndexKeys RPC still has some activeHashes
        /// unread, copy as much of them into activeHashes as possible.
        if (lookupRpc.status == RESULT_READY && lookupRpc.numHashes > 0) {
            while (lookupRpc.numHashes > 0
                   && numInserted - numRemoved < MAX_NUM_PK) {
                //TODO(zhihao): consider copy all PKHashes at once
                activeHashes[numInserted & ARRAY_MASK]
                   = *lookupRpc.resp.getOffset<KeyHash>(lookupRpc.offset);
                activeRpcIds[numInserted & ARRAY_MASK] = RPC_ID_NOT_ASSIGN;
                lookupRpc.offset += sizeof32(KeyHash);
                lookupRpc.numHashes--;
                numInserted++;
            }
        }
        /// Rule 3:
        /// If an returned lookupIndexKeys RPC has no unread PKHashes,
        /// consider issuing the next lookupIndexKeys RPC.
        if (lookupRpc.status == RESULT_READY && lookupRpc.numHashes == 0) {
            // Here we exploit the fact that 'nextKeyLength == 0'
            // indicates the index server contains the index key up to lastKey
            if (nextKeyLength == 0) {
                finishedLookup = true;
                lookupRpc.status = FREE;
            } else {
                lookupRpc.rpc.construct(ramcloud, tableId, indexId, nextKey,
                                        nextKeyLength, nextKeyHash,
                                        lastKey, lastKeyLength,
                                        maxNumHashes, &lookupRpc.resp);
                lookupRpc.status = SENT;
            }
        }
    }

    // If there are active hashes that have not yet been
    // assigned to an indexedRead RPC, try to assign the first unassigned
    // PKHashes to a indexedRead RPC. We assign PKHashes in their index order.
    while (numAssigned < numInserted) {
        // Rule 4:
        // If the PKHash pointed by numAssigned has been assigned (some server
        // crashes may cause numAssigned rolled back, and mark some previously
        // assigned PKHash RPC_ID_NOT_ASSIGN)
        if (activeRpcIds[numAssigned & ARRAY_MASK] != RPC_ID_NOT_ASSIGN) {
            numAssigned++;
            continue;
        }
        KeyHash pKHash = activeHashes[numAssigned & ARRAY_MASK];
        Transport::SessionRef session =
            ramcloud->objectFinder.lookup(tableId, pKHash);
        // Rule 5:
        // Try to assign the current key hash to an existing RPC
        // to the same server.
        uint8_t readactiveRpcId = RPC_ID_NOT_ASSIGN;
        for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
            if (session == readRpc[i].session
                    && readRpc[i].status == LOADING
                    // The below check is to make sure that
                    // pKHashes in every read RPC obey index order
                    // The check is needed during retries after server crashes.
                    && readRpc[i].maxPos < numAssigned
                    && readRpc[i].numHashes < MAX_PKHASHES_PER_RPC) {
                readactiveRpcId = i;
                break;
            }
        }
        // Rule 6:
        // If the hash can't go in an existing RPC, see if we can start a new
        // one for it.
        if (readactiveRpcId == RPC_ID_NOT_ASSIGN) {
            for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
                if (readRpc[i].status == FREE) {
                    readactiveRpcId = i;
                    readRpc[i].session = session;
                    readRpc[i].numHashes = 0;
                    readRpc[i].pKHashes.reset();
                    readRpc[i].status = LOADING;
                    readRpc[i].maxPos = 0;
                    break;
                }
            }
        }
        if (readactiveRpcId != RPC_ID_NOT_ASSIGN) {
            readRpc[readactiveRpcId].pKHashes.emplaceAppend<KeyHash>(pKHash);
            readRpc[readactiveRpcId].numHashes++;
            readRpc[readactiveRpcId].maxPos = numAssigned;
            activeRpcIds[numAssigned & ARRAY_MASK] = readactiveRpcId;
            numAssigned++;
        } else {
            // break means all readRpc are in use. We should stoping assigning
            // more PKHashes
            break;
        }
    }

    // Start to check rules for every indexedRead RPC
    for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
        // Rule 7:
        // If some indexedRead RPC get enough PKHASHES to send, launch that
        // indexedRead RPC
        if (readRpc[i].status == LOADING
                && readRpc[i].numHashes >= MAX_PKHASHES_PER_RPC) {
            launchReadRpc(i);
        }

        // Rule 8:
        // Handle the completion of an indexedReadRPC.
        if (readRpc[i].status == SENT
                && readRpc[i].rpc->isReady()) {
            uint32_t numProcessedPKHashes =
                readRpc[i].rpc->wait(&readRpc[i].numUnreadObjects);
            readRpc[i].offset = sizeof32(WireFormat::IndexedRead::Response);
            readRpc[i].status = RESULT_READY;
            // Update objectFinder if no pKHashes got processed in a readRpc.
            if (numProcessedPKHashes == 0)
                ramcloud->objectFinder.flush(tableId);
            if (numProcessedPKHashes < readRpc[i].numHashes) {
                for (size_t p = numRemoved; p < numInserted; p ++) {
                    // Some of the key hashes couldn't be looked up in this
                    // request (either because they aren't stored by that
                    // server, because the server crashed, or because there
                    // wasn't enough space in the response message). Mark
                    // the unprocessed hashes so they will get reassigned to
                    // new RPCs.
                    if (activeRpcIds[p] == i) {
                        if (numProcessedPKHashes > 0) {
                            numProcessedPKHashes--;
                        } else {
                            if (p < numAssigned)
                                numAssigned = p;
                            activeRpcIds[p] = RPC_ID_NOT_ASSIGN;
                        }
                    }
                }
            }
        }

        // Rule 9:
        // If all objects have been read by user, this RPC is free to be
        // reused.
        if (readRpc[i].status == RESULT_READY
            && readRpc[i].numUnreadObjects == 0) {
            readRpc[i].status = FREE;
        }
    }

    // Rule 10:
    // If the activeHashes is empty and there is no ongoing lookup RPC,
    // then we come to a conclusion that all available objects has been
    // returned.
    if (numRemoved == numInserted) {
        if (finishedLookup)
            return true;
        else
            return false;
    }

    // Rule 11:
    // If the next PKHash to be returned hasn't been assigned, we should
    // return false, which makes upper-level function invoke isReady() again,
    // and the next isReady() will try to assign the next PKHash to be
    // returned.
    if (activeRpcIds[numRemoved & ARRAY_MASK] == RPC_ID_NOT_ASSIGN)
        return false;
    uint8_t readRpcId = activeRpcIds[numRemoved & ARRAY_MASK];

    // Rule 12:
    // If the next PKHash to be returned is in a LOADING readRpc, launch that
    // readRpc asap.
    if (readRpc[readRpcId].status == LOADING) {
        launchReadRpc(readRpcId);
    }

    // Rule 13:
    // If the indexedRead RPC that contains the next pKHash to return has
    // returned, tell invoker that getNext() will not block if it is invoked.
    if (readRpc[readRpcId].status == RESULT_READY) {
        return true;
    }

    // Rule 14:
    // If the next PKHash to be returned is in a SENT readRpc, wait until
    // this readRpc returns.
    assert(readRpc[readRpcId].status == SENT);
    return false;
}

/**
 * Wait until either another object is available or all
 * objects have been returned.
 *
 * Client should invoke this function before calling getKey/getValue.
 *
 * \return
 *      True means the next index-order object is RESULT_READY. Client
 *      can use getKey/getValue to check object data.
 *      False means we have already reached the last object, and there
 *      are no more objects.
 */
bool
IndexLookup::getNext()
{
    // This is to check if the client has read some objects. If this is
    // not the first key to call getNext(), we should mark the previous
    // objects as read, and free corresponding buffers if necessary.
    if (curIdx != RPC_ID_NOT_ASSIGN)
        readRpc[curIdx].numUnreadObjects--;
    while (!isReady());
    // If there is no new object to return to client, just return false.
    if (numRemoved == numInserted && finishedLookup)
        return false;
    curIdx = activeRpcIds[numRemoved & ARRAY_MASK];
    // TODO(zhihao): possible information leakage. Consider use some other
    // class to access the detailed contents of the RPC result.
    uint64_t version =
        *readRpc[curIdx].resp.getOffset<uint64_t>(
            readRpc[curIdx].offset);
    readRpc[curIdx].offset += sizeof32(uint64_t);
    uint32_t length =
        *readRpc[curIdx].resp.getOffset<uint32_t>(
            readRpc[curIdx].offset);
    readRpc[curIdx].offset += sizeof32(uint32_t);
    curObj.construct(tableId, version, 0, readRpc[curIdx].resp,
                     readRpc[curIdx].offset, length);
    readRpc[curIdx].offset += length;

    // TODO(zhihao): (shouldn't move numRemoved on)
    numRemoved++;

    return true;
}

/**
 * Returns a pointer to one of the object's keys. The key is guaranteed to
 * be in contiguous memory (if it wasn't already contiguous, it will be
 * copied into a contiguous region).
 *
 * This method should only be invoked if getNext has been invoked and has
 * returned true.
 *
 * \param keyIndex
 *      Index position of this key
 * \param[out] keyLength
 *      Pointer to word that will be filled in with the length of the key
 *      indicated by keyIndex; if NULL then no length is returned.
 *
 * \return
 *      Pointer to the key which will be contiguous, or NULL if there is no
 *      key corresponding to keyIndex
 */
const void*
IndexLookup::getKey(KeyIndex keyIndex, KeyLength *keyLength)
{
    return curObj->getKey(keyIndex, keyLength);
}

/**
 * Get number of keys in current object.
 *
 * This method should only be invoked if getNext has been invoked and has
 * returned true.
 *
 * \return
 *      Number of keys in this object.
 */
KeyCount
IndexLookup::getKeyCount()
{
    return curObj->getKeyCount();
}

/**
 * Obtain the length of the key at position keyIndex.
 * If keyIndex >= numKeys, then endKeyOffset(keyIndex) will
 * return 0. This function should return 0 in such cases.
 *
 * This method should only be invoked if getNext has been invoked and has
 * returned true.
 *
 * \param keyIndex
 *      Numeric position of the index
 */
KeyLength
IndexLookup::getKeyLength(KeyIndex keyIndex)
{
    return curObj->getKeyLength(keyIndex);
}

/**
 * Obtain a pointer to a contiguous copy of this object's value.
 * This will not contain the number of keys, the key lengths and the keys.
 * This function is primarily used by unit tests
 *
 * This method should only be invoked if getNext has been invoked and has
 * returned true.
 *
 *
 * \param[out] valueLength
 *      The length of the object's value in bytes.
 *
 * \return
 *      NULL if the object is malformed,
 *      a pointer to a contiguous copy of the object's value otherwise
 */
const void*
IndexLookup::getValue(uint32_t *valueLength)
{
    return curObj->getValue(valueLength);
}

/**
 * Obtain the length of the object's value.
 *
 * This method should only be invoked if getNext has been invoked and has
 * returned true.
 */
uint32_t
IndexLookup::getValueLength()
{
    return curObj->getValueLength();
}

/**
 * Launch indexedRead RPC with index number i.
 *
 * \param i
 *      The index of RPC to be launched.
 */
void
IndexLookup::launchReadRpc(uint8_t i)
{
    assert(readRpc[i].status == LOADING);
    readRpc[i].rpc.construct(ramcloud, tableId,
                             readRpc[i].numHashes,
                             &readRpc[i].pKHashes,
                             indexId, firstKey, firstKeyLength,
                             lastKey, lastKeyLength,
                             &readRpc[i].resp);
    readRpc[i].status = SENT;
}

} // end RAMCloud
