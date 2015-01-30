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
    , keyRange{indexId, firstKey, firstKeyLength, lastKey, lastKeyLength}
    , flags(flags)
    , maxNumHashes(maxNumHashes)
    , nextKey(NULL)
    , nextKeyLength(0)
    , nextKeyHash(0)
    , numInserted(0)
    , numRemoved(0)
    , numAssigned(0)
    , curObj()
    , curIdx(RPC_ID_NOT_ASSIGNED)
    , finishedLookup(false)
{
    for (uint8_t i = 0; i < NUM_READ_RPCS; i++) {
        readRpcs[i].status = FREE;
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
            uint16_t oldKeyLength = nextKeyLength; // should be 0 for first rpc
            lookupRpc.rpc->wait(&lookupRpc.numHashes, &nextKeyLength,
                                &nextKeyHash);
            lookupRpc.offset = sizeof32(WireFormat::LookupIndexKeys::Response);

            // Save the "next key" information from this response,
            // which will be used as the starting key for the next
            // lookupIndexKeys request.
            if (nextKeyLength > 0) {
                // malloc for first use or if previous key was smaller
                if (nextKeyLength > oldKeyLength) {
                    if (nextKey != NULL)
                        free(nextKey);
                    nextKey = malloc(nextKeyLength);
                }
                uint32_t off = lookupRpc.offset
                    + (lookupRpc.numHashes * (uint32_t) sizeof(KeyHash));
                lookupRpc.resp.copy(off, nextKeyLength, nextKey);
            }
            lookupRpc.status = RESULT_READY;
        }
        /// Rule 2:
        /// If an returned lookupIndexKeys RPC still has some activeHashes
        /// unread, copy as much of them into activeHashes as possible.
        if (lookupRpc.status == RESULT_READY && lookupRpc.numHashes > 0) {
            while (lookupRpc.numHashes > 0
                    && numInserted - numRemoved < MAX_NUM_PK) {
                //TODO(zhihao): consider copy all PKHashes at once
                // Greg: probably wont help much, as of fall 2014 most of the
                // time few hashes are moved (bottlenecked by obj reads)
                activeHashes[numInserted & ARRAY_MASK]
                    = *lookupRpc.resp.getOffset<KeyHash>(lookupRpc.offset);
                activeRpcIds[numInserted & ARRAY_MASK] = RPC_ID_NOT_ASSIGNED;
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
                lookupRpc.rpc.construct(ramcloud, tableId, keyRange.indexId,
                                        nextKey, nextKeyLength, nextKeyHash,
                                        keyRange.lastKey,
                                        keyRange.lastKeyLength, maxNumHashes,
                                        &lookupRpc.resp);
                lookupRpc.status = SENT;
            }
        }
    }

    // If there are active hashes that have not yet been
    // assigned to an readHashes RPC, try to assign the first unassigned
    // PKHashes to a readHashes RPC. We assign PKHashes in their index order.
    while (numAssigned < numInserted) {
        // Rule 4:
        // If the PKHash pointed by numAssigned has been assigned (some server
        // crashes may cause numAssigned rolled back, and mark some previously
        // assigned PKHash RPC_ID_NOT_ASSIGNED)
        if (activeRpcIds[numAssigned & ARRAY_MASK] != RPC_ID_NOT_ASSIGNED) {
            numAssigned++;
            continue;
        }
        KeyHash pKHash = activeHashes[numAssigned & ARRAY_MASK];
        Transport::SessionRef session =
            ramcloud->objectFinder.lookup(tableId, pKHash);
        // Rule 5:
        // Try to assign the current key hash to an existing RPC
        // to the same server.
        uint8_t readactiveRpcId = RPC_ID_NOT_ASSIGNED;
        for (uint8_t i = 0; i < NUM_READ_RPCS; i++) {
            if (session == readRpcs[i].session
                    && readRpcs[i].status == LOADING
                    // The below check is to make sure that
                    // pKHashes in every read RPC obey index order
                    // The check is needed during retries after server crashes.
                    && readRpcs[i].maxPos < numAssigned
                    && readRpcs[i].numHashes < MAX_PKHASHES_PER_RPC) {
                readactiveRpcId = i;
                break;
            }
        }
        // Rule 6:
        // If the hash can't go in an existing RPC, see if we can start a new
        // one for it.
        if (readactiveRpcId == RPC_ID_NOT_ASSIGNED) {
            for (uint8_t i = 0; i < NUM_READ_RPCS; i++) {
                if (readRpcs[i].status == FREE) {
                    readactiveRpcId = i;
                    readRpcs[i].session = session;
                    readRpcs[i].numHashes = 0;
                    readRpcs[i].pKHashes.reset();
                    readRpcs[i].status = LOADING;
                    readRpcs[i].maxPos = 0;
                    break;
                }
            }
        }
        if (readactiveRpcId != RPC_ID_NOT_ASSIGNED) {
            readRpcs[readactiveRpcId].pKHashes.emplaceAppend<KeyHash>(pKHash);
            readRpcs[readactiveRpcId].numHashes++;
            readRpcs[readactiveRpcId].maxPos = numAssigned;
            activeRpcIds[numAssigned & ARRAY_MASK] = readactiveRpcId;
            numAssigned++;
        } else {
            // break means all readRpcs are in use. We should stoping assigning
            // more PKHashes
            break;
        }
    }

    // Start to check rules for every readHashes RPC
    for (uint8_t i = 0; i < NUM_READ_RPCS; i++) {
        // Rule 7:
        // If some readHashes RPC has enough PKHASHES to send
        // or all PKHASHES have been assigned, launch that RPC
        if (readRpcs[i].status == LOADING
                && (readRpcs[i].numHashes >= MAX_PKHASHES_PER_RPC
                    || finishedLookup)) {
            launchReadRpc(i);
        }

        // Rule 8:
        // Handle the completion of an readHashesRPC.
        if (readRpcs[i].status == SENT
                && readRpcs[i].rpc->isReady()) {
            uint32_t numProcessedPKHashes =
                readRpcs[i].rpc->wait(&readRpcs[i].numUnreadObjects);
            readRpcs[i].offset = sizeof32(WireFormat::ReadHashes::Response);
            readRpcs[i].status = RESULT_READY;
            // Update objectFinder if no pKHashes got processed in a readRpc.
            if (numProcessedPKHashes == 0)
                ramcloud->objectFinder.flush(tableId);
            if (numProcessedPKHashes < readRpcs[i].numHashes) {
                for (size_t p = numRemoved; p < numInserted; p ++) {
                    // Some of the key hashes couldn't be looked up in this
                    // request (either because they aren't stored by that
                    // server, because the server crashed, or because there
                    // wasn't enough space in the response message). Mark
                    // the unprocessed hashes so they will get reassigned to
                    // new RPCs.
                    if (activeRpcIds[p] == i) {
                        // got first numProcessedPKHashes in this Rpc
                        if (numProcessedPKHashes > 0) {
                            numProcessedPKHashes--;
                        } else { // the rest need to be re-assigned
                            activeRpcIds[p] = RPC_ID_NOT_ASSIGNED;
                            if (p < numAssigned)
                                numAssigned = p;
                        }
                    }
                }
            }
        }

        // Rule 9:
        // If all objects have been read by user, this RPC is free to be
        // reused.
        if (readRpcs[i].status == RESULT_READY
            && readRpcs[i].numUnreadObjects == 0) {
            readRpcs[i].status = FREE;
        }
    }

    // Rule 10:
    // If the activeHashes is empty and there is no ongoing lookup RPC,
    // then we come to a conclusion that all available objects has been
    // returned.
    if (numRemoved == numInserted) {
        return finishedLookup;
    }

    // Rule 11:
    // If the next PKHash to be returned hasn't been assigned, we should
    // return false, which makes upper-level function invoke isReady() again,
    // and the next isReady() will try to assign the next PKHash to be
    // returned.
    if (activeRpcIds[numRemoved & ARRAY_MASK] == RPC_ID_NOT_ASSIGNED)
        return false;
    uint8_t readRpcId = activeRpcIds[numRemoved & ARRAY_MASK];

    // Rule 12:
    // If the next PKHash to be returned is in a LOADING readRpc, launch that
    // readRpc asap.
    if (readRpcs[readRpcId].status == LOADING) {
        launchReadRpc(readRpcId);
    }

    // Rule 13:
    // If the readHashes RPC that contains the next pKHash to return has
    // returned, tell invoker that getNext() will not block if it is invoked.
    if (readRpcs[readRpcId].status == RESULT_READY) {
        return true;
    }

    // Rule 14:
    // If the next PKHash to be returned is in a SENT readRpc, wait until
    // this readRpc returns.
    assert(readRpcs[readRpcId].status == SENT);
    return false;
}

/**
 * Wait until either another object in the proper index range
 * is available or all objects have been returned.
 *
 * Client should invoke this function before calling currentObject
 *
 * \return
 *      True means the next index-order object is RESULT_READY and in the
 *      index range. Client can use currentObject to check object data.
 *      False means we have already reached the last object, and there
 *      are no more objects.
 */
bool
IndexLookup::getNext()
{
    do {
        // This is to check if the client has read some objects. If this is
        // not the first key to call getNext(), we should mark the previous
        // objects as read, and free corresponding buffers if necessary.
        if (curIdx != RPC_ID_NOT_ASSIGNED)
            readRpcs[curIdx].numUnreadObjects--;

        while (!isReady()) {
            ramcloud->clientContext->dispatch->poll();
        };

        // If there is no new object to return to client, just return false.
        if (numRemoved == numInserted && finishedLookup)
            return false;
        curIdx = activeRpcIds[numRemoved & ARRAY_MASK];
        Buffer& curBuff = readRpcs[curIdx].resp;
        uint32_t& curOffset = readRpcs[curIdx].offset;
        // TODO(zhihao): possible information leakage. Consider use some other
        // class to access the detailed contents of the RPC result.
        uint64_t version = *curBuff.getOffset<uint64_t>(curOffset);
        curOffset += sizeof32(uint64_t);

        uint32_t length = *curBuff.getOffset<uint32_t>(curOffset);
        curOffset += sizeof32(uint32_t);

        curObj.construct(tableId, version, 0, curBuff, curOffset, length);
        curOffset += length;

        // TODO(zhihao): (shouldn't move numRemoved on)
        numRemoved++;
    } while (!IndexKey::isKeyInRange(curObj.get(), &keyRange));

    return true;
}

/**
 * Returns a pointer to the current object.
 *
 * This method should only be invoked if getNext has been invoked and has
 * returned true.
 */
Object*
IndexLookup::currentObject()
{
    return curObj.get();
}

/**
 * Launch the readHashes RPC with index number i.
 *
 * \param i
 *      The index of RPC to be launched.
 */
void
IndexLookup::launchReadRpc(uint8_t i)
{
    assert(readRpcs[i].status == LOADING);
    readRpcs[i].rpc.construct(ramcloud, tableId,
                             readRpcs[i].numHashes,
                             &readRpcs[i].pKHashes,
                             &readRpcs[i].resp);
    readRpcs[i].status = SENT;
}

} // end RAMCloud
