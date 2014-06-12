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
 * \param ramcloud
 *      The RAMCloud object that governs this class.
 * \param tableId
 *      Id of the table in which lookup is to be done.
 * \param indexId
 *      Id of the index for which keys have to be compared.
 * \param firstKey
 *      Starting key for the key range in which keys are to be matched.
 *      The key range includes the firstKey.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param firstKeyLength
 *      Length in bytes of the firstKey.
 * \param lastKey
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param lastKeyLength
 *      Length in byes of the lastKey.
 * \param flags
 *      User specific flags.
 */
IndexLookup::IndexLookup(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                         const void* firstKey, uint16_t firstKeyLength,
                         const void* lastKey, uint16_t lastKeyLength,
                         Flags flags)
    : ramcloud(ramcloud)
    , lookupRpc()
    , tableId(tableId)
    , indexId(indexId)
    , firstKey(firstKey)
    , firstKeyLength(firstKeyLength)
    , nextKey(NULL)
    , nextKeyLength(0)
    , lastKey(lastKey)
    , lastKeyLength(lastKeyLength)
    , flags(flags)
    , insertPos(0)
    , removePos(0)
    , assignPos(0)
    , curObj()
    , curIdx(RPC_ID_NOTASSIGN)
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
                            &lookupRpc.resp);
}

IndexLookup::~IndexLookup()
{
    free(nextKey);
    curObj.destroy();
}


/**
 * Check if the next object is RESULT_READY. This function is implemented
 * in a DCFT module, each execution of isReady() tries to make small
 * progress, and getNext() invokes isReady() in a while loop, until
 * isReady() returns true.
 *
 * isReady() is implemented in a rule-based approach. We check different rules
 * by following a particular order, and perform certain action if some rule
 * is satisfied.
 *
 * \return
 *      True means the next Object is available. Otherwise, return false.
 */
bool
IndexLookup::isReady()
{
    if (!finishedLookup) {
    	/// Rule 1:
        /// If an SENT lookupIndexKeys RPC returns, mark the status
        /// RESULT_READY
        if (lookupRpc.status == SENT && lookupRpc.rpc->isReady()) {
            lookupRpc.rpc->wait(&lookupRpc.numHashes, &nextKeyLength,
                                &lookupRpc.nextKeyHash);
            lookupRpc.offset = sizeof32(WireFormat::LookupIndexKeys::Response);
            if (nextKey != NULL)
                free(nextKey);
            /// This malloc and memcpy can be expensive, but it is expected
            /// index lookup won't span accross many servers.
            /// TODO(zhihao): if we can make sure that lookupIndexKeys
            /// construction doesn't clear the response buffer before copying
            /// firstKey into request buffer, we can consider removing this
            /// malloc & memcpy.
            nextKey = malloc(nextKeyLength);
            uint32_t off = lookupRpc.offset
                           + lookupRpc.numHashes * (uint32_t) sizeof(KeyHash);
            memcpy(nextKey, lookupRpc.resp.getOffset<char>(off),
                   nextKeyLength);
            lookupRpc.status = RESULT_READY;
        }
        /// Rule 2:
        /// If an RESULT_READY lookupIndexKeys RPC still has some PKHashes
        /// unread, copy as much of them into PKHash buffer as possible.
        if (lookupRpc.status == RESULT_READY && lookupRpc.numHashes > 0) {
            while (lookupRpc.numHashes > 0
                   && insertPos - removePos < MAX_NUM_PK) {
                //TODO(zhihao): consider copy all PKHashes at once
                activeHashes[insertPos & ARRAY_MASK]
                   = *lookupRpc.resp.getOffset<KeyHash>(lookupRpc.offset);
                activeRpcId[insertPos & ARRAY_MASK] = RPC_ID_NOTASSIGN;
                lookupRpc.offset += sizeof32(KeyHash);
                lookupRpc.numHashes--;
                insertPos++;
            }
        }
        /// Rule 3:
        /// If an RESULT_READY lookupIndexKeys RPC has no unread PKHashes,
        /// consider issuing the next lookupIndexKeys RPC.
        if (lookupRpc.status == RESULT_READY && lookupRpc.numHashes == 0) {
            //Here we exploit the fact that 'nextKeyLength == 0'
            // indicates the index server contains the index key up to lastKey
            if (nextKeyLength == 0) {
                finishedLookup = true;
                lookupRpc.status = FREE;
                lookupRpc.rpc.destroy();
            } else {
                lookupRpc.rpc.construct(ramcloud, tableId, indexId, nextKey,
                                        nextKeyLength, lookupRpc.nextKeyHash,
                                        lastKey, lastKeyLength,
                                        &lookupRpc.resp);
                lookupRpc.status = SENT;
            }
        }
    }

    /// If there is any unassigned PKHashes, try to assign the first unassigned
    /// PKHashes to a indexedRead RPC. We assign PKHashes in their index order.
    while (assignPos < insertPos) {
    	/// Rule 4:
    	/// If the PKHash pointed by assignPos has been assigned (some server
    	/// crashes may cause assignPos rolled back, and mark some previously
    	/// assigned PKHash RPC_ID_NOTASSIGN)
        if (activeRpcId[assignPos & ARRAY_MASK] != RPC_ID_NOTASSIGN) {
            assignPos++;
            continue;
        }
        KeyHash pKHash = activeHashes[assignPos & ARRAY_MASK];
        Transport::SessionRef session =
            ramcloud->objectFinder.lookup(tableId, pKHash);
        /// Rule 5:
        /// If there is a LOADING readRpc using the same session as PKHash
        /// pointed by assignPos, and the last PKHash in that readRPC is
        /// smaller than current assigning PKHash, then we put assigning
        /// PKHash into that readRPC
        uint8_t readactiveRpcId = RPC_ID_NOTASSIGN;
        for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
            if (session == readRpc[i].session
                && readRpc[i].status == LOADING
                /// The below check is to guarantee that
                /// pKHashes in every read RPC obey index order
                && readRpc[i].maxPos < assignPos
                && readRpc[i].numHashes < MAX_PKHASHES_PERRPC) {
                readactiveRpcId = i;
                break;
            }
        }
        /// Rule 6:
        /// If Rule 5 cannot be satisfied, and we can find a FREE readRpc,
        /// then we using this FREE readRpc to transmit current PKHash.
        if (readactiveRpcId == RPC_ID_NOTASSIGN) {
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
        if (readactiveRpcId != RPC_ID_NOTASSIGN) {
            new(&readRpc[readactiveRpcId].pKHashes, APPEND) KeyHash(pKHash);
            readRpc[readactiveRpcId].numHashes++;
            readRpc[readactiveRpcId].maxPos = assignPos;
            activeRpcId[assignPos & ARRAY_MASK] = readactiveRpcId;
            assignPos++;
        } else {
        	/// break means all readRpc are in use. We should stoping assigning
        	/// more PKHashes
            break;
        }
    }

    /// Start to check rules for every indexedRead RPC
    for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
        /// Rule 7:
    	/// If some indexedRead RPC get enough PKHASHES to send, launch that
        /// indexedRead RPC
        if (readRpc[i].status == LOADING
            && readRpc[i].numHashes >= MAX_PKHASHES_PERRPC) {
            launchReadRpc(i);
        }

        /// Rule 8:
        /// If some indexedRead RPC returns, change its status to RESULT_READY.
        /// If some PKHashes don't returns, mark the corresponding slots in
        /// PKHahes buffer as unassigned.
        if (readRpc[i].status == SENT
            && readRpc[i].rpc->isReady()) {
            uint32_t numProcessedPKHashes =
                readRpc[i].rpc->wait(&readRpc[i].numUnreadObjects);
            readRpc[i].offset = sizeof32(WireFormat::IndexedRead::Response);
            readRpc[i].status = RESULT_READY;
            if (numProcessedPKHashes < readRpc[i].numHashes) {
                for (size_t p = removePos; p < insertPos; p ++) {
                    if (activeRpcId[p] == i) {
                        if (numProcessedPKHashes > 0) {
                            numProcessedPKHashes--;
                        } else {
                            if (p < assignPos)
                            	assignPos = p;
                            activeRpcId[p] = RPC_ID_NOTASSIGN;
                        }
                    }
                }
            }
        }

        /// Rule 9:
        /// If all objects have been read by user, this RPC is free to be
        /// resued. Change its status to be FREE.
        if (readRpc[i].status == RESULT_READY
            && readRpc[i].numUnreadObjects == 0) {
            readRpc[i].status = FREE;
        }
    }

    /// Rule 10:
    /// If the activeHashes is empty and there is no ongoing lookup RPC,
    /// then we come to a conclusion that all available objects has been
    /// returned.
    if (removePos == insertPos) {
        if (finishedLookup)
            return true;
        else
            return false;
    }

    /// Rule 11:
    /// If the next PKHash to be returned hasn't been assigned, we should
    /// return false, which makes upper-level function invoke isReady() again,
    /// and the next isReady() will try to assign the next PKHash to be
    /// returned.
    if (activeRpcId[removePos & ARRAY_MASK] == RPC_ID_NOTASSIGN)
        return false;
    uint8_t readRpcId = activeRpcId[removePos & ARRAY_MASK];

    /// Rule 12:
    /// If the next PKHash to be returned is in a LOADING readRpc, launch that
    /// readRpc asap.
    if (readRpc[readRpcId].status == LOADING) {
        launchReadRpc(readRpcId);
    }

    /// Rule 13:
    /// If the indexedRead RPC that contains the next PKHash to return is
    /// RESULT_READY, we move current object one step further
    if (readRpc[readRpcId].status == RESULT_READY) {
        return true;
    }

    /// Rule 14:
    /// If the next PKHash to be returned is in a SENT readRpc, wait until
    /// this readRpc returns.
    assert(readRpc[readRpcId].status == SENT);
    return false;
}

/**
 * Busy wait until the next index-order object is RESULT_READY.
 * Client should invoke this function before calling getKey/getValue.
 *
 * \return
 *      True means the next index-order object is RESULT_READY. Client
 *      can use getKey/getValue to check object data.
 *      False means we have already reached the last object, and there
 *      is NO next object RESULT_READY.
 */
bool
IndexLookup::getNext()
{
    /// This is to check if the client has read some objects. If this is
	/// not the first key to call getNext(), we should mark the previous
	/// objects as read, and free corresponding buffers if necessary.
    if (curIdx != RPC_ID_NOTASSIGN)
        readRpc[curIdx].numUnreadObjects--;
    while (!isReady());
    /// If there is no new object to return to client, just return false.
    if (removePos == insertPos && finishedLookup)
        return false;
    curIdx = activeRpcId[removePos & ARRAY_MASK];
    uint64_t version =
        *readRpc[curIdx].resp.getOffset<uint64_t>(
            readRpc[curIdx].offset);
    readRpc[curIdx].offset += sizeof32(uint64_t);
    uint32_t length =
        *readRpc[curIdx].resp.getOffset<uint32_t>(
            readRpc[curIdx].offset);
    readRpc[curIdx].offset += sizeof32(uint32_t);
    curObj.destroy();
    curObj.construct(tableId, version, 0, readRpc[curIdx].resp,
                     readRpc[curIdx].offset, length);
    readRpc[curIdx].offset += length;

    /// Make curValueIter points to the beginning of value bits of
    /// current object

    // TODO (shouldn't move removePos on)
    removePos++;

    return true;
}

/**
 * Returns a pointer to one of the object's keys. The key is guaranteed to
 * be in contiguous memory (if it wasn't already contiguous, it will be
 * copied into a contiguous region).
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
 * Obtain the length of the object's value
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
    readRpc[i].resp.reset();
    readRpc[i].rpc.construct(ramcloud, tableId,
                             readRpc[i].numHashes,
                             &readRpc[i].pKHashes,
                             indexId, firstKey, firstKeyLength,
                             lastKey, lastKeyLength,
                             &readRpc[i].resp);
    readRpc[i].status = SENT;
}

} // end RAMCloud
