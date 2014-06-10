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
IndexLookup::IndexLookup(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                         const void* in_firstKey, uint16_t firstKeyLength,
                         uint64_t firstAllowedKeyHash,
                         const void* in_lastKey, uint16_t lastKeyLength,
                         RpcFlags flags)
    : ramcloud(ramcloud)
    , lookupRpc()
    , tableId(tableId)
    , indexId(indexId)
    , firstKey(NULL)
    , firstKeyLength(firstKeyLength)
    , nextKey(NULL)
    , nextKeyLength(0)
    , lastKey(NULL)
    , lastKeyLength(lastKeyLength)
    , insertPos(0)
    , removePos(0)
    , assignPos(0)
    , curObj()
    , curIdx(RPC_IDX_NOTASSIGN)
    , finishedLookup(false)
    , flags(flags)
{
    for (size_t i = 0; i < NUM_READ_RPC; i++) {
        readRpc[i].status = FREE;
    }
    firstKey = malloc(firstKeyLength);
    memcpy(firstKey, in_firstKey, firstKeyLength);
    lastKey = malloc(lastKeyLength);
    memcpy(lastKey, in_lastKey, lastKeyLength);

    lookupRpc.resp.reset();
    lookupRpc.status = INPROCESS;
    lookupRpc.rpc.construct(ramcloud, tableId, indexId, firstKey, firstKeyLength,
                            firstAllowedKeyHash, lastKey, lastKeyLength,
                            &lookupRpc.resp);
}

IndexLookup::~IndexLookup()
{
    free(firstKey);
    free(nextKey);
    free(lastKey);
    curObj.destroy();
}


/**
 * Check if the next object is RESULT_READY. This function is implemented
 * in a DCFT module, each execution of isReady() tries to make small
 * progress, and getNext() invokes isReady() in a while loop, until
 * isReady() returns true.
 *
 * \return
 *      True means the next Object is RESULT_READY. Otherwise, return false.
 */
bool
IndexLookup::isReady()
{
    if (!finishedLookup) {
        /// If an INPROCESS lookupIndexKeys RPC returns, mark the status
        /// RESULT_READY
        if (lookupRpc.status == INPROCESS && lookupRpc.rpc->isReady()) {
            lookupRpc.rpc->wait(&lookupRpc.numHashes, &nextKeyLength,
                                &lookupRpc.nextKeyHash);
            printf("numHashes = %lu, nextKeyLength = %u, nextKeyHash = %llu\n", lookupRpc.numHashes, nextKeyLength, lookupRpc.nextKeyHash);
            lookupRpc.offset = sizeof32(WireFormat::LookupIndexKeys::Response);
            if (nextKey != NULL)
                free (nextKey);
            /// This malloc and memcpy can be expensive, but it is expected
            /// index lookup won't span accross many servers.
            /// TODO(zhihao): if we can make sure that lookupIndexKeys
            /// construction doesn't clear the response buffer before copying
            /// firstKey into request buffer, we can consider removing this
            /// malloc & memcpy.
            nextKey = malloc(nextKeyLength);
            memcpy(nextKey, lookupRpc.resp.getOffset<char>(lookupRpc.offset),
                   nextKeyLength);
            lookupRpc.offset += nextKeyLength;
            lookupRpc.status = RESULT_READY;
        }
        /// If an RESULT_READY lookupIndexKeys RPC still has some PKHashes
        /// unread, copy as much of them into PKHash buffer as possible.
        if (lookupRpc.status == RESULT_READY && lookupRpc.numHashes > 0) {
            while (lookupRpc.numHashes > 0
                   && insertPos - removePos < MAX_NUM_PK) {
                //TODO: consider copy all PKHashes at once
                pKBuffer[insertPos & BUFFER_MASK]
                   = *lookupRpc.resp.getOffset<KeyHash>(lookupRpc.offset);
                rpcIdx[insertPos & BUFFER_MASK] = RPC_IDX_NOTASSIGN;
                lookupRpc.offset += sizeof32(KeyHash);
                lookupRpc.numHashes --;
                insertPos ++;
            }
        }
        /// If an RESULT_READY lookupIndexKeys RPC has no unread PKHashes,
        /// consider issuing the next lookupIndexKeys RPC.
        if (lookupRpc.status == RESULT_READY && lookupRpc.numHashes == 0) {
            //TODO(zhihao): currently we use the fact that 'nextKeyHash == 0'
            // indicates the index server contains the index key up to lastKey
            if (lookupRpc.nextKeyHash == 0) {
                finishedLookup = true;
                lookupRpc.status = FREE;
                lookupRpc.rpc.destroy();
            }
            else {
            	lookupRpc.rpc.construct(ramcloud, tableId, indexId, nextKey,
                                        nextKeyLength, lookupRpc.nextKeyHash,
                                        lastKey, lastKeyLength,
                                        &lookupRpc.resp);
                lookupRpc.status = INPROCESS;
            }
        }
    }

    /// If there is any unassigned PKHashes, try to assign the first unassigned
    /// PKHashes to a indexedRead RPC. We assign PKHashes in their index order. 
    while (assignPos < insertPos) {
        if (rpcIdx[assignPos & BUFFER_MASK] != RPC_IDX_NOTASSIGN) {
            assignPos ++;
            continue;
        }
        KeyHash pKHash = pKBuffer[assignPos & BUFFER_MASK];
        //TODO: use session instead of locator
        string locator = 
            ramcloud->objectFinder.lookupTablet(tableId, pKHash)
                    ->serviceLocator;
        uint8_t readRpcIdx = RPC_IDX_NOTASSIGN;
        for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
            if (locator == readRpc[i].serviceLocator
                && readRpc[i].status == LOADING
                // doc
                && readRpc[i].maxIdx < assignPos) {
                readRpcIdx = i;
                break;
            }
        }
        if (readRpcIdx == RPC_IDX_NOTASSIGN)
            for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
                if (readRpc[i].status == FREE) {
                    readRpcIdx = i;
                    readRpc[i].serviceLocator = locator;
                    readRpc[i].numHashes = 0;
                    readRpc[i].pKHashes.reset();
                    readRpc[i].status = LOADING;
                    readRpc[i].maxIdx = 0;
                    break;
                }
            }
        if (readRpcIdx != RPC_IDX_NOTASSIGN) {
            new(&readRpc[readRpcIdx].pKHashes, APPEND) KeyHash(pKHash);
            readRpc[readRpcIdx].numHashes ++;
            readRpc[readRpcIdx].maxIdx = assignPos;
            rpcIdx[assignPos & BUFFER_MASK] = readRpcIdx;
            assignPos++;
        }
        else
            break;
    }

    /// Start to check rules for every indexedRead RPC
    for (uint8_t i = 0; i < NUM_READ_RPC; i++) {
        /// If some indexedRead RPC get enough PKHASHES to send, launch that
        /// indexedRead RPC
        if (readRpc[i].status == LOADING
            && readRpc[i].numHashes > MAX_PKHASHES_PERRPC) {
            launchReadRpc(i);            
        }
        
        /// If some indexedRead RPC returns, change its status to RESULT_READY.
        /// If some PKHashes don't returns, mark the corresponding slots in
        /// PKHahes buffer as unassigned.
        if (readRpc[i].status == INPROCESS
            && readRpc[i].rpc->isReady()) {
            uint32_t numProcessedPKHashes =
                readRpc[i].rpc->wait(&readRpc[i].numUnreadObjects);
            readRpc[i].offset = sizeof32(WireFormat::IndexedRead::Response);
            readRpc[i].status = RESULT_READY;
            if (numProcessedPKHashes < readRpc[i].numHashes) {
                for (size_t p = removePos; p < insertPos; p ++) {
                    if (rpcIdx[p] == i) {
                        if (numProcessedPKHashes > 0)
                        	numProcessedPKHashes --;
                        else {
                            if (p < assignPos) assignPos = p;
                            rpcIdx[p] = RPC_IDX_NOTASSIGN;
                        }
                    }
                }
            }
        }

        /// If all objects have been read by user, this RPC is free to be
        /// resued. Change its status to be FREE.
        if (readRpc[i].status == RESULT_READY
            && readRpc[i].numUnreadObjects == 0) {
            readRpc[i].status = FREE;
        }
    }
    
    if (removePos == insertPos) {
        if (finishedLookup)
            return true;
        else 
            return false;
    }
    if (rpcIdx[removePos & BUFFER_MASK] == RPC_IDX_NOTASSIGN)
        return false;
    uint8_t readRpcIdx = rpcIdx[removePos & BUFFER_MASK];
    if (readRpc[readRpcIdx].status == LOADING) {
        launchReadRpc(readRpcIdx);
    }
    
    /// If the indexedRead RPC that contains the next PKHash to return is
    /// RESULT_READY, we move current object one step further
    if (readRpc[readRpcIdx].status == RESULT_READY) {
        return true;
    }
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
    if (curIdx != RPC_IDX_NOTASSIGN)
        readRpc[curIdx].numUnreadObjects --;
    while (!isReady());
    if (removePos == insertPos && finishedLookup)
        return false;
    curIdx = rpcIdx[removePos & BUFFER_MASK];
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
    removePos ++;

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
    if (i > 0) printf("launch read to server: %u\n", i);
    readRpc[i].resp.reset();
    readRpc[i].rpc.construct(ramcloud, tableId,
                             readRpc[i].numHashes,
                             &readRpc[i].pKHashes,
                             indexId, firstKey, firstKeyLength,
                             lastKey, lastKeyLength,
                             &readRpc[i].resp);
    readRpc[i].status = INPROCESS;
}

} // end RAMCloud
