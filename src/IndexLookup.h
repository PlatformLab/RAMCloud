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

#ifndef RAMCLOUD_INDEXLOOKUP_H
#define RAMCLOUD_INDEXLOOKUP_H

#include "RamCloud.h"

namespace RAMCloud {

/*
 * This class implements the client side framework for lookup indexes.
 * It manages a single LookupIndexKeys RPC and multiple indexedRead
 * RPCs. The number of concurrent indexedRead RPCs is defined in macros
 * above. 
 *
 * To use IndexLookup, client create a object of this class by providing
 * all necessary information. After construction of IndexLookup, client
 * can call getNext() function to move to next available object. If
 * getNext() returns false, it means we reached the last object. Client
 * can use getKey/getKeyLength/getValue/getValueLength to get object data
 * of current object.
 */

class IndexLookup {
  public:
    enum RpcFlags : uint32_t {
        EMPTY = 0,
        RETURN_IN_ORDER = 1
    };
    IndexLookup(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                const void* firstKey, uint16_t firstKeyLength,
                uint64_t firstAllowedKeyHash,
                const void* lastKey, uint16_t lastKeyLength,
                RpcFlags flags = EMPTY);
    ~IndexLookup();
    bool getNext();
    const void* getKey(KeyIndex keyIndex = 0, KeyLength *keyLength = NULL);
    KeyLength getKeyLength(KeyIndex keyIndex = 0);
    const void* getValue(uint32_t *valueLength = NULL);
    uint32_t getValueLength();

  private:
    enum Rpc_Config {
        NUM_READ_RPC = 10,
        MAX_PKHASHES_PERRPC = 256,
        RPC_IDX_NOTASSIGN = NUM_READ_RPC,
    };
    enum Buffer_Config {
        /// logarithm buffer size. We want to make the size of buffer a power
        /// of 2, since we want reuse buffer in a circular way. By enforcing
        /// the size of buffer a power of 2, we can use bit operations to
        /// find buffer pos.
        LG_BUFFER_SIZE = 10,
        BUFFER_MASK = ((1 << LG_BUFFER_SIZE) - 1),
        MAX_NUM_PK = (1 << LG_BUFFER_SIZE)
    };
    enum RpcStatus {
        FREE,
        LOADING, /// Loading status only provide for indexedRead RPC
        INPROCESS,
        RESULT_READY
    };
    /// struct for indexedRead RPC.
    struct ReadRpc {
        Tub<IndexedReadRpc> rpc;
        RpcStatus status;
        Buffer pKHashes;
        Buffer resp;
        uint32_t numHashes;
        uint32_t numUnreadObjects;
        uint32_t offset;
        size_t maxIdx;
        string serviceLocator;
        ReadRpc()
        : rpc()
        , status(FREE)
        , pKHashes()
        , resp()
        , numHashes()
        , numUnreadObjects()
        , offset()
        , maxIdx()
        , serviceLocator()
        {}
    };
    /// struct for lookupIndexKeys RPC.
    struct LookupRpc {
        Tub<LookupIndexKeysRpc> rpc;
        RpcStatus status;
        Buffer resp;
        uint32_t numHashes;
        uint64_t nextKeyHash;
        uint32_t offset;
        LookupRpc()
        : rpc()
        , status(FREE)
        , resp()
        , numHashes()
        , nextKeyHash()
        , offset()
        {}
    };
    bool isReady();
    void launchReadRpc(uint8_t i);

    /// Overall client state information.
    RamCloud* ramcloud;

    /// struct for lookupIndexKeys RPC, we only keep a single
    /// lookupIndexKeys RPC at a time, since each lookupIndexKeys
    /// rpc needs the return value of this previous call.
    LookupRpc lookupRpc;

    /// struct for indexedRead RPC, we can have up to NUM_READ_RPC
    /// indexedRead RPC at a time. Each single RPC handles communication
    /// before client and a single data server
    ReadRpc readRpc[NUM_READ_RPC];

    /// Table Id we are handling in this IndexLookup class.
    uint64_t tableId;

    /// Index Id we are handling in this IndexLookup class.
    uint8_t indexId;

    /// Key blob marking the start of the indexed key range for this
    /// query
    void *firstKey;

    /// Length of first key string
    uint16_t firstKeyLength;

    /// Key blob marking the start of the indexed key range for next
    /// lookupIndexKeys
    void *nextKey;

    /// Length of next key string
    uint16_t nextKeyLength;

    /// Key blob marking the end of the indexed key range for this
    /// query
    void *lastKey;

    /// Length of last key string
    uint16_t lastKeyLength;

    /// Buffer stores PKHashes that we should return to users
    /// in order. The buffer is reused as a cycle.
    KeyHash pKBuffer[MAX_NUM_PK];

    /// Buffer stores indexedRead RPC indexes on which this PKHashes
    /// is sent to data server to acquire object data. Stores special
    /// value RPC_IDX_NOTASSIGN if it is not assigned.
    uint8_t rpcIdx[MAX_NUM_PK];

    /// +----------------------------------------------------------+
    /// |   |   PKHashes assigned     |  PKHashes to  |            |
    /// |   |   to indexRead PRC      |  be assign    |            |
    /// +----------------------------------------------------------+
    ///      ^                         ^               ^
    ///      |                         |               |
    ///   removePos                 assignPos       insertPos
    ///
    /// Note that removePos and assignPos point to occupied entries
    /// while insertPos points to empty entry

    /// The position if we want to insert the next PKHashes into the
    /// buffer
    size_t insertPos;

    /// The position of the next PKHashes to be remove from the buffer
    /// if user calls get_next()
    size_t removePos;

    /// The postion of the
    size_t assignPos;

    /// Current object. This is the object returns to user if user
    /// calls getKey/getKeyLength/getValue/getValueLength
    Tub<Object> curObj;

    /// Current indexedRead RPC index that user is currently reading.
    /// Need to keep track of this because curObj doesn't copy the object
    /// from response buffer. Need to make sure the life time of response
    /// buffer until user switch to next object.
    uint8_t curIdx;

    /// Boolean indicating if all lookupIndexKeys are finished.
    bool finishedLookup;

    /// flags
    RpcFlags flags;

    DISALLOW_COPY_AND_ASSIGN(IndexLookup);
};

} // end RAMCloud
#endif // RAMCLOUD_INDEXLOOKUP_H
