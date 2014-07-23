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
 * This class implements the client side framework for range queries on
 * secondary indexes.
 *
 * To use IndexLookup, a client creates a object of this class.
 * After construction of IndexLookup, client can call getNext() function
 * to move to next available object. If getNext() returns false, it means
 * we reached the last object. In the case getNext() returns true, client
 * can use getKey/getKeyLength/getValue/getValueLength to get object
 * information about the new object.
 */

class IndexLookup {
  PUBLIC:
    enum Flags : uint32_t {
        /// No flag is set by default.
        DEFAULT = 0,
        /// If EXCLUDE_FIRST_KEY is set, IndexLookup will not return objects
        /// that matches firstKey
        EXCLUDE_FIRST_KEY = 1,
        /// If EXCLUDE_FIRST_KEY is set, IndexLookup will not return objects
        /// that matches lastKey
        EXCLUDE_LAST_KEY = 2
    };
    IndexLookup(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                const void* firstKey, uint16_t firstKeyLength,
                const void* lastKey, uint16_t lastKeyLength,
                uint32_t maxNumHashes, Flags flags = DEFAULT);
    ~IndexLookup();
    bool isReady();
    bool getNext();
    const void* getKey(KeyIndex keyIndex = 0, KeyLength *keyLength = NULL);
    KeyCount getKeyCount();
    KeyLength getKeyLength(KeyIndex keyIndex = 0);
    const void* getValue(uint32_t *valueLength = NULL);
    uint32_t getValueLength();

  PRIVATE:
    /// The lookupIndexKeys RPC and each of the indexedRead RPCs
    /// is in one of the following states at all times.
    enum RpcStatus {
        /// FREE means this RPC is currently available to use/reuse
        FREE,
        /// LOADING means we are adding PKHashes into this indexedRead RPC.
        /// LOADING status is only designed for indexedRead RPC
        LOADING,
        /// SENT means this RPC has been sent to server, and is
        /// currently waiting for response
        SENT,
        /// RESULT_READY means the response of the RPC has returned,
        /// and is currently available to read
        RESULT_READY
    };

    /// struct for indexedRead RPC.
    struct ReadRpc {
        /// The tub that contains low-level indexedRead RPC
        Tub<IndexedReadRpc> rpc;

        /// The status of rpc.
        RpcStatus status;

        /// Used to collect the primary key hashes that will be
        /// requested when this RPC is issued.
        Buffer pKHashes;

        /// Number of hashes in pKHashes buffer
        uint32_t numHashes;

        /// Holds the response for the RPC, which contains the objects
        /// corresponding to the hashes in pKHashes.
        Buffer resp;

        /// Number of objects in resp buffer, that hasn't been read by client
        uint32_t numUnreadObjects;

        /// Offset of the first object in this buffer that the client
        /// has not yet processed.
        uint32_t offset;

        /// If we have to reassign a pKHash whose index is smaller than maxPos,
        /// that pKHash cannot go with this readRpc, it must go into a new
        /// readRPC. This is because the pKHashes in any given readRPC has to
        /// be returned to the client in index order.
        size_t maxPos;

        /// Session that will be used to transmit RPC
        Transport::SessionRef session;

        ReadRpc()
            : rpc(), status(FREE), pKHashes(), numHashes(), resp()
            , numUnreadObjects(), offset(), maxPos(), session()
        {}
    };

    /// struct for lookupIndexKeys RPC.
    struct LookupRpc {
        /// The tub that contains low-level LookupIndexKeys RPC
        Tub<LookupIndexKeysRpc> rpc;

        /// The status of rpc.
        RpcStatus status;

        /// Holds the response when rpc is issued.
        Buffer resp;

        /// The number that have not yet been moved to activeHashes.
        uint32_t numHashes;

        /// Offset of the first pKHash in this buffer that has not been copied
        /// to activeHashes
        uint32_t offset;

        LookupRpc()
            : rpc(), status(FREE), resp(), numHashes(), offset()
        {}
    };
    void launchReadRpc(uint8_t i);

    /// Overall client state information.
    RamCloud* ramcloud;

    /// struct for lookupIndexKeys RPC, we only keep a single
    /// lookupIndexKeys RPC at a time, since each lookupIndexKeys
    /// rpc needs the return value of this previous call.
    LookupRpc lookupRpc;

    /// Max number of current indexedRead RPCs
    static const uint8_t NUM_READ_RPC = 10;

    /// Max number of PKHashes that will be sent in a single indexedRead RPC
    static const uint32_t MAX_PKHASHES_PER_RPC = 256;

    /// A special value indicating the corresponding PKHash has not been
    /// assigned to any ongoing indexedRead RPC
    static const uint8_t RPC_ID_NOT_ASSIGN = NUM_READ_RPC;

    /// struct for indexedRead RPC, we can have up to NUM_READ_RPC
    /// indexedRead RPC at a time. Each single RPC handles communication
    /// between client and a single data server
    ReadRpc readRpc[NUM_READ_RPC];

    // The following variables are copy of constructor arguments

    /// Table Id we are handling in this IndexLookup class.
    uint64_t tableId;

    /// Index Id we are handling in this IndexLookup class.
    uint8_t indexId;

    /// Key blob marking the start of the indexed key range for this
    /// query
    const void *firstKey;

    /// Length of first key string
    uint16_t firstKeyLength;

    /// Key blob marking the end of the indexed key range for this
    /// query
    const void *lastKey;

    /// Length of last key string
    uint16_t lastKeyLength;

    /// Flags supplied by client to the constructor.
    Flags flags;

    // The next four variables are used to handle the case where we have
    // to issue multiple lookupIndexKeys RPC, since indexes span among
    // multiple servers.

    /// Maximum number of hashes that the server is allowed to return
    /// in a single rpc.
    uint32_t maxNumHashes;

    /// Key blob marking the start of the indexed key range for next
    /// lookupIndexKeys
    void *nextKey;

    /// Length of next key string
    uint16_t nextKeyLength;

    /// Results starting at nextKey + nextKeyHash couldn't be returned.
    /// We need to send another request according to this.
    uint64_t nextKeyHash;

    // The following declarations are used to manages a collection
    // of "active hashes". This is a circular buffer of primary key
    // hashes that have been returned from index server, and are
    // in the process of being fetched from tablet servers and/or
    // returned to the client. Once the client has processed an
    // active hash, its space in the active hashes array can be
    // reused. The active hashes information ensures that objects
    // are returned to the client in index order, even though they
    // may be fetched from different tablet servers in different
    // orders.

    /// Used to convert variables such as numInserted into indexes into
    /// activeHashes and activeRpcIds in order to implement circular usage.
    static const size_t LOG_ARRAY_SIZE = 10;

    /// Array mask for activeHashes. When need to find the corresponding
    /// position in activeHashes, ARRAY_MASK is used to perform bit operation.
    static const size_t ARRAY_MASK = ((1 << LOG_ARRAY_SIZE) - 1);

    /// Max number of PKHashes that activeHashes can hold at once.
    static const size_t MAX_NUM_PK = (1 << LOG_ARRAY_SIZE);

    /// Holds the active primary key hashes. Objects must be
    /// returned to the client in the same order as the entries
    /// in this array. The array is reused circularly.
    KeyHash activeHashes[MAX_NUM_PK];

    /// Each entry stores the index into readRpc of the RPC that will
    /// be used to fetch the corresponding pKHash from activeHashes.
    /// The value RPC_ID_NOT_ASSIGNED means that the corresponding
    /// cache is not yet been assigned to an outgoing indexedRead
    /// RPC.
    uint8_t activeRpcIds[MAX_NUM_PK];

    /// +----------------------------------------------------------+
    /// |   |   PKHashes assigned     |  PKHashes to  |            |
    /// |   |   to indexRead PRC      |  be assigned  |            |
    /// +----------------------------------------------------------+
    ///      ^                         ^               ^
    ///      |                         |               |
    ///   numRemoved                 numAssigned       numInserted
    ///
    /// Note that numRemoved and numAssigned point to occupied entries
    /// while numInserted points to empty entry

    /// The total number of hashes that have been added to activeHashes
    /// so far.
    /// The next hash will be added at index (numToInsert & ARRAY_MASK).
    size_t numInserted;

    /// The total number of hashes that have been removed from activeHashes
    /// since client has invoked get_next() multiple times.
    /// The next hash will be removed at index (numRemoved & ARRAY_MASK).
    size_t numRemoved;

    /// The total number of hashes that have been assigned to indexedRead
    /// RPCs.
    /// The next hash will be assigned at index (numAssigned & ARRAY_MASK)
    size_t numAssigned;

    /// Current object. This is the object returns to user if user
    /// calls getKey/getKeyLength/getValue/getValueLength
    Tub<Object> curObj;

    /// Index into readRpc that contains curObj.
    /// Current indexedRead RPC index that user is currently reading.
    /// Need to keep track of this because curObj doesn't copy the object
    /// from response buffer. Need to make sure the life time of response
    /// buffer until user switch to next object.
    /// The initial value of curIdx is RPC_ID_NOT_ASSIGN, which means there
    /// is no object available for client.
    uint8_t curIdx;

    /// True means that all of the relevant key hashes have been
    /// received from index servers, so no more lookupIndexKeys
    /// RPCs need to be issued.
    bool finishedLookup;

    DISALLOW_COPY_AND_ASSIGN(IndexLookup);
};

} // end RAMCloud
#endif // RAMCLOUD_INDEXLOOKUP_H
