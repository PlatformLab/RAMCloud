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
 * RPCs. Client side just includes "IndexLookup.h" in its header to
 * use IndexLookup class.
 *
 * Several parameters can be set in the config below:
 *   - The number of concurrent indexedRead RPCs
 *   - The max number of PKHashes a indexedRead RPC can hold at a time
 *   - The size of the active PKHashes
 *
 * To use IndexLookup, client create a object of this class by providing
 * all necessary information. After construction of IndexLookup, client
 * can call getNext() function to move to next available object. If
 * getNext() returns false, it means we reached the last object. Client
 * can use getKey/getKeyLength/getValue/getValueLength to get object data
 * of current object.
 */

class IndexLookup {
  PUBLIC:
    enum Flags : uint32_t {
        DEFAULT = 0,
        EXCLUDE_FIRST_KEY = 1,
        EXCLUDE_LAST_KEY = 2
    };
    IndexLookup(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                const void* firstKey, uint16_t firstKeyLength,
                const void* lastKey, uint16_t lastKeyLength,
                Flags flags = DEFAULT);
    ~IndexLookup();
    bool isReady();
    bool getNext();
    const void* getKey(KeyIndex keyIndex = 0, KeyLength *keyLength = NULL);
    KeyCount getKeyCount();
    KeyLength getKeyLength(KeyIndex keyIndex = 0);
    const void* getValue(uint32_t *valueLength = NULL);
    uint32_t getValueLength();

  PRIVATE:
    /// Max number of current indexedRead RPCs
    static const uint8_t NUM_READ_RPC = 10;

    /// Max number of PKHashes that can be stored on a single indexedRead RPC
    static const uint32_t MAX_PKHASHES_PERRPC = 256;

    /// A special value indicating the corresponding PKHash has not been
    /// assigned to any ongoing indexedRead RPC
    static const uint8_t RPC_ID_NOTASSIGN = NUM_READ_RPC;

    /// logarithm of activeHashes size. We want to make the size of
    /// activeHashes a power of 2, since we want reuse buffer in a
    /// circular way. By enforcing the size of buffer a power of 2,
    /// we can use bit operations to find position in activeHashes.
    static const size_t LG_BUFFER_SIZE = 10;

    /// Array mask for activeHashes. When need to find the corresponding
    /// position in activeHashes, ARRAY_MASK is used to perform bit operation.
    static const size_t ARRAY_MASK = ((1 << LG_BUFFER_SIZE) - 1);

    /// Max number of PKHashes that activeHashes can hold at once.
    static const size_t MAX_NUM_PK = (1 << LG_BUFFER_SIZE);

    enum RpcStatus {
        /// FREE means this RPC is currently available to use/reuse
        FREE,
        /// LOADING means we are adding PKHashes into this indexedRead RPC.
        /// LOADING status is only used for indexedRead RPC
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
        /// The status of current Read RPC
        RpcStatus status;
        /// Buffer containing all the primary key hashes for which an indexed
        /// read is to be done.
        Buffer pKHashes;
        /// Buffer that contains all the objects that match the index lookup
        /// along with their versions, in the format specified by
        /// WireFormat::IndexedRead::Response.
        Buffer resp;
        /// Number of hashes in pKHashes buffer
        uint32_t numHashes;
        /// Number of objects in resp buffer, that hasn't been read by client
        uint32_t numUnreadObjects;
        /// Current offset in resp Buffer
        uint32_t offset;
        /// The index rank (position) of the last PKHash in pKHashes buffer.
        /// To return objects in index order, we need to make sure pKHashes
        /// saves primary key hashes in the same index order. maxPos is used
        /// to save the index order of the last PKHash
        size_t maxPos;
        /// Session that will be used to transmit RPC
        Transport::SessionRef session;
        ReadRpc()
        : rpc(), status(FREE), pKHashes(), resp(), numHashes()
        , numUnreadObjects(), offset(), maxPos(), session()
        {}
    };

    /// struct for lookupIndexKeys RPC.
    struct LookupRpc {
    	/// The tub that contains low-level LookupIndexKeys RPC
        Tub<LookupIndexKeysRpc> rpc;
        /// The status of current Lookup RPC
        RpcStatus status;
        /// Buffer that contains all the PKHashes that match the index lookup
        Buffer resp;
        /// Number of objects that matched the lookup, for which
        /// the primary key hashes are being returned here.
        uint32_t numHashes;
        /// Results starting at nextKey + nextKeyHash couldn't be returned.
        /// Client can send another request according to this.
        uint64_t nextKeyHash;
        /// Current offset in resp Buffer
        uint32_t offset;
        LookupRpc()
        : rpc(), status(FREE), resp(), numHashes(), nextKeyHash(), offset()
        {}
    };
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
    /// This is a copy of the constructor arg
    const void *firstKey;

    /// Length of first key string
    uint16_t firstKeyLength;

    /// Key blob marking the start of the indexed key range for next
    /// lookupIndexKeys
    void *nextKey;

    /// Length of next key string
    uint16_t nextKeyLength;

    /// Key blob marking the end of the indexed key range for this
    /// query
    const void *lastKey;

    /// Length of last key string
    uint16_t lastKeyLength;

    /// flags
    Flags flags;

    /// Buffer stores PKHashes that we should return to users
    /// in index order. The buffer is reused as a cycle.
    KeyHash activeHashes[MAX_NUM_PK];

    /// Each entry stores the index into readRpc of the Rpc that will
    /// be used to fetch the corresponding PKHash from pKBuffer.
    /// Buffer stores indexedRead RPC indexes on which this PKHashes
    /// is sent to data server to acquire object data. Stores special
    /// value RPC_IDX_NOTASSIGN if the corresponding PKHash has not
    /// been assigned.
    uint8_t activeRpcId[MAX_NUM_PK];

    /// +----------------------------------------------------------+
    /// |   |   PKHashes assigned     |  PKHashes to  |            |
    /// |   |   to indexRead PRC      |  be assigned  |            |
    /// +----------------------------------------------------------+
    ///      ^                         ^               ^
    ///      |                         |               |
    ///   removePos                 assignPos       insertPos
    ///
    /// Note that removePos and assignPos point to occupied entries
    /// while insertPos points to empty entry

    /// The position if we want to insert the next PKHashes into
    /// activeHashes.
    /// insertPos never decreases, and the corresponding position
    /// in activeHashes is (insertPos & ARRAY_MASK)
    size_t insertPos;

    /// The position of the next PKHash to be removed from activeHashes
    /// if user calls get_next()
    /// removePos never decreases, and the corresponding position
    /// in activeHashes is (removePos & ARRAY_MASK)
    size_t removePos;

    /// The position of the next PKHash to be assigned to an indexedRead
    /// RPC
    /// assignPos always stays between insertPos and removePos, and the
    /// corresponding position in activeHashes is (assignPos & ARRAY_MASK)
    size_t assignPos;

    /// Current object. This is the object returns to user if user
    /// calls getKey/getKeyLength/getValue/getValueLength
    Tub<Object> curObj;

    /// Index into readRpc that contains curObj.
    /// Current indexedRead RPC index that user is currently reading.
    /// Need to keep track of this because curObj doesn't copy the object
    /// from response buffer. Need to make sure the life time of response
    /// buffer until user switch to next object.
    uint8_t curIdx;

    /// Boolean indicating if all lookupIndexKeys are finished.
    bool finishedLookup;

    DISALLOW_COPY_AND_ASSIGN(IndexLookup);
};

} // end RAMCloud
#endif // RAMCLOUD_INDEXLOOKUP_H
