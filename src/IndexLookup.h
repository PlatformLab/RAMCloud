/* Copyright (c) 2014-2015 Stanford University
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
#include "IndexKey.h"

namespace RAMCloud {

/*
 * This class implements the client side framework for index-based queries.
 *
 * To use IndexLookup, a client creates an instance of this class.
 * The client can then call getNext() function to move to next available object.
 * If getNext() returns false, it means we reached the last object.
 * If getNext() returns true, client can use getKey() and/or getKeyLength()
 * and/or getValue() and/or getValueLength() to get information about
 * that object.
 */

class IndexLookup {
  PUBLIC:

    IndexLookup(RamCloud* ramcloud, uint64_t tableId,
            IndexKey::IndexKeyRange keyRange);
    ~IndexLookup();

    bool isReady();
    bool getNext();

    Object* currentObject();

  PRIVATE:

    /// The lookupIndexKeys RPC and each of the readHashes RPCs
    /// is in one of the following states at all times.
    enum RpcStatus {
        /// FREE means this RPC is currently available to use/reuse.
        FREE,
        /// LOADING means PKHashes are being currently added into this
        /// readHashes RPC.
        /// This status is only designed for readHashes RPC.
        LOADING,
        /// SENT means this RPC has been sent to server, and is
        /// currently waiting for response.
        SENT,
        /// RESULT_READY means the response of the RPC has returned,
        /// and the response is currently available to be read.
        RESULT_READY
    };

    /// Struct for lookupIndexKeys RPC.
    struct LookupRpc {
        /// The tub that contains RamCloud::LookupIndexKeysRpc.
        Tub<LookupIndexKeysRpc> rpc;

        /// The status of rpc.
        RpcStatus status;

        /// Holds the response when rpc is issued.
        Buffer resp;

        /// The number of primary key hashes from resp buffer that have not yet
        /// been copied to activeHashes.
        uint32_t numHashes;

        /// Offset of the first primary key hash in resp buffer that has not
        /// been copied to activeHashes.
        uint32_t offset;

        LookupRpc()
            : rpc(), status(FREE), resp(), numHashes(), offset()
        {}
    };

    /// Struct for readHashes RPC.
    struct ReadRpc {
        /// The tub that contains RamCloud::ReadHashesRpc.
        Tub<ReadHashesRpc> rpc;

        /// The status of rpc.
        RpcStatus status;

        /// Used to collect the primary key hashes that will be
        /// requested when this RPC is issued.
        Buffer pKHashes;

        /// Number of hashes in pKHashes buffer.
        uint32_t numHashes;

        /// Holds the response for the rpc, which contains the objects
        /// corresponding to the hashes in pKHashes.
        Buffer resp;

        /// Number of objects in resp buffer, that haven't yet been read by
        /// the client.
        uint32_t numUnreadObjects;

        /// Offset of the first object in resp buffer that the client
        /// has not yet processed.
        uint32_t offset;

        /// If we have to reassign a pKHash whose index is smaller than maxPos,
        /// that pKHash cannot go with this readRpc, it must go into a new
        /// readRPC. This is because the pKHashes in any given ReadRpc have to
        /// be returned to the client in index order.
        size_t maxPos;

        /// Session that will be used to transmit RPC.
        Transport::SessionRef session;

        ReadRpc()
            : rpc(), status(FREE), pKHashes(), numHashes(), resp()
            , numUnreadObjects(), offset(), maxPos(), session()
        {}
    };

    void launchReadRpc(uint8_t i);

    /// Overall client state information.
    RamCloud* ramcloud;

    /// Instance of a LookupRpc.
    /// We only keep a single LookupRpc at a time, since each
    /// RamCloud::LookupIndexKeysRpc needs the return value  of the
    /// previous call to an rpc of the same type.
    LookupRpc lookupRpc;

    //////////////////////////////////////////////////////////////////////////
    // Declare constants and maintain state for ReadRpcs.
    //////////////////////////////////////////////////////////////////////////

    /// Max number of allowed ReadRpc's.
    static const uint8_t NUM_READ_RPCS = 10;

    /// Max number of PKHashes that will be sent in a single
    /// RamCloud::ReadHashesRpc.
    static const uint32_t MAX_PKHASHES_PER_RPC = 256;

    /// A special value to be assigned to any activeRpcIds[i] when the
    /// corresponding PKHash (given by activeHashes[i]) has not been
    /// assigned to any ongoing RamCloud::ReadHashesRpc's.
    static const uint8_t RPC_ID_NOT_ASSIGNED = NUM_READ_RPCS;

    /// Instances of ReadRpc's.
    /// Each single RPC handles communication between the client and
    /// a single data server.
    ReadRpc readRpcs[NUM_READ_RPCS];

    //////////////////////////////////////////////////////////////////////////
    // The following variables keep a copy of constructor arguments.
    //////////////////////////////////////////////////////////////////////////

    /// Id of the table in which lookup is to be done.
    uint64_t tableId;

    /// Stores the index id and first and last keys for this range lookup.
    struct IndexKey::IndexKeyRange keyRange;

    //////////////////////////////////////////////////////////////////////////
    // The next four variables are used to handle the case where we have
    // to issue multiple RamCloud::LookupIndexKeysRpc's, since indexes may span
    // multiple servers.
    //////////////////////////////////////////////////////////////////////////

    /// Maximum number of hashes that the server is allowed to return
    /// in a single rpc.
    static const uint32_t MAX_ALLOWED_HASHES = 1000;

    /// Key blob marking the start of the indexed key range for the next
    /// RamCloud::LookupIndexKeysRpc.
    void *nextKey;

    /// Length of nextKey in bytes.
    uint16_t nextKeyLength;

    /// Lowest allowed pKHash corresponding to nextKey, for which objects are
    /// to be returned in the next RamCloud::LookupIndexKeysRpc.
    uint64_t nextKeyHash;

    //////////////////////////////////////////////////////////////////////////
    // The following declarations are used to manage a collection
    // of "active hashes". This is a circular buffer of primary key
    // hashes that have been returned from index server, and the objects
    // corresponding to which are being fetched from tablet servers and/or
    // returned to the client. Once the client has processed an
    // active hash, its space in the active hashes array can be
    // reused. The active hashes information ensures that objects
    // are returned to the client in index order, even though they
    // may be fetched from different tablet servers in different
    // orders.
    //////////////////////////////////////////////////////////////////////////

    /// Used to convert variables such as numInserted into indexes into
    /// activeHashes and activeRpcIds in order to implement circular usage.
    static const size_t LOG_ARRAY_SIZE = 11;

    /// Array mask for activeHashes. When we need to find the corresponding
    /// position in activeHashes, ARRAY_MASK is used to perform bit operation.
    static const size_t ARRAY_MASK = ((1 << LOG_ARRAY_SIZE) - 1);

    /// Max number of PKHashes that activeHashes can hold at once.
    static const size_t MAX_NUM_PK = (1 << LOG_ARRAY_SIZE);

    /// Holds the active primary key hashes. Objects must be
    /// returned to the client in the same order as the entries
    /// in this array. The array is reused circularly.
    KeyHash activeHashes[MAX_NUM_PK];

    /// The i-th entry of activeRpcIds stores the index into readRpcs
    /// for the RPC that will be used to fetch the object corresponding to
    /// the i-th pKHash from activeHashes.
    /// The value RPC_ID_NOT_ASSIGNED means that the corresponding
    /// cache is not yet been assigned to an outgoing readHashes
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
    /// numRemoved and numAssigned point to occupied entries
    /// while numInserted points to empty entry.
    /// In the figure, numX is a shorthand for (numX & ARRAY_MASK).

    /// The total number of hashes that have been added to activeHashes so far.
    /// The next hash will be added at index (numInserted & ARRAY_MASK).
    size_t numInserted;

    /// The total number of hashes that have been removed from activeHashes
    /// since client has invoked get_next() any number of times.
    /// The next hash will be processed at index (numRemoved & ARRAY_MASK).
    size_t numRemoved;

    /// The total number of hashes that have been assigned to readRpc's.
    /// The next hash will be assigned at index (numAssigned & ARRAY_MASK).
    size_t numAssigned;

    /// Current object. This is the object for which information is returned
    /// to user if the user calls getKey/getKeyLength/getValue/getValueLength.
    Tub<Object> curObj;

    /// Index into readRpc's that contains curObj.
    /// We need to keep track of this because curObj doesn't copy the object
    /// from response buffer. So we need to ensure the lifetime of response
    /// buffer until user moves to next object.
    /// The initial value of curIdx is RPC_ID_NOT_ASSIGNED, which means that
    /// there is no object currently available for client.
    uint8_t curIdx;

    /// True means that all of the relevant key hashes have been
    /// received from index servers, so no more RamCloud::LookupIndexKeysRpc's
    /// need to be issued.
    bool finishedLookup;

    DISALLOW_COPY_AND_ASSIGN(IndexLookup);
};

} // end RAMCloud
#endif // RAMCLOUD_INDEXLOOKUP_H
