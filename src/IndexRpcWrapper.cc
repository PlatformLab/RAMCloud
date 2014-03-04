/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "ShortMacros.h"
#include "Logger.h"
#include "IndexRpcWrapper.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Constructor for IndexRpcWrapper objects.
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of the index for which keys have to be compared.
 * \param firstKey
 *      Starting key for the key range in which keys are to be matched.
 *      The key range includes the firstKey.
 *      If it is a point lookup instead of range search, the keys will
 *      only be matched on the firstKey.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param firstKeyLength
 *      Length in bytes of the firstKey.
 * \param lastKey
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 *      If NULL, then it is a point lookup instead of range search.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param lastKeyLength
 *      Length in byes of the lastKey.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked here to ensure that they
 *      contain at least this much data, and a pointer to the header
 *      will be stored in the responseHeader for the use of wrapper
 *      subclasses.
 * 
 * \param[out] totalNumHashes
 *      Number of key hashes being returned in totalResponse.
 * \param[out] totalResponse
 *      Client-supplied buffer to use to return all the keyHashes.
 *      This may include result from multiple RPC's, if required.
 */
IndexRpcWrapper::IndexRpcWrapper(
            RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
            const void* firstKey, uint16_t firstKeyLength,
            const void* lastKey, uint16_t lastKeyLength,
            uint32_t responseHeaderLength,
            uint32_t* totalNumHashes, Buffer* totalResponse)
    : RpcWrapper(responseHeaderLength) // Use defaultResponse buffer for
                                       // individual rpcs to the masters.
    , ramcloud(ramcloud)
    , tableId(tableId)
    , indexId(indexId)
    , nextKey(firstKey)
    , nextKeyLength(firstKeyLength)
    , lastKey(lastKey)
    , lastKeyLength(lastKeyLength)
    , totalNumHashes(totalNumHashes)
    , totalResponse(totalResponse)
{
    totalNumHashes = 0;
}

// See RpcWrapper for documentation.
bool
IndexRpcWrapper::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        // The object isn't where we thought it should be. Refresh our
        // configuration cache and try again.
        LOG(NOTICE,
            "Server %s doesn't store given secondary key"
                "for table %lu, index id %u; refreshing object map",
            session->getServiceLocator().c_str(), tableId, indexId);
        ramcloud->objectFinder.flush(tableId);
        send();
        return false;
    }
    // TODO(anktiak): In lookup code in index server, make sure that
    // we return STATUS_OK when done, STATUS_FETCH_MORE along with next key
    // to fetch, if there's more to fetch.
    if (responseHeader->status == STATUS_FETCH_MORE) {
        // The current rpc has successfully fetched values, but there are more
        // that need to be fetched.
        // Append received key hashes to the totalResponse, and
        // send the next rpc with the next key to fetch.
        const WireFormat::LookupIndexKeys::Response* indexRespHdr =
                static_cast<const WireFormat::LookupIndexKeys::Response*>(
                        response->getRange(0, responseHeaderLength));
        uint32_t currentNumHashes = indexRespHdr->numHashes;
        totalNumHashes += currentNumHashes;
        nextKeyLength = indexRespHdr->nextKeyLength;
        uint32_t respOffset = responseHeaderLength;
        nextKey = response->getRange(respOffset, nextKeyLength);
        respOffset += nextKeyLength;
        memcpy(new(totalResponse, APPEND) char[currentNumHashes*8],
               response, sizeof32(currentNumHashes*8));
        send();
        return false;
    }
    return true;
}

// See RpcWrapper for documentation.
bool
IndexRpcWrapper::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mapping for our object.
    // Then retry.
    session = NULL;
    ramcloud->objectFinder.flush(tableId);
    send();
    return false;
}

// See RpcWrapper for documentation.
void
IndexRpcWrapper::send()
{
    session = ramcloud->objectFinder.lookup(tableId, indexId,
                                            nextKey, nextKeyLength);
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

} // namespace RAMCloud
