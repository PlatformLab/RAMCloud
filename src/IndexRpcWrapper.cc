/* Copyright (c) 2014-2016 Stanford University
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

#include "IndexRpcWrapper.h"
#include "ShortMacros.h"
#include "Logger.h"
#include "ObjectFinder.h"

namespace RAMCloud {

/**
 * Constructor for IndexRpcWrapper objects.
 *
 * \param context
 *      Overall information about RAMCloud cluster or the master server that
 *      governs this RPC.
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of the index to which the indexed key belongs.
 * \param key
 *      Variable length indexed key corresponding to an object.
 *      The location of key determines which server this rpc will be sent to.
 *      It does not necessarily have to be null terminated. The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Length in bytes of the key.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked here to ensure that they
 *      contain at least this much data, and a pointer to the header
 *      will be stored in the responseHeader for the use of wrapper
 *      subclasses.
 * \param responseBuffer
 *      Optional client-supplied buffer to use for the RPC's response;
 *      if NULL then we use a built-in buffer. Any existing contents
 *      of this buffer will be cleared automatically by the transport.
 */
IndexRpcWrapper::IndexRpcWrapper(
            Context* context, uint64_t tableId, uint8_t indexId,
            const void* key, uint16_t keyLength,
            uint32_t responseHeaderLength, Buffer* responseBuffer)
    : RpcWrapper(responseHeaderLength, responseBuffer)
    , context(context)
    , tableId(tableId)
    , indexId(indexId)
    , key(key)
    , keyLength(keyLength)
{
}

// See RpcWrapper for documentation.
bool
IndexRpcWrapper::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_INDEXLET) {
        // The index entry isn't where we thought it should be. Refresh our
        // configuration cache and try again.
        LOG(NOTICE,
            "Server %s doesn't store given secondary key "
                "for table %lu, index id %u; refreshing object map",
            session->serviceLocator.c_str(), tableId, indexId);
        context->objectFinder->flush(tableId);
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
    context->objectFinder->flushSession(tableId, indexId, key, keyLength);
    session = NULL;
    context->objectFinder->flush(tableId);
    send();
    return false;
}

/**
 * Handle the case where the RPC cannot be completed as the index containing
 * the key doesn't exist anywhere in the system.
 */
void
IndexRpcWrapper::handleIndexDoesntExist()
{
    LOG(DEBUG, "Index not found for tableId %lu, indexId %u",
            tableId, indexId);
    response->reset();
    response->emplaceAppend<WireFormat::ResponseCommon>()->status =
            STATUS_INDEX_DOESNT_EXIST;
}

// See RpcWrapper for documentation.
void
IndexRpcWrapper::send()
{
    bool indexDoesntExist;
    session = context->objectFinder->tryLookup(
            tableId, indexId, key, keyLength, &indexDoesntExist);
    if (session) {
        state = IN_PROGRESS;
        session->sendRequest(&request, response, this);
    } else if (indexDoesntExist) {
        handleIndexDoesntExist();
        state = FINISHED;
    } else {
        retry(0, 0);
    }
}

} // namespace RAMCloud
