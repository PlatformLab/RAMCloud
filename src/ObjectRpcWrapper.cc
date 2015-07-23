/* Copyright (c) 2012-2015 Stanford University
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
#include "ObjectFinder.h"
#include "ObjectRpcWrapper.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Constructor for ObjectRpcWrapper objects.
 * \param context
 *      Overall information about RAMCloud cluster.
 * \param tableId
 *      The table containing the desired object.
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It need not be null terminated.  The caller is responsible for ensuring
 *      that this key remains valid until the call completes.
 * \param keyLength
 *      Size in bytes of key.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked here to ensure that they
 *      contain at least this much data, and a pointer to the header
 *      will be stored in the responseHeader for the use of wrapper
 *      subclasses.
 * \param response
 *      Optional client-supplied buffer to use for the RPC's response;
 *      if NULL then we use a built-in buffer. Any existing contents
 *      of this buffer will be cleared automatically by the transport.
 */
ObjectRpcWrapper::ObjectRpcWrapper(Context* context, uint64_t tableId,
        const void* key, uint16_t keyLength, uint32_t responseHeaderLength,
        Buffer* response)
    : RpcWrapper(responseHeaderLength, response)
    , context(context)
    , tableId(tableId)
    , keyHash(Key::getHash(tableId, key, keyLength))
{
}

/**
 * Alternate constructor for ObjectRpcWrapper objects, in which the desired
 * server is specified with a key hash, rather than a key value.
 * \param context
 *      Overall information about RAMCloud cluster.
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      Key hash that identifies a particular tablet (and, hence, the
 *      server storing that tablet).
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked here to ensure that they
 *      contain at least this much data, and a pointer to the header
 *      will be stored in the responseHeader for the use of wrapper
 *      subclasses.
 * \param response
 *      Optional client-supplied buffer to use for the RPC's response;
 *      if NULL then we use a built-in buffer.
 */
ObjectRpcWrapper::ObjectRpcWrapper(Context* context, uint64_t tableId,
        uint64_t keyHash, uint32_t responseHeaderLength, Buffer* response)
    : RpcWrapper(responseHeaderLength, response)
    , context(context)
    , tableId(tableId)
    , keyHash(keyHash)
{
}

// See RpcWrapper for documentation.
bool
ObjectRpcWrapper::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        // The object isn't where we thought it should be. Refresh our
        // configuration cache and try again.
        LOG(NOTICE, "Server %s doesn't store <%lu, 0x%lx>; "
                "refreshing object map",
                session->getServiceLocator().c_str(),
                tableId, keyHash);
        context->objectFinder->flush(tableId);
        send();
        return false;
    }
    return true;
}

// See RpcWrapper for documentation.
bool
ObjectRpcWrapper::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mapping for our object.
    // Then retry.
    context->objectFinder->flushSession(tableId, keyHash);
    session = NULL;
    context->objectFinder->flush(tableId);
    send();
    return false;
}

// See RpcWrapper for documentation.
void
ObjectRpcWrapper::send()
{
    session = context->objectFinder->lookup(tableId, keyHash);
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

} // namespace RAMCloud
