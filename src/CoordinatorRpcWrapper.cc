/* Copyright (c) 2012 Stanford University
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
#include "Context.h"
#include "CoordinatorSession.h"
#include "CoordinatorRpcWrapper.h"

namespace RAMCloud {

/**
 * Constructor for CoordinatorRpcWrapper objects.
 * \param context
 *      Overall information about the RAMCloud server or client.
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
CoordinatorRpcWrapper::CoordinatorRpcWrapper(Context* context,
        uint32_t responseHeaderLength, Buffer* response)
    : RpcWrapper(responseHeaderLength, response)
    , context(context)
{
}

// See RpcWrapper for documentation.
bool
CoordinatorRpcWrapper::handleTransportError()
{
    // There was a transport-level failure. The transport should already
    // have logged this. All we have to do is retry.
    context->coordinatorSession->flush();
    send();
    return false;
}

// See RpcWrapper for documentation.
void
CoordinatorRpcWrapper::send()
{
    session = context->coordinatorSession->getSession();
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

} // namespace RAMCloud
