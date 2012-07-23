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
#include "CoordinatorServerList.h"
#include "CoordinatorSession.h"
#include "ServerIdRpcWrapper.h"
#include "Cycles.h"
#include "RamCloud.h"

namespace RAMCloud {

bool ServerIdRpcWrapper::convertExceptionsToDoesntExist = false;

/**
 * Constructor for ServerIdRpcWrapper objects.
 * \param context
 *      Overall information about the RAMCloud server.
 * \param id
 *      The server to which this RPC should be sent. The RPC will be
 *      retried as long as this server is still up.
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
ServerIdRpcWrapper::ServerIdRpcWrapper(Context& context, ServerId id,
        uint32_t responseHeaderLength, Buffer* response)
    : RpcWrapper(responseHeaderLength, response)
    , context(context)
    , id(id)
    , serverDown(false)
{
}

// See RpcWrapper for documentation.
bool
ServerIdRpcWrapper::handleTransportError()
{
    if (convertExceptionsToDoesntExist) {
        serverDown = true;
        return true;
    }

    // There was a transport-level failure. The transport should already
    // have logged this. Retry unless the server is down.
    bool up;
    try {
        if (context.serverList != NULL) {
            up = context.serverList->isUp(id);
        } else {
            up = context.coordinatorServerList->isUp(id);
        }
    }
    catch (Exception& e) {
        // This exception happens if the server can't be found in the
        // server list.  This means the server is down.
        up = false;
    }
    if (session) {
        context.transportManager->flushSession(
                session->getServiceLocator().c_str());
    }
    if (!up) {
        serverDown = true;
        return true;
    }
    send();
    return false;
}

// See RpcWrapper for documentation.
void
ServerIdRpcWrapper::send()
{
    assert(context.serverList != NULL ||
            context.coordinatorServerList != NULL);
    try {
        const char* locator =
                (context.serverList != NULL)
                ? context.serverList->getLocator(id)
                : context.coordinatorServerList->getLocator(id);
        session = context.transportManager->getSession(locator, id);
    }
    catch (Exception& e) {
        LOG(WARNING, "ServerIdRpcWrapper couldn't get session: %s",
                e.message.c_str());
        state = FAILED;
        return;
    }
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Wait for the RPC to complete, and throw exceptions for any errors.
 *
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
void
ServerIdRpcWrapper::waitAndCheckErrors()
{
    // Note: this method is a generic shared version for RPCs that don't
    // return results and don't need to do any processing of the response
    // packet except checking for errors.
    waitInternal(*context.dispatch);
    if (serverDown) {
        throw ServerDoesntExistException(HERE);
    }
    if (responseHeader->status != STATUS_OK)
        ClientException::throwException(HERE, responseHeader->status);
}

} // namespace RAMCloud
