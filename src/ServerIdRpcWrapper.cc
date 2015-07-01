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

/// For testing; prefer using ConvertExceptionsToDoesntExist where possible.
/// When set instead of retrying the rpc on a TransportException all
/// instances of this wrapper will internally flag the server as down
/// instead. This causes waiting on the rpc throw a
/// ServerNotUpException. Useful with MockTransport to convert
/// responses set with transport.setInput(NULL) to
/// ServerNotUpExceptions.
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
ServerIdRpcWrapper::ServerIdRpcWrapper(Context* context, ServerId id,
        uint32_t responseHeaderLength, Buffer* response)
    : RpcWrapper(responseHeaderLength, response)
    , context(context)
    , id(id)
    , transportErrors(0)
    , serverCrashed(false)
{
}

// See RpcWrapper for documentation.
bool
ServerIdRpcWrapper::checkStatus()
{
    if (responseHeader->status == STATUS_WRONG_SERVER) {
        // Somehow the session we used doesn't actually connect to the
        // correct server (most likely problem: we're trying to talk to
        // a server that has crashed and restarted under the same locator,
        // and we ended up talking to the new server, not the old one).
        LOG(NOTICE, "STATUS_WRONG_SERVER in %s RPC to %s",
                WireFormat::opcodeSymbol(&request),
                context->serverList->toString(id).c_str());
        context->serverList->flushSession(id);
        if (!context->serverList->isUp(id)) {
            serverCrashed = true;
            return true;
        }
        send();
        return false;
    }
    return true;
}

// See RpcWrapper for documentation.
bool
ServerIdRpcWrapper::handleTransportError()
{
    if (convertExceptionsToDoesntExist) {
        serverCrashed = true;
        return true;
    }

    // There was a transport-level failure. The transport should already
    // have logged this. Retry unless the server is down.

    if (serverCrashed) {
        // We've already done everything we can; no need to repeat the
        // work (returning now eliminates some duplicate log messages that
        // would occur during testing otherwise).
        return true;
    }
    context->serverList->flushSession(id);
    if (!context->serverList->isUp(id)) {
        serverCrashed = true;
        return true;
    }

    // If there are repeated failures, delay the retries to reduce log
    // chatter and system load.
    transportErrors++;
    if (transportErrors < 3) {
        // Retry immediately for the first couple of times.
        send();
    } else {
        // The server is probably down. If we are running in the
        // coordinator, start crash recovery.  This is needed to handle
        // the following situation:
        // * The coordinator crashes; while it is down, a master crashes.
        // * When the coordinator restarts, it attempts to communicate
        //   with the crashed master during startup (e.g. to make sure it
        //   knows about tablet ownership).
        // * The master is still listed as "up" in the coordinator's
        //   recovered server list, but in fact it is down, so it cannot
        //   reply.
        // * Normally, the failure detection mechanism would eventually
        //   cause the master to become marked as "down", but this requires
        //   the coordinator to process hintServerCrashed RPCS. It can't
        //   do this until it finishes initializing itself, which it can't
        //   do until the RPC completes or terminates because the target
        //   has crashed.
        // * Result: deadlock.
        // The solution is to mark the server is crashed here, rather than
        // depending on the normal failure detection mechanism.
        if (context->coordinatorServerList != NULL) {
            LOG(WARNING, "server %s considered crashed: no response to %s RPC",
                    context->serverList->toString(id).c_str(),
                    WireFormat::opcodeSymbol(&request));
            context->coordinatorServerList->serverCrashed(id);
            serverCrashed = true;
            return true;
        }

        // We're not running in the coordinator. Delay a while before retrying;
        // this should be enough time for the failure to be detected and
        // propagated to us.
        retry(500000, 1500000);
    }
    return false;
}

// See RpcWrapper for documentation.
void
ServerIdRpcWrapper::send()
{
    assert(context->serverList != NULL);
    session = context->serverList->getSession(id);
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Wait for the RPC to complete, and throw exceptions for any errors.
 *
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
void
ServerIdRpcWrapper::waitAndCheckErrors()
{
    // Note: this method is a generic shared version for RPCs that don't
    // return results and don't need to do any processing of the response
    // packet except checking for errors.
    waitInternal(context->dispatch);
    if (serverCrashed) {
        throw ServerNotUpException(HERE);
    }
    if (responseHeader->status != STATUS_OK)
        ClientException::throwException(HERE, responseHeader->status);
}

} // namespace RAMCloud
