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

#include <functional>
#include "Logger.h"
#include "WorkerSession.h"

namespace RAMCloud {

/**
 * Construct a WorkerSession.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param wrapped
 *      Another Session object, to which #sendRequest and other methods
 *      will be forwarded.
 */
WorkerSession::WorkerSession(Context* context,
        Transport::SessionRef wrapped)
    : Session(wrapped->serviceLocator)
    , context(context)
    , wrapped(wrapped)
{
    RAMCLOUD_TEST_LOG("created");
}

/**
 * Destructor for WorkerSessions.
 */
WorkerSession::~WorkerSession()
{
    // Make sure that the underlying session is released while the
    // dispatch thread is locked.
    Dispatch::Lock lock(context->dispatch);
    wrapped = NULL;
}

/// \copydoc Transport::Session::abort
void
WorkerSession::abort()
{
    // Must make sure that the dispatch thread isn't running when we
    // invoke the real abort.
    Dispatch::Lock lock(context->dispatch);
    return wrapped->abort();
}

/// \copydoc Transport::Session::cancelRequest
void
WorkerSession::cancelRequest(Transport::RpcNotifier* notifier)
{
    // This request has to run in the dispatch thread. However, it's
    // important that this method doesn't return until the cancel
    // operation has completed. Once this method returns, the caller may
    // reuse various resources for the request, such as the buffers and
    // the notifier, and it isn't safe to do that until the transport
    // has finished the cancel operation.
    uint64_t id = context->dispatchExec->addRequest<CancelRequestWrapper>(
            notifier, wrapped);
    context->dispatchExec->sync(id);
}

/// \copydoc Transport::Session::getRpcInfo
string
WorkerSession::getRpcInfo()
{
    // Must make sure that the dispatch thread isn't running when we
    // invoke the real getRpcInfo.
    Dispatch::Lock lock(context->dispatch);
    return wrapped->getRpcInfo();
}

/// \copydoc Transport::Session::sendRequest
void
WorkerSession::sendRequest(Buffer* request, Buffer* response,
        Transport::RpcNotifier* notifier)
{
    // Enqueue the request for the dispatch thread.
    context->dispatchExec->addRequest<SendRequestWrapper>(
            request, response, notifier, wrapped);
}

} // namespace RAMCloud
