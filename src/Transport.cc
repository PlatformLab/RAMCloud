/* Copyright (c) 2011 Stanford University
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

#include "Dispatch.h"
#include "Rpc.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * Wait for the RPC response to arrive (if it hasn't already) and throw
 * an exception if there were any problems. Once this method has returned
 * the caller can access the response message using the #Buffer that was
 * passed to #clientSend().
 *
 * \throw TransportException
 *      Something went wrong at the transport level; typically this means
 *      the transport is no longer usable.
 */
void
Transport::ClientRpc::wait()
{
    Dispatch& dispatch = *Context::get().dispatch;

    // When invoked in RAMCloud servers there is a separate dispatch thread,
    // so we just busy-wait here. When invoked on RAMCloud clients we're in
    // the dispatch thread so we have to invoke the dispatcher while waiting.
    bool isDispatchThread = dispatch.isDispatchThread();

    while (!finished.load()) {
        if (isDispatchThread)
            dispatch.poll();
    }

    if (errorMessage) {
        throw TransportException(HERE, *errorMessage);
    }
}

/**
 * This method is invoked by the underlying Transport to indicate that the
 * RPC has completed (either normally or with an error).
 *
 * \param errorMessage
 *      If the RPC completed normally this is NULL; otherwise it refers
 *      to an error message, which is copied and used to throw an exception
 *      during the next invocation of #wait.
 */
void
Transport::ClientRpc::markFinished(const char* errorMessage)
{
    if (errorMessage != NULL) {
        this->errorMessage.construct(errorMessage);
    }
    finished.store(1);
}

/**
 * This method is invoked by the underlying Transport to indicate that the
 * RPC has completed with an error.
 *
 * \param errorMessage
 *      An error message, which is copied and used to throw an exception
 *      during the next invocation of #wait.
 */
void
Transport::ClientRpc::markFinished(const string& errorMessage)
{
    this->errorMessage.construct(errorMessage);
    finished.store(1);
}

/**
 * Abort the RPC (if it hasn't already completed).  Once this method
 * returns the transport is free to reallocate any resources
 * associated with the RPC, and it is safe for the client to destroy
 * the RPC object; #isReady will return true and #wait will throw
 * TransportException (unless the RPC completed before #cancel
 * was invoked).
 *
 * \param message
 *      Message giving the reason why the RPC was canceled; will be
 *      included in the message of the exception thrown by #wait.  If
 *      this is an empty string then a default message will be used.
 */
void
Transport::ClientRpc::cancel(const string& message)
{
    Dispatch::Lock lock;
    if (isReady())
        return;
    cancelCleanup();
    const RpcRequestCommon* header = request->getStart<RpcRequestCommon>();
    string fullMessage = format("%s RPC cancelled",
            (header != NULL) ?
            Rpc::opcodeToSymbol(RpcOpcode(header->opcode)) : "empty");
    if (message.size() > 0) {
        fullMessage.append(": ");
        fullMessage.append(message);
    }
    markFinished(fullMessage);
}

void
Transport::ClientRpc::cancel(const char* message)
{
    string s(message);
    cancel(s);
}

} // namespace RAMCloud
