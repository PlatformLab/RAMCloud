/* Copyright (c) 2011-2012 Stanford University
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
#include "Fence.h"
#include "Transport.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * This method is invoked in the dispatch thread by a transport when
 * a response message is successfully received for RPC.
 */
void
Transport::RpcNotifier::completed() {
}

/**
 * This method is invoked in the dispatch thread by a transport if a
 * transport-level error prevents an RPC from completing. In this case,
 * the wrapper should assume that the session for the RPC is dead;
 * it will typically open a new session and retry the operation.
 */
void
Transport::RpcNotifier::failed() {
}

/**
 * This method is invoked by boost::intrusive_ptr as part of the
 * implementation of SessionRef; do not call explicitly.
 *
 * \param session
 *      WorkerSession for which a new WorkerSessionRef  is being
 *      created.
 */
void
intrusive_ptr_add_ref(Transport::Session* session) {
    session->refCount.fetch_add(1, std::memory_order_relaxed);
}

/**
 * This method is invoked by boost::intrusive_ptr as part of the
 * implementation of SessionRef; do not call explicitly. It decrements
 * the reference count on workerSession and frees session when the
 * reference count becomes zero.
 *
 * \param session
 *      Session for which a SessionRef was just deleted.
 */
void
intrusive_ptr_release(Transport::Session* session) {
    // Inter-thread ordering constraint: any possible access to the Session
    // object in one thread must happen before the destruction of this object
    // in a different thread. This is enforced by a "release" operation after
    // dropping a reference, and an "acquire" operation before destructing
    // the session.
    if (session->refCount.fetch_sub(1, std::memory_order_release) == 1) {
        std::atomic_thread_fence(std::memory_order_acquire);
        session->release();
    }
}

} // namespace RAMCloud
