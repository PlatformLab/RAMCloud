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

bool Transport::Session::testingSimulateConflict = false;

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
 * implementation of SessionRef; do not call explicitly. It decrements
 * the reference count on workerSession and frees session when the
 * reference count becomes zero.
 *
 * \param session
 *      Session for which a SessionRef was just deleted.
 */
void
intrusive_ptr_release(Transport::Session* session) {
    // This code is tricky. In order to be thread-safe, we must atomically
    // decrement refCount. Furthermore, if all remaining references are
    // deleted at the same time, we must make sure that the underlying
    // object is released exactly once (i.e. it it must not be possible for
    // two calls to this method both to see a zero reference count).
    //
    // An alternative to this approach would be to use a lock to synchronize
    // access to refCount (both here and in intrusive_ptr_add_ref) but that
    // is considerably more expensive.

    int before, after;
    while (1) {
        before = session->refCount;
        after = before-1;
#ifdef TESTING
        if (Transport::Session::testingSimulateConflict) {
            // During tests, pretend that a concurrent call to this method
            // modified the reference count: the compareExchange should
            // fail.
            before--;
        }
#endif
        // This ensures that no-one else sees the same "before" and "after"
        // values that we see.
        if (session->refCount.compareExchange(before, after) == before) {
            break;
        }
    }
    if (after == 0) {
        session->release();
    }
}

} // namespace RAMCloud
