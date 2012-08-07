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

} // namespace RAMCloud
