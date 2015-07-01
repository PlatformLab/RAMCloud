/* Copyright (c) 2011-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
#include "DispatchExec.h"
#include "malloc.h"


namespace RAMCloud {

/**
 * This method is invoked by the dispatch poller; it checks for DispatchExec
 * requests and executes them.
 * \return
 *      1 is returned if there was at least one request to execute; 0
 *      is returned if this method found nothing to do.
 */
int DispatchExec::poll() {
    int foundWork = 0;
    while (requests[removeIndex].data.full == 1) {
        Fence::enter();
        requests[removeIndex].getLambda()->invoke();
        Fence::leave();
        requests[removeIndex].data.full = 0;
        removeIndex++;
        if (removeIndex == NUM_WORKER_REQUESTS) removeIndex = 0;
        foundWork = 1;
    }
    return foundWork;
}

/**
 * Construct a DispatchExec.
 */
DispatchExec::DispatchExec(Dispatch* dispatch)
    : Poller(dispatch, "DispatchExec")
    , requests()
    , removeIndex(0)
    , pad()
    , lock("DispatchExec:requestLock")
    , addIndex(0) {
        posix_memalign(reinterpret_cast<void**>(&requests), CACHE_LINE_SIZE,
                sizeof(LambdaBox) * NUM_WORKER_REQUESTS);
        // Double checking to make sure we get proper cache alignment.
        assert((reinterpret_cast<uint64_t>(requests) & 0x3f) == 0);

        // Zero-initialize the LambdaBox array so that all the 'full' bits are
        // clear at the outset.
        memset(requests, 0, NUM_WORKER_REQUESTS * sizeof(pad));
        Fence::sfence();
}
}
