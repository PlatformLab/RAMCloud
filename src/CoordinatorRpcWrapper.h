/* Copyright (c) 2012-2015 Stanford University
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

#ifndef RAMCLOUD_COORDINATORRPCWRAPPER_H
#define RAMCLOUD_COORDINATORRPCWRAPPER_H

#include "RpcWrapper.h"

namespace RAMCloud {
class RamCloud;
class Context;

/**
 * CoordinatorRpcWrapper manages the client side of most RPCs that are sent to
 * the coordinator.  Its strategy is to retry these RPCs over and over
 * again (reopening sessions if needed) until eventually they succeed.
 */
class CoordinatorRpcWrapper : public RpcWrapper {
  public:
    explicit CoordinatorRpcWrapper(Context* context,
            uint32_t responseHeaderLength,
            Buffer* response = NULL);

    /**
     * Destructor for CoordinatorRpcWrapper.
     */
    virtual ~CoordinatorRpcWrapper() {}

  PROTECTED:
    virtual bool handleTransportError();
    virtual void send();

    /// Shared RAMCloud information.
    Context* context;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorRpcWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_COORDINATORRPCWRAPPER_H
