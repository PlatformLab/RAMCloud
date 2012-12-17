/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_MULTIREAD_H
#define RAMCLOUD_MULTIREAD_H

#include "MultiOp.h"

namespace RAMCloud {

/**
 * This class implements the client side of multiRead operations. It
 * uses the MultiOp Framework to manage multiple concurrent RPCs,
 * each writing one or more objects to a single server. The behavior
 * of this class is similar to an RpcWrapper, but it isn't an
 * RpcWrapper subclass because it doesn't correspond to a single RPC.
 */

class MultiRead : public MultiOp {
    static const WireFormat::MultiOp::OpType type =
                                        WireFormat::MultiOp::OpType::READ;
  PUBLIC:
    MultiRead(RamCloud* ramcloud,
              MultiReadObject* const requests[],
              uint32_t numRequests);

  PROTECTED:
    void appendRequest(MultiOpObject* request, Buffer* buf);
    bool readResponse(MultiOpObject* request, Buffer* response,
                      uint32_t* respOffset);

    DISALLOW_COPY_AND_ASSIGN(MultiRead);
};

} // end RAMCloud
#endif /* RAMCLOUD_MULTIREAD_H */
