/* Copyright (c) 2011 Stanford University
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

#ifndef RAMCLOUD_MOCKSERVICE_H
#define RAMCLOUD_MOCKSERVICE_H

#include "Service.h"

namespace RAMCloud {

/**
 * This class provides a trivial service that is used for testing.
 */
class MockService : public Service {
  public:

    MockService() : log(), delay(false), sendReply(false) { }
    virtual ~MockService() {}
    virtual void dispatch(RpcType type, Rpc& rpc)
    {
        if (!log.empty()) {
             log.append(", ");
        }
        log.append("rpc: ");
        log.append(toString(&rpc.requestPayload));

        // Create a response that increments each of the (integer) values
        // in the request.  Throw an error if value 54321 appears.
        for (uint32_t i = 0; i < rpc.requestPayload.getTotalLength()-3;
                i += 4) {
            int32_t inputValue = *(rpc.requestPayload.getOffset<int32_t>(i));
            if (inputValue == 54321) {
                throw ClientException(HERE, STATUS_REQUEST_FORMAT_ERROR);
            }
            *(new(&rpc.replyPayload, APPEND) int32_t) = inputValue+1;
        }

        // The following code is used to test the sendReply Rpc method.
        if (sendReply)
            rpc.sendReply();

        while (delay)
            usleep(1000);
    }

    /// Records information about each request dispatched to this service.
    string log;

    /// The following variable may be set to true to delay completion
    /// of a request. You must clear the variable before the request will
    /// complete.
    bool delay;

    /// The following variable may be set to true to cause the service to
    /// invoke sendReply before returning.
    bool sendReply;

    DISALLOW_COPY_AND_ASSIGN(MockService);
};

} // end RAMCloud

#endif  // RAMCLOUD_MOCKSERVICE_H
