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

#ifndef RAMCLOUD_MOCKWRAPPER_H
#define RAMCLOUD_MOCKWRAPPER_H

#include "Transport.h"
#include "WireFormat.h"

namespace RAMCloud {
class RamCloud;

/**
 * This class defines a trivial RPC wrapper useful for testing transports.
 * Its main function is to provide buffers and record information about calls
 * to #completed and #failed.
 */
class MockWrapper : public Transport::RpcNotifier {
  public:
    /**
     * Constructor for MockWrapper.
     *
     * \param requestInfo
     *      Initial value for the request buffer.  NULL means don't
     *      set an initial value.
     */
    explicit MockWrapper(const char* requestInfo = NULL)
        : request()
        , response()
        , completedCount(0)
        , failedCount(0)
    {
        if (requestInfo != NULL) {
            request.append(requestInfo,
                           downCast<uint32_t>(strlen(requestInfo)));
        }
    }

    virtual void completed()
    {
        completedCount++;
    }
    virtual void failed()
    {
        failedCount++;
    }

    /**
     * Returns a string indicating how many times the #completed and
     * #failed methods have been invoked. This string is only valid
     * up until the next call to this method.
     */
    char* getState()
    {
        static char buffer[100];
        snprintf(buffer, sizeof(buffer), "completed: %d, failed: %d",
            completedCount, failedCount);
        return buffer;
    }

    void reset()
    {
        completedCount = failedCount = 0;
    }

    /**
     * Set the opcode field in the request buffer.
     *
     * \param opcode
     *      Store this in the opcode field of the request buffer.
     */
    void setOpcode(WireFormat::Opcode opcode) {
        WireFormat::RequestCommon* header;
        if (request.getTotalLength() < sizeof(WireFormat::RequestCommon)) {
            request.reset();
            header = request.emplaceAppend<WireFormat::RequestCommon>();
        } else {
            header = const_cast<WireFormat::RequestCommon*>(
                    request.getStart<WireFormat::RequestCommon>());
        }
        header->opcode = opcode;
    }

    /// Request and response messages.
    Buffer request;
    Buffer response;

    /// Number of times that #completed has been called.
    int completedCount;

    /// Number of times that #failed has been called.
    int failedCount;

    DISALLOW_COPY_AND_ASSIGN(MockWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_MOCKWRAPPER_H
