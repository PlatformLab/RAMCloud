/* Copyright (c) 2011-2012 Stanford University
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

#include <mutex>
#include "Service.h"
#include "ShortMacros.h"
#include "TestLog.h"

namespace RAMCloud {

/**
 * This class provides a trivial service that is used for testing.
 */
class MockService : public Service {
  public:

    explicit MockService(int threadLimit = 3) : mutex(), log(),
            gate(0), sendReply(false),
            threadLimit(threadLimit) { }
    virtual ~MockService() {}
    virtual void dispatch(WireFormat::Opcode opcode, Rpc* rpc)
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            if (!log.empty()) {
                 log.append(", ");
            }
            log.append("rpc: ");
            log.append(TestUtil::toString(rpc->requestPayload));
        }

        // Create a response that increments each of the (integer) values
        // in the request.  Throw errors if certain values appear.
        for (uint32_t i = 0; i < rpc->requestPayload->getTotalLength()-3;
                i += 4) {
            int32_t inputValue = *(rpc->requestPayload->getOffset<int32_t>(i));
            if (inputValue == 54321) {
                throw ClientException(HERE, STATUS_REQUEST_FORMAT_ERROR);
            }
            if (inputValue == 54322) {
                throw RetryException(HERE, 100, 200, "server overloaded");
            }
            rpc->replyPayload->emplaceAppend<int32_t>(inputValue+1);
        }
        int secondWord = *(rpc->requestPayload->getOffset<int>(4));

        // The following code is used to test the sendReply Rpc method.
        // Be careful not to access rpc after this point.
        if (sendReply)
            rpc->sendReply();

        // The following code is used to delay completion of requests.
        while (gate != 0) {
            if (gate == secondWord)
                break;
            usleep(1000);
        }

        // Wait for a random period of time from 1-64 us.  This causes
        // variations in completion order when there are concurrent
        // requests, in the hopes of flushing out any timing problems.
        usleep(downCast<uint32_t>(generateRandom() & 0x3f));
    }
    virtual int maxThreads() {
        return threadLimit;
    }
    virtual void initOnceEnlisted() {
        TEST_LOG("called");
    }

    /// Used to serialize access to #log.
    std::mutex mutex;

    /// Records information about each request dispatched to this service.
    string log;

    /// The following variable is used to delay completion of requests.  If
    /// the variable is nonzero, requests will not complete until the value
    /// of the variable equals the second word of the request (#secondWord
    /// in dispatch).
    int gate;

    /// The following variable may be set to true to cause the service to
    /// invoke sendReply before returning.
    bool sendReply;

    /// Return value from maxThreads.
    int threadLimit;

    DISALLOW_COPY_AND_ASSIGN(MockService);
};

} // end RAMCloud

#endif  // RAMCLOUD_MOCKSERVICE_H
