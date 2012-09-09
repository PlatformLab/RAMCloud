/* Copyright (c) 2010-2012 Stanford University
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

#ifndef RAMCLOUD_MOCKTRANSPORTFACTORY_H
#define RAMCLOUD_MOCKTRANSPORTFACTORY_H

#include "TransportFactory.h"

namespace RAMCloud {

/**
 * This class defines a TransportFactory that wraps any transport class
 * for testing.
 */
class MockTransportFactory : public TransportFactory {
  public:
    /**
     * Construct a MockTransportFactory.
     *
     * \param context
     *      Overall information about the RAMCloud server
     * \param transport
     *      Transport (owned by the caller) to return whenever
     *      is invoked #createTransport.  If NULL, a new MockTransport
     *      will be created by each call to #createTransport.
     * \param protocol
     *      Protocol supported by #transport.
     */
    MockTransportFactory(Context* context, Transport* transport,
                         const char* protocol = "mock")
        : TransportFactory(protocol)
        , context(context)
        , transport(transport)
        , protocol(protocol)
    {}
    Transport* createTransport(Context* context,
                               const ServiceLocator* local) {
        if (strcmp(protocol, "error") == 0) {
            RAMCLOUD_TEST_LOG("exception thrown");
            throw TransportException(HERE, "boom!");
        }
        if (transport == NULL) {
            return new MockTransport(context, local);
        }
        return transport;
    }
    Context* context;
    Transport* transport;
    const char* protocol;
    DISALLOW_COPY_AND_ASSIGN(MockTransportFactory);
};

} // end RAMCloud

#endif  // RAMCLOUD_MOCKTRANSPORTFACTORY_H
