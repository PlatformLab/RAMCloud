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

#include "FastTransport.h"

#ifndef RAMCLOUD_MOCKFASTTRANSPORT_H
#define RAMCLOUD_MOCKFASTTRANSPORT_H

namespace RAMCloud {

/**
 * This class is used for testing drivers; it accepts callbacks from drivers
 * and makes the packet data available for examination in tests.
 */
class MockFastTransport : public FastTransport {
  public:
    explicit MockFastTransport(Context* context, Driver *driver)
            : FastTransport(context, driver), packetData(), sender(NULL) { }
    ~MockFastTransport() {
        delete sender;
    }
    void handleIncomingPacket(Driver::Received *received) {
        if (packetData.size() != 0) {
            packetData.append(", ");
        }
        packetData.append(received->payload, received->len);
        sender = received->sender->clone();
    }
    string packetData;
    const Driver::Address *sender;
  private:
    DISALLOW_COPY_AND_ASSIGN(MockFastTransport);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_MOCKFASTTRANSPORT_H
