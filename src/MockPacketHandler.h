/* Copyright (c) 2016 Stanford University
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

#include "Driver.h"

#ifndef RAMCLOUD_MOCKPACKETHANDLER_H
#define RAMCLOUD_MOCKPACKETHANDLER_H

namespace RAMCloud {

/**
 * This class is used for testing drivers; it accepts callbacks from drivers
 * and makes the packet data available for examination in tests.
 */
class MockPacketHandler : public Driver::IncomingPacketHandler {
  public:
    explicit MockPacketHandler(Driver *driver)
        : driver(driver)
        , packetData()
        , sender(NULL)
    {
        driver->connect(this);
    }

    ~MockPacketHandler() {
        delete sender;
    }

    void handlePacket(Driver::Received* received)
    {
        if (packetData.size() != 0) {
            packetData.append(", ");
        }
        packetData.append(received->payload, received->len);
        sender = received->sender->clone();
    }

    // Used to wait for data to arrive on a driver by invoking the
    // dispatcher's polling loop; gives up if a long time goes by with
    // no data. Returns the contents of the incoming packet.
    const char *receivePacket(Context* context) {
        packetData.clear();
        uint64_t start = Cycles::rdtsc();
        while (true) {
            context->dispatch->poll();
            if (packetData.size() != 0) {
                return packetData.c_str();
            }
            if (Cycles::toSeconds(Cycles::rdtsc() - start) > .1) {
                return "no packet arrived";
            }
        }
    }

    Driver* driver;
    string packetData;
    const Driver::Address *sender;
  private:
    DISALLOW_COPY_AND_ASSIGN(MockPacketHandler);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_MOCKPACKETHANDLER_H
