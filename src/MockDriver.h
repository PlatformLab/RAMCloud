/* Copyright (c) 2010 Stanford University
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

#include <string>
#include "Driver.h"

#ifndef RAMCLOUD_MOCKDRIVER_H
#define RAMCLOUD_MOCKDRIVER_H

namespace RAMCloud {

/**
 * A Driver that allows unit tests to run without a network or a
 * remote counterpart.  It logs output packets and provides a mechanism for
 * prespecifying input packets.
 */
class MockDriver : public Driver {
  public:
    /// The type of a customer header serializer.  See headerToString.
    typedef string (*HeaderToString)(const void*, uint32_t);

    MockDriver();
    explicit MockDriver(HeaderToString headerToString);
    virtual ~MockDriver() {}
    static void bufferToString(Buffer *buffer, string* const s);
    static string bufToHex(const void* buf, uint32_t bufLen);
    virtual uint32_t getMaxPayloadSize() { return 1400; }
    virtual void release(char *payload, uint32_t len);
    virtual void sendPacket(const sockaddr *addr,
                            socklen_t addrlen,
                            void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    void setInput(Driver::Received* received);
    virtual bool tryRecvPacket(Received *received);

    /**
     * A function that serializes the header using a specific string format.
     * Headers aren't included in the log string if this is NULL.
     */
    HeaderToString headerToString;

    /**
     * Used as the next input message required by either serverRecv
     * or getReply.
     */
    Driver::Received* inputReceived;

    /**
     * Records information from each call to send/recv packets.
     */
    string outputLog;

    // The following variables count calls to various methods, for use
    // by tests.
    uint32_t sendPacketCount;
    uint32_t tryRecvPacketCount;
    uint32_t releaseCount;

    DISALLOW_COPY_AND_ASSIGN(MockDriver);
};

}  // namespace RAMCloud

#endif
