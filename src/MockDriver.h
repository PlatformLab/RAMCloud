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

/**
 * \file
 * This file defines an implementation of Driver that allows unit
 * tests to run without a network or a remote counterpart (it logs
 * output packets and provides a mechanism for prespecifying input
 * packets).
 */

#include <string>
#include "Driver.h"

#ifndef RAMCLOUD_MOCKDRIVER_H
#define RAMCLOUD_MOCKDRIVER_H

namespace RAMCloud {

class MockDriver : public Driver {
  public:
    typedef string (*HeaderToString)(const void*, uint32_t);

    MockDriver();
    explicit MockDriver(HeaderToString headerToString);
    virtual ~MockDriver() {}

    virtual uint32_t getMaxPayloadSize() { return 1400; }
    virtual void sendPacket(const sockaddr *addr,
                            socklen_t addrlen,
                            void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual bool tryRecvPacket(Received *received);
    virtual void release(char *payload, uint32_t len);

    void setInput(char* msg, uint32_t msgLen);
    static void bufferToString(Buffer *buffer, string& s);
    static void stringToBuffer(const char* s, Buffer* buffer);
    static string bufToHex(const void* buf, uint32_t bufLen);

    /**
     * Records information from each call to send/recv packets.
     */
    string outputLog;

    /**
     * Used as the next input message required by either serverRecv
     * or getReply.
     */
    char* inputMessage;
    uint32_t inputMessageLen;

    // The following variables count calls to various methods, for use
    // by tests.
    uint32_t sendPacketCount;
    uint32_t tryRecvPacketCount;
    uint32_t releaseCount;

    HeaderToString headerToString;

    DISALLOW_COPY_AND_ASSIGN(MockDriver);
};

}  // namespace RAMCloud

#endif
