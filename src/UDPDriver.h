/* Copyright (c) 2010 Stanford University
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

/**
 * \file
 * Header file for #RAMCloud::UDPDriver.
 */

#ifndef RAMCLOUD_UDPDRIVER_H
#define RAMCLOUD_UDPDRIVER_H

#include "Common.h"
#include "Driver.h"

namespace RAMCloud {

class UDPDriver : public Driver {
  public:
    static const uint32_t MAX_PAYLOAD_SIZE = 1400;
    virtual uint32_t getMaxPayloadSize();
    virtual void sendPacket(const sockaddr *addr,
                            socklen_t addrlen,
                            void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual bool tryRecvPacket(Received *received);
    virtual void release(char *payload, uint32_t len);
    UDPDriver();
    UDPDriver(const sockaddr *addr, socklen_t addrlen);
    virtual ~UDPDriver();
  private:
    void send(const Buffer* payload);
    int socketFd;
    int packetBufsUtilized;
    DISALLOW_COPY_AND_ASSIGN(UDPDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_UDPDRIVER_H
