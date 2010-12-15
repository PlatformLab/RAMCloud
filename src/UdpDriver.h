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

#ifndef RAMCLOUD_UDPDRIVER_H
#define RAMCLOUD_UDPDRIVER_H

#include <boost/pool/pool.hpp>
#include <vector>

#include "Common.h"
#include "Driver.h"
#include "IpAddress.h"

namespace RAMCloud {

/**
 * A Driver for kernel-provided UDP communication.  Simple packet send/receive
 * style interface. See Driver for more detail.
 */
class UdpDriver : public Driver {
  public:
    /// The maximum number bytes we can stuff in a UDP packet payload.
    static const uint32_t MAX_PAYLOAD_SIZE = 1400;

    explicit UdpDriver(const ServiceLocator* localServiceLocator = NULL);
    virtual ~UdpDriver();
    virtual uint32_t getMaxPacketSize();
    virtual void release(char *payload, uint32_t len);
    virtual void sendPacket(const Address *addr,
                            const void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual bool tryRecvPacket(Received *received);

    virtual Address* newAddress(const ServiceLocator& serviceLocator) {
        return new IpAddress(serviceLocator);
    }

    /**
     * Structure to hold an incoming packet.
     */
    struct PacketBuf {
        PacketBuf() : ipAddress() {}
        IpAddress ipAddress;                   /// Address of sender (used to
                                               /// send reply).
        char payload[MAX_PAYLOAD_SIZE];        /// Packet data (may not fill all
                                               /// of the allocated space).
    };

    /// File descriptor of the UDP socket this driver uses for communication.
    int socketFd;

    /// Holds packet buffers that are no longer in use, for use any future
    /// requests; saves the overhead of calling malloc/free for each request.
    boost::pool<> packetBufPool;

    /// Tracks number of outstanding allocated payloads.  For detecting leaks.
    int packetBufsUtilized;

    /// Counts the number of packet buffers freed during destructors;
    /// used primarily for testing.
    static int packetBufsFreed;

    DISALLOW_COPY_AND_ASSIGN(UdpDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_UDPDRIVER_H
