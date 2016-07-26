/* Copyright (c) 2010-2016 Stanford University
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

#include <sys/socket.h>
#include <vector>

#include "Dispatch.h"
#include "Driver.h"
#include "IpAddress.h"
#include "ObjectPool.h"
#include "QueueEstimator.h"
#include "Syscall.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A Driver for kernel-provided UDP communication.  Simple packet send/receive
 * style interface. See Driver for more detail.
 */
class UdpDriver : public Driver {
  public:
    /// The maximum number bytes we can stuff in a UDP packet payload.
    static const uint32_t MAX_PAYLOAD_SIZE = 1400;

    explicit UdpDriver(Context* context,
                       const ServiceLocator* localServiceLocator = NULL);
    virtual ~UdpDriver();
    void close();
    virtual uint32_t getMaxPacketSize();
    virtual int getTransmitQueueSpace(uint64_t currentTime);
    virtual void receivePackets(int maxPackets,
            std::vector<Received>* receivedPackets);
    virtual void release(char *payload);
    virtual void sendPacket(const Address *addr,
                            const void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual string getServiceLocator();

    virtual Address* newAddress(const ServiceLocator* serviceLocator) {
        return new IpAddress(serviceLocator);
    }

    /**
     * Structure to hold an incoming packet.
     */
    struct PacketBuf {
        PacketBuf() : ipAddress(), iovec()
        {
            iovec.iov_base = payload;
            iovec.iov_len = MAX_PAYLOAD_SIZE;
        }

        /// Address of sender (used to  send reply).
        IpAddress ipAddress;

        /// Tells kernel call where to place incoming packet data.
        struct iovec iovec;

        /// Packet data (may not fill all of the allocated space).
        char payload[MAX_PAYLOAD_SIZE];
    };

    /// Shared RAMCloud information.
    Context* context;

    /// File descriptor of the UDP socket this driver uses for communication.
    /// -1 means socket was closed because of error.
    int socketFd;

    /// Maximum number of incoming packets we are prepared to receive in
    /// a single kernel call.
    static const int MAX_PACKETS_AT_ONCE = 10;

    /// Holds the arguments and results from a call to recvmmsg; see the
    /// man page for that kernel call for details.
    struct mmsghdr messageHeaders[MAX_PACKETS_AT_ONCE];

    /// Entries in this array correspond to those in messageHeaders; they
    /// hold the buffers that will be returned to transports.  NULL means
    /// no PacketBuf has been allocated in that slot; it also means that
    /// the corresponding slot in messageHeaders is uninitialized. If there
    /// are NULL entries, they are always adjacent and occupy the first
    /// slots in the array.
    PacketBuf* buffers[MAX_PACKETS_AT_ONCE];

    /// Holds packet buffers that are no longer in use, for use in future
    /// requests; saves the overhead of calling malloc/free for each request.
    ObjectPool<PacketBuf> packetBufPool;

    /// Tracks number of outstanding allocated payloads, including those
    /// currently reference in buffers as well as those that have been
    /// returned to transports.  For detecting leaks.
    int packetBufsUtilized;

    /// Counts the number of packet buffers freed during destructors;
    /// used primarily for testing.
    static int packetBufsFreed;

    static Syscall* sys;

    /// The original ServiceLocator string. May be empty if the constructor
    /// argument was NULL. May also differ if dynamic ports are used.
    string locatorString;

    // Effective network bandwidth, in Gbits/second.
    int bandwidthGbps;

    /// Used to estimate # bytes outstanding in the NIC's transmit queue.
    QueueEstimator queueEstimator;

    /// Upper limit on how many bytes should be queued for transmission
    /// at any given time.
    uint32_t maxTransmitQueueSize;

    DISALLOW_COPY_AND_ASSIGN(UdpDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_UDPDRIVER_H
