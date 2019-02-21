/* Copyright (c) 2010-2017 Stanford University
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
#include "SpinLock.h"
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
    virtual void receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets);
    virtual void release();
    virtual void sendPacket(const Address* addr,
                            const void* header,
                            uint32_t headerLen,
                            Buffer::Iterator* payload,
                            int priority = 0,
                            TransmitQueueState* txQueueState = NULL);
    virtual string getServiceLocator();

    virtual Address* newAddress(const ServiceLocator* serviceLocator) {
        return new IpAddress(serviceLocator);
    }

  PROTECTED:
    static void readerThreadMain(UdpDriver* driver);
    void stopReaderThread();

    struct PacketBuf : Driver::PacketBuf<IpAddress, MAX_PAYLOAD_SIZE> {
        PacketBuf()
            : Driver::PacketBuf<IpAddress, MAX_PAYLOAD_SIZE>()
            , iovec{payload, MAX_PAYLOAD_SIZE}
        {}

        /// Tells kernel call where to place incoming packet data.
        struct iovec iovec;
    };

    /**
     * The structure is used by a background thread to receive a group of
     * incoming packets from the kernel; it is then used to pass those
     * packets from the background thread to UdpDriver:: receivePackets.
     */
    struct PacketBatch {
        /// Maximum number of packets that this structure can hold at
        /// once; this is also the maximum number of packets we can receive
        /// from the kernel in a single kernel call.
        static const int MAX_PACKETS = 10;

        /// Number of packets that have were received from the kernel.
        /// 0 means all of the packets in the last batch have been returned by
        /// receivePackets; it's now up to the background thread to receive
        /// another batch of packets from the kernel. This variable is used
        /// for synchronization between the dispatch thread and the background
        /// thread, so Fences typically need to be placed after each read
        /// and write.
        Atomic<int> packetsAvailable;

        /// Number of packets that have been returned by receivePackets.
        /// Not used by the background thread.
        int packetsRemoved;

        /// Holds the arguments and results from the most recent call to
        /// recvmmsg; see the man page for that kernel call for details.
        struct mmsghdr messageHeaders[MAX_PACKETS];

        /// Entries in this array correspond to those in messageHeaders; they
        /// hold the buffers that will be returned to transports.  NULL means
        /// no PacketBuf has been allocated in that slot; it also means that
        /// the corresponding slot in messageHeaders is uninitialized. If there
        /// are NULL entries, they are always adjacent and occupy the first
        /// slots in the array.
        PacketBuf* buffers[MAX_PACKETS];

        PacketBatch()
            : packetsAvailable(0)
            , packetsRemoved(0)
            , messageHeaders()
            , buffers()
        {
            for (int i = 0; i < MAX_PACKETS; i++) {
                buffers[i] = NULL;
            }
        }
    };

    /// File descriptor of the UDP socket this driver uses for communication.
    /// -1 means socket was closed because of error.
    int socketFd;

    /// Keeping two of these structures allows the background thread to
    /// read the next batch of packets while the dispatch thread is processing
    /// the previous batch of packets.
    PacketBatch packetBatches[2];

    /// 0 or 1: indicates which element of packetGroups will be processed
    /// next by the dispatch thread.
    int currentBatch;

    /// Holds packet buffers that are no longer in use, for use in future
    /// requests; saves the overhead of calling malloc/free for each request.
    ObjectPool<PacketBuf> packetBufPool;

    /// Used to synchronize accesses to packetBufPool.
    SpinLock mutex;

    /// Counts the number of packet buffers freed during destructors;
    /// used primarily for testing.
    static int packetBufsFreed;

    static Syscall* sys;

    /// The original ServiceLocator string. May be empty if the constructor
    /// argument was NULL. May also differ if dynamic ports are used.
    string locatorString;

    // Effective network bandwidth, in Gbits/second.
    int bandwidthGbps;

    /// The following thread runs in the background to wait for kernel calls
    /// that receive packets.
    Tub<std::thread> readerThread;

    /// If the reader thread ever sees a true value in this variable, it
    /// will exit immediately.
    bool readerThreadExit;

    DISALLOW_COPY_AND_ASSIGN(UdpDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_UDPDRIVER_H
