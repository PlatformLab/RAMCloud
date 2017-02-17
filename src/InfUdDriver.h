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

/**
 * \file
 * Header file for #RAMCloud::InfUdDriver.
 */

#ifndef RAMCLOUD_INFUDDRIVER_H
#define RAMCLOUD_INFUDDRIVER_H

#include "Common.h"
#include "Dispatch.h"
#include "Driver.h"
#include "Infiniband.h"
#include "MacAddress.h"
#include "ObjectPool.h"
#include "QueueEstimator.h"
#include "SpinLock.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A Driver for Infiniband unreliable datagram (UD) communication.
 * Simple packet send/receive style interface. See Driver for more detail.
 */
class InfUdDriver : public Driver {
    typedef Infiniband::QueuePair QueuePair;
    typedef Infiniband::Address Address;

  public:
    explicit InfUdDriver(Context* context,
            const ServiceLocator* localServiceLocator, bool ethernet);
    virtual ~InfUdDriver();
    virtual void dumpStats() { infiniband->dumpStats(); }
    virtual uint32_t getMaxPacketSize();
    virtual uint32_t getBandwidth();
    virtual int getTransmitQueueSpace(uint64_t currentTime);
    virtual void receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets);
    virtual void registerMemory(void* base, size_t bytes);
    virtual void release(char *payload);
    virtual void sendPacket(const Driver::Address* addr, const void* header,
                            uint32_t headerLen, Buffer::Iterator* payload,
                            int priority = 0);
    virtual string getServiceLocator();

    virtual Driver::Address* newAddress(const ServiceLocator* serviceLocator) {
        if (localMac) {
            return new MacAddress(
                serviceLocator->getOption<const char*>("mac"));
        } else {
            return new Address(*infiniband, ibPhysicalPort, serviceLocator);
        }
    }

  PRIVATE:

    /**
     * Stores information about a single packet buffer (used for both
     * transmit and receive buffers).
     */
    struct BufferDescriptor {
        /// First byte of the packet buffer.
        char* buffer;

        /// Length of the buffer, in bytes.
        uint32_t length;

        /// Infiniband memory region in which buffer is allocated.
        ibv_mr* memoryRegion;

        // Fields above here do not change once this structure has been
        // allocated. Fields below are modified based on the buffer's usage,
        // and may not always be valid.

        /// If the buffer currently holds a packet, this gives the length
        /// of that packet, in bytes.
        uint32_t packetLength;

        /// Unique Infiniband identifier for the HCA to which the packet will
        /// be sent (or from which the packet was received).
        uint16_t remoteLid;

        /// One of the following addresses is used for each received packet
        /// to identify the sender. Not used for transmitted packets.
        Tub<Address> infAddress;
        Tub<MacAddress> macAddress;

        BufferDescriptor(char *buffer, uint32_t length, ibv_mr *region)
            : buffer(buffer), length(length), memoryRegion(region),
              packetLength(0), remoteLid(0), infAddress(), macAddress() {}

      private:
        DISALLOW_COPY_AND_ASSIGN(BufferDescriptor);
    };

    /**
     * Represents a collection of buffers allocated in a memory region
     * that has been registered with the HCA.
     */
    struct BufferPool {
        BufferPool(Infiniband* infiniband, uint32_t bufferSize,
                uint32_t numBuffers);
        ~BufferPool();

        /// Dynamically allocated memory for the buffers (must be freed).
        char *bufferMemory;

        /// HCA region associated with bufferMemory.
        ibv_mr* memoryRegion;

        /// Dynamically allocated array holding one descriptor for each
        /// packet buffer in bufferMemory, in the same order as the
        /// corresponding packet buffers.
        BufferDescriptor* descriptors;

        /// Buffers that are currently unused.
        vector<BufferDescriptor*> freeBuffers;

        /// Total number of buffers (and descriptors) allocated.)
        uint32_t numBuffers;

        DISALLOW_COPY_AND_ASSIGN(BufferPool);
    };

    BufferDescriptor* getTransmitBuffer();
    void reapTransmitBuffers();
    void refillReceiver();

    /// Total receive buffers allocated. At any given time, some may be in
    /// the possession of the HCA, some (holding received data) may be in
    /// the possession of higher-level software processing requests, and
    /// some may be idle (in freeRxBuffers). We need a *lot* of these,
    /// if we're going to handle multiple 8-MB incoming RPCs at once.
    static const uint32_t TOTAL_RX_BUFFERS = 50000;

    /// Maximum number of receive buffers that will be in the possession
    /// of the HCA at once.
    static const uint32_t MAX_RX_QUEUE_DEPTH = 1000;

    /// Maximum number of transmit buffers that may be outstanding at once.
    static const uint32_t MAX_TX_QUEUE_DEPTH = 50;

    /*
     * Note that in UD mode, Infiniband receivers prepend a 40-byte
     * Global Routing Header (GRH) to all incoming frames. Immediately
     * following is the data transmitted. The interface is not symmetric:
     * Sending applications do not include a GRH in the buffers they pass
     * to the HCA.
     */
    static const uint32_t GRH_SIZE = 40;

    struct EthernetHeader {
        uint8_t destAddress[6];
        uint8_t sourceAddress[6];
        uint16_t etherType;         // network order
        uint16_t length;            // host order, length of payload,
                                    // used to drop padding from end of short
                                    // packets
    } __attribute__((packed));

    /// Shared RAMCloud information.
    Context* context;

    /// See #infiniband.
    Tub<Infiniband> realInfiniband;

    /// Used by this class to make all Infiniband verb calls.  In normal
    /// production use it points to #realInfiniband; for testing it points to a
    /// mock object.
    Infiniband* infiniband;

    /// Packet buffers used for receiving incoming packets.
    Tub<BufferPool> rxPool;

    /// Must be held whenever accessing rxPool.freeBuffers: it is shared
    /// between worker threads (returning packet buffers when they are
    /// finished) and the dispatch thread (moving buffers from there to the
    /// HCA).
    SpinLock mutex;

    /// Number of receive buffers currently in the possession of the
    /// HCA.
    uint32_t rxBuffersInHca;

    /// Used to log messages when receive buffer usage hits a new high.
    /// Log the next message when the number of free receive buffers
    /// drops to this level.
    uint32_t rxBufferLogThreshold;

    /// Packet buffers used to transmit outgoing packets.
    Tub<BufferPool> txPool;

    /// This value appears to be used for security. Each outgoing datagram
    /// contains a QKey, which must match a QKey values stored with the
    /// receiving queue pair.  This driver simply hard-wires this value.
    const uint32_t QKEY;

    /// Completion queue for receiving incoming packets.
    ibv_cq* rxcq;

    /// Completion queue used by the NIC to return buffers for
    /// transmitted packets.
    ibv_cq* txcq;

    /// Queue pair object associated with rxcq and txcq.
    QueuePair* qp;

    /// Physical port on the HCA used by this transport.
    int ibPhysicalPort;

    /// Identifies our HCA uniquely among all those in the Infiband
    /// network; roughly equivalent to a host address.
    int lid;

    /// Unique identifier for qp, among all queue pairs allocated by
    /// this machine.
    int qpn;

    /// Our MAC address, if we're using an Ethernet port. If this has not
    /// been constructed, it means we are using an Infiniband port, not
    /// Ethernet.
    Tub<MacAddress> localMac;

    /// Our ServiceLocator, including the dynamic lid and qpn
    string locatorString;

    // Effective network bandwidth, in Gbits/second.
    int bandwidthGbps;

    /// Used to estimate # bytes outstanding in the NIC's transmit queue.
    QueueEstimator queueEstimator;

    /// Upper limit on how many bytes should be queued for transmission
    /// at any given time.
    uint32_t maxTransmitQueueSize;

    /// Address of the first byte of the "zero-copy region". This is an area
    /// of memory that is addressable directly by the HCA. When transmitting
    /// data from this region, we don't need to copy the data into packet
    /// buffers; we can point the HCA at the memory directly. NULL if no
    /// zero-copy region.
    char* zeroCopyStart;

    /// Address of the byte just after the last one of the zero-copy region.
    /// NULL if no zero-copy region.
    char* zeroCopyEnd;

    /// Infiniband memory region associated with the zero-copy region, or
    /// NULL if there is no zero-copy region.
    ibv_mr* zeroCopyRegion;

    /// Used to invoke reapTransmitBuffers after every Nth packet is sent.
    int sendsSinceLastReap;

    DISALLOW_COPY_AND_ASSIGN(InfUdDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_INFUDDRIVER_H
