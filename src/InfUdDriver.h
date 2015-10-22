/* Copyright (c) 2010-20121 Stanford University
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
#include "SpinLock.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A Driver for Infiniband unreliable datagram (UD) communication.
 * Simple packet send/receive style interface. See Driver for more detail.
 */
class InfUdDriver : public Driver {
    typedef Infiniband::BufferDescriptor BufferDescriptor;
    typedef Infiniband::QueuePairTuple QueuePairTuple;
    typedef Infiniband::QueuePair QueuePair;
    typedef Infiniband::Address Address;
    typedef Infiniband::RegisteredBuffers RegisteredBuffers;

  public:
    explicit InfUdDriver(Context* context,
                         const ServiceLocator* localServiceLocator,
                         bool ethernet);
    virtual ~InfUdDriver();
    virtual void connect(IncomingPacketHandler* incomingPacketHandler);
    virtual void disconnect();
    virtual void dumpStats() { infiniband->dumpStats(); }
    virtual uint32_t getMaxPacketSize();
    virtual void release(char *payload);
    virtual void sendPacket(const Driver::Address *addr,
                            const void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual string getServiceLocator();

    virtual Driver::Address* newAddress(const ServiceLocator* serviceLocator) {
        if (localMac) {
            return new MacAddress(
                serviceLocator->getOption<const char*>("mac"));
        } else {
            return new Address(*infiniband, ibPhysicalPort, serviceLocator);
        }
    }

  private:
    BufferDescriptor* getTransmitBuffer();

    static const uint32_t MAX_RX_QUEUE_DEPTH = 64;
    static const uint32_t MAX_TX_QUEUE_DEPTH = 8;
    static const uint32_t MAX_RX_SGE_COUNT = 1;
    static const uint32_t MAX_TX_SGE_COUNT = 1;
    // see comment at top of src/InfUdDriver.cc
    static const uint32_t GRH_SIZE = 40;

    struct EthernetHeader {
        uint8_t destAddress[6];
        uint8_t sourceAddress[6];
        uint16_t etherType;         // network order
        uint16_t length;            // host order, length of payload,
                                    // used to drop padding from end of short
                                    // packets
    } __attribute__((packed));

    /**
     * Structure to hold an incoming packet.
     */
    struct PacketBuf {
        PacketBuf() : infAddress(), macAddress() {}
        /**
         * Address of sender (used to send reply).
         */
        Tub<Address> infAddress;
        Tub<MacAddress> macAddress;
        /**
         * Packet data (may not fill all of the allocated space).
         */
        char payload[2048 - GRH_SIZE];
    };

    /// Shared RAMCloud information.
    Context* context;

    /// See #infiniband.
    Tub<Infiniband> realInfiniband;

    /**
     * Used by this class to make all Infiniband verb calls.  In normal
     * production use it points to #realInfiniband; for testing it points to a
     * mock object.
     */
    Infiniband* infiniband;

    const uint32_t QKEY;

    ibv_cq*                rxcq;           // verbs rx completion queue
    ibv_cq*                txcq;           // verbs tx completion queue
    QueuePair* qp;             // verbs queue pair wrapper

    /// Holds packet buffers that are no longer in use, for use any future
    /// requests; saves the overhead of calling malloc/free for each request.
    ObjectPool<PacketBuf> packetBufPool;

    /// Number of current allocations from packetBufPool.
    uint64_t            packetBufsUtilized;

    /// Must be held when manipulating packetBufPool or packetBufsUtilized
    /// (allows the release method to run in worker threads without
    /// acquiring the Dispatch lock).
    SpinLock mutex;
    typedef std::unique_lock<SpinLock> Lock;

    /// Infiniband receive buffers, written directly by the HCA.
    Tub<RegisteredBuffers> rxBuffers;

    /// Infiniband transmit buffers
    Tub<RegisteredBuffers> txBuffers;

    /// Infiniband transmit buffers that are not currently in use
    vector<BufferDescriptor*> freeTxBuffers;

    int ibPhysicalPort;                 // our HCA's physical port index
    int lid;                            // our infiniband local id
    int qpn;                            // our queue pair number
    Tub<MacAddress> localMac;           // our MAC address (if Ethernet)

    /// Our ServiceLocator, including the dynamic lid and qpn
    string              locatorString;

    /// Handler to invoke whenever packets arrive.
    /// NULL means #connect hasn't been called yet.
    std::unique_ptr<IncomingPacketHandler> incomingPacketHandler;

    /**
     * The following object is invoked by the dispatcher's polling loop;
     * it reads incoming packets and passes them on to #transport.
     */
    class Poller : public Dispatch::Poller {
      public:
        explicit Poller(Context* context, InfUdDriver* driver)
            : Dispatch::Poller(context->dispatch, "InfUdDriver::Poller")
            , driver(driver) { }
        virtual int poll();
      private:
        // Driver on whose behalf this poller operates.
        InfUdDriver* driver;
        DISALLOW_COPY_AND_ASSIGN(Poller);
    };
    Tub<Poller> poller;

    DISALLOW_COPY_AND_ASSIGN(InfUdDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_INFUDDRIVER_H
