/* Copyright (c) 2015 Stanford University
 * Copyright (c) 2014-2015 Huawei Technologies Co. Ltd.
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

#ifndef RAMCLOUD_DPDKDRIVER_H
#define RAMCLOUD_DPDKDRIVER_H

#include <vector>

#include "FastTransport.h"
#include "MacAddress.h"
#include "ObjectPool.h"
#include "NetUtil.h"
#include "ServiceLocator.h"
#include "Tub.h"

// number of descriptors to allocate for the tx/rx rings
#define NDESC 256
// maximum number of packet buffers that the memory pool can hold
#define NB_MBUF 8192
// per-element size for the packet buffer memory pool
#define MBUF_SIZE (2048 + sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM)

//Forward declarations, so we don't have to include DPDK headers here.
struct rte_mempool;
struct rte_ring;

namespace RAMCloud
{

/**
 * A Driver for  DPDK communication.  Simple packet send/receive
 * style interface. See Driver.h for more detail.
 */

class DpdkDriver : public Driver
{
  public:
    static const uint32_t MAX_PAYLOAD_SIZE = 1400;
    friend class Poller;

    explicit DpdkDriver(Context* context,
                        const ServiceLocator* localServiceLocator = NULL);
    virtual ~DpdkDriver();
    void close();
    virtual void connect(IncomingPacketHandler* incomingPacketHandler);
    virtual void disconnect();
    virtual uint32_t getMaxPacketSize();
    virtual void release(char *payload);
    virtual void sendPacket(const Address *addr,
                            const void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual string getServiceLocator();

    /**
     * Structure to hold an incoming packet.
     */
    struct PacketBuf
    {
        Tub<MacAddress> dpdkAddress;           /// Address of sender (used to
                                               /// send reply).
        char payload[MAX_PAYLOAD_SIZE];        /// Packet data.
        PacketBuf() : dpdkAddress(), payload() { }
    };

    virtual Address* newAddress(const ServiceLocator* serviceLocator)
    {
        return new MacAddress(serviceLocator->getOption<const char*>("mac"));
    }

    Context* context;

    /// Handler to invoke whenever packets arrive.
    std::unique_ptr<IncomingPacketHandler> incomingPacketHandler;

    /**
     * An event handler that reads incoming packets and passes them on to
     * #transport.
     */

    class Poller : public Dispatch::Poller {
      public:
        explicit Poller(Context* context, DpdkDriver* driver)
            : Dispatch::Poller(context->dispatch, "DpdkDriver::Poller"),
            driver(driver) {}

        virtual int poll();
      private:

        // Driver on whose behalf this poller operates.
        DpdkDriver* driver;
        DISALLOW_COPY_AND_ASSIGN(Poller);
    };
    Tub<Poller> poller;

    /// Holds packet buffers that are no longer in use, for use in future
    /// requests; saves the overhead of calling malloc/free for each request.
    ObjectPool<PacketBuf> packetBufPool;

    /// Tracks number of outstanding allocated payloads.  For detecting leaks.
    int packetBufsUtilized;

    /// Counts the number of packet buffers freed during destructors;
    /// used primarily for testing.
    static int packetBufsFreed;

    /// The original ServiceLocator string. May be empty if the constructor
    /// argument was NULL. May also differ if dynamic ports are used.
    string locatorString;

    /// Stores the MAC address of the NIC (either native or overriden).
    Tub<MacAddress> localMac;

    /// Stores the NIC's physical port id addressed by the instantiated driver.
    uint8_t portId;

    /// Holds packet buffers that are dequeued from the NIC's HW queues
    /// via DPDK.
    struct rte_mempool *packetPool;

    /// Holds packets that are addressed to localhost instead of going through
    /// the HW queues.
    struct rte_ring *loopbackRing;

    DISALLOW_COPY_AND_ASSIGN(DpdkDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_DPDKDRIVER_H
