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
 * Header file for #RAMCloud::InfUdDriver.
 */

#ifndef RAMCLOUD_INFUDDRIVER_H
#define RAMCLOUD_INFUDDRIVER_H

#include <boost/pool/object_pool.hpp>

#include "Common.h"
#include "Driver.h"
#include "Infiniband.h"
#include "ObjectTub.h"

namespace RAMCloud {

/**
 * A Driver for Infiniband unreliable datagram (UD) communication.
 * Simple packet send/receive style interface. See Driver for more detail.
 */
template<typename Infiniband = RealInfiniband>
class InfUdDriver : public Driver {
    typedef typename Infiniband::BufferDescriptor BufferDescriptor;
    typedef typename Infiniband::QueuePairTuple QueuePairTuple;
    typedef typename Infiniband::QueuePair QueuePair;
    typedef typename Infiniband::Address Address;

  public:
    /// The maximum number bytes we can stuff in a UDP packet payload.
    static const uint32_t MAX_PAYLOAD_SIZE = 1024;

    explicit InfUdDriver(const ServiceLocator* localServiceLocator = NULL);
    virtual ~InfUdDriver();
    virtual uint32_t getMaxPacketSize();
    virtual void release(char *payload, uint32_t len);
    virtual void sendPacket(const Driver::Address *addr,
                            const void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual bool tryRecvPacket(Received *received);
    virtual ServiceLocator getServiceLocator();

    virtual Address* newAddress(const ServiceLocator& serviceLocator) {
        return new Address(*infiniband, ibPhysicalPort, serviceLocator);
    }

  private:
    static const uint32_t MAX_RX_QUEUE_DEPTH = 64;
    static const uint32_t MAX_TX_QUEUE_DEPTH = 1;
    static const uint32_t MAX_RX_SGE_COUNT = 1;
    static const uint32_t MAX_TX_SGE_COUNT = 1;
    static const uint32_t QKEY = 0xdeadbeef;

    /**
     * Structure to hold an incoming packet.
     */
    struct PacketBuf {
        PacketBuf() : infAddress() {}
        /**
         * Address of sender (used to send reply).
         */
        ObjectTub<Address> infAddress;
        /**
         * Packet data (may not fill all of the allocated space).
         */
        char payload[MAX_PAYLOAD_SIZE];
    };

    /// See #infiniband.
    ObjectTub<Infiniband> realInfiniband;

    /**
     * Used by this class to make all Infiniband verb calls.  In normal
     * production use it points to #realInfiniband; for testing it points to a
     * mock object.
     */
    Infiniband* infiniband;

    ibv_cq*                rxcq;           // verbs rx completion queue
    ibv_cq*                txcq;           // verbs tx completion queue
    QueuePair* qp;             // verbs queue pair wrapper

    /// Holds packet buffers that are no longer in use, for use any future
    /// requests; saves the overhead of calling malloc/free for each request.
    boost::object_pool<PacketBuf> packetBufPool;

    /// Number of current allocations from packetBufPool.
    uint64_t            packetBufsUtilized;

    /// Infiniband receive buffers, written directly by the HCA.
    BufferDescriptor    rxBuffers[MAX_RX_QUEUE_DEPTH];
    int                 currentRxBuffer;

    /// Sole infiniband transmit buffer.
    BufferDescriptor    txBuffer;

    int ibPhysicalPort;                 // our HCA's physical port index
    int lid;                            // our infiniband local id
    int qpn;                            // our queue pair number

    /// Our ServiceLocator, including the dynamic lid and qpn
    string              locatorString;

    DISALLOW_COPY_AND_ASSIGN(InfUdDriver);
};

extern template class InfUdDriver<RealInfiniband>;

} // end RAMCloud

#endif  // RAMCLOUD_INFUDDRIVER_H
