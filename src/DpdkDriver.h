/* Copyright (c) 2015-2017 Stanford University
 * Copyright (c) 2014-2015 Huawei Technologies Co. Ltd.
 * Copyright (c) 2014-2016 NEC Corporation
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

#include "Dispatch.h"
#include "Driver.h"
#include "FileLogger.h"
#include "MacAddress.h"
#include "ObjectPool.h"
#include "QueueEstimator.h"
#include "ServiceLocator.h"
#include "Tub.h"

// Number of descriptors to allocate for the tx/rx rings
#define NDESC 256
// Maximum number of packet buffers that the memory pool can hold. The
// documentation of `rte_mempool_create` suggests that the optimum value
// (in terms of memory usage) of this number is a power of two minus one.
#define NB_MBUF 8191
// per-element size for the packet buffer memory pool
#define MBUF_SIZE (2048 + static_cast<uint32_t>(sizeof(struct rte_mbuf)) \
                   + RTE_PKTMBUF_HEADROOM)

// Forward declarations, so we don't have to include DPDK headers here.
struct rte_mempool;
struct rte_ring;

namespace RAMCloud
{

/**
 * A Driver for DPDK communication. Simple packet send/receive
 * style interface. See Driver.h for more detail.
 */

class DpdkDriver : public Driver
{
  public:
#if TESTING
    explicit DpdkDriver();
#endif
    explicit DpdkDriver(Context* context, int port = 0);
    virtual ~DpdkDriver();
    virtual int getHighestPacketPriority();
    virtual uint32_t getMaxPacketSize();
    virtual uint32_t getBandwidth();
    virtual uint32_t getPacketOverhead();
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

    virtual Address* newAddress(const ServiceLocator* serviceLocator)
    {
        return new MacAddress(serviceLocator->getOption<const char*>("mac"));
    }

  PRIVATE:
    /// The MTU (Maximum Transmission Unit) size of an Ethernet frame, which
    /// is the maximum size of the packet an Ethernet frame can carry in its
    /// payload.
    static const uint32_t MAX_PAYLOAD_SIZE = 1500;

    /// Size of the space used to store PacketBufType, in bytes.
    static const uint32_t PACKETBUF_TYPE_SIZE = 1;

    /// Size of VLAN tag, in bytes. We are using the PCP (Priority Code Point)
    /// field defined in the VLAN tag to specify the packet priority.
    static const uint32_t VLAN_TAG_LEN = 4;

    /// Size of Ethernet header including VLAN tag, in bytes.
    static const uint32_t ETHER_VLAN_HDR_LEN = 14 + VLAN_TAG_LEN;

    /// Overhead of a physical layer Ethernet packet, in bytes, which includes
    /// the preamble (7 bytes), the start of frame delimiter (1 byte), the
    /// frame checking sequence (4 bytes) and the interpacket gap (12 bytes).
    /// Note: it doesn't include anything inside the Ethernet frame (e.g.,
    /// ETHER_VLAN_HDR_LEN).
    static const uint32_t ETHER_PACKET_OVERHEAD = 24;

    /// Map from priority levels to values of the PCP field. Note that PCP = 1
    /// is actually the lowest priority, while PCP = 0 is the second lowest.
    static constexpr uint16_t PRIORITY_TO_PCP[8] =
            {1 << 13, 0 << 13, 2 << 13, 3 << 13, 4 << 13, 5 << 13, 6 << 13,
             7 << 13};

    /// Tracks number of outstanding allocated payloads.  For detecting leaks.
    int packetBufsUtilized;

    /// The original ServiceLocator string. May be empty if the constructor
    /// argument was NULL. May also differ if dynamic ports are used.
    string locatorString;

    /// Stores the MAC address of the NIC (either native or overriden).
    Tub<MacAddress> localMac;

    /// Stores the NIC's physical port id addressed by the instantiated driver.
    uint8_t portId;

    /// Holds packet buffers that are dequeued from the NIC's HW queues
    /// via DPDK.
    struct rte_mempool* mbufPool;

    /// Holds packets that are addressed to localhost instead of going through
    /// the HW queues.
    struct rte_ring* loopbackRing;

    /// Hardware packet filter is provided by the NIC
    bool hasHardwareFilter;

    /// Effective network bandwidth, in Mbits/second.
    uint32_t bandwidthMbps;

    /// Used to redirect log entries from the DPDK log into the RAMCloud log.
    FileLogger fileLogger;

    DISALLOW_COPY_AND_ASSIGN(DpdkDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_DPDKDRIVER_H
