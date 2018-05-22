/* Copyright (c) 2014-2017 Stanford University
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

#ifndef RAMCLOUD_SOLARFLAREDRIVER_H
#define RAMCLOUD_SOLARFLAREDRIVER_H

#include <netinet/in.h>
#include <etherfabric/memreg.h>
#include <etherfabric/pd.h>
#include <etherfabric/vi.h>

#include "ArpCache.h"
#include "Common.h"
#include "Dispatch.h"
#include "Driver.h"
#include "NetUtil.h"
#include "ObjectPool.h"
#include "MacIpAddress.h"
#include "QueueEstimator.h"
#include "ServiceLocator.h"
#include "ShortMacros.h"
#include "Transport.h"
#include "Tub.h"

namespace RAMCloud {


/**
 * A simple driver for SolarFlare NICs to send and receive unreliable datagrams.
 * This driver does not provide any reliability guarantees and is intended to be
 * used with higher level transport code.
 * See `Driver' class for more details.
 */

class SolarFlareDriver : public Driver {
  public:
    explicit SolarFlareDriver(Context* context,
                              const ServiceLocator* localServiceLocator);
    virtual ~SolarFlareDriver();
    virtual uint32_t getMaxPacketSize();
    virtual void receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets);
    virtual void release(char* payload);
    virtual void sendPacket(const Driver::Address* recipient,
                            const void* header,
                            uint32_t headerLen,
                            Buffer::Iterator* payload,
                            int priority = 0,
                            TransmitQueueState* txQueueState = NULL);
    virtual string getServiceLocator();
    virtual Driver::Address* newAddress(const ServiceLocator& serviceLocator);

    /// Defines the total number of buffers that the driver is allowed to pack
    /// in TX ring. Large values increases the latency for small objects.
    /// Maximum value for this constant is 4096.
    static const uint32_t TX_RING_CAP = 4096;

    /// The size, in bytes, of the packet buffer for both RX and TX ring.
    /// As per SolarFlare recommendation, this should be equal to 2048.
    static const uint32_t ADAPTER_BUFFER_SIZE = 2048;

    /// Defines the total number of buffers that the driver is allowed to pack
    /// into RX ring. Larger values increases the latency for small objects.
    /// Maximum value for this constant is 4096.
    static const uint32_t RX_RING_CAP = 4096;

    /// Size of the event queue on the adapter.
    static const uint32_t EF_VI_EVENT_POLL_NUM_EVS = 128;

    /// Due to performance considerations, we refill RX ring in a batch of
    /// size equal to below constant.
    static const uint32_t RX_REFILL_BATCH_SIZE = 64;

    /// Offset of payload data portion of an Ethernet frame measured from first
    /// byte of Ethernet header. We assume the Ethernet frame contains IP header
    /// and UDP header right after Ethernet header and then comes the payload
    /// data.
    static const uint32_t ETH_DATA_OFFSET = sizeof(NetUtil::EthernetHeader) +
            sizeof(NetUtil::IpHeader) + sizeof(NetUtil::UdpHeader);

    /**
     * Defines a memory buffer that is to be pushed into the RX or TX ring
     * of the SolarFlare adapter for receiving/transmitting packets. Each
     * PacketBuff must be within the region of memory that SolarFlare NIC has
     * DMA access to. As per SolarFlare documentations, the packet buffers
     * better be 2KB sized even though the data part is less than 2KB.
     * The extra space is then used to keep few auxiliary fields for efficient
     * handling of the PacketBuffs.
     */
    struct PacketBuff {
        PacketBuff()
            : dmaBufferAddress()
            , id(0)
            , macIpAddress()
            , dmaBuffer()
        {}

        /// Holds the DMA address of the beginning of the data array
        /// (ie. dmaBuffer[]) of this buffer.
        ef_addr dmaBufferAddress;

        /// This is the id that we will pass it to the RX/TX ring and the NIC
        /// will return it to us in events. It helps to easily identify this
        /// packet buffer among all the PacketBuff registered to the NIC.
        int id;

        /// For every RX packet, this address will be constructed from senders
        /// information and will be passed to the higher level transport code
        /// This address will be used for sending replies back.
        Tub<MacIpAddress> macIpAddress;

        /// The packet content starts at this address and it's cache aligned for
        /// performance optimization. N.B. In the receive path, SolarFlare NIC
        /// adds some prefix data at this start and the received Ethernet packet
        /// starts right after the prefix data.
        uint8_t dmaBuffer[NetUtil::ETHERNET_MAX_DATA_LEN] CACHE_ALIGN;
    };

    /**
     * A container for a list (pool) of packet buffers that are registered to
     * the NIC and ready to be pushed to RX or TX ring. Provides methods for
     * accessing PacketBuffs quickly, getting free and ready to use packetBuffs
     * or putting the PacketBuffs back into the container after they are
     * unbundled from RX/TX ring.
     */
    class RegisteredBuffs {
      public:
        RegisteredBuffs(int bufferSize, int numBuffers,
            SolarFlareDriver* driver);
        ~RegisteredBuffs();

        PacketBuff* getBufferById(int packetId);
        PacketBuff* popFreeBuffer();

      PRIVATE:
        friend class SolarFlareDriver;

        /// Address of the first byte of the memory region that is registered to
        /// the NIC for DMA access and has been subdivided into packet buffers
        /// in this container.
        char* memoryChunk;

        /// A data struct that contains DMA addresses of memory pages within the
        /// region of memory that is registered to the NIC container.
        ef_memreg registeredMemRegion;

        /// Total size in bytes of each packet buffer in the list.
        int bufferSize;

        /// Total number of buffers initially registered to NIC in this class
        /// and placed into the freeBuffersVec.
        int numBuffers;

        /// Contains pointers to packet buffers that are free and ready to be
        /// pushed into TX or RX ring.
        std::vector<PacketBuff*> freeBuffersVec;

        /// Pointer to the driver that owns this object.
        SolarFlareDriver* driver;
        DISALLOW_COPY_AND_ASSIGN(RegisteredBuffs);
    };

  PRIVATE:
    friend class RegisteredBuffs;

    /// Shared RAMCloud information.
    Context* context;

    /// Used to resolve destination mac address of outgoing packets
    Tub<ArpCache> arpCache;

    /// Keeps a copy of locator string for this SolarFlareDriver.
    string localStringLocator;

    /// Keeps a copy of the address object for this SolarFlareDriver.
    Tub<MacIpAddress> localAddress;

    /// Handler that is invoked whenever a new packet comes in.
    std::unique_ptr<IncomingPacketHandler> incomingPacketHandler;

    /// Handle to talk to SolarFlare NIC. Needed to allocate resources.
    ef_driver_handle driverHandle;

    /// Determines how memory should be protected for a Virtual Interface (VI).
    /// A protection domain is a collection of VIs and memory regions tied to
    /// single user interface.
    ef_pd protectionDomain;

    /// The Solarflare ef_vi API is a layer 2 API that grants an application
    /// direct (kernel bypassed) access to the Solarflare network adapter
    /// data path. ef_vi is the internal API for sending and receiving packets
    /// that do not require a POSIX socket interface.
    /// Users of ef_vi must first allocate a virtual interface (VI),
    /// encapsulated by the type "ef_vi". A VI includes: A receive descriptor
    /// ring (for receiving packets); A transmit descriptor ring (for sending
    /// packets); An event queue (to receive notifications from the hardware).
    ef_vi virtualInterface;

    /// A container that allocates some number of packet buffers that are
    /// registered to the NIC and will be used for receiving packets.
    Tub<RegisteredBuffs> rxBufferPool;

    /// A container that allocates some number of packet buffers that are
    /// registered to the NIC and will be used for transmitting packets.
    Tub<RegisteredBuffs> txBufferPool;

    /// Newly received packets in RX ring buffers will be copied over to a
    /// buffer from this pool and will be handed off to the higher level
    /// transport layer code.
    ObjectPool<PacketBuff> rxCopyPool;

    /// Length in bytes of the prefix metadata that SolarFlare NIC adds to the
    /// beginning of each received packet right before the Ethernet header.
    /// This will be used as offset to beginning of the received packet buffer.
    int rxPrefixLen;

    /// Number of  packetBuffs that are allocated from rxCopyPool but not
    /// yet released by the higher level transport code. At the end of the
    /// life of this driver, all buffers must have been released and
    /// therefore this variable must be zero.
    uint64_t buffsNotReleased;

    /// Tracks the total number of packet buffers currently living on the
    /// RX ring plus the packets that are initialized but they have not yet been
    /// pushed to the RX ring.
    int rxRingFillLevel;

    /// A socket descriptor for allocating a free port number from the kernel
    /// for this driver. This is only necessary when this driver is being
    /// used on a client side and no locator string has been provided for the
    /// driver in the construction time.
    int fd;

    /// Effective network bandwidth, in Gbits/second.
    int bandwidthGbps;

    void refillRxRing();
    void handleReceived(int packetId, int packetLen,
            std::vector<Received>* receivedPackets);
    const char* rxDiscardTypeToStr(int type);
    const char* txErrTypeToStr(int type);

    /// Name of the physical SolarFlare NIC that will send and receive packets
    /// for this SolarFlareDriver on the local machine.
    static const char ifName[];
    static Syscall* sys;
    DISALLOW_COPY_AND_ASSIGN(SolarFlareDriver);
};
}//end RAMCloud
#endif //RAMCLOUD_SOLARFLAREDRIVER_H
