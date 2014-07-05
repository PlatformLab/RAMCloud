/* Copyright (c) 2014 Stanford University
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

#include "Common.h"
#include "Dispatch.h"
#include "Driver.h"
#include "EthernetUtil.h"
#include "ObjectPool.h"
#include "SolarFlareAddress.h"
#include "ServiceLocator.h"
#include "Tub.h"

namespace RAMCloud {

using namespace EthernetUtil; //NOLINT

/**
* A simple driver for SolarFlare NICs to send and receive unreliable datagrams. 
* This driver does not provide any reliablity guarantees and intended to be
* used with higher level transport code (eg FastTransport)
* See Driver.h for some more details.
*/

// This makes a macro for compiler cache aligning to the cash line size.
// This is necessary as a perfomance boost for the data portion of packet
// buffers.
#define CACHE_ALIGN  __attribute__((aligned(CACHE_LINE_SIZE)))

class SolarFlareDriver : public Driver {
  public:
    explicit SolarFlareDriver(Context* context,
                              const ServiceLocator* localServiceLocator);
    virtual ~SolarFlareDriver();
    virtual void connect(IncomingPacketHandler* incomingPacketHandler);
    virtual void disconnect();
    virtual uint32_t getMaxPacketSize();
    virtual void release(char* payload);
    virtual void sendPacket(const Driver::Address* recipient,
                            const void* header,
                            const uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual string getServiceLocator();
    virtual Driver::Address* newAddress(const ServiceLocator& serviceLocator) {
        return new SolarFlareAddress(serviceLocator);
    }

    // This is the size, in bytes, of the buffers for in both RX and TX ring.
    // As per SolarFlare recommendation, this should be equal to 2048.
    static const uint32_t ADAPTER_BUFFER_SIZE = 2048;

    // Defines the total number of buffers that could be packed in RX ring.
    // Maximum value for this constant is 4096.
    static const uint32_t NUM_RX_BUFS = 512;

    // Defines the total number of buffers that could be packed in TX ring.
    // Maximum value for this constant is 4096.
    static const uint32_t NUM_TX_BUFS = 512;

    // Size of the event queue on the adapter.
    static const uint32_t EF_VI_EVENT_POLL_NUM_EVS = 128;

    // Standard size of an ethernet frame including header.
    static const uint32_t ETHERNET_MAX_DATA_LEN = 1500;

    // Due to performance considerations, we refill RX ring in a batch of
    // size equal to below constant.
    static const uint32_t RX_REFILL_BATCH_SIZE = 32;

    // This data structure defines the buffer that is pushed onto the RX and TX
    // ring of the SolarFlare adapter.
    struct PacketBuff {
        struct PacketBuff* next;

        // Holds the dma address to the begining of the data array
        // (dmaBuffer[]) of this buffer.
        ef_addr dmaBufferAddress;

        // This is the id that we will pass it to the RX/TX ring and the NIC
        // and the NIC will return it to us in events to identify this packet
        // buffer.
        int id;

        // The actual packet content in this buffer starts at this address.
        // It's cache aligned for performance optimization.
        uint8_t dmaBuffer[0] CACHE_ALIGN;
    };

    // payload of every received packet will be coppied over to a buffer of
    // this type and will be handed over to the next layer of software
    // (eg. FastTransport) so that the original packet buffer from RX ring can
    // be pushed back to the RX the ring. We pre-allocate a pool of buffers
    // of this kind to reduce the overhead of memory allocation, deallocation,
    // and reallocation.
    struct ReceivedBuffer {
        ReceivedBuffer() : solarFlareAddress() {}
        char payload[ADAPTER_BUFFER_SIZE];
        Tub<SolarFlareAddress> solarFlareAddress;
    };

  PRIVATE:
    // Shared RAMCloud information.
    Context* context;

    // Keeps a copy of locator string for this SolarFlareDriver.
    string localStringLocator;

    // Keeps a copy of the address object for this SolarFlareDriver.
    Tub<SolarFlareAddress> localAddress;

    // Handler that is invoked whenever a new packet comes in.
    std::unique_ptr<IncomingPacketHandler> incomingPacketHandler;

    // Handle to talk to SolarFlare nic. Needed to allocate resources.
    ef_driver_handle driverHandle;

    // Determines how memory should be protected for a Virtual Interface (VI).
    // A protection domain is a collection of VIs and memory regions tied to
    // single user interface.
    ef_pd protectionDomain;

    // The Solarflare ef_vi API is a layer 2 API that grants an application
    // direct (kernel bypassed) access to the Solarflare network adapter
    // datapath. ef_vi is the internal API for sending and receiving packets
    // that do not require a POSIX socket interface.
    // Users of ef_vi must first allocate a virtual interface (VI), encapsulated
    // by the type "ef_vi". A VI includes: A receive descriptor ring
    // (for receiving packets); A transmit descriptor ring (for sending
    // packets); An event queue (to receive notifications from the hardware).
    ef_vi virtualInterface;

    // Pointer to the chunk of memory that is subdivided to packet buffers
    // for transmitting. Each packet within this chunck will be pushed to TX
    // ring as soon as it is filled with data.
    void* transmitMemory;

    // Pointer to the chunk of memory that is subdivided to packet buffers
    // for receiving. Packets within this chunck will be pushed to RX
    // in batches.
    void* receiveMemory;

    // Pointer to the head of the list of buffers that are free to be filled
    // with data and pushed to the TX ring.
    PacketBuff* freeTransmitList;

    // Pointer to the head of the list of buffers that are free and ready to be
    // pushed to the RX ring for receiving data.
    PacketBuff* freeReceiveList;

    // Length in bytes of the prefix metadata that SolarFlare nic adds to the
    // beginning of each received packet. This is only used as an offset to
    // beginning of the received packet buffer.
    int rxPrefixLen;

    // Memory pool for received packets
    ObjectPool<ReceivedBuffer> rxBufferPool;

    // Number of ReceivedBuffer that are allocated from rxBufferPool
    // but not yet released by the higher level transport code (eg.
    // FastTransport). At the end of the life of this driver, all buffers
    // must have been released and therefore this variable must be zero.
    uint64_t buffsNotReleased;

    // The number of packets for which we have called init() since the last
    // time we called push(). In other words this is the number of packets
    // that are prepared to be pushed to RX ring however not yet
    // pushed as we want to push the in batch size of RX_REFILL_BATCH_SIZE
    // for performance efficiency.
    uint64_t rxPktsReadyToPush;

    PacketBuff* initPacketBuffer(void* memoryChunk,
                                 int numBufs,
                                 ef_memreg* memoryReg);
    void refillRxRing();
    PacketBuff* getFreeTransmitBuffer();
    void handleReceived(int packetId, int packetLen);
    const string rxDiscardTypeToStr(int type);
    const string txErrTypeToStr(int type);

    /**
     * This is the object that polls the notifications from the event
     * queue of the NIC. Dispatch loop invokes the poll() function of this 
     * object to fetech the received packets and transmit notifications off 
     * of the NIC. 
     */
    class Poller : public Dispatch::Poller {
      public:
        explicit Poller(Context* context, SolarFlareDriver* solarFlareDriver)
            : Dispatch::Poller(context->dispatch, "SolarFlareDriver::Poller"),
            driver(solarFlareDriver) {}

        virtual void poll();
      private:

      // Pointer to the driver corresponding to this Poller object.
      SolarFlareDriver* driver;
      DISALLOW_COPY_AND_ASSIGN(Poller);
    };
    friend class Poller;
    Tub<Poller> poller;
    DISALLOW_COPY_AND_ASSIGN(SolarFlareDriver);
};
}//end RAMCloud
#endif //RAMCLOUD_SOLARFLAREDRIVER_H
