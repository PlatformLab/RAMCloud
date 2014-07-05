/** Copyright (c) 2014 Stanford University
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

#include <net/if.h>
#include "SolarFlareDriver.h"
#include "FastTransport.h"
#include "Memory.h"
#include "Buffer.h"
#include "ShortMacros.h"
#include "IpAddress.h"
#include "MacAddress.h"

namespace RAMCloud {

using namespace EthernetUtil; //NOLINT

/**
 * Constructs a SolarFlareDriver 
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param localServiceLocator 
 *      Specifies the mac address, ip address, and port that will be used to 
 *      to send and receive packets.
 */
SolarFlareDriver::SolarFlareDriver(Context* context,
                                   const ServiceLocator* localServiceLocator)
    : context(context)
    , localStringLocator()
    , localAddress()
    , incomingPacketHandler()
    , driverHandle()
    , protectionDomain()
    , virtualInterface()
    , transmitMemory()
    , receiveMemory()
    , freeTransmitList()
    , freeReceiveList()
    , rxPrefixLen()
    , rxBufferPool()
    , buffsNotReleased(0)
    , rxPktsReadyToPush(0)
    , poller()
{
    if (localServiceLocator == NULL) {

        // If localServiceLocator is NULL, we have to make a locatorString for
        // this driver. We use the actual MAC and IP address of this machine
        // along wih a randomly generated number for port.
        std::stringstream locatorStream;
        locatorStream << "fast+sf:mac=" << getLocalMac("eth0").c_str() << ",";
        locatorStream << "host=" << getLocalIp("eth0").c_str() << ",";
        locatorStream << "port=" << 10000 + (generateRandom() % 4000);

        ServiceLocator sl(locatorStream.str().c_str());
        localStringLocator = sl.getOriginalString();
        LOG(NOTICE, "No SolarFlare locator provided! "
            "Created the locator string: %s"
            , localStringLocator.c_str());
        localAddress.construct(sl);

    } else {
        localAddress.construct(*localServiceLocator);
        localStringLocator = localServiceLocator->getOriginalString();
    }

    // Adapter initializations. Fills driverHandle with driver resources
    // that is needed to talk to the NIC.
    int rc = ef_driver_open(&driverHandle);
    if (rc < 0) {
        DIE("Failed to open driver for SolarFlare NIC!");
    }

    // Allocates protection domain which specifies how memory must be protected
    // for the VI of this driver.
    rc =  ef_pd_alloc(&protectionDomain, driverHandle, if_nametoindex("eth0"),
                        static_cast<ef_pd_flags>(0));
    if (rc < 0) {
        DIE("Failed to allocate a protection domain for SolarFlareDriver!");
    }

    // Allocates an RX and Tx ring, an event queue, timers and interupt on the
    // adapter card and fills out the structures needed to access them in
    // the software.
    rc = ef_vi_alloc_from_pd(&virtualInterface, driverHandle, &protectionDomain,
                             driverHandle, -1, -1, -1, NULL, -1,
                             static_cast<enum ef_vi_flags>(0));

    if (rc < 0) {
        DIE("Failed to allocate VI for SolarFlareDriver!");
    }

    // Setting filters on the nic. SolarFlare nic by default sends all the
    // packets to the kernel except for the ones that match filters below.
    // Those are sent to be handled by this driver.
    ef_filter_spec filterSpec;
    ef_filter_spec_init(&filterSpec, EF_FILTER_FLAG_NONE);
    const sockaddr_in *addr = reinterpret_cast<const sockaddr_in*>
                                   (&localAddress->ipAddress.address);
    uint32_t localIp = addr->sin_addr.s_addr;
    uint32_t localPort = addr->sin_port;

    rc = ef_filter_spec_set_ip4_local(&filterSpec, IPPROTO_UDP, localIp,
                                 localPort);
    if (rc < 0) {
        DIE("Failed to set filter specificaions for SolarFlareDriver!");
    }
    rc = ef_vi_filter_add(&virtualInterface, driverHandle, &filterSpec, NULL);
    if (rc < 0) {
        DIE("Failed to add the specified filter to the SolarFlare NIC!");
    }

    rxPrefixLen = ef_vi_receive_prefix_len(&virtualInterface);

    // Handle for an area of contiguous memory used to hold buffers that are
    // shared with the NIC (associated with a particular VI within a protection
    // domain). This data struce will contain the DMA address for the first byte
    // of the memory region (either receiveMemory or transmitMemory).
    ef_memreg registeredMemory;

    // Memory allocation and initialization for receiving
    int totalRecvBytes = NUM_RX_BUFS * ADAPTER_BUFFER_SIZE;
    receiveMemory = Memory::xmemalign(HERE, 4096, totalRecvBytes);
    rc = ef_memreg_alloc(&registeredMemory, driverHandle, &protectionDomain,
                         driverHandle, receiveMemory, totalRecvBytes);

    if (rc < 0) {
        DIE("Failed to allocate a registered memory region in SolarFlare NIC"
        " for receive packet buffers!");
    }
    freeReceiveList = initPacketBuffer(receiveMemory,
                                       NUM_RX_BUFS,
                                       &registeredMemory);

    refillRxRing();

    // Memory allocation and initialization for transmittion.
    int totalTransBytes = NUM_TX_BUFS * ADAPTER_BUFFER_SIZE;
    transmitMemory = Memory::xmemalign(HERE, 4096, totalTransBytes);
    rc = ef_memreg_alloc(&registeredMemory, driverHandle, &protectionDomain,
                    driverHandle, transmitMemory, totalTransBytes);
    if (rc < 0) {
        DIE("Failed to allocate a registered memory region in SolarFlare NIC"
        " for transmit packet buffers!");
    }
    freeTransmitList = initPacketBuffer(transmitMemory,
                                        NUM_TX_BUFS,
                                        &registeredMemory);
}

/**
 * Given a chunk of memory, this method divides it up into PacketBuff 
 * structures, initializes the structures, and links them together 
 * into a list. 
 * 
 * \param memoryChunk
 *      Pointer to the piece of memory that is supposed to contain the packet
 *      buffers. The memory chunk must already been registered to the
 *      virtual interface of this driver and must be associated with protection
 *      domain of the virtual interface. Size of each buffer is defined as a
 *      static variable in header file.
 * \param numBufs
 *      Total number of packet buffers within the memory region
 * \param memoryReg
 *      A pointer to registered memory region that is allocated for the virtual
 *      interface of this driver. This will be used to find dma address to the 
 *      data portion of each packet buffer. 
 * \return
 *      Pointer to the head of the linked list of initialized the packet 
 *      buffers.
 *
 */

SolarFlareDriver::PacketBuff*
SolarFlareDriver::initPacketBuffer(void* memoryChunk,
                                   int numBufs,
                                   ef_memreg* memoryReg)
{
    struct PacketBuff* head = NULL;
    for (int i = 0; i < numBufs; i++) {
        struct PacketBuff* packetBuff =
            reinterpret_cast<PacketBuff*>(reinterpret_cast<char*>(memoryChunk)
            + i * ADAPTER_BUFFER_SIZE);

        packetBuff->id = i;
        packetBuff->dmaBufferAddress = ef_memreg_dma_addr(memoryReg,
                                        i * ADAPTER_BUFFER_SIZE);

        packetBuff->dmaBufferAddress += OFFSET_OF(struct PacketBuff,
                                                  dmaBuffer);

        packetBuff->next = head;
        head = packetBuff;
    }
    return head;
}

/**
 * A helper method to push back the received packet buffer to the receiving 
 * descriptor ring. This should be called either in the constructor of the 
 * SolareFlareDriver or after the packet is polled off of the nic and handled.
 */
void
SolarFlareDriver::refillRxRing()
{

    // Pushing packets to the RX ring happens in two steps. First we must
    // initialize the packet to the VI and then we push it to the rx ring.
    while (freeReceiveList) {

        // ef_vi_receive_init() initialized the packet to the VI but
        // doesn't actually push it. This function is fast so we call it for
        // every individual packet in the free list.
        ef_vi_receive_init(&virtualInterface,
                           freeReceiveList->dmaBufferAddress,
                           freeReceiveList->id);
        freeReceiveList = freeReceiveList->next;
        rxPktsReadyToPush++;

        // ef_vi_receive_push() does the actual job of pushing initialized
        // packets to RX ring and it is much slower than initilizing function
        // so we call it for a batch of RX_REFILL_BATCH_SIZE buffers
        // that have been already initialized.
        if (rxPktsReadyToPush == RX_REFILL_BATCH_SIZE) {
            ef_vi_receive_push(&virtualInterface);
            rxPktsReadyToPush = 0;
        }
    }
}

/**
 * Destroys a SolareFlareDriver and frees all the resources.
 */

SolarFlareDriver::~SolarFlareDriver() {
    if (buffsNotReleased != 0) {
        LOG(WARNING, "%lu packets are not released",
            buffsNotReleased);
    }
    ef_vi_free(&virtualInterface, driverHandle);
    ef_pd_free(&protectionDomain, driverHandle);
    ef_driver_close(driverHandle);
    free(transmitMemory);
    free(receiveMemory);
}


// See docs in the ``Driver'' class.
void
SolarFlareDriver::connect(IncomingPacketHandler* incomingPacketHandler)
{
    this->incomingPacketHandler.reset(incomingPacketHandler);
    poller.construct(context, this);
}

// See docs in the ``Driver'' class.
void
SolarFlareDriver::disconnect()
{
    poller.destroy();
    this->incomingPacketHandler.reset();
}

// See docs in the ``Driver'' class.
uint32_t
SolarFlareDriver::getMaxPacketSize()
{
    return ETHERNET_MAX_DATA_LEN -
           downCast<uint32_t>(sizeof(IpHeader) + sizeof(UdpHeader));
}

// See docs in the ``Driver'' class.
void
SolarFlareDriver::release(char* payload)
{
    Dispatch::Lock _(context->dispatch);
    assert(buffsNotReleased > 0);
    buffsNotReleased--;
    struct ReceivedBuffer* buf =
       reinterpret_cast<ReceivedBuffer*>(payload - OFFSET_OF(
                                                    ReceivedBuffer, payload));
    rxBufferPool.destroy(buf);
}

// See docs in the ``Driver'' class.
void
SolarFlareDriver::sendPacket(const Driver::Address* recipient,
                             const void* header,
                             const uint32_t headerLen,
                             Buffer::Iterator *payload)
{

    // To transmit a packet, we write the packet contents (including
    // all headers) into a packet buffers.
    uint32_t udpPayloadLen = downCast<uint32_t>(headerLen
                                + (payload ? payload->size() : 0));
    uint16_t udpLen = downCast<uint16_t>(udpPayloadLen + sizeof(UdpHeader));
    uint16_t ipLen = downCast<uint16_t>(udpLen + sizeof(IpHeader));
    assert(udpPayloadLen <= getMaxPacketSize());
    struct PacketBuff* toSend = getFreeTransmitBuffer();

    // Fill ethernet header
    EthernetHeader* ethHdr = new(toSend->dmaBuffer) EthernetHeader;
    const SolarFlareAddress* recipientAddress =
        static_cast<const SolarFlareAddress*>(recipient);
    memcpy(ethHdr->destAddress, recipientAddress->macAddress.address,
           sizeof(ethHdr->destAddress));
    memcpy(ethHdr->srcAddress, localAddress->macAddress.address,
           sizeof(ethHdr->srcAddress));
    ethHdr->etherType = HTONS(EthernetType::IP_V4);

    // Fill ip header
    IpHeader* ipHdr =
        new(reinterpret_cast<char*>(ethHdr) + sizeof(EthernetHeader))IpHeader;
    ipHdr->ipIhlVersion = (4u << 4u) | (sizeof(IpHeader) >> 2u);
    ipHdr->tos = 0;
    ipHdr->totalLength = HTONS(ipLen);
    ipHdr->id = 0;
    ipHdr->fragmentOffset = 0;
    ipHdr->ttl = 64;
    ipHdr->ipProtocol = static_cast<uint8_t>(IPPROTO_UDP);
    ipHdr->headerChecksum = 0;
    sockaddr_in* srcAddr =
        reinterpret_cast<sockaddr_in*>(&localAddress->ipAddress.address);
    ipHdr->ipSrcAddress = srcAddr->sin_addr.s_addr;
    const sockaddr_in* destAddr = reinterpret_cast<const sockaddr_in*>(
                                       &recipientAddress->ipAddress.address);
    ipHdr->ipDestAddress = destAddr->sin_addr.s_addr;

    // Fill udp header
    UdpHeader* udpHdr =
        new(reinterpret_cast<char*>(ipHdr) + sizeof(IpHeader)) UdpHeader;
    udpHdr->srcPort = srcAddr->sin_port;
    udpHdr->destPort = destAddr->sin_port;
    udpHdr->totalLength = HTONS(udpLen);
    udpHdr->totalChecksum = 0;

    // Copy the header and payload over to the packet buffer
    uint8_t* transportHdr = reinterpret_cast<uint8_t*>(udpHdr)
                            + sizeof(UdpHeader);
    memcpy(transportHdr, header, headerLen);
    uint8_t* data = transportHdr + headerLen;
    while (payload && !payload->isDone()) {
        memcpy(data, payload->getData(), payload->getLength());
        data += payload->getLength();
        payload->next();
    }

    // Transmit the buffer
    uint32_t totalLen =  downCast<uint32_t>(sizeof(EthernetHeader) + ipLen);
    //LOG(NOTICE, "%s", (ethernetHeaderToStr(ethHdr)).c_str());
    //LOG(NOTICE, "%s", (ipHeaderToStr(ipHdr)).c_str());
    //LOG(NOTICE, "%s", (udpHeaderToStr(udpHdr)).c_str());

    // By invoking ef_vi_transmit() the descriptor that describes the
    // packet is queued in the transmit ring, and a doorbell is rung to
    // inform the adapter that the transmit ring is non-empty. Later on in
    // Poller::poll() we fetch notifications off of the event queue that
    // implies which packet trasnmit is completed.
    ef_vi_transmit(&virtualInterface, toSend->dmaBufferAddress,
                   downCast<int>(totalLen), toSend->id);
}

/**
 * A helper method to obtain a packet buffer within transmit memory.
 *
 * \return 
 *      a pointer to the packet that is retrieved within transmit memory.
 */
SolarFlareDriver::PacketBuff*
SolarFlareDriver::getFreeTransmitBuffer()
{
    while (!freeTransmitList) {
        //no buffer is available, we should poll on the event queue
        //to get a free transmit buffer
        LOG(NOTICE, "calling poll() from getFreeTransmitBuffer function");
        context->dispatch->poll();
    }
    struct PacketBuff* buf = freeTransmitList;
    freeTransmitList = freeTransmitList->next;
    return buf;
}

/**
 * This method fetches the notifications off of the event queue on the 
 * nic. The received packets are then forwarded to be handled by 
 * transport code. This function is always called from dispatch loop.
 * See docs in the ``Driver'' class.
 */
void
SolarFlareDriver::Poller::poll()
{
    assert(driver->context->dispatch->isDispatchThread());

    // To receive packets, descriptors each identifying a buffer,
    // are queued in the RX ring. The event queue is a channel
    // from the adapter to software which notifies software when
    // packets arrive from the network, and when transmits complete
    // (so that the buffers can be freed or reused).
    // events[] array contains the notifications that are fetched off
    // of the event queue on nic and correpond to this VI of this driver.
    // Each member of the array could be a transmit completion notification
    // or a notification to a received packet or an error that happened
    // in receive or transmist.
    ef_event events[EF_VI_EVENT_POLL_NUM_EVS];

    // Contains the id of packets that have been successfully transmitted
    // or failed. It will be used for fast access to the packet buffer
    // that has been used for that transmission.
    ef_request_id packetIds[EF_VI_TRANSMIT_BATCH];

    // The application retrieves these events from event queue by calling
    // ef_eventq_poll().
    int eventCnt = ef_eventq_poll(&driver->virtualInterface, events,
                                  sizeof(events)/sizeof(events[0]));
    for (int i = 0; i < eventCnt; i++) {
        LOG(DEBUG, "%d events polled off of nic", eventCnt);
        int numCompleted = 0;
        int txErrorType = 0;
        int rxDiscardType = 0;
        switch (EF_EVENT_TYPE(events[i])) {

            // A new packet has been recieved.
            case EF_EVENT_TYPE_RX:

                // The SolarFlare library provides function functions
                // EF_EVENT_RX_RQ_ID and EF_EVENT_RX_BYTES to find the id and
                // length of the received packet from the notification that's
                // been polled off of the event queue.
                driver->handleReceived(EF_EVENT_RX_RQ_ID(events[i]),
                                       EF_EVENT_RX_BYTES(events[i]));
                break;

            // A packet transmit has been completed
            case EF_EVENT_TYPE_TX:
                 numCompleted =
                    ef_vi_transmit_unbundle(&driver->virtualInterface,
                                            &events[i],
                                            packetIds);
                for (int j = 0; j < numCompleted; j++) {
                    struct PacketBuff* packetBuff =
                        reinterpret_cast<struct PacketBuff*>(
                        reinterpret_cast<char*>(driver->transmitMemory)
                        + packetIds[j] * driver->ADAPTER_BUFFER_SIZE);

                    packetBuff->next = driver->freeTransmitList;
                    driver->freeTransmitList = packetBuff;
                }
                break;

            // A packet has been received but it must be discarded because
            // either its erroneous or is not targetted for this driver.
            case EF_EVENT_TYPE_RX_DISCARD:

                // This "if" statement will be taken out from the final code.
                // Currently serves for the cases that we might specify
                // an arbitrary mac address for string locator (something
                // other than the actual mac address of the SolarFlare card)
                if (EF_EVENT_RX_DISCARD_TYPE(events[i])
                             == EF_EVENT_RX_DISCARD_OTHER) {
                    driver->handleReceived(
                        EF_EVENT_RX_DISCARD_RQ_ID(events[i]),
                        EF_EVENT_RX_DISCARD_BYTES(events[i]));
                } else {
                    rxDiscardType = EF_EVENT_RX_DISCARD_TYPE(events[i]);
                    LOG(NOTICE, "Received discarded packet of type %d (%s)",
                        rxDiscardType,
                        driver->rxDiscardTypeToStr(rxDiscardType).c_str());

                    // The buffer for the discarded received packet must be
                    // returned to the receive free list
                    int packetId = EF_EVENT_RX_DISCARD_RQ_ID(events[i]);
                    struct PacketBuff* packetBuff =
                        reinterpret_cast<struct PacketBuff*>(
                        reinterpret_cast<char*>(driver->receiveMemory)
                        + packetId * driver->ADAPTER_BUFFER_SIZE);

                    packetBuff->next = driver->freeReceiveList;
                    driver->freeReceiveList = packetBuff;
                }
                break;

            // Error happened in transmitting a packet.
            case EF_EVENT_TYPE_TX_ERROR:
                txErrorType = EF_EVENT_TX_ERROR_TYPE(events[i]);
                LOG(WARNING, "TX error type %d (%s) happened!",
                    txErrorType,
                    driver->txErrTypeToStr(txErrorType).c_str());
                break;
            // Type of the event does not match to any above case statements.
            default:
                LOG(WARNING, "Unexpected event for event id %d", i);
                break;
        }
    }

    // At the end of the poller loop, the buffers in the freeReceiveList will
    // be pushed back to the RX ring.
    driver->refillRxRing();
}

/**
 * A helper method for the packets that arrive on the nic. It passes the packet 
 * payload to the next layer in transport software (eg. FastTransport)
 * 
 * \param packetId
 *      This is the id of the packetBuff within the received memory chunk.
 *      This is used for fast calcualtion of the address of the packet within
 *      that memory chunk.
 * \param packetLen
 *      The actual length in bytes of the packet that's been received in nic.
 *      
 */
void
SolarFlareDriver::handleReceived(int packetId, int packetLen)
{
    assert(downCast<uint32_t>(packetLen) >=
                        rxPrefixLen + sizeof(EthernetHeader) +
                        sizeof(IpHeader) + sizeof(UdpHeader));
    assert(downCast<uint32_t>(packetLen) < NUM_RX_BUFS);

    //get a handle on the arrived packet in receive ring
    struct PacketBuff* packetBuff =
        reinterpret_cast<struct PacketBuff*>(
        reinterpret_cast<char*>(receiveMemory)
        + packetId * ADAPTER_BUFFER_SIZE);

    uint8_t* data = packetBuff->dmaBuffer + rxPrefixLen;
    uint32_t totalLen = packetLen - rxPrefixLen;
    EthernetHeader* ethHdr = reinterpret_cast<EthernetHeader*>(data);
    IpHeader* ipHdr = reinterpret_cast<IpHeader*>(
                      reinterpret_cast<char*>(ethHdr)
                      + sizeof(EthernetHeader));

    UdpHeader* udpHdr = reinterpret_cast<UdpHeader*>(
                            reinterpret_cast<char*>(ipHdr)
                            + sizeof(IpHeader));

    //LOG(NOTICE, "%s", ethernetHeaderToStr(ethHdr).c_str());
    //LOG(NOTICE, "%s",ipHeaderToStr(ipHdr).c_str());
    //LOG(NOTICE, "%s",udpHeaderToStr(udpHdr).c_str());

    //offset to the beginning of the payload after the ethernet header,
    //ip header and udp header
    uint32_t offset = sizeof(EthernetHeader) + sizeof(IpHeader)
                      + sizeof(UdpHeader);

    //find mac, ip and port of the sender
    uint8_t* mac = ethHdr->srcAddress;
    uint32_t ip = ntohl(ipHdr->ipSrcAddress);
    uint16_t port = NTOHS(udpHdr->srcPort);

    //allocate buffer from the pool and copy the payload packetBuff
    //to the buffer.
    struct ReceivedBuffer* buffer = rxBufferPool.construct();
    buffsNotReleased++;
    Received received;
    received.payload = buffer->payload;
    received.len = downCast<uint32_t>(NTOHS(udpHdr->totalLength)
                        - downCast<uint16_t>(sizeof(UdpHeader)));
    memcpy(buffer->payload, data + offset, received.len);
    received.driver = this;
    received.sender = buffer->solarFlareAddress.construct(mac, ip, port);
    if (received.len != (totalLen - offset)) {
        LOG(WARNING, "total payload bytes received is %u,"
            " but udp payload length is %u bytes",
            (totalLen - offset), received.len);
    }
    (*incomingPacketHandler)(&received);

    // Prepend original packet buffer to freeReceiveList.
    // Later on, we'll push it back to rx ring
    packetBuff->next = freeReceiveList;
    freeReceiveList = packetBuff;
}

/**
 * Returns the string equivalent of error type of a discarded received packet.
 *
 * \param type
 *      The numeric value of RX discard type.
 * \return
 *      The string equivalent of a numeric value for discard type.  
 */
const string
SolarFlareDriver::rxDiscardTypeToStr(int type)
{
    switch (type) {

        // Checksum value in IP or UDP header is erroneous.
        case EF_EVENT_RX_DISCARD_CSUM_BAD:
            return string("EF_EVENT_RX_DISCARD_CSUM_BAD");

        // The packet is a multicast packet that is not targeted to this driver.
        case EF_EVENT_RX_DISCARD_MCAST_MISMATCH:
            return string("EF_EVENT_RX_DISCARD_MCAST_MISMATCH");

        // Error in CRC checksum of the ethernet fram.
        case EF_EVENT_RX_DISCARD_CRC_BAD:
            return string("EF_EVENT_RX_DISCARD_CRC_BAD");

        // Ethernet frame truncated and shorter than expected.
        case EF_EVENT_RX_DISCARD_TRUNC:
            return string("EF_EVENT_RX_DISCARD_TRUNC");

        // The buffer owner id of this event does not match the one for this
        // driver meaning that this event was delivered to this driver by
        // mistake.
        case EF_EVENT_RX_DISCARD_RIGHTS:
            return string("EF_EVENT_RX_DISCARD_RIGHTS");

        // Unexpected error happened for the current event. The current event
        // is not what it is expected to be which means that an event is lost.
        case EF_EVENT_RX_DISCARD_EV_ERROR:
            return string("EF_EVENT_RX_DISCARD_EV_ERROR");

        // Any other type of RX error not covered in above cases.
        default:
            return string("EF_EVENT_RX_DISCARD_OTHER");
    }
}

/**
 * Returns the string equivalent of error that happened in transmitting a
 * packet.
 * \param type
 *      The numeric value of TX error type.
 * \return
 *      A string describing the type of error happend in transmit.
 */
const string
SolarFlareDriver::txErrTypeToStr(int type)
{
    switch (type) {

        // The buffer owner id of this packet doesnot match to this driver.
        case EF_EVENT_TX_ERROR_RIGHTS:
            return string("EF_EVENT_TX_ERROR_RIGHTS");

        // Transmit wait queue overflow.
        case EF_EVENT_TX_ERROR_OFLOW:
            return string("EF_EVENT_TX_ERROR_OFLOW");

        // The transmit packet is too big to send.
        case EF_EVENT_TX_ERROR_2BIG:
            return string("EF_EVENT_TX_ERROR_2BIG");

        // Bus error happened while transmitting this packet.
        case EF_EVENT_TX_ERROR_BUS:
            return string("EF_EVENT_TX_ERROR_BUS");

        // None of the errors above.
        default:
            return string("NO_TYPE_SPECIFIED_FOR_THIS_ERR");
    }
}

/**
 * See docs in the ``Driver'' class.
 */
string
SolarFlareDriver::getServiceLocator()
{
    return localStringLocator;
}
} // end RAMCloud
