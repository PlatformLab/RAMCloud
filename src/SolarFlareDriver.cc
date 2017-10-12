/** Copyright (c) 2014-2017 Stanford University
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
#include "Memory.h"
#include "Buffer.h"
#include "Cycles.h"
#include "IpAddress.h"
#include "MacAddress.h"
#include "Syscall.h"

namespace RAMCloud {

/**
 * The value of this parameter must be replaced by the SolarFlare NIC name on
 * the local machine.
 */
const char SolarFlareDriver::ifName[] = "eth0";

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* SolarFlareDriver::sys = &defaultSyscall;

using namespace NetUtil; //NOLINT

/**
 * Constructs a SolarFlareDriver 
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param localServiceLocator 
 *      Specifies the mac address, IP address, and port that will be used to 
 *      to send and receive packets. NULL means that this driver is being
 *      constructed in the client side.
 */
SolarFlareDriver::SolarFlareDriver(Context* context,
                                   const ServiceLocator* localServiceLocator)
    : context(context)
    , arpCache()
    , localStringLocator()
    , localAddress()
    , driverHandle()
    , protectionDomain()
    , virtualInterface()
    , rxBufferPool()
    , txBufferPool()
    , rxCopyPool()
    , rxPrefixLen()
    , buffsNotReleased(0)
    , rxRingFillLevel(0)
    , fd(-1)
    , bandwidthGbps(10)                   // Default bandwidth = 10 gbs
{
    if (localServiceLocator == NULL) {

        // If localServiceLocator is NULL, we have to make a locatorString for
        // this driver. To that end, We use the actual MAC and IP address of
        // the machine along with a port number that we let the Kernel to choose
        // for us.
        sockaddr sockAddr;
        string localIpStr =  getLocalIp(ifName);
        sockaddr_in *sockInAddr = reinterpret_cast<sockaddr_in*>(&sockAddr);
        sockInAddr->sin_family = AF_INET;
        inet_aton(localIpStr.c_str(), &sockInAddr->sin_addr);

        fd = sys->socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            string msg = "Could not create socket for SolarFlareDriver.";
            LOG(WARNING, "%s", msg.c_str());
            throw DriverException(HERE, msg.c_str(), errno);
        }

        // Let the kernel choose a port when we bind the socket
        sockInAddr->sin_port = HTONS(0);
        int r = sys->bind(fd, &sockAddr, sizeof(sockAddr));
        if (r < 0) {
            sys->close(fd);
            string msg =
                format("SolarFlareDriver could not bind the socket to %s.",
                inet_ntoa(sockInAddr->sin_addr));
            LOG(WARNING, "%s", msg.c_str());
            throw DriverException(HERE, msg.c_str(), errno);
        }

        // Read back the port number that Kernel has chosen for us.
        socklen_t addrLen = sizeof(sockAddr);
        r = sys->getsockname(fd, &sockAddr, &addrLen);
        if (r < 0 || !NTOHS(sockInAddr->sin_port)) {
            sys->close(fd);
            string msg = format("Error in binding SolarFlare socket to a"
                    " Kernel socket port.");
            LOG(WARNING, "%s", msg.c_str());
            throw DriverException(HERE, msg.c_str(), errno);
        }

        localStringLocator = format("fast+sf:mac=%s,host=%s,port=%hu",
            getLocalMac(ifName).c_str(),
            localIpStr.c_str(),
            NTOHS(sockInAddr->sin_port));
        ServiceLocator sl(localStringLocator.c_str());
        LOG(NOTICE, "No SolarFlare locator provided. "
            "Created the locator string: %s", localStringLocator.c_str());
        localAddress.construct(sl);

    } else {
        if (!localServiceLocator->getOption<const char*>("mac", NULL)) {

            // Find the mac address of this machine and append it to the service
            // locator;
            string macString = ",mac=" + getLocalMac(ifName);
            localStringLocator = localServiceLocator->getOriginalString();
            ServiceLocator sl(localStringLocator + macString);
            localAddress.construct(sl);
        } else {
            localStringLocator = localServiceLocator->getOriginalString();
            localAddress.construct(*localServiceLocator);
        }

        try {
            bandwidthGbps = localServiceLocator->getOption<int>("gbs");
        } catch (ServiceLocator::NoSuchKeyException& e) {}
    }
    queueEstimator.setBandwidth(1000*bandwidthGbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthGbps)
            * MAX_DRAIN_TIME / 8.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    if (maxTransmitQueueSize < 2*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = 2*maxPacketSize;
    }

    // Adapter initializations. Fills driverHandle with driver resources
    // that is needed to talk to the NIC.
    int rc = ef_driver_open(&driverHandle);
    if (rc < 0) {
        string msg = "Failed to open driver for SolarFlare NIC.";
        LOG(WARNING, "%s", msg.c_str());
        throw DriverException(HERE, msg.c_str(), errno);
    }

    // Allocates protection domain which specifies how memory must be protected
    // for the VI of this driver. N.B. for EF_PD_VF flag to work, you must have
    // SR-IOV enabled on SolarFlare NIC.
    rc = ef_pd_alloc(&protectionDomain, driverHandle,
            if_nametoindex(ifName), EF_PD_DEFAULT);
    if (rc < 0) {
        string msg =
        "Failed to allocate a protection domain for SolarFlareDriver.";
        LOG(WARNING, "%s", msg.c_str());
        throw DriverException(HERE, msg.c_str(), errno);
    }

    // Allocates an RX and TX ring, an event queue, timers and interrupt on the
    // adapter card and fills out the structures needed to access them in
    // the software.
    rc = ef_vi_alloc_from_pd(&virtualInterface, driverHandle, &protectionDomain,
                             driverHandle, -1, -1, -1, NULL, -1,
                             static_cast<enum ef_vi_flags>(0));
    if (rc < 0) {
        string msg = "Failed to allocate VNIC for SolarFlareDriver.";
        LOG(WARNING, "%s", msg.c_str());
        throw DriverException(HERE, msg.c_str(), errno);
    }

    // Setting filters on the NIC. SolarFlare NIC by default sends all the
    // packets to the kernel except for the ones that match filters below.
    // Those are sent to be handled by this driver.
    ef_filter_spec filterSpec;
    ef_filter_spec_init(&filterSpec, EF_FILTER_FLAG_NONE);
    const sockaddr_in *addr = reinterpret_cast<const sockaddr_in*>
                                   (&localAddress->ipAddress->address);
    uint32_t localIp = addr->sin_addr.s_addr;
    uint32_t localPort = addr->sin_port;

    rc = ef_filter_spec_set_ip4_local(&filterSpec, IPPROTO_UDP, localIp,
                                 localPort);
    if (rc < 0) {
        string msg =
            "Failed to set filter specifications for SolarFlareDriver.";
        LOG(WARNING, "%s", msg.c_str());
        throw DriverException(HERE, msg.c_str(), errno);
    }

    rc = ef_vi_filter_add(&virtualInterface, driverHandle, &filterSpec, NULL);
    if (rc < 0) {
        string msg =
            "Failed to add the specified filter to the SolarFlare NIC.";
        LOG(WARNING, "%s", msg.c_str());
        throw DriverException(HERE, msg.c_str(), errno);
    }

    rxPrefixLen = ef_vi_receive_prefix_len(&virtualInterface);

    int bufferSize = ADAPTER_BUFFER_SIZE;
    int numRxBuffers = RX_RING_CAP;
    rxBufferPool.construct(bufferSize, numRxBuffers, this);
    refillRxRing();
    int numTxBuffers = TX_RING_CAP;
    txBufferPool.construct(bufferSize, numTxBuffers, this);
    arpCache.construct(context, localIp, ifName);

    LOG(NOTICE, "SolarFlareDriver bandwidth: %d Gbits/sec, "
            "maxTransmitQueueSize: %u bytes",
            bandwidthGbps, maxTransmitQueueSize);
}

/**
 * Constructor for RegisteredBuffs object:
 * \param bufferSize
 *      Size of each packet buffer in the freeBuffersVec.
 * \param numBuffers
 *      Total number of packetBuffs to be registered to NIC in this class.
 * \param driver
 *      Pointer to the SolarFlareDriver that owns this instance of
 *      RegisteredBuffs class.
 */
SolarFlareDriver::RegisteredBuffs::RegisteredBuffs(int bufferSize,
        int numBuffers, SolarFlareDriver* driver)
    : memoryChunk()
    , registeredMemRegion()
    , bufferSize(bufferSize)
    , numBuffers(numBuffers)
    , freeBuffersVec()
    , driver(driver)
{
    int totalBytes = bufferSize * numBuffers;

    // Allocates a chunk of memory that is aligned to the page size.
    memoryChunk = static_cast<char*>(Memory::xmemalign(HERE, 4096, totalBytes));
    if (!memoryChunk) {
        string msg =
            "Failed to allocate memory for packet buffers for"
            " SolarFlare Driver.";
        LOG(WARNING, "%s", msg.c_str());
        throw DriverException(HERE, msg.c_str());
    }

    // Register the memory chunk to NIC.
    int rc =
        ef_memreg_alloc(&registeredMemRegion, driver->driverHandle,
        &driver->protectionDomain, driver->driverHandle,
        memoryChunk, totalBytes);
    if (rc < 0) {
        string msg =
            "Failed to register memory region to SolarFlare NIC"
            " for packet buffers.";
        LOG(WARNING, "%s", msg.c_str());
        throw DriverException(HERE, msg.c_str());
    }

    // Divide the chunk into PacketBuffs and push them into the freeBuffersVec.
    for (int i = 0; i < numBuffers; i++) {
        struct PacketBuff* packetBuff =
            reinterpret_cast<PacketBuff*>(memoryChunk + i * bufferSize);

        packetBuff->id = i;
        packetBuff->dmaBufferAddress = ef_memreg_dma_addr(&registeredMemRegion,
                                        i * bufferSize);
        packetBuff->dmaBufferAddress += OFFSET_OF(struct PacketBuff,
                                                  dmaBuffer);
        freeBuffersVec.push_back(packetBuff);
    }
}

/**
 * Destructor for RegisteredBuffs class.
 */
SolarFlareDriver::RegisteredBuffs::~RegisteredBuffs() {
    free(memoryChunk);
    ef_memreg_free(&registeredMemRegion, driver->driverHandle);
}

/**
 * A helper function to find the address of an specific PacketBuff within the
 * memory chunk of RegisteredBuffs class.
 *
 * \param packetId
 *      The packetId of the PacketBuff that we want to find its address. This
 *      parameter must be less than #RegisteredBuffs::numBuffers.
 * \return
 *      A pointer to the retrieved PacketBuff.
 */
SolarFlareDriver::PacketBuff*
SolarFlareDriver::RegisteredBuffs::getBufferById(int packetId)
{
    assert(packetId < numBuffers);
    return reinterpret_cast<struct PacketBuff*>(memoryChunk
            + packetId * bufferSize);
}

/**
 * Returns a free and ready to use PacketBuff from the freeBuffersVec and
 * removes it from the list.
 *
 * \return
 *      A pointer to a free PacketBuff that is removed from the list. Null, if
 *      no PacketBuff left free on the list.
 */
SolarFlareDriver::PacketBuff*
SolarFlareDriver::RegisteredBuffs::popFreeBuffer()
{
    if (freeBuffersVec.empty())
        return NULL;
    PacketBuff* buf = freeBuffersVec.back();
    freeBuffersVec.pop_back();
    return buf;
}

/**
 * A helper method to push back the received packet buffer to the RX
 * descriptor ring. This should be called either in the constructor of the 
 * SolareFlareDriver or after some packets are polled off of the RX ring.
 */
void
SolarFlareDriver::refillRxRing()
{
    int numPktToPost = RX_RING_CAP - rxRingFillLevel;

    // Pushing packets to the RX ring happens in two steps. First we must
    // initialize the packets to the VI and then we push them to the RX ring in
    // a batch.
    for (int i = 0; i < numPktToPost; i++) {
        PacketBuff* pktBuf = rxBufferPool->popFreeBuffer();
        if (!pktBuf) {
            LOG(WARNING, "No packet buffer left free to push into RX ring of"
            " SolarFlare NIC.");
        }

        // ef_vi_receive_init() initialized the packet to the VI but
        // doesn't actually push it. This function is fast so we call it for
        // every individual packet in the free list.
        ef_vi_receive_init(&virtualInterface,
                           pktBuf->dmaBufferAddress,
                           pktBuf->id);
        rxRingFillLevel++;

        // ef_vi_receive_push() does the actual job of pushing initialized
        // packets to RX ring and it is much slower than initializing function
        // so we call it for a batch of RX_REFILL_BATCH_SIZE buffers
        // that have been already initialized.
        if (!(rxRingFillLevel %  RX_REFILL_BATCH_SIZE)) {
            ef_vi_receive_push(&virtualInterface);
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
    rxBufferPool.destroy();
    txBufferPool.destroy();
    if (fd > 0) {
        sys->close(fd);
    }
}

// See docs in the ``Driver'' class.
uint32_t
SolarFlareDriver::getMaxPacketSize()
{
    return ETHERNET_MAX_DATA_LEN -
           downCast<uint32_t>(sizeof(IpHeader) + sizeof(UdpHeader));
}

// See docs in Driver class.
void
SolarFlareDriver::receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets)
{
    assert(driver->context->dispatch->isDispatchThread());
    if (maxPackets > EF_VI_EVENT_POLL_NUM_EVS) {
        maxPackets = EF_VI_EVENT_POLL_NUM_EVS;
    }

    // To receive packets, descriptors each identifying a buffer,
    // are queued in the RX ring. The event queue is a channel
    // from the adapter to software which notifies software when
    // packets arrive from the network, and when transmits complete
    // (so that the buffers can be freed or reused).
    // events[] array contains the notifications that are fetched off
    // of the event queue on NIC and correspond to this VI of this driver.
    // Each member of the array could be a transmit completion notification
    // or a notification to a received packet or an error that happened
    // in receive or transmits.
    ef_event events[EF_VI_EVENT_POLL_NUM_EVS];

    // Contains the id of packets that have been successfully transmitted
    // or failed. It will be used for fast access to the packet buffer
    // that has been used for that transmission.
    ef_request_id packetIds[EF_VI_TRANSMIT_BATCH];

    // The application retrieves these events from event queue by calling
    // ef_event_poll().
    int eventCnt = ef_eventq_poll(&driver->virtualInterface, events,
                                  maxPackets);
    for (int i = 0; i < eventCnt; i++) {
        LOG(DEBUG, "%d events polled off of NIC", eventCnt);
        int numCompleted = 0;
        int txErrorType = 0;
        int rxDiscardType = 0;
        switch (EF_EVENT_TYPE(events[i])) {

            // A new packet has been received.
            case EF_EVENT_TYPE_RX:

                // The SolarFlare library provides macro functions
                // EF_EVENT_RX_RQ_ID and EF_EVENT_RX_BYTES to find the id and
                // length of the received packet from the notification that's
                // been polled off of the event queue.
                driver->handleReceived(EF_EVENT_RX_RQ_ID(events[i]),
                                       EF_EVENT_RX_BYTES(events[i]),
                                       receivedPackets);
                driver->rxRingFillLevel--;
                break;

            // A packet transmit has been completed
            case EF_EVENT_TYPE_TX:
                 numCompleted =
                    ef_vi_transmit_unbundle(&driver->virtualInterface,
                                            &events[i],
                                            packetIds);
                for (int j = 0; j < numCompleted; j++) {
                    PacketBuff* packetBuff =
                        driver->txBufferPool->getBufferById(packetIds[j]);
                    driver->txBufferPool->freeBuffersVec.push_back(packetBuff);
                }
                break;

            // A packet has been received but it must be discarded because
            // either its erroneous or is not targeted for this driver.
            case EF_EVENT_TYPE_RX_DISCARD:

                // This "if" statement will be taken out from the final code.
                // Currently serves for the cases that we might specify
                // an arbitrary mac address for string locator (something
                // other than the actual mac address of the SolarFlare card)
                if (EF_EVENT_RX_DISCARD_TYPE(events[i])
                             == EF_EVENT_RX_DISCARD_OTHER) {
                    driver->handleReceived(EF_EVENT_RX_DISCARD_RQ_ID(events[i]),
                                           EF_EVENT_RX_DISCARD_BYTES(events[i]),
                                           receivedPackets);
                } else {
                    rxDiscardType = EF_EVENT_RX_DISCARD_TYPE(events[i]);
                    LOG(NOTICE, "Received discarded packet of type %d (%s)",
                        rxDiscardType,
                        driver->rxDiscardTypeToStr(rxDiscardType));

                    // The buffer for the discarded received packet must be
                    // returned to the receive free list
                    int packetId = EF_EVENT_RX_DISCARD_RQ_ID(events[i]);
                    PacketBuff* packetBuff =
                        driver->rxBufferPool->getBufferById(packetId);
                    driver->rxBufferPool->freeBuffersVec.push_back(packetBuff);
                }
                driver->rxRingFillLevel--;
                break;

            // Error happened in transmitting a packet.
            case EF_EVENT_TYPE_TX_ERROR:
                txErrorType = EF_EVENT_TX_ERROR_TYPE(events[i]);
                LOG(WARNING, "TX error type %d (%s) happened!",
                    txErrorType,
                    driver->txErrTypeToStr(txErrorType));
                break;
            // Type of the event does not match to any above case statements.
            default:
                LOG(WARNING, "Unexpected event for event id %d", i);
                break;
        }
    }

    // Push back packet buffers to the RX ring to provide enough ring buffers
    // for the incoming packets.
    driver->refillRxRing();
}

/// See docs in the ``Driver'' class.
void
SolarFlareDriver::release(char* payload)
{
    Dispatch::Lock _(context->dispatch);
    assert(buffsNotReleased > 0);
    buffsNotReleased--;
    PacketBuff* packetBuff = reinterpret_cast<PacketBuff*>(payload
            - OFFSET_OF(PacketBuff, dmaBuffer));
    rxCopyPool.destroy(packetBuff);
}

/// See docs in the ``Driver'' class.
Driver::Address*
SolarFlareDriver::newAddress(const ServiceLocator& serviceLocator) {
    return new MacIpAddress(serviceLocator);
}

/// See docs in the ``Driver'' class.
void
SolarFlareDriver::sendPacket(const Driver::Address* recipient,
                             const void* header,
                             uint32_t headerLen,
                             Buffer::Iterator* payload,
                             int priority,
                             TransmitQueueState* txQueueState)
{

    uint32_t udpPayloadLen = downCast<uint32_t>(headerLen
                                + (payload ? payload->size() : 0));
    uint16_t udpLen = downCast<uint16_t>(udpPayloadLen + sizeof(UdpHeader));
    uint16_t ipLen = downCast<uint16_t>(udpLen + sizeof(IpHeader));
    assert(udpPayloadLen <= getMaxPacketSize());
    uint32_t totalLen =  downCast<uint32_t>(sizeof(EthernetHeader) + ipLen);

    const MacIpAddress* recipientAddress =
        static_cast<const MacIpAddress*>(recipient);

    // We need one transmit buffer for the Eth+IP+UDP header and possibly the
    // whole message if the payload is not in the log memory.
    PacketBuff* txRegisteredBuf = txBufferPool->popFreeBuffer();
    while (!txRegisteredBuf) {

        // No buffer is available, we have to poll on the event queue
        // to get a transmit buffer that's finished transmission.
        LOG(WARNING, "No free TX buffer available! Calling poll() to"
            " get free buffers");
        context->dispatch->poll();
    }

    // Fill Ethernet header except recipient's mac address
    EthernetHeader* ethHdr = new(txRegisteredBuf->dmaBuffer) EthernetHeader;
    memcpy(ethHdr->srcAddress, localAddress->macAddress->address,
           sizeof(ethHdr->srcAddress));
    const uint8_t *recvMac = recipientAddress->macAddress->address;
    memcpy(ethHdr->destAddress, recvMac, sizeof(ethHdr->destAddress));
    ethHdr->etherType = HTONS(EthPayloadType::IP_V4);

    // Fill IP header
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
        reinterpret_cast<sockaddr_in*>(&localAddress->ipAddress->address);
    ipHdr->ipSrcAddress = srcAddr->sin_addr.s_addr;
    const sockaddr_in* destAddr = reinterpret_cast<const sockaddr_in*>(
                                       &recipientAddress->ipAddress->address);
    ipHdr->ipDestAddress = destAddr->sin_addr.s_addr;

    // Fill UDP header
    UdpHeader* udpHdr =
        new(reinterpret_cast<char*>(ipHdr) + sizeof(IpHeader)) UdpHeader;
    udpHdr->srcPort = srcAddr->sin_port;
    udpHdr->destPort = destAddr->sin_port;
    udpHdr->totalLength = HTONS(udpLen);
    udpHdr->totalChecksum = 0;

    // Copy the header over to the registered buffer.
    uint8_t* transportHdr =
        reinterpret_cast<uint8_t*>(udpHdr) + sizeof(UdpHeader);
    memcpy(transportHdr, header, headerLen);

    // The address where the next chunk of data must be copied to in
    // txRegisteredBuf.
    uint8_t* nextChunkStart = transportHdr + headerLen;

    // Copy the payload to the transmit buffer.
    while (payload && !payload->isDone()) {
        memcpy(nextChunkStart, payload->getData(), payload->getLength());
        nextChunkStart += payload->getLength();
        payload->next();
    }


    // Resolve recipient mac address, if needed, and send the packet out.
    if (recipientAddress->macProvided ||
            arpCache->arpLookup(ipHdr->ipDestAddress, ethHdr->destAddress)) {

        // By invoking ef_vi_transmitv() the descriptor that describes the
        // packet is queued in the transmit ring, and a doorbell is rung to
        // inform the adapter that the transmit ring is non-empty. Later on
        // in Poller::poll() we fetch notifications off of the event queue
        // that implies which packet transmit is completed.
        ef_vi_transmit(&virtualInterface, txRegisteredBuf->dmaBufferAddress
                , downCast<int>(totalLen), txRegisteredBuf->id);
        //LOG(NOTICE, "%s", (ethernetHeaderToStr(ethHdr)).c_str());
        //LOG(NOTICE, "%s", (ipHeaderToStr(ipHdr)).c_str());
        //LOG(NOTICE, "%s", (udpHeaderToStr(udpHdr)).c_str())
    } else {

            // Could not resolve mac from the local ARP cache nor kernel ARP
            // cache. This probably means we don't have access to kernel ARP
            // cache or kernel ARP cache times out pretty quickly or the ARP
            // packets take a long time to travel in network. Any ways, we
            // return immediately without sending the packet out and let the
            // higher level transport code to take care of retransmission later.
            txBufferPool->freeBuffersVec.push_back(txRegisteredBuf);
            in_addr destInAddr = {ipHdr->ipDestAddress};
            LOG(WARNING, "Was not able to resolve the MAC address for the"
                " packet destined to %s", inet_ntoa(destInAddr));
    }
    uint64_t now = Cycles::rdtsc();
    queueEstimator.packetQueued(totalLen, now, txQueueState);
}

/**
 * A helper method for the packets that arrive on the NIC. It creates a
 * Received that describes the incoming packet, copies the packet into
 * the Received's buffer, and returns the packet buffer to the free pool.
 * 
 * \param packetId
 *      This is the id of the packetBuff within the received memory chunk.
 *      This is used for fast calculation of the address of the packet within
 *      that memory chunk.
 * \param packetLen
 *      The actual length in bytes of the packet that's been received in NIC.
 * \param receivedPackets
 *      A Received containing the incoming packet is pushed onto the back
 *      of this vector.
 *      
 */
void
SolarFlareDriver::handleReceived(int packetId, int packetLen,
        std::vector<Received>* receivedPackets)
{
    assert(downCast<uint32_t>(packetLen) >=
                        rxPrefixLen + sizeof(EthernetHeader) +
                        sizeof(IpHeader) + sizeof(UdpHeader));

    assert(downCast<uint32_t>(packetLen) < ETHERNET_MAX_DATA_LEN);
    struct PacketBuff* packetBuff = rxBufferPool->getBufferById(packetId);
    char* ethPkt = reinterpret_cast<char*>(packetBuff->dmaBuffer) + rxPrefixLen;
    uint32_t totalLen = packetLen - rxPrefixLen;
    EthernetHeader* ethHdr = reinterpret_cast<EthernetHeader*>(ethPkt);
    IpHeader* ipHdr = reinterpret_cast<IpHeader*>(
                      reinterpret_cast<char*>(ethHdr)
                      + sizeof(EthernetHeader));

    UdpHeader* udpHdr = reinterpret_cast<UdpHeader*>(
                            reinterpret_cast<char*>(ipHdr)
                            + sizeof(IpHeader));

    //LOG(NOTICE, "%s", ethernetHeaderToStr(ethHdr).c_str());
    //LOG(NOTICE, "%s",ipHeaderToStr(ipHdr).c_str());
    //LOG(NOTICE, "%s",udpHeaderToStr(udpHdr).c_str());

    // Find mac, ip and port of the sender
    uint8_t* mac = ethHdr->srcAddress;
    uint32_t ip = ntohl(ipHdr->ipSrcAddress);
    uint16_t port = NTOHS(udpHdr->srcPort);

    // Construct a buffer and copy the udp payload of the received packet into
    // the buffer.
    PacketBuff* copyBuffer = rxCopyPool.construct();
    uint32_t length =
        downCast<uint32_t>(NTOHS(udpHdr->totalLength) -
        downCast<uint16_t>(sizeof(UdpHeader)));
    if (length != (totalLen - ETH_DATA_OFFSET)) {
        LOG(WARNING, "total payload bytes received is %u,"
            " but UDP payload length is %u bytes",
            (totalLen - ETH_DATA_OFFSET), length);
    }
    memcpy(copyBuffer->dmaBuffer, ethPkt + ETH_DATA_OFFSET, length);
    buffsNotReleased++;

    receivedPackets->emplace_back(
            copyBuffer->macIpAddress.construct(ip, port, mac), this,
            length, reinterpret_cast<char*>(copyBuffer->dmaBuffer));

    // return original RX ring buffer descriptor to the pool of RX buffer
    // descriptors.
    rxBufferPool->freeBuffersVec.push_back(packetBuff);

}

/**
 * Returns the string equivalent of error type of a discarded received packet.
 *
 * \param type
 *      The numeric value of RX discard type.
 * \return
 *      The string equivalent of a numeric value for discard type.  
 */
const char*
SolarFlareDriver::rxDiscardTypeToStr(int type)
{
    switch (type) {

        // Checksum value in IP or UDP header is erroneous.
        case EF_EVENT_RX_DISCARD_CSUM_BAD:
            return "EF_EVENT_RX_DISCARD_CSUM_BAD";

        // The packet is a multicast packet that is not targeted to this driver.
        case EF_EVENT_RX_DISCARD_MCAST_MISMATCH:
            return "EF_EVENT_RX_DISCARD_MCAST_MISMATCH";

        // Error in CRC checksum of the Ethernet frame.
        case EF_EVENT_RX_DISCARD_CRC_BAD:
            return "EF_EVENT_RX_DISCARD_CRC_BAD";

        // Ethernet frame truncated and shorter than expected.
        case EF_EVENT_RX_DISCARD_TRUNC:
            return "EF_EVENT_RX_DISCARD_TRUNC";

        // The buffer owner id of this event does not match the one for this
        // driver meaning that this event was delivered to this driver by
        // mistake.
        case EF_EVENT_RX_DISCARD_RIGHTS:
            return "EF_EVENT_RX_DISCARD_RIGHTS";

        // Unexpected error happened for the current event. The current event
        // is not what it is expected to be which means that an event is lost.
        case EF_EVENT_RX_DISCARD_EV_ERROR:
            return "EF_EVENT_RX_DISCARD_EV_ERROR";

        // Any other type of RX error not covered in above cases.
        default:
            return "EF_EVENT_RX_DISCARD_OTHER";
    }
}

/**
 * Returns the string equivalent of error that happened in transmitting a
 * packet.
 * \param type
 *      The numeric value of TX error type.
 * \return
 *      A string describing the type of error happened in transmit.
 */
const char*
SolarFlareDriver::txErrTypeToStr(int type)
{
    switch (type) {

        // The buffer owner id of this packet doesn't match to this driver.
        case EF_EVENT_TX_ERROR_RIGHTS:
            return "EF_EVENT_TX_ERROR_RIGHTS";

        // Transmit wait queue overflow.
        case EF_EVENT_TX_ERROR_OFLOW:
            return "EF_EVENT_TX_ERROR_OFLOW";

        // The transmit packet is too big to send.
        case EF_EVENT_TX_ERROR_2BIG:
            return "EF_EVENT_TX_ERROR_2BIG";

        // Bus error happened while transmitting this packet.
        case EF_EVENT_TX_ERROR_BUS:
            return "EF_EVENT_TX_ERROR_BUS";

        // None of the errors above.
        default:
            return "NO_TYPE_SPECIFIED_FOR_THIS_ERR";
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
