/* Copyright (c) 2010-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * Implementation for #RAMCloud::InfUdDriver, an Infiniband packet
 * driver using unconnected datagram queue-pairs (UD).
 */

/*
 * Note that in UD mode, Infiniband receivers prepend a 40-byte
 * Global Routing Header (GRH) to all incoming frames. Immediately
 * following is the data transmitted. The interface is not symmetric:
 * Sending applications do not include a GRH in the buffers they pass
 * to the HCA.
 */

#include <errno.h>
#include <sys/types.h>

#include "Common.h"
#include "BitOps.h"
#include "FastTransport.h"
#include "InfUdDriver.h"
#include "PcapFile.h"
#include "PerfStats.h"
#include "ServiceLocator.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Construct an InfUdDriver.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param sl
 *      Specifies the Infiniband device and physical port to use.
 *      If NULL, the first device and port are used by default.
 *      Note that Infiniband has no concept of statically allocated
 *      queue pair numbers (ports). This makes this driver unsuitable
 *      for servers until we add some facility for dynamic addresses
 *      and resolution.
 * \param ethernet
 *      Whether to use the Ethernet port.
 */
InfUdDriver::InfUdDriver(Context* context,
                                     const ServiceLocator *sl,
                                     bool ethernet)
    : context(context)
    , realInfiniband()
    , infiniband()
    , QKEY(ethernet ? 0 : 0xdeadbeef)
    , rxcq(0)
    , txcq(0)
    , qp()
    , packetBufPool()
    , packetBufsUtilized(0)
    , mutex()
    , rxBuffers()
    , txBuffers()
    , freeTxBuffers()
    , ibPhysicalPort(ethernet ? 2 : 1)
    , lid(0)
    , qpn(0)
    , localMac()
    , locatorString()
    , incomingPacketHandler()
    , poller()
{
    const char *ibDeviceName = NULL;
    bool macAddressProvided = false;

    if (sl != NULL) {
        locatorString = sl->getOriginalString();

        if (ethernet) {
            try {
                localMac.construct(sl->getOption<const char*>("mac"));
                macAddressProvided = true;
            } catch (ServiceLocator::NoSuchKeyException& e) {}
        }

        try {
            ibDeviceName   = sl->getOption<const char *>("dev");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

        try {
            ibPhysicalPort = sl->getOption<int>("devport");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

    }

    if (ethernet && !macAddressProvided)
        localMac.construct(MacAddress::RANDOM);

    infiniband = realInfiniband.construct(ibDeviceName);

    // allocate rx and tx buffers
    uint32_t bufSize = (getMaxPacketSize() +
        (localMac ? sizeof32(EthernetHeader) : GRH_SIZE));
    bufSize = BitOps::powerOfTwoGreaterOrEqual(bufSize);
    rxBuffers.construct(realInfiniband->pd, bufSize,
                        uint32_t(MAX_RX_QUEUE_DEPTH));
    txBuffers.construct(realInfiniband->pd, bufSize,
                        uint32_t(MAX_TX_QUEUE_DEPTH));

    // create completion queues for receive and transmit
    rxcq = infiniband->createCompletionQueue(MAX_RX_QUEUE_DEPTH);
    if (rxcq == NULL) {
        LOG(ERROR, "failed to create receive completion queue");
        throw DriverException(HERE, errno);
    }

    txcq = infiniband->createCompletionQueue(MAX_TX_QUEUE_DEPTH);
    if (txcq == NULL) {
        LOG(ERROR, "failed to create transmit completion queue");
        throw DriverException(HERE, errno);
    }

    qp = infiniband->createQueuePair(localMac ? IBV_QPT_RAW_ETH
                                              : IBV_QPT_UD,
                                     ibPhysicalPort, NULL,
                                     txcq, rxcq, MAX_TX_QUEUE_DEPTH,
                                     MAX_RX_QUEUE_DEPTH,
                                     QKEY);

    // cache these for easier access
    lid = infiniband->getLid(ibPhysicalPort);
    qpn = qp->getLocalQpNumber();

    // update our locatorString, if one was provided, with the dynamic
    // address
    if (!locatorString.empty()) {
        char c = locatorString[locatorString.size()-1];
        if ((c != ':') && (c != ',')) {
            locatorString += ",";
        }
        if (localMac) {
            if (!macAddressProvided)
                locatorString += "mac=" + localMac->toString();
        } else {
            locatorString += format("lid=%u,qpn=%u", lid, qpn);
        }
        LOG(NOTICE, "Locator for InfUdDriver: %s", locatorString.c_str());
    }

    // add receive buffers so we can transition to RTR
    foreach (auto& bd, *rxBuffers)
        infiniband->postReceive(qp, &bd);
    foreach (auto& bd, *txBuffers)
        freeTxBuffers.push_back(&bd);

    qp->activate(localMac);
}

/**
 * Destroy an InfUdDriver and free allocated resources.
 */
InfUdDriver::~InfUdDriver()
{
    if (packetBufsUtilized != 0) {
        LOG(WARNING, "packetBufsUtilized: %lu",
            packetBufsUtilized);
    }

    // The constructor creates a bunch of Infiniband resources that may
    // need to be freed here (but aren't right now).

    delete qp;
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::connect(IncomingPacketHandler* incomingPacketHandler) {
    this->incomingPacketHandler.reset(incomingPacketHandler);
    poller.construct(context, this);
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::disconnect() {
    poller.destroy();
    this->incomingPacketHandler.reset();
}

/*
 * See docs in the ``Driver'' class.
 */
uint32_t
InfUdDriver::getMaxPacketSize()
{
    const uint32_t eth = 1500 + 14 - sizeof(EthernetHeader);
    const uint32_t inf = 2048 - GRH_SIZE;
    const size_t payloadSize = sizeof(static_cast<PacketBuf*>(NULL)->payload);
    static_assert(payloadSize >= eth,
                  "InfUdDriver PacketBuf too small for Ethernet payload");
    static_assert(payloadSize >= inf,
                  "InfUdDriver PacketBuf too small for Infiniband payload");
    return localMac ? eth : inf;
}

/**
 * Return a free transmit buffer, wrapped by its corresponding
 * BufferDescriptor. If there are none, block until one is available.
 *
 * Any errors from previous transmissions are basically
 *               thrown on the floor, though we do log them. We need
 *               to think a bit more about how this 'fire-and-forget'
 *               behaviour impacts our Transport API.
 * This code is copied from InfRcTransport. It should probably
 *               move in some form to the Infiniband class.
 */
Infiniband::BufferDescriptor*
InfUdDriver::getTransmitBuffer()
{
    // if we've drained our free tx buffer pool, we must wait.
    while (freeTxBuffers.empty()) {
        ibv_wc retArray[MAX_TX_QUEUE_DEPTH];
        int n = infiniband->pollCompletionQueue(txcq,
                                                MAX_TX_QUEUE_DEPTH,
                                                retArray);
        for (int i = 0; i < n; i++) {
            BufferDescriptor* bd =
                reinterpret_cast<BufferDescriptor*>(retArray[i].wr_id);
            freeTxBuffers.push_back(bd);

            if (retArray[i].status != IBV_WC_SUCCESS) {
                LOG(ERROR, "Transmit failed: %s",
                    infiniband->wcStatusToString(retArray[i].status));
            }
        }
    }

    BufferDescriptor* bd = freeTxBuffers.back();
    freeTxBuffers.pop_back();
    return bd;
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::release(char *payload)
{
    Lock lock(mutex);

    // Note: the payload is actually contained in a PacketBuf structure,
    // which we return to a pool for reuse later.
    assert(packetBufsUtilized > 0);
    packetBufsUtilized--;
    packetBufPool.destroy(
        reinterpret_cast<PacketBuf*>(payload - OFFSET_OF(PacketBuf, payload)));
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::sendPacket(const Driver::Address *addr,
                        const void *header,
                        uint32_t headerLen,
                        Buffer::Iterator *payload)
{
    uint32_t totalLength = headerLen +
                           (payload ? payload->size() : 0);
    assert(totalLength <= getMaxPacketSize());

    BufferDescriptor* bd = getTransmitBuffer();

    // copy buffer over
    char *p = bd->buffer;
    if (localMac) {
        auto& ethHdr = *new(p) EthernetHeader;
        memcpy(ethHdr.destAddress,
               static_cast<const MacAddress*>(addr)->address, 6);
        memcpy(ethHdr.sourceAddress, localMac->address, 6);
        ethHdr.etherType = HTONS(0x8001);
        ethHdr.length = downCast<uint16_t>(totalLength);
        p += sizeof(ethHdr);
    }
    memcpy(p, header, headerLen);
    p += headerLen;
    while (payload && !payload->isDone()) {
        memcpy(p, payload->getData(), payload->getLength());
        p += payload->getLength();
        payload->next();
    }
    uint32_t length = static_cast<uint32_t>(p - bd->buffer);

    if (pcapFile)
        pcapFile->append(bd->buffer, length);

    try {
        LOG(DEBUG, "sending %u bytes to %s...", length,
            addr->toString().c_str());
        infiniband->postSend(qp, bd, length,
                             localMac ? NULL
                                      : static_cast<const Address*>(addr),
                             QKEY);
        PerfStats::threadStats.networkOutputBytes += length;
        LOG(DEBUG, "sent successfully!");
    } catch (...) {
        LOG(DEBUG, "send failed!");
        throw;
    }
}

/*
 * See docs in the ``Driver'' class.
 */
int
InfUdDriver::Poller::poll()
{
    assert(driver->context->dispatch->isDispatchThread());
    static const int MAX_COMPLETIONS = 10;
    ibv_wc wc[MAX_COMPLETIONS];
    int numPackets = driver->infiniband->pollCompletionQueue(driver->qp->rxcq,
            MAX_COMPLETIONS, wc);
    if (numPackets <= 0) {
        if (numPackets < 0) {
            LOG(ERROR, "pollCompletionQueue failed with result %d", numPackets);
        }
        return 0;
    }

    // Each iteration of the following loop processes one incoming packet.
    for (int i = 0; i < numPackets; i++) {
        ibv_wc* incoming = &wc[i];
        BufferDescriptor *bd =
                reinterpret_cast<BufferDescriptor *>(incoming->wr_id);
        bd->messageBytes = incoming->byte_len;
        PacketBuf* buffer = NULL;
        Received received;
        if (incoming->status != IBV_WC_SUCCESS) {
            LOG(ERROR, "error in Infiniband completion (%d: %s)",
                incoming->status,
                driver->infiniband->wcStatusToString(incoming->status));
            goto error;
        }
        prefetch(bd->buffer, incoming->byte_len);

        if (pcapFile)
            pcapFile->append(bd->buffer, bd->messageBytes);

        if (bd->messageBytes < (driver->localMac ? 60 : GRH_SIZE)) {
            LOG(ERROR, "received impossibly short packet: %d bytes",
                    bd->messageBytes);
            goto error;
        }

        {
            Lock lock(driver->mutex);
            buffer = driver->packetBufPool.construct();
            driver->packetBufsUtilized++;
        }

        received.driver = driver;
        received.payload = buffer->payload;
        PerfStats::threadStats.networkInputBytes += bd->messageBytes;
        // copy from the infiniband buffer into our dynamically allocated
        // buffer.
        if (driver->localMac) {
            auto& ethHdr = *reinterpret_cast<EthernetHeader*>(bd->buffer);
            received.sender =
                    buffer->macAddress.construct(ethHdr.sourceAddress);
            received.len = ethHdr.length;
            if (received.len + sizeof(ethHdr) > bd->messageBytes) {
                LOG(ERROR, "corrupt packet (data length %d, packet length %d",
                        received.len, bd->messageBytes);
                goto error;
            }
            memcpy(received.payload, bd->buffer + sizeof(ethHdr), received.len);
        } else {
            buffer->infAddress.construct(*driver->infiniband,
                    driver->ibPhysicalPort, incoming->slid, incoming->src_qp);
            received.sender = buffer->infAddress.get();
            received.len = bd->messageBytes - GRH_SIZE;
            memcpy(received.payload, bd->buffer + GRH_SIZE, received.len);
        }
        driver->incomingPacketHandler->handlePacket(&received);

        // post the original infiniband buffer back to the receive queue
        driver->infiniband->postReceive(driver->qp, bd);
        continue;

      error:
        if (buffer != NULL) {
            Lock lock(driver->mutex);
            driver->packetBufPool.destroy(buffer);
            driver->packetBufsUtilized--;
        }
        driver->infiniband->postReceive(driver->qp, bd);
    }
    return numPackets;
}

/**
 * See docs in the ``Driver'' class.
 */
string
InfUdDriver::getServiceLocator()
{
    return locatorString;
}

} // namespace RAMCloud
