/* Copyright (c) 2010-2016 Stanford University
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


#include <errno.h>
#include <sys/types.h>

#include "Common.h"
#include "Cycles.h"
#include "BitOps.h"
#include "InfUdDriver.h"
#include "PcapFile.h"
#include "PerfStats.h"
#include "ServiceLocator.h"
#include "ShortMacros.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to enable time tracing in
// this driver.
#define TIME_TRACE 0

/**
 * Construct an InfUdDriver.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param sl
 *      Service locator for transport that will be using this driver.
 *      May contain any of the following parameters, which are used
 *      to configure the new driver:
 *      dev -      Infiniband device name to use.
 *      devPort -  Infiniband port to use.
 *      gbs -      Bandwidth of the Infiniband network, in Gbits/sec.
 *                 Used for estimating transmit queue lengths.
 *      mac -      MAC address for this host; if ethernet is true.
 *      Specifies the Infiniband device and physical port to use.
 *      If NULL, the first device and port are used by default.
 * \param ethernet
 *      Whether to use the Ethernet port.
 */
InfUdDriver::InfUdDriver(Context* context, const ServiceLocator *sl,
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
    , mutex("InfUdDriver")
    , rxBuffers()
    , txBuffers()
    , freeTxBuffers()
    , ibPhysicalPort(ethernet ? 2 : 1)
    , lid(0)
    , qpn(0)
    , localMac()
    , locatorString()
    , bandwidthGbps(24)                   // Default bandwidth = 24 gbs
    , queueEstimator(0)
    , maxTransmitQueueSize(0)
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

        try {
            bandwidthGbps = sl->getOption<int>("gbs");
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
    LOG(NOTICE, "InfUdDriver bandwidth: %d Gbits/sec, maxTransmitQueueSize: "
            "%u bytes", bandwidthGbps, maxTransmitQueueSize);

    if (ethernet && !macAddressProvided)
        localMac.construct(MacAddress::RANDOM);

    infiniband = realInfiniband.construct(ibDeviceName);

    // allocate rx and tx buffers
    uint32_t bufSize = (maxPacketSize +
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
    if (freeTxBuffers.empty()) {
        reapTransmitBuffers();
        if (freeTxBuffers.empty()) {
            // We are temporarily out of buffers. Time how long it takes
            // before a transmit buffer becomes available again (a long
            // time is a bad sign); in the normal case this code should
            // not be invoked.
            uint64_t start = Cycles::rdtsc();
            while (freeTxBuffers.empty()) {
                reapTransmitBuffers();
            }
            double waitMs = 1e03 * Cycles::toSeconds(Cycles::rdtsc() - start);
            if (waitMs > 5.0)  {
                LOG(WARNING, "Long delay waiting for transmit buffers "
                        "(%.1f ms elapsed, %lu buffers now free)",
                        waitMs, freeTxBuffers.size());
            }
        }
    }

    BufferDescriptor* bd = freeTxBuffers.back();
    freeTxBuffers.pop_back();
    return bd;
}

// See Driver.h for documentation
int
InfUdDriver::getTransmitQueueSpace(uint64_t currentTime)
{
    return maxTransmitQueueSize - queueEstimator.getQueueSize(currentTime);
}

/**
 * Check the NIC to see if it is ready to return transmit buffers
 * from previously-transmit packets. If there are any available,
 * reclaim them. This method also detects and logs transmission errors.
 */
void
InfUdDriver::reapTransmitBuffers()
{
#define MAX_TO_RETRIEVE 20
    ibv_wc retArray[MAX_TO_RETRIEVE];
    int numBuffers = infiniband->pollCompletionQueue(txcq,
            MAX_TO_RETRIEVE, retArray);
    for (int i = 0; i < numBuffers; i++) {
        BufferDescriptor* bd =
            reinterpret_cast<BufferDescriptor*>(retArray[i].wr_id);
        freeTxBuffers.push_back(bd);

        if (retArray[i].status != IBV_WC_SUCCESS) {
            LOG(WARNING, "Infud transmit failed: %s",
                infiniband->wcStatusToString(retArray[i].status));
        }
    }
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
    bd->messageBytes = length;

    if (pcapFile)
        pcapFile->append(bd->buffer, length);

    try {
        LOG(DEBUG, "sending %u bytes to %s...", length,
            addr->toString().c_str());
#if TIME_TRACE
        TimeTrace::record("before postSend in InfUdDriver");
#endif
        infiniband->postSend(qp, bd, length,
                             localMac ? NULL
                                      : static_cast<const Address*>(addr),
                             QKEY);
#if TIME_TRACE
        TimeTrace::record("after postSend in InfUdDriver");
#endif
        queueEstimator.packetQueued(length, Cycles::rdtsc());
        PerfStats::threadStats.networkOutputBytes += length;
        LOG(DEBUG, "sent successfully!");
    } catch (...) {
        LOG(DEBUG, "send failed!");
        throw;
    }

    // If there are a lot of outstanding transmit buffers, see if we
    // can reclaim some of them. This code isn't strictly necessary, since
    // we'll eventually reclaim them in getTransmitBuffer. However, if we
    // wait until then, there could be a lot of them to reclaim, which will
    // take a lot of time. Since that code is on the critical path, it could
    // add to tail latency. If we do it here, there's some chance that we
    // didn't actually have anything else to do, so the cost of reclamation
    // will be hidden; even if it isn't, we'll pay in smaller chunks, which
    // will have less of an impact on tail latency.
    if (freeTxBuffers.size() < MAX_TX_QUEUE_DEPTH-10) {
        reapTransmitBuffers();
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::receivePackets(int maxPackets,
            std::vector<Received>* receivedPackets)
{
    static const int MAX_COMPLETIONS = 50;
    ibv_wc wc[MAX_COMPLETIONS];
    uint32_t maxToReceive = (maxPackets < MAX_COMPLETIONS) ? maxPackets
            : MAX_COMPLETIONS;
    int numPackets = infiniband->pollCompletionQueue(qp->rxcq,
            maxToReceive, wc);
    if (numPackets <= 0) {
        if (numPackets < 0) {
            LOG(ERROR, "pollCompletionQueue failed with result %d", numPackets);
        }
        return;
    }
#if TIME_TRACE
    TimeTrace::record(context->dispatch->currentTime,
            "start of poller loop");
    TimeTrace::record("InfUdDriver received %d packets", numPackets);
#endif

    // First, prefetch the initial bytes of all the incoming packets. This
    // allows us to process multiple cache misses concurrently, which improves
    // throughput under load.
    for (int i = 0; i < numPackets; i++) {
        ibv_wc* incoming = &wc[i];
        BufferDescriptor *bd =
                reinterpret_cast<BufferDescriptor *>(incoming->wr_id);
        prefetch(bd->buffer,
                incoming->byte_len > 256 ? 256 : incoming->byte_len);
    }

    // Each iteration of the following loop processes one incoming packet.
    for (int i = 0; i < numPackets; i++) {
        ibv_wc* incoming = &wc[i];
        BufferDescriptor *bd =
                reinterpret_cast<BufferDescriptor *>(incoming->wr_id);
        PacketBuf* buffer = NULL;
        if (incoming->status != IBV_WC_SUCCESS) {
            LOG(ERROR, "error in Infiniband completion (%d: %s)",
                incoming->status,
                infiniband->wcStatusToString(incoming->status));
            goto error;
        }

        bd->messageBytes = incoming->byte_len;
        if (bd->messageBytes < (localMac ? 60 : GRH_SIZE)) {
            LOG(ERROR, "received impossibly short packet: %d bytes",
                    bd->messageBytes);
            goto error;
        }

        {
            Lock lock(mutex);
            buffer = packetBufPool.construct();
            packetBufsUtilized++;
        }

        uint32_t length;
        const Driver::Address* sender;
        PerfStats::threadStats.networkInputBytes += bd->messageBytes;
        // copy from the infiniband buffer into our dynamically allocated
        // buffer.
        if (localMac) {
            auto& ethHdr = *reinterpret_cast<EthernetHeader*>(bd->buffer);
            sender = buffer->macAddress.construct(ethHdr.sourceAddress);
            length = ethHdr.length;
            if (length + sizeof(ethHdr) > bd->messageBytes) {
                LOG(ERROR, "corrupt packet (data length %d, packet length %d",
                        length, bd->messageBytes);
                goto error;
            }
            memcpy(buffer->payload, bd->buffer + sizeof(ethHdr), length);
        } else {
            buffer->infAddress.construct(*infiniband,
                    ibPhysicalPort, incoming->slid, incoming->src_qp);
            sender = buffer->infAddress.get();
            length = bd->messageBytes - GRH_SIZE;
            memcpy(buffer->payload, bd->buffer + GRH_SIZE, length);
        }
        receivedPackets->emplace_back(sender, this, length, buffer->payload);

        // post the original infiniband buffer back to the receive queue
        infiniband->postReceive(qp, bd);
        continue;

      error:
        if (buffer != NULL) {
            Lock lock(mutex);
            packetBufPool.destroy(buffer);
            packetBufsUtilized--;
        }
        infiniband->postReceive(qp, bd);
    }
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
