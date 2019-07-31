/* Copyright (c) 2010-2017 Stanford University
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
#include "NetUtil.h"
#include "OptionParser.h"
#include "PcapFile.h"
#include "PerfStats.h"
#include "ServiceLocator.h"
#include "ShortMacros.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to enable time tracing in this driver.
#define TIME_TRACE 0

// Provides a cleaner way of invoking TimeTrace::record, with the code
// conditionally compiled in or out by the TIME_TRACE #ifdef. Arguments
// are made uint64_t (as opposed to uin32_t) so the caller doesn't have to
// frequently cast their 64-bit arguments into uint32_t explicitly: we will
// help perform the casting internally.
namespace {
    inline void
    timeTrace(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
#if TIME_TRACE
        TimeTrace::record(format, uint32_t(arg0), uint32_t(arg1),
                uint32_t(arg2), uint32_t(arg3));
#endif
    }
}

using EthernetHeader = NetUtil::EthernetHeader;

// Size of the Ethernet frame header (currently excluding the optional VLAN tag)
#define ETH_HEADER_SIZE sizeof32(EthernetHeader)


/**
 * Construct an InfUdDriver.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param sl
 *      Service locator for transport that will be using this driver.
 *      May contain any of the following parameters, which are used
 *      to configure the new driver:
 *      hca -      Infiniband device name to use.
 *      port -     Infiniband port to use.
 *      eth -      Ethernet interface name to use. Only meaningful if
 *                 option "hca" is not specified.
 */
InfUdDriver::InfUdDriver(Context* context, const ServiceLocator *sl)
    : Driver(context)
    , realInfiniband()
    , infiniband()
    , loopbackPkts()
    , rxPool()
    , rxBuffersInHca(0)
    , rxBufferLogThreshold(0)
    , txPool()
    , txBuffersInHca()
    , QKEY(0xdeadbeef)
    , rxcq(0)
    , txcq(0)
    , qp()
    , ibPhysicalPort()
    , lid(0)
    , mtu(0)
    , qpn(0)
    , localMac()
    , locatorString("infud:")
    , bandwidthGbps(~0u)
    , sendRequests()
    , sendsSinceLastSignal(0)
    , zeroCopyStart(NULL)
    , zeroCopyEnd(NULL)
    , zeroCopyRegion(NULL)
{
    ServiceLocator config = readDriverConfigFile();
    const char* ibDeviceName = config.getOption<const char*>("hca", NULL);
    const char* ethIfName = config.getOption<const char*>("eth", NULL);
    ibPhysicalPort = config.getOption<int>("port", 1);
    LOG(NOTICE, "InfUdDriver config: %s", config.getOriginalString().c_str());

    // Open and initialize the specified device.
    infiniband = realInfiniband.construct(ibDeviceName);
#if 1
    bandwidthGbps = std::min(bandwidthGbps,
            infiniband->getBandwidthGbps(ibPhysicalPort));
#else
    // As of 11/2018, the network bandwidth of our RC machines at Stanford is
    // actually limited by the effective bandwidth of PCIe 2.0x4, which should
    // be ~29Gbps when taking into account the overhead of PCIe headers.
    // For example, suppose the HCA's MTU is 256B and the PCIe headers are 24B
    // in total, the effective bandwidth of PCIe 2.0x4 is
    //      32Gbps * 256 / (256 + 24) = 29.25Gbps
    // Unfortunately, it appears that our ConnextX-2 HCA somehow cannot fully
    // utilize the 29Gbps PCIe bandwidth when sending UD packets. This can be
    // verified by running one or more ib_send_bw programs on two machines.
    // The maximum outgoing bandwidth we can achieve in practice is ~3020MB/s,
    // or 23.6Gbps. Note that we need to set the outgoing bandwidth slightly
    // higher than 24Gbps in order to saturate the 23.6Gbps outgoing bandwidth.
    // This is because the throughput of the HCA has non-negligible variation:
    // when it's running faster than 24Gbps, we don't want the transport to
    // throttle the throughput and leave the HCA idle.
    bandwidthGbps = std::min(bandwidthGbps, 26u);
#endif
    bool ethernet = ethIfName;
    mtu = ethernet ? (1500 + ETH_HEADER_SIZE) :
            infiniband->getMtu(ibPhysicalPort);
    if (ethernet) {
        localMac.construct(NetUtil::getLocalMac(ethIfName).c_str());
    }

    // Setup queue estimator
    queueEstimator.setBandwidth(1000*bandwidthGbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthGbps)
            * MAX_DRAIN_TIME / 8.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    if (maxTransmitQueueSize < 2*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = 2*maxPacketSize;
    }
    LOG(NOTICE, "InfUdDriver bandwidth: %u Gbits/sec, maxTransmitQueueSize: "
            "%u bytes, maxPacketSize %u bytes", bandwidthGbps,
            maxTransmitQueueSize, maxPacketSize);

    // Allocate buffer pools.
    uint32_t bufSize = (maxPacketSize +
        (localMac ? ETH_HEADER_SIZE : GRH_SIZE));
    bufSize = BitOps::powerOfTwoGreaterOrEqual(bufSize);
    uint64_t start = Cycles::rdtsc();

    // The "+0" syntax below is a hack that avoids linker "Undefined reference"
    // errors that would occur otherwise (as of 8/2016).
    rxPool.construct(infiniband, bufSize, TOTAL_RX_BUFFERS+0);
    rxBufferLogThreshold = TOTAL_RX_BUFFERS - 1000;
    txPool.construct(infiniband, bufSize, MAX_TX_QUEUE_DEPTH+0);
    double seconds = Cycles::toSeconds(Cycles::rdtsc() - start);
    LOG(NOTICE, "Initialized InfUdDriver buffers: %u receive buffers (%u MB), "
            "%u transmit buffers (%u MB), took %.1f ms",
            TOTAL_RX_BUFFERS, (TOTAL_RX_BUFFERS*bufSize)/(1024*1024),
            MAX_TX_QUEUE_DEPTH, (MAX_TX_QUEUE_DEPTH*bufSize)/(1024*1024),
            seconds*1e03);

    // Create completion queues for receive and transmit.
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

    qp = infiniband->createQueuePair(
            localMac ? IBV_QPT_RAW_PACKET : IBV_QPT_UD,
            ibPhysicalPort, NULL, txcq, rxcq, MAX_TX_QUEUE_DEPTH,
            MAX_RX_QUEUE_DEPTH, 2, QKEY);

    // Cache these for easier access.
    lid = infiniband->getLid(ibPhysicalPort);
    qpn = qp->getLocalQpNumber();

    // Update our locatorString, if one was provided, with the dynamic
    // address.
    if (!locatorString.empty()) {
        char c = locatorString[locatorString.size()-1];
        if ((c != ':') && (c != ',')) {
            locatorString += ",";
        }
        if (localMac) {
            locatorString += "mac=" + localMac->toString();
        } else {
            locatorString += format("lid=%u,qpn=%u", lid, qpn);
        }
        LOG(NOTICE, "Locator for InfUdDriver: %s", locatorString.c_str());
    }

    refillReceiver();
    qp->activate(localMac);
}

/**
 * Destroy an InfUdDriver and free allocated resources.
 */
InfUdDriver::~InfUdDriver()
{
    size_t buffersInUse = TOTAL_RX_BUFFERS - rxPool->freeBuffers.size()
            - rxBuffersInHca;
    if (buffersInUse != 0) {
        LOG(WARNING, "Infiniband destructor called with %lu receive "
                "buffers in use", buffersInUse);
    }

    delete qp;
    ibv_destroy_cq(rxcq);
    ibv_destroy_cq(txcq);
}

/*
 * See docs in the ``Driver'' class.
 */
uint32_t
InfUdDriver::getMaxPacketSize()
{
    return localMac ? mtu - ETH_HEADER_SIZE : mtu - GRH_SIZE;
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
InfUdDriver::BufferDescriptor*
InfUdDriver::getTransmitBuffer()
{
    // if we've drained our free tx buffer pool, we must wait.
    if (unlikely(txPool->freeBuffers.empty())) {
        reapTransmitBuffers();
        if (txPool->freeBuffers.empty()) {
            // We are temporarily out of buffers. Time how long it takes
            // before a transmit buffer becomes available again (a long
            // time is a bad sign); in the normal case this code should
            // not be invoked.
            uint64_t start = Cycles::rdtsc();
            uint32_t count = 1;
            while (txPool->freeBuffers.empty()) {
                reapTransmitBuffers();
                count++;
            }
            timeTrace("TX buffers refilled after polling CQ %u times", count);
            double waitMillis = 1e03 * Cycles::toSeconds(Cycles::rdtsc()
                    - start);
            if (waitMillis > 1.0)  {
                LOG(WARNING, "Long delay waiting for transmit buffers "
                        "(%.1f ms elapsed, %lu buffers now free)",
                        waitMillis, txPool->freeBuffers.size());
            }
        }
    }

    BufferDescriptor* bd = txPool->freeBuffers.back();
    txPool->freeBuffers.pop_back();
    return bd;
}

/**
 * Read the driver configuration file (if there is one) and parse the infud
 * config to return a service locator.
 */
ServiceLocator
InfUdDriver::readDriverConfigFile()
{
    string configDir = "config";
    if (context->options) {
        configDir = context->options->getConfigDir();
    }
    std::ifstream configFile(configDir + "/driver.conf");
    Tub<ServiceLocator> config;
    if (configFile.is_open()) {
        std::string sl;
        try {
            while (std::getline(configFile, sl)) {
                if ((sl.find('#') == 0) || (sl.find("infud") == string::npos)) {
                    // Skip comments and irrelevant lines.
                    continue;
                }
                return ServiceLocator(sl);
            }
        } catch (ServiceLocator::BadServiceLocatorException&) {
            LOG(ERROR, "Ignored bad driver configuration: '%s'", sl.c_str());
        }
    }
    return ServiceLocator("infud:");
}

/**
 * Check the NIC to see if it is ready to return transmit buffers
 * from previously-transmit packets. If there are any available,
 * reclaim them. This method also detects and logs transmission errors.
 */
void
InfUdDriver::reapTransmitBuffers()
{
    // Fetch up to MAX_TO_RETRIEVE completion entries; each entry corresponds
    // to SIGNALED_SEND_PERIOD completed send requests. Therefore, fetching
    // up to 4 CQEs when SIGNALED_SEND_PERIOD is 32 means we may have to
    // replenish up to 128 transmit buffers before return.
#define MAX_TO_RETRIEVE 4
    ibv_wc retArray[MAX_TO_RETRIEVE];
    int cqes = ibv_poll_cq(txcq, MAX_TO_RETRIEVE, retArray);
    if (cqes) {
        timeTrace("polling TX completion queue returned %d CQEs", cqes);
    }
    for (int i = 0; i < cqes; i++) {
        BufferDescriptor* signaledCompletion =
            reinterpret_cast<BufferDescriptor*>(retArray[i].wr_id);
        bool found = false;
        while (!txBuffersInHca.empty()) {
            BufferDescriptor* bd = txBuffersInHca.front();
            txBuffersInHca.pop_front();
            txPool->freeBuffers.push_back(bd);
            if (bd == signaledCompletion) {
                found = true;
                timeTrace("reaped %d TX buffers", SIGNALED_SEND_PERIOD);
                break;
            }
        }
        if (!found) {
            DIE("Couldn't find the send request (SR) just completed");
        }

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
InfUdDriver::registerMemory(void* base, size_t bytes)
{
    // We can only remember one region (the first)
    if (zeroCopyRegion == NULL) {
        // Local read access is always enabled for MR; no more access flag is
        // needed for zero-copy TX.
        zeroCopyRegion = ibv_reg_mr(infiniband->pd.pd, base, bytes, 0);
        if (zeroCopyRegion == NULL) {
            LOG(ERROR, "ibv_reg_mr failed to register %lu bytes at %p",
                    bytes, base);
            return;
        }
        zeroCopyStart = reinterpret_cast<char*>(base);
        zeroCopyEnd = zeroCopyStart + bytes;
        RAMCLOUD_LOG(NOTICE, "Created zero-copy region with %lu bytes at %p",
                bytes, base);
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::release()
{
    while (!packetsToRelease.empty()) {
        // Payload points to the first byte of the packet buffer after the
        // Ethernet header or GRH header; from that, compute the address of its
        // corresponding buffer descriptor.
        char* payload = packetsToRelease.back();
        packetsToRelease.pop_back();
        if (localMac) {
            payload -= ETH_HEADER_SIZE;
        } else {
            payload -= GRH_SIZE;
        }
        int index = downCast<int>((payload - rxPool->bufferMemory)
                /rxPool->descriptors[0].length);
        BufferDescriptor* bd = &rxPool->descriptors[index];
        assert(payload == bd->buffer);
        rxPool->freeBuffers.push_back(bd);
    }
}

/**
 * Optimized code path for sending packets that are addressed to ourselves
 * without actually transmitting bytes over the NIC.
 */
void
InfUdDriver::sendLoopbackPacket(const void* header, uint32_t headerLen,
        Buffer::Iterator* messageIt, uint32_t payloadSize)
{
    // This method bypasses the underlying NIC driver: the payload is copied
    // directly into a receive buffer and delivered via the loopback queue
    // mechanism. Note: as of 07/2019, we haven't been able to send loopback
    // packets in raw ethernet mode with ibv_post_send on CloudLab xl170.
    assert(localMac.get());
    if (unlikely(rxPool->freeBuffers.empty())) {
        DIE("No receive buffer available for loopback packets");
    }

    BufferDescriptor* bd = rxPool->freeBuffers.back();
    rxPool->freeBuffers.pop_back();
    bd->packetLength = ETH_HEADER_SIZE + headerLen + payloadSize;

    // Optimization: for loopback packets, no need to build the ethernet header
    char* dst = bd->buffer + ETH_HEADER_SIZE;
    memcpy(dst, header, headerLen);
    dst += headerLen;

    uint32_t bytesToCopy = payloadSize;
    while (bytesToCopy > 0) {
        // The current buffer chunk contains the rest of the packet.
        if (messageIt->getLength() >= bytesToCopy) {
            memcpy(dst, messageIt->getData(), bytesToCopy);
            messageIt->advance(bytesToCopy);
            break;
        }

        memcpy(dst, messageIt->getData(), messageIt->getLength());
        dst += messageIt->getLength();
        bytesToCopy -= messageIt->getLength();
        messageIt->next();
    }

    loopbackPkts.push_back(bd);
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::sendPacket(const Driver::Address* addr,
                        const void* header,
                        uint32_t headerLen,
                        Buffer::Iterator* payload,
                        int priority,
                        TransmitQueueState* txQueueState)
{
    uint32_t payloadSize = payload ? payload->size() : 0;
    const uint32_t totalLength =
            (localMac ? ETH_HEADER_SIZE : 0) + headerLen + payloadSize;
    assert(totalLength <= getMaxPacketSize());

    // In raw ethernet mode, loopback packets must be handled specially on
    // CloudLab xl170 machines.
    const MacAddress* destMac = static_cast<const MacAddress*>(addr);
    if (unlikely(localMac && destMac->equal(*localMac.get()))) {
        sendLoopbackPacket(header, headerLen, payload, payloadSize);
        return;
    }

    // Grab a free packet buffer.
    BufferDescriptor* bd = getTransmitBuffer();
    bd->packetLength = totalLength;

    // Create the IB work request. wr_id is used to locate the BufferDescriptor
    // from the completion notification.
    ibv_sge sges[2];
    ibv_send_wr workRequest = {};
    workRequest.wr_id = reinterpret_cast<uint64_t>(bd);
    workRequest.sg_list = sges;
    workRequest.num_sge = 1;
    workRequest.next = NULL;
    workRequest.opcode = IBV_WR_SEND;

    // Construct the ethernet header when running in raw ethernet mode;
    // otherwise, initialize the work request's UD destination.
    char *dst = bd->buffer;
    if (localMac) {
        EthernetHeader* ethHdr = reinterpret_cast<EthernetHeader*>(dst);
        MacAddress::copy(ethHdr->destAddress, destMac->address);
        MacAddress::copy(ethHdr->srcAddress, localMac->address);
        ethHdr->etherType = HTONS(NetUtil::EthPayloadType::RAMCLOUD);
        dst += ETH_HEADER_SIZE;
    } else {
        const Address* address = static_cast<const Address*>(addr);
        workRequest.wr.ud.ah = address->ah;
        workRequest.wr.ud.remote_qpn = address->qpn;
        workRequest.wr.ud.remote_qkey = QKEY;
    }

    // Copy transport header into packet buffer.
    memcpy(dst, header, headerLen);
    dst += headerLen;

    // Copy payload into packet buffer or apply zero-copy when approapriate.
    sges[0] = {
        .addr = reinterpret_cast<uint64_t>(bd->buffer),
        .length = bd->packetLength,
        .lkey = bd->memoryRegion->lkey
    };
    while (payload && !payload->isDone()) {
        // Use zero copy for the last chunk of the packet, if it's in the
        // zero copy region and is large enough to justify the overhead
        // of an addition scatter-gather element.
        const char *currentChunk =
                reinterpret_cast<const char*>(payload->getData());
        uint32_t chunkSize = payload->getLength();
        if ((chunkSize >= 500) && (chunkSize == payload->size())
                && (currentChunk >= zeroCopyStart)
                && (currentChunk + chunkSize < zeroCopyEnd)) {
            sges[1] = {
                .addr = reinterpret_cast<uint64_t>(currentChunk),
                .length = chunkSize,
                .lkey = zeroCopyRegion->lkey
            };
            sges[0].length -= chunkSize;
            workRequest.num_sge = 2;
            break;
        } else {
            memcpy(dst, currentChunk, chunkSize);
            dst += chunkSize;
            timeTrace("0-copy not applicable; copied %u bytes", chunkSize);
        }
        payload->next();
    }

    // Generate one completion notification for every batch of send requests.
    sendsSinceLastSignal = (sendsSinceLastSignal + 1) % SIGNALED_SEND_PERIOD;
    workRequest.send_flags |= sendsSinceLastSignal ? 0 : IBV_SEND_SIGNALED;

    // We can get a substantial latency improvement (nearly 2usec less per RTT)
    // by inlining data with the WQE for small messages. The Verbs library
    // automatically takes care of copying from the SGEs to the WQE.
    if (bd->packetLength <= Infiniband::MAX_INLINE_DATA)
        workRequest.send_flags |= IBV_SEND_INLINE;

    ibv_send_wr *bad_txWorkRequest;
    lastTransmitTime = Cycles::rdtsc();
    if (ibv_post_send(qp->qp, &workRequest, &bad_txWorkRequest)) {
        DIE("Error posting transmit packet: %s", strerror(errno));
    } else {
        txBuffersInHca.push_back(bd);
    }
    timeTrace("sent packet with %u bytes, %u free buffers", bd->packetLength,
            txPool->freeBuffers.size());
    queueEstimator.packetQueued(bd->packetLength, lastTransmitTime,
            txQueueState);
    PerfStats::threadStats.networkOutputBytes += bd->packetLength;
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets)
{
    // Attempt to receive loopback packets first (only in raw ethernet mode).
    if (unlikely(!loopbackPkts.empty())) {
        assert(localMac.get());
        do {
            BufferDescriptor* bd = loopbackPkts.front();
            loopbackPkts.pop_front();
            receivedPackets->emplace_back(localMac.get(), this,
                    bd->packetLength - ETH_HEADER_SIZE,
                    bd->buffer + ETH_HEADER_SIZE);
            maxPackets--;
        } while (!loopbackPkts.empty() && (maxPackets > 0));
    }

    static const int MAX_COMPLETIONS = 50;
    ibv_wc wc[MAX_COMPLETIONS];
    uint32_t maxToReceive = (maxPackets < MAX_COMPLETIONS) ? maxPackets
            : MAX_COMPLETIONS;
    int numPackets = ibv_poll_cq(qp->rxcq, maxToReceive, wc);
    if (numPackets <= 0) {
        if (unlikely(numPackets < 0)) {
            LOG(ERROR, "ibv_poll_cq failed with result %d", numPackets);
        }
        return;
    }
    timeTrace("InfUdDriver received %d packets", numPackets);

    rxBuffersInHca -= numPackets;
    if (unlikely(rxBuffersInHca == 0)) {
        RAMCLOUD_CLOG(WARNING, "Infiniband receiver temporarily ran "
                "out of packet buffers; could result in dropped packets");
    }

    // First, prefetch the initial bytes of all the incoming packets. This
    // allows us to process multiple cache misses concurrently, which improves
    // throughput under load.
    for (int i = 0; i < numPackets; i++) {
        ibv_wc* incoming = &wc[i];
        BufferDescriptor *bd =
                reinterpret_cast<BufferDescriptor*>(incoming->wr_id);
        prefetch(bd->buffer,
                incoming->byte_len > 256 ? 256 : incoming->byte_len);
    }

    // Give the RX queue a chance to replenish.
    refillReceiver();

    // Each iteration of the following loop processes one incoming packet.
    for (int i = 0; i < numPackets; i++) {
        ibv_wc* incoming = &wc[i];
        BufferDescriptor *bd =
                reinterpret_cast<BufferDescriptor*>(incoming->wr_id);
        if (unlikely(incoming->status != IBV_WC_SUCCESS)) {
            DIE("Infiniband receive error (%d: %s)", incoming->status,
                    infiniband->wcStatusToString(incoming->status));
        }

        bd->packetLength = incoming->byte_len;
#define MIN_ETH_FRAME_SIZE (ETH_HEADER_SIZE + 46)
        uint32_t minPacketLength = localMac ? MIN_ETH_FRAME_SIZE : GRH_SIZE;
        if (unlikely(bd->packetLength < minPacketLength)) {
            LOG(ERROR, "received impossibly short packet: %d bytes",
                    bd->packetLength);
            goto error;
        }

        PerfStats::threadStats.networkInputBytes += bd->packetLength;
        if (localMac) {
            EthernetHeader* ethHdr = reinterpret_cast<EthernetHeader*>(
                    bd->buffer);
            assert(NTOHS(ethHdr->etherType) ==
                    NetUtil::EthPayloadType::RAMCLOUD);
            bd->macAddress.construct(ethHdr->srcAddress);
            receivedPackets->emplace_back(bd->macAddress.get(), this,
                    bd->packetLength - ETH_HEADER_SIZE,
                    bd->buffer + ETH_HEADER_SIZE);
        } else {
            ibv_ah* ah;
            auto it = infiniband->ahMap.find(incoming->slid);
            if (unlikely(it == infiniband->ahMap.end())) {
                Infiniband::Address infAddress(*infiniband, ibPhysicalPort,
                        incoming->slid, incoming->src_qp);
                ah = infAddress.getHandle();
            } else {
                ah = it->second;
            }

            static_assert(GRH_SIZE >= sizeof(Address), "Not enough space");
            new(bd->buffer) Address(ah, incoming->src_qp);
            receivedPackets->emplace_back(
                    reinterpret_cast<Address*>(bd->buffer), this,
                    bd->packetLength - GRH_SIZE, bd->buffer + GRH_SIZE);
        }
        continue;

      error:
        rxPool->freeBuffers.push_back(bd);
    }
    timeTrace("InfUdDriver::receivePackets done");
}

/**
 * See docs in the ``Driver'' class.
 */
string
InfUdDriver::getServiceLocator()
{
    return locatorString;
}

// See docs in Driver class.
uint32_t
InfUdDriver::getBandwidth()
{
    return bandwidthGbps*1000;
}

/**
 * Fill up the HCA's queue of pending receive buffers.
 */
void
InfUdDriver::refillReceiver()
{
    // Always refill in the batch of REFILL_BATCH to amortize the cost of
    // ibv_post_recv.
    static const uint32_t REFILL_BATCH = 16;
    uint32_t maxRefill = std::min(MAX_RX_QUEUE_DEPTH - rxBuffersInHca,
            downCast<uint32_t>(rxPool->freeBuffers.size()));
    if (maxRefill < REFILL_BATCH) {
        return;
    }

    // Create a linked list of receive requests to be posted to the RX queue.
    ibv_recv_wr receiveRequests[REFILL_BATCH];
    ibv_sge sges[REFILL_BATCH];
    for (uint32_t i = 0; i < REFILL_BATCH; i++) {
        BufferDescriptor* bd = rxPool->freeBuffers.back();
        rxPool->freeBuffers.pop_back();
        sges[i] = {
            .addr   = reinterpret_cast<uint64_t>(bd->buffer),
            .length = bd->length,
            .lkey   = bd->memoryRegion->lkey
        };
        receiveRequests[i] = {
            .wr_id   = reinterpret_cast<uint64_t>(bd),
            .next    = &receiveRequests[i + 1],
            .sg_list = &sges[i],
            .num_sge = 1
        };
    }
    receiveRequests[REFILL_BATCH-1].next = NULL;
    rxBuffersInHca += REFILL_BATCH;

    ibv_recv_wr *badWorkRequest;
    if (ibv_post_recv(qp->qp, receiveRequests, &badWorkRequest) != 0) {
        DIE("Couldn't post Infiniband receive buffer: %s",
                strerror(errno));
    }
    timeTrace("receive queue refilled");

    // Generate log messages every time buffer usage reaches a significant new
    // high. Running out of buffers is a bad thing, so we want warnings in the
    // log long before that happens.
    uint32_t freeBuffers = downCast<uint32_t>(rxPool->freeBuffers.size());
    if (unlikely(freeBuffers <= rxBufferLogThreshold)) {
        double percentUsed = 100.0*static_cast<double>(
                TOTAL_RX_BUFFERS - freeBuffers)/TOTAL_RX_BUFFERS;
        LOG((percentUsed >= 80.0) ? WARNING : NOTICE,
                "%u receive buffers now in use (%.1f%%)",
                TOTAL_RX_BUFFERS - freeBuffers, percentUsed);
        do {
            rxBufferLogThreshold -= 1000;
        } while (freeBuffers < rxBufferLogThreshold);
    }
}

/**
 * Constructor for BufferPool objects.
 * 
 * \param infiniband
 *      Infiniband object that the buffers will be associated with.
 * \param bufferSize
 *      Size of each packet buffer, in bytes.
 * \param numBuffers
 *      Number of buffers to allocate in the pool.
 */
InfUdDriver::BufferPool::BufferPool(Infiniband* infiniband,
        uint32_t bufferSize, uint32_t numBuffers)
    : bufferMemory(NULL)
    , memoryRegion(NULL)
    , descriptors()
    , freeBuffers()
    , numBuffers(numBuffers)
{
    // Allocate space for the packet buffers (page aligned, full pages).
    size_t bytesToAllocate = ((bufferSize * numBuffers) + 4095) & ~0xfff;
    bufferMemory = reinterpret_cast<char*>(Memory::xmemalign(HERE, 4096,
            bytesToAllocate));

    // Local write access is required by the receive buffers; this allows the
    // NIC to write incoming data directly into the buffer memory.
    memoryRegion = ibv_reg_mr(infiniband->pd.pd, bufferMemory, bytesToAllocate,
                IBV_ACCESS_LOCAL_WRITE);
    if (memoryRegion == NULL) {
        DIE("Couldn't register Infiniband memory region: %s", strerror(errno));
    }
    descriptors = reinterpret_cast<BufferDescriptor*>(
            malloc(numBuffers*sizeof(BufferDescriptor)));
    char* buffer = bufferMemory;
    for (uint32_t i = 0; i < numBuffers; i++) {
        new(&descriptors[i]) BufferDescriptor(buffer, bufferSize, memoryRegion);
        freeBuffers.push_back(&descriptors[i]);
        buffer += bufferSize;
    }
}

/**
 * Destructor for BufferPools.
 */
InfUdDriver::BufferPool::~BufferPool()
{
    if (memoryRegion != NULL) {
        ibv_dereg_mr(memoryRegion);
    }
    // `bufferMemory` and `descriptors` are allocated using malloc.
    free(bufferMemory);
    for (uint32_t i = 0; i < numBuffers; i++) {
        descriptors[i].~BufferDescriptor();
    }
    free(descriptors);
}

} // namespace RAMCloud
