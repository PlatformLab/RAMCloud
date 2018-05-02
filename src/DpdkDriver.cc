/* Copyright (c) 2015-2017 Stanford University
 * Copyright (c) 2014-2015 Huawei Technologies Co. Ltd.
 * Copyright (c) 2014-2016 NEC Corporation
 * The original version of this module was contributed by Anthony Iliopoulos
 * at DBERC, Huawei
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

#define __STDC_LIMIT_MACROS
#include <errno.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#pragma GCC diagnostic ignored "-Wconversion"
#include <rte_config.h>
#include <rte_common.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_memcpy.h>
#include <rte_ring.h>
#include <rte_version.h>
#pragma GCC diagnostic warning "-Wconversion"

#include "Common.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "DpdkDriver.h"
#include "NetUtil.h"
#include "PerfStats.h"
#include "StringUtil.h"
#include "TimeTrace.h"
#include "Util.h"
#include "BasicTransport.h"

namespace RAMCloud
{

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this driver.
#define TIME_TRACE 0

// Provides a cleaner way of invoking TimeTrace::record, with the code
// conditionally compiled in or out by the TIME_TRACE #ifdef.
namespace {
    inline void
    timeTrace(const char* format,
            uint32_t arg0 = 0, uint32_t arg1 = 0, uint32_t arg2 = 0,
            uint32_t arg3 = 0)
    {
#if TIME_TRACE
        TimeTrace::record(format, arg0, arg1, arg2, arg3);
#endif
    }
}

// Short-hand to obtain a reference to the metadata storage space that we
// used to store the PacketBufType.
#define packet_buf_type(payload) *(payload - PACKETBUF_TYPE_SIZE)

// Short-hand to obtain the starting address of a DPDK rte_mbuf based on its
// payload address.
#define payload_to_mbuf(payload) reinterpret_cast<struct rte_mbuf*>( \
    payload - ETHER_HDR_LEN - RTE_PKTMBUF_HEADROOM - sizeof(struct rte_mbuf))

constexpr uint16_t DpdkDriver::PRIORITY_TO_PCP[8];

#if TESTING
/*
 * Construct a mock DpdkDriver, used for testing only.
 */
DpdkDriver::DpdkDriver()
    : context(NULL)
    , packetBufPool()
    , packetBufsUtilized(0)
    , locatorString()
    , localMac()
    , portId(0)
    , mbufPool(NULL)
    , loopbackRing(NULL)
    , bandwidthMbps(10000)
    , highestPriorityAvail(7)
    , lowestPriorityAvail(0)
    , fileLogger(NOTICE, "DPDK: ")
    , basicTransport(NULL)
{
    localMac.construct("01:23:45:67:89:ab");
    queueEstimator.setBandwidth(bandwidthMbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthMbps)
            * MAX_DRAIN_TIME / 8000.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    if (maxTransmitQueueSize < 2*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = 2*maxPacketSize;
    }
}
#endif

uint8_t DpdkDriver::nextQueueId = 0;
std::mutex DpdkDriver::lifetimeMutex;
struct rte_ring* DpdkDriver::loopbackRings[MAX_NUM_QUEUES];
bool DpdkDriver::hasHardwareFilter = true; // Cleared if not applicable
rte_mempool* DpdkDriver::mbufPools[MAX_NUM_QUEUES];
uint64_t DpdkDriver::queueIdToClientId[MAX_NUM_QUEUES];
std::unordered_set<DpdkDriver*> DpdkDriver::allInstances;

/*
 * Construct a DpdkDriver.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param port
 *      Selects which physical port to use for communication.
 */

DpdkDriver::DpdkDriver(Context* context, int port)
    : context(context)
    , packetBufPool()
    , packetBufsUtilized(0)
    , locatorString()
    , localMac()
    , portId(0)
    , queueId(0)
    , rxQueueOwned(false)
    , isClient(false)
    , mbufPool(NULL)
    , loopbackRing(NULL)
    , bandwidthMbps(9800)                // Default bandwidth = 10 gbs
    // Assume we are allowed to use all 8 ethernet priorities.
    , highestPriorityAvail(7)
    , lowestPriorityAvail(0)
    , fileLogger(NOTICE, "DPDK: ")
    , basicTransport(NULL)
{
    std::lock_guard<std::mutex> guard(lifetimeMutex);
    // On servers, this should always be 0
    queueId = nextQueueId++;
    struct ether_addr mac;
    uint8_t numPorts;
    struct rte_eth_conf portConf;
    int ret;
    portId = downCast<uint8_t>(port);
    isClient = context->isClient();
    fprintf(stderr, "QUEUEID = %d\n", queueId);
    // This block should always run on servers
    if (queueId == 0) {
        rxQueueOwned = true;
        // Initialize the DPDK environment with some default parameters.
        // --file-prefix is needed to avoid false lock conflicts if servers
        // run on different nodes, but with a shared NFS home directory.
        // This is a bug in DPDK as of 9/2016; if the bug gets fixed, then
        // the --file-prefix argument can be removed.
        LOG(NOTICE, "Using DPDK version %s", rte_version());
        char nameBuffer[1000];
        if (gethostname(nameBuffer, sizeof(nameBuffer)) != 0) {
            throw DriverException(HERE, format("gethostname failed: %s",
                        strerror(errno)));
        }
        nameBuffer[sizeof(nameBuffer)-1] = 0;   // Needed if name was too long.
        const char *argv[] = {"rc", "--file-prefix", nameBuffer, "-c", "1",
            "-n", "1", NULL};
        int argc = static_cast<int>(sizeof(argv) / sizeof(argv[0])) - 1;

        rte_openlog_stream(fileLogger.getFile());
        ret = rte_eal_init(argc, const_cast<char**>(argv));
        if (ret < 0) {
            throw DriverException(HERE, "rte_eal_init failed");
        }

        // ensure that DPDK was able to detect a compatible and available NIC
        numPorts = rte_eth_dev_count();

        if (numPorts <= portId) {
            throw DriverException(HERE, format(
                        "Ethernet port %u doesn't exist (%u ports available)",
                        portId, numPorts));
        }

        // Read the MAC address from the NIC via DPDK.
        rte_eth_macaddr_get(portId, &mac);
        localMac.construct(mac.addr_bytes);
        locatorString = format("dpdk:mac=%s", localMac->toString().c_str());

        // configure some default NIC port parameters
        memset(&portConf, 0, sizeof(portConf));
        portConf.rxmode.max_rx_pkt_len = ETHER_MAX_VLAN_FRAME_LEN;
        if (!isClient) {
            // Server side only uses one queue
            rte_eth_dev_configure(portId, 1, 1, &portConf);
        } else {
            // Client side uses multiple tx queues and a single rx queue
            ret = rte_eth_dev_configure(portId, 1, MAX_NUM_QUEUES, &portConf);
            if (ret < 0) {
                LOG(ERROR, "rte_eth_dev_configure failed with ret code %d (%s)\n", ret, strerror(ret));
                abort();
            }
        }

        // Set up a NIC/HW-based filter on the ethernet type so that only
        // traffic to a particular port is received by this driver.
        struct rte_eth_ethertype_filter filter;
        ret = rte_eth_dev_filter_supported(portId, RTE_ETH_FILTER_ETHERTYPE);
        if (ret < 0) {
            LOG(NOTICE, "ethertype filter is not supported on port %u.", portId);
            hasHardwareFilter = false;
        } else {
            memset(&filter, 0, sizeof(filter));
            ret = rte_eth_dev_filter_ctrl(portId, RTE_ETH_FILTER_ETHERTYPE,
                    RTE_ETH_FILTER_ADD, &filter);
            if (ret < 0) {
                LOG(WARNING, "failed to add ethertype filter\n");
                hasHardwareFilter = false;
            }
        }

        // setup and initialize the receive and transmit NIC queues,
        // and activate the port.

        // create an memory pool for accommodating packet buffers
        mbufPool = createPool("mbuf_pool");
        rte_eth_rx_queue_setup(portId, queueId, NDESC, 0, NULL, mbufPool);

        if (!isClient) {
            rte_eth_tx_queue_setup(portId, queueId, NDESC, 0, NULL);

            // create an in-memory ring, used as a software loopback in order to handle
            // packets that are addressed to the localhost.
            loopbackRing = rte_ring_create("dpdk_loopback_ring", 4096,
                    SOCKET_ID_ANY, 0);
            if (NULL == loopbackRing) {
                throw DriverException(HERE, format(
                            "Failed to allocate loopback ring: %s",
                            rte_strerror(rte_errno)));
            }
        } else {
            // Allocate enough memory for the maximum number of queues and set
            // up all the queues before activating the port.
            char loopback_ring_name[32];
            char poolName[100];


            for (uint16_t i = 0; i < MAX_NUM_QUEUES; i++) {
                rte_eth_tx_queue_setup(portId, i, NDESC, 0, NULL);
                snprintf(loopback_ring_name, sizeof(loopback_ring_name), "dpdk_loopback_ring%d", i);
                loopbackRings[i] =  rte_ring_create(loopback_ring_name, 4096,
                        SOCKET_ID_ANY, 0);
                if (NULL == loopbackRings[i]) {
                    throw DriverException(HERE, format(
                                "Failed to allocate loopback ring: %s",
                                rte_strerror(rte_errno)));
                }
                snprintf(poolName, sizeof(poolName), "mbufPool%d", i);
                mbufPools[i] = createPool(poolName);
            }
            // We own the first ring, pool 0
            loopbackRing = loopbackRings[0];
            mbufPool = mbufPools[0];
        }

        // set the MTU that the NIC port should support
        ret = rte_eth_dev_set_mtu(portId, MAX_PAYLOAD_SIZE);
        if (ret != 0) {
            throw DriverException(HERE, format(
                        "Failed to set the MTU on Ethernet port  %u: %s",
                        portId, rte_strerror(rte_errno)));
        }

        ret = rte_eth_dev_start(portId);
        if (ret != 0) {
            throw DriverException(HERE, format(
                        "Couldn't start port %u, error %d (%s)", portId,
                        ret, strerror(ret)));
        }

        // Retrieve the link speed and compute information based on it.
        struct rte_eth_link link;
        rte_eth_link_get(portId, &link);
        if (!link.link_status) {
            throw DriverException(HERE, format(
                        "Failed to detect a link on Ethernet port %u", portId));
        }
        if (link.link_speed != ETH_SPEED_NUM_NONE) {
            // Be conservative about the link speed. We use bandwidth in
            // QueueEstimator to estimate # bytes outstanding in the NIC's
            // TX queue. If we overestimate the bandwidth, under high load,
            // we may keep queueing packets faster than the NIC can consume,
            // and build up a queue in the TX queue.
            bandwidthMbps = (uint32_t) (link.link_speed * 0.98);
        } else {
            LOG(WARNING, "Can't retrieve network bandwidth from DPDK; "
                    "using default of %d Mbps", bandwidthMbps);
        }

        // Ensure that there are no accidental matches on this table.
        memset(queueIdToClientId, 0, sizeof(queueIdToClientId));

        // DPDK during initialization (rte_eal_init()) pins the running thread
        // to a single processor. This becomes a problem as the master worker
        // threads are created after the initialization of the transport, and
        // thus inherit the (very) restricted affinity to a single core. This
        // essentially kills performance, as every thread is contenting for a
        // single core. Revert this, by restoring the affinity to the default
        // (all cores).
        Util::clearCpuAffinity();
    } else {
        // If this is server code, then we made a mistake somewhere.
        if (!isClient) {
            LOG(ERROR, "Dpdk invocation for secondary client thread invoked on master or coordinator.");
            abort();
        }
        // Read the MAC address from the NIC via DPDK. This is needed across all threads.
        rte_eth_macaddr_get(portId, &mac);
        localMac.construct(mac.addr_bytes);
        locatorString = format("dpdk:mac=%s", localMac->toString().c_str());

        // Get the ring we are assigned to.
        loopbackRing = loopbackRings[queueId];
        mbufPool = mbufPools[queueId];
    }

    // NOTE: QueueEstimator bandwidth is currently overset for clients, since
    // each RAMCloud instance has its own, but this seems to be helpful rather
    // than harmful until we figure out how to do an actual multithreaded queue
    // estimator.

    // Every instance of DPDK needs to set the bandwidth on the queue
    // estimator; forgetting to set this causes queue to never grow.
    queueEstimator.setBandwidth(bandwidthMbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthMbps)
            * MAX_DRAIN_TIME / 8000.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    if (maxTransmitQueueSize < 2*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = 2*maxPacketSize;
    }

    LOG(NOTICE, "DpdkDriver locator: %s, bandwidth: %d Mbits/sec, "
            "maxTransmitQueueSize: %u bytes",
            locatorString.c_str(), bandwidthMbps, maxTransmitQueueSize);
    allInstances.insert(this);
}
// Create a memory pool for packet receive buffers and check for errors.
struct rte_mempool*
DpdkDriver::createPool(const char* name) {
    struct rte_mempool* bufPool = rte_mempool_create(name, NB_MBUF,
            MBUF_SIZE, 32,
            sizeof32(struct rte_pktmbuf_pool_private),
            rte_pktmbuf_pool_init, NULL,
            rte_pktmbuf_init, NULL,
            rte_socket_id(), 0);

    if (!bufPool) {
        throw DriverException(HERE, format(
                    "Failed to allocate memory for packet buffers: %s",
                    rte_strerror(rte_errno)));
    }
    return bufPool;
}

/**
 * Destroy the DpdkDriver.
 */
DpdkDriver::~DpdkDriver()
{
    std::lock_guard<std::mutex> guard(lifetimeMutex);
    if (packetBufsUtilized != 0)
        LOG(ERROR, "DpdkDriver deleted with %d packets still in use",
            packetBufsUtilized);

    // Remove ourselves from allInstances
    allInstances.erase(this);

    // Pass off ownership of the rx queue
    if (rxQueueOwned && !allInstances.empty()) {
        (*allInstances.begin())->rxQueueOwned = true;
    }

    if (allInstances.empty()) {
        // Free the various allocated resources (e.g. ring, mempool) and close
        // the NIC.
        rte_eth_dev_stop(portId);
        rte_eth_dev_close(portId);
        rte_openlog_stream(NULL);
    }
}

// See docs in Driver class.
int
DpdkDriver::getHighestPacketPriority()
{
    return highestPriorityAvail - lowestPriorityAvail;
}

// See docs in Driver class.
uint32_t
DpdkDriver::getMaxPacketSize()
{
    return MAX_PAYLOAD_SIZE;
}

// See docs in Driver class.
uint32_t
DpdkDriver::getPacketOverhead()
{
    return ETHER_VLAN_HDR_LEN + ETHER_PACKET_OVERHEAD;
}

void
DpdkDriver::setBasicTransport(void* basicTransport) {
    this->basicTransport =  basicTransport;
    queueIdToClientId[queueId] = ((BasicTransport*) basicTransport)->getClientId();
}

uint8_t
DpdkDriver::getQueueId(uint64_t clientId) {
    for (uint8_t i = 0; i < MAX_NUM_QUEUES; i++) {
        if (queueIdToClientId[i] == clientId)
            return i;
    }
    return 0;
}

// See docs in Driver class.
void
DpdkDriver::receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets)
{
#define MAX_PACKETS_AT_ONCE 32
    if (maxPackets > MAX_PACKETS_AT_ONCE) {
        maxPackets = MAX_PACKETS_AT_ONCE;
    }
    struct rte_mbuf* mPkts[MAX_PACKETS_AT_ONCE];

#if TIME_TRACE
    uint64_t timestamp = Cycles::rdtsc();
#endif
    // attempt to dequeue a batch of received packets from the NIC
    // as well as from the loopback ring.
    uint32_t incomingPkts = 0;
    // Only the instance that owns the rxQueue should be polling on it.
    if (rxQueueOwned) {
        incomingPkts = rte_eth_rx_burst(portId, 0, mPkts,
                downCast<uint16_t>(maxPackets));
        if (incomingPkts > 0) {
#if TIME_TRACE
            TimeTrace::record(timestamp, "DpdkDriver about to receive packets");
            TimeTrace::record("DpdkDriver received %u packets", incomingPkts);
#endif
        }
    }
    uint32_t loopbackPkts = 0;
    // Instances that do not own the rxQueue wil get all their packets through
    // this loopback ring.
    loopbackPkts = rte_ring_count(loopbackRing);
    if (incomingPkts + loopbackPkts > maxPackets) {
        loopbackPkts = maxPackets - incomingPkts;
    }
    for (uint32_t i = 0; i < loopbackPkts; i++) {
        rte_ring_dequeue(loopbackRing,
                reinterpret_cast<void**>(&mPkts[incomingPkts + i]));
    }
    uint32_t totalPkts = incomingPkts + loopbackPkts;

    // Process received packets by constructing appropriate Received objects.
    for (uint32_t i = 0; i < totalPkts; i++) {
        struct rte_mbuf* m = mPkts[i];
        rte_prefetch0(rte_pktmbuf_mtod(m, void *));
        if (unlikely(m->nb_segs > 1)) {
            RAMCLOUD_CLOG(WARNING,
                    "Can't handle packet with %u segments; discarding",
                    m->nb_segs);
            rte_pktmbuf_free(m);
            continue;
        }

        struct ether_hdr* ethHdr = rte_pktmbuf_mtod(m, struct ether_hdr*);
        uint16_t ether_type = ethHdr->ether_type;
        uint32_t headerLength = ETHER_HDR_LEN;
        char* payload = reinterpret_cast<char *>(ethHdr + 1);
        if (ether_type == rte_cpu_to_be_16(ETHER_TYPE_VLAN)) {
            struct vlan_hdr* vlanHdr =
                    reinterpret_cast<struct vlan_hdr*>(payload);
            ether_type = vlanHdr->eth_proto;
            headerLength += VLAN_TAG_LEN;
            payload += VLAN_TAG_LEN;
        }
        if (!hasHardwareFilter) {
            // Perform packet filtering by software to skip irrelevant
            // packets such as ipmi or kernel TCP/IP traffics.
            if (ether_type !=
                    rte_cpu_to_be_16(NetUtil::EthPayloadType::RAMCLOUD)) {
                rte_pktmbuf_free(m);
                continue;
            }
        }

        // XXX: Examine the payload to decide whether it should be handled by us or
        // forwarded to a different thread.
        uint64_t clientId = ((BasicTransport*)basicTransport)->extractClientId(payload);
        // Perform a table lookup.
        uint8_t clientQueueId = getQueueId(clientId);

        // Packet was not destined for this instance, forward it to the right instance
        // Only forward it if we own the rx queue; otherwise we forward in a loop ==> infinite forwarding.
        // Unknowns clientIds will default to 0, and we should not forward
        // unknowns IDs, since other client threads will not know how to deal
        // with them any better than us.
        if (rxQueueOwned && clientQueueId != queueId && clientQueueId != 0) {
            int ret = rte_ring_enqueue(loopbackRings[clientQueueId], m);
            if (unlikely(ret != 0)) {
                LOG(WARNING, "rte_ring_enqueue returned %d; packet may be lost?",
                        ret);
                rte_pktmbuf_free(m);
            }
            continue;
        }

        PerfStats::threadStats.networkInputBytes += rte_pktmbuf_pkt_len(m);

        // By default, we would like to construct the Received object using
        // the payload directly (as opposed to copying out the payload to a
        // PacketBuf first). Therefore, we use the headroom in rte_mbuf to
        // store the packet sender address (as required by the Received object)
        // and the packet buf type (so we know where this payload comes from).
        // See http://dpdk.org/doc/guides/prog_guide/mbuf_lib.html for the
        // diagram of rte_mbuf's internal structure.
        MacAddress* sender = reinterpret_cast<MacAddress*>(m->buf_addr);
        if (unlikely(reinterpret_cast<char*>(sender + 1) >
                rte_pktmbuf_mtod(m, char*))) {
            LOG(ERROR, "Not enough headroom in the packet mbuf; "
                    "dropping packet");
            rte_pktmbuf_free(m);
            continue;
        }
        new(sender) MacAddress(ethHdr->s_addr.addr_bytes);
        packet_buf_type(payload) = DPDK_MBUF;
        packetBufsUtilized++;
        uint32_t length = rte_pktmbuf_pkt_len(m) - headerLength;
        assert(length <= MAX_PAYLOAD_SIZE);
        receivedPackets->emplace_back(sender, this, length, payload);
        timeTrace("received packet processed, payload size %u", length);
    }
}

// See docs in Driver class.
void
DpdkDriver::release(char *payload)
{
    Tub<Dispatch::Lock> dispatchLock;
    if (!isClient) {
        // Must sync with the dispatch thread, since this method could potentially
        // be invoked in a worker.
        // This is only needed on the server, and it is causing deadlocks when
        // used concurrently with RamCloud client object destruction.
        dispatchLock.construct(context->dispatch);
    }

    packetBufsUtilized--;
    assert(packetBufsUtilized >= 0);
    if (packet_buf_type(payload) == DPDK_MBUF) {
        rte_pktmbuf_free(payload_to_mbuf(payload));
    } else {
        packetBufPool.destroy(reinterpret_cast<PacketBuf*>(
                    payload - OFFSET_OF(PacketBuf, payload)));
    }
}

// See docs in Driver class.
void
DpdkDriver::releaseHwPacketBuf(Driver::Received* received)
{
    struct rte_mbuf* mbuf = payload_to_mbuf(received->payload);
    MacAddress* sender = reinterpret_cast<MacAddress*>(mbuf->buf_addr);
    // Copy the sender address and payload to our PacketBuf.
    PacketBuf* packetBuf = packetBufPool.construct();
    packetBuf->sender.construct(*sender);
    packet_buf_type(packetBuf->payload) = RAMCLOUD_PACKET_BUF;
    rte_memcpy(packetBuf->payload, received->payload, received->len);
    // Replace rte_mbuf with our PacketBuf and release it.
    received->sender = packetBuf->sender.get();
    received->payload = packetBuf->payload;
    rte_pktmbuf_free(mbuf);
}

// See docs in Driver class.
void
DpdkDriver::sendPacket(const Address* addr,
                       const void* header,
                       uint32_t headerLen,
                       Buffer::Iterator* payload,
                       int priority,
                       TransmitQueueState* txQueueState)
{
    // Convert transport-level packet priority to Ethernet priority.
    assert(priority >= 0 && priority <= getHighestPacketPriority());
    priority += lowestPriorityAvail;

    uint32_t etherPayloadLength = headerLen + (payload ? payload->size() : 0);
    assert(etherPayloadLength <= MAX_PAYLOAD_SIZE);
    timeTrace("sendPacket invoked, payload size %u", etherPayloadLength);
    uint32_t frameLength = etherPayloadLength + ETHER_VLAN_HDR_LEN;
    uint32_t physPacketLength = frameLength + ETHER_PACKET_OVERHEAD;

#if TESTING
    struct rte_mbuf mockMbuf;
    struct rte_mbuf* mbuf = &mockMbuf;
#else
    struct rte_mbuf* mbuf = rte_pktmbuf_alloc(mbufPool);
#endif
    if (unlikely(NULL == mbuf)) {
        uint32_t numMbufsAvail = rte_mempool_avail_count(mbufPool);
        uint32_t numMbufsInUse = rte_mempool_in_use_count(mbufPool);
        RAMCLOUD_CLOG(WARNING,
                "Failed to allocate a packet buffer; dropping packet; "
                "%u mbufs available, %u mbufs in use",
                numMbufsAvail, numMbufsInUse);
        return;
    }

#if TESTING
    uint8_t mockEtherFrame[ETHER_MAX_VLAN_FRAME_LEN] = {};
    char* data = reinterpret_cast<char*>(mockEtherFrame);
#else
    char* data = rte_pktmbuf_append(mbuf, downCast<uint16_t>(frameLength));
#endif
    if (unlikely(NULL == data)) {
        RAMCLOUD_CLOG(NOTICE,
                "rte_pktmbuf_append call failed; dropping packet");
        rte_pktmbuf_free(mbuf);
        return;
    }

    // Fill out the destination and source MAC addresses plus the Ethernet
    // frame type (i.e., IEEE 802.1Q VLAN tagging).
    char *p = data;
    struct ether_hdr* ethHdr = reinterpret_cast<struct ether_hdr*>(p);
    rte_memcpy(&ethHdr->d_addr, static_cast<const MacAddress*>(addr)->address,
            ETHER_ADDR_LEN);
    rte_memcpy(&ethHdr->s_addr, localMac->address, ETHER_ADDR_LEN);
    ethHdr->ether_type = rte_cpu_to_be_16(ETHER_TYPE_VLAN);
    p += ETHER_HDR_LEN;

    // Fill out the PCP field and the Ethernet frame type of the encapsulated
    // frame (DEI and VLAN ID are not relevant and trivially set to 0).
    struct vlan_hdr* vlanHdr = reinterpret_cast<struct vlan_hdr*>(p);
    vlanHdr->vlan_tci = rte_cpu_to_be_16(PRIORITY_TO_PCP[priority]);
    vlanHdr->eth_proto = rte_cpu_to_be_16(NetUtil::EthPayloadType::RAMCLOUD);
    p += VLAN_TAG_LEN;

    // Copy `header` and `payload`.
    rte_memcpy(p, header, headerLen);
    p += headerLen;
    while (payload && !payload->isDone())
    {
        rte_memcpy(p, payload->getData(), payload->getLength());
        p += payload->getLength();
        payload->next();
    }
    timeTrace("about to enqueue outgoing packet");

#if TESTING
    string hexEtherHeader;
    for (unsigned i = 0; i < ETHER_VLAN_HDR_LEN; i++) {
        char hex[3];
        snprintf(hex, sizeof(hex), "%02x", mockEtherFrame[i]);
        hexEtherHeader += hex;
    }
    LOG(NOTICE, "Ethernet frame header %s, payload %s", hexEtherHeader.c_str(),
            &mockEtherFrame[ETHER_VLAN_HDR_LEN]);
#else
    // loopback if src mac == dst mac
    if (!memcmp(static_cast<const MacAddress*>(addr)->address,
            localMac->address, 6)) {
        if (loopbackRing) {
            int ret = rte_ring_enqueue(loopbackRing, mbuf);
            if (unlikely(ret != 0)) {
                LOG(WARNING, "rte_ring_enqueue returned %d; packet may be lost?",
                        ret);
                rte_pktmbuf_free(mbuf);
            }
            timeTrace("loopback packet enqueued");
            return;
        } else {
            // ASSUME that loopback is unused, per discussion with Yilong.
            LOG(WARNING, "Tried to use a NULL loopback ring");
        }
    }

    uint32_t ret = rte_eth_tx_burst(portId, queueId, &mbuf, 1);
    if (unlikely(ret != 1)) {
        LOG(WARNING, "rte_eth_tx_burst returned %u; packet may be lost?", ret);
        // The congestion at the TX queue must be pretty bad if we got here:
        // set the queue size to be relatively large.
        queueEstimator.setQueueSize(maxTransmitQueueSize*2, Cycles::rdtsc());
        rte_pktmbuf_free(mbuf);
        return;
    }
    timeTrace("outgoing packet enqueued");
#endif
    lastTransmitTime = Cycles::rdtsc();
    queueEstimator.packetQueued(physPacketLength, lastTransmitTime,
            txQueueState);
    PerfStats::threadStats.networkOutputBytes += physPacketLength;
}

// See docs in Driver class.
string
DpdkDriver::getServiceLocator()
{
    return locatorString;
}

// See docs in Driver class.
uint32_t
DpdkDriver::getBandwidth()
{
    return bandwidthMbps;
}

} // namespace RAMCloud
