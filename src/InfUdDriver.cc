/* Copyright (c) 2010 Stanford University
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
 *
 * The code below has a few 40-byte constants sprinkled about to deal
 * with this phenomenon.
 */

#include <errno.h>
#include <sys/types.h>

#include "Common.h"
#include "InfUdDriver.h"

#define error_check_null(x, s)                              \
    do {                                                    \
        if ((x) == NULL) {                                  \
            LOG(ERROR, "%s: %s", __func__, s);              \
            throw DriverException(HERE, errno);             \
        }                                                   \
    } while (0)

namespace RAMCloud {

/**
 * Construct an InfUdDriver.
 *
 * \param sl
 *      Specifies the Infiniband device and physical port to use.
 *      If NULL, the first device and port are used by default.
 *      Note that Infiniband has no concept of statically allocated
 *      queue pair numbers (ports). This makes this driver unsuitable
 *      for servers until we add some facility for dynamic addresses
 *      and resolution.
 */
InfUdDriver::InfUdDriver(const ServiceLocator *sl)
    : ctxt(0), pd(0), rxcq(0), txcq(0), qp(NULL), packetBufPool(),
      packetBufsUtilized(0), currentRxBuffer(0), txBuffer(),
      ibPhysicalPort(1), lid(0), qpn(0), locatorString()
{
    const char *ibDeviceName = NULL;

    if (sl != NULL) {
        locatorString = sl->getOriginalString();

        try {
            ibDeviceName   = sl->getOption<const char *>("dev");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

        try {
            ibPhysicalPort = sl->getOption<int>("devport");
        } catch (ServiceLocator::NoSuchKeyException& e) {}
    }

    ctxt = Infiniband::openDevice(ibDeviceName);
    error_check_null(ctxt, "failed to open infiniband device");

    pd = Infiniband::allocateProtectionDomain(ctxt);
    error_check_null(pd, "failed to allocate infiniband pd");

    // XXX- for now we allocate one TX buffer and RX buffers as a ring.
    for (uint32_t i = 0; i < MAX_RX_QUEUE_DEPTH; i++) {
        rxBuffers[i] = Infiniband::allocateBufferDescriptorAndRegister(
            pd, getMaxPacketSize() + 40);
    }
    txBuffer = Infiniband::allocateBufferDescriptorAndRegister(
        pd, getMaxPacketSize() + 40);

    // create completion queues for receive and transmit
    rxcq = Infiniband::createCompletionQueue(ctxt, MAX_RX_QUEUE_DEPTH);
    error_check_null(rxcq, "failed to create receive completion queue");

    txcq = Infiniband::createCompletionQueue(ctxt, MAX_TX_QUEUE_DEPTH);
    error_check_null(txcq, "failed to create transmit completion queue");

    qp = new QueuePair(IBV_QPT_UD, ctxt, ibPhysicalPort, pd, NULL, txcq, rxcq,
        MAX_TX_QUEUE_DEPTH, MAX_RX_QUEUE_DEPTH, QKEY);

    // cache these for easier access
    lid = Infiniband::getLid(ctxt, ibPhysicalPort);
    qpn = qp->getLocalQpNumber();

    // update our locatorString, if one was provided, with the dynamic
    // address
    if (!locatorString.empty())
        locatorString += format("lid=%u,qpn=%u", lid, qpn);

    // add receive buffers so we can transition to RTR
    for (uint32_t i = 0; i < MAX_RX_QUEUE_DEPTH; i++)
        Infiniband::postReceive(qp, &rxBuffers[i]);

    qp->activate();
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

    // XXX- cleanup

    delete qp;
}

/*
 * See docs in the ``Driver'' class.
 */
uint32_t
InfUdDriver::getMaxPacketSize()
{
    return MAX_PAYLOAD_SIZE;
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::release(char *payload, uint32_t len)
{
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
InfUdDriver::sendPacket(const Address *addr,
                        const void *header,
                        uint32_t headerLen,
                        Buffer::Iterator *payload)
{
    uint32_t totalLength = headerLen +
                           (payload ? payload->getTotalLength() : 0);
    assert(totalLength <= getMaxPacketSize());

    const InfAddress *infAddr = static_cast<const InfAddress *>(addr);

    // XXX for UD, we need to allocate an address handle. this should _not_
    // be done on the fly (it takes tens of microseconds!), but should be
    // instead associated with sessions somehow.
    ibv_ah_attr attr;
    attr.dlid = infAddr->address.lid;
    attr.src_path_bits = 0;
    attr.is_global = 0;
    attr.sl = 0;
    attr.port_num = ibPhysicalPort;

    ibv_ah *ah = Infiniband::createAddressHandle(pd, &attr);
    error_check_null(ah, "failed to create ah");

    // use the sole TX buffer
    BufferDescriptor* bd = &txBuffer;

    // copy buffer over
    char *p = bd->buffer;
    memcpy(p, header, headerLen);
    p += headerLen;
    while (payload && !payload->isDone()) {
        memcpy(p, payload->getData(), payload->getLength());
        p += payload->getLength();
        payload->next();
    }
    uint32_t length = p - bd->buffer;

    uint32_t remoteQpn = infAddr->address.qpn;
    try {
        LOG(DEBUG, "%s: sending %u bytes to %s...", __func__, length,
            infAddr->toString().c_str());
        Infiniband::postSendAndWait(qp, bd, length, ah, remoteQpn, QKEY);
        LOG(DEBUG, "%s: sent successfully!", __func__);
    } catch (...) {
        LOG(DEBUG, "%s: send failed!", __func__);
        Infiniband::destroyAddressHandle(ah);
        throw;
    }

    Infiniband::destroyAddressHandle(ah);
}

/*
 * See docs in the ``Driver'' class.
 */
bool
InfUdDriver::tryRecvPacket(Received *received)
{
    PacketBuf* buffer = packetBufPool.construct();
    BufferDescriptor* bd = NULL;

    try {
        bd = Infiniband::tryReceive(qp, &buffer->infAddress);
    } catch (...) {
        packetBufPool.destroy(buffer);
        throw;
    }

    if (bd == NULL) {
        packetBufPool.destroy(buffer);
        return false;
    }

    if (bd->messageBytes < 40) {
        LOG(ERROR, "%s: received packet without GRH!", __func__);
        packetBufPool.destroy(buffer);
    } else {
        LOG(DEBUG, "%s: received %u byte packet (not including GRH) from %s",
            __func__, bd->messageBytes - 40,
            buffer->infAddress.toString().c_str());

        // copy from the infiniband buffer into our dynamically allocated
        // buffer.
        memcpy(buffer->payload, bd->buffer + 40, bd->messageBytes - 40);

        packetBufsUtilized++;
        received->payload = buffer->payload;
        received->len = bd->messageBytes - 40;
        received->sender = &buffer->infAddress;
        received->driver = this;
    }

    // post the original infiniband buffer back to the receive queue
    Infiniband::postReceive(qp, bd);

    return true;
}

/**
 * See docs in the ``Driver'' class.
 */
ServiceLocator
InfUdDriver::getServiceLocator()
{
    return ServiceLocator(locatorString);
}

} // namespace RAMCloud
