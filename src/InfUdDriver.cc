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
      ibPhysicalPort(1), lid(0), qpn(0)
{
    const char *ibDeviceName = NULL;

    if (sl != NULL) {
        try {
            ibDeviceName   = sl->getOption<const char *>("dev");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

        try {
            ibPhysicalPort = sl->getOption<int>("devport");
        } catch (ServiceLocator::NoSuchKeyException& e) {}
    }

    ctxt = Infiniband::openDevice(ibDeviceName);
    error_check_null(ctxt, "failed to open infiniband device");

    pd = ibv_alloc_pd(ctxt);
    error_check_null(pd, "failed to allocate infiniband pd");

    // XXX- for now we allocate one TX buffer and RX buffers as a ring.
    for (uint32_t i = 0; i < MAX_RX_QUEUE_DEPTH; i++) {
        rxBuffers[i] = Infiniband::allocateBufferDescriptorAndRegister(
            pd, getMaxPayloadSize());
    }
    txBuffer = Infiniband::allocateBufferDescriptorAndRegister(
        pd, getMaxPayloadSize());

    // create completion queues for receive and transmit
    ibv_cq *rxcq = ibv_create_cq(ctxt, MAX_RX_QUEUE_DEPTH, NULL, NULL, 0);
    error_check_null(rxcq, "failed to create receive completion queue");

    ibv_cq *txcq = ibv_create_cq(ctxt, MAX_TX_QUEUE_DEPTH, NULL, NULL, 0);
    error_check_null(txcq, "failed to create transmit completion queue");

    qp = new QueuePair(IBV_QPT_UD, ibPhysicalPort, pd, NULL, txcq, rxcq,
        MAX_TX_QUEUE_DEPTH, MAX_RX_QUEUE_DEPTH, QKEY);

    // cache these for easier access
    lid = Infiniband::getLid(ctxt, ibPhysicalPort);
    qpn = qp->getLocalQpNumber();

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
InfUdDriver::getMaxPayloadSize()
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
    assert(totalLength <= getMaxPayloadSize());

    // XXX for UD, we need to allocate an address handle. this should _not_
    // be done on the fly (it takes tens of microseconds!), but should be
    // instead associated with sessions somehow.
    ibv_ah_attr attr;
    attr.dlid = static_cast<const InfAddress *>(addr)->address.lid;
    attr.src_path_bits = 0;
    attr.is_global = 0;
    attr.sl = 0;
    attr.port_num = ibPhysicalPort;

    ibv_ah *ah = ibv_create_ah(pd, &attr);
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

    uint32_t remoteQpn = static_cast<const InfAddress *>(addr)->address.qpn;
    try {
        Infiniband::postSendAndWait(qp, bd, length, txcq, ah, remoteQpn, QKEY);
    } catch (...) {
        ibv_destroy_ah(ah);
        throw;
    }

    ibv_destroy_ah(ah);
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

    // copy from the infiniband buffer into our dynamically allocated
    // buffer.
    memcpy(buffer->payload, bd->buffer, bd->messageBytes);

    packetBufsUtilized++;
    received->payload = buffer->payload;
    received->len = bd->messageBytes;
    received->sender = &buffer->infAddress;
    received->driver = this;

    // post the original infiniband buffer back to the receive queue
    Infiniband::postReceive(qp, bd);

    return true;
}

} // namespace RAMCloud
