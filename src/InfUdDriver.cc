/* Copyright (c) 2010-2011 Stanford University
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
#include "FastTransport.h"
#include "InfUdDriver.h"

#define error_check_null(x, s)                              \
    do {                                                    \
        if ((x) == NULL) {                                  \
            LOG(ERROR, "%s", s);                            \
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
template<typename Infiniband>
InfUdDriver<Infiniband>::InfUdDriver(const ServiceLocator *sl)
    : realInfiniband()
    , infiniband()
    , rxcq(0)
    , txcq(0)
    , qp(NULL)
    , packetBufPool()
    , packetBufsUtilized(0)
    , currentRxBuffer(0)
    , txBuffer(NULL)
    , ibPhysicalPort(1)
    , lid(0)
    , qpn(0)
    , locatorString()
    , incomingPacketHandler(NULL)
    , poller()
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

    infiniband = realInfiniband.construct(ibDeviceName);

    // XXX- for now we allocate one TX buffer and RX buffers as a ring.
    for (uint32_t i = 0; i < MAX_RX_QUEUE_DEPTH; i++) {
        rxBuffers[i] = infiniband->allocateBufferDescriptorAndRegister(
            getMaxPacketSize() + 40);
    }
    txBuffer = infiniband->allocateBufferDescriptorAndRegister(
        getMaxPacketSize() + 40);

    // create completion queues for receive and transmit
    rxcq = infiniband->createCompletionQueue(MAX_RX_QUEUE_DEPTH);
    error_check_null(rxcq, "failed to create receive completion queue");

    txcq = infiniband->createCompletionQueue(MAX_TX_QUEUE_DEPTH);
    error_check_null(txcq, "failed to create transmit completion queue");

    qp = infiniband->createQueuePair(IBV_QPT_UD, ibPhysicalPort, NULL,
                                     txcq, rxcq, MAX_TX_QUEUE_DEPTH,
                                     MAX_RX_QUEUE_DEPTH, QKEY);

    // cache these for easier access
    lid = infiniband->getLid(ibPhysicalPort);
    qpn = qp->getLocalQpNumber();

    // update our locatorString, if one was provided, with the dynamic
    // address
    if (!locatorString.empty())
        locatorString += format("lid=%u,qpn=%u", lid, qpn);

    // add receive buffers so we can transition to RTR
    for (uint32_t i = 0; i < MAX_RX_QUEUE_DEPTH; i++)
        infiniband->postReceive(qp, rxBuffers[i]);

    qp->activate();
}

/**
 * Destroy an InfUdDriver and free allocated resources.
 */
template<typename Infiniband>
InfUdDriver<Infiniband>::~InfUdDriver()
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
template<typename Infiniband>
void
InfUdDriver<Infiniband>::connect(IncomingPacketHandler*
                                                incomingPacketHandler) {
    this->incomingPacketHandler.reset(incomingPacketHandler);
    poller.construct(this);
}

/*
 * See docs in the ``Driver'' class.
 */
template<typename Infiniband>
void
InfUdDriver<Infiniband>::disconnect() {
    poller.destroy();
    this->incomingPacketHandler.reset();
}

/*
 * See docs in the ``Driver'' class.
 */
template<typename Infiniband>
uint32_t
InfUdDriver<Infiniband>::getMaxPacketSize()
{
    return MAX_PAYLOAD_SIZE;
}

/*
 * See docs in the ``Driver'' class.
 */
template<typename Infiniband>
void
InfUdDriver<Infiniband>::release(char *payload)
{
    // Must sync with the dispatch thread, since this method could potentially
    // be invoked in a worker.
    Dispatch::Lock _;

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
template<typename Infiniband>
void
InfUdDriver<Infiniband>::sendPacket(const Driver::Address *addr,
                        const void *header,
                        uint32_t headerLen,
                        Buffer::Iterator *payload)
{
    uint32_t totalLength = headerLen +
                           (payload ? payload->getTotalLength() : 0);
    assert(totalLength <= getMaxPacketSize());

    const Address *infAddr = static_cast<const Address *>(addr);

    // use the sole TX buffer
    BufferDescriptor* bd = txBuffer;

    // copy buffer over
    char *p = bd->buffer;
    memcpy(p, header, headerLen);
    p += headerLen;
    while (payload && !payload->isDone()) {
        memcpy(p, payload->getData(), payload->getLength());
        p += payload->getLength();
        payload->next();
    }
    uint32_t length = static_cast<uint32_t>(p - bd->buffer);

    try {
        LOG(DEBUG, "sending %u bytes to %s...", length,
            infAddr->toString().c_str());
        infiniband->postSendAndWait(qp, bd, length, infAddr, QKEY);
        LOG(DEBUG, "sent successfully!");
    } catch (...) {
        LOG(DEBUG, "send failed!");
        throw;
    }
}

/*
 * See docs in the ``Driver'' class.
 */
template<typename Infiniband>
void
InfUdDriver<Infiniband>::Poller::poll()
{
    assert(dispatch->isDispatchThread());
    PacketBuf* buffer = driver->packetBufPool.construct();
    BufferDescriptor* bd = NULL;

    try {
        bd = driver->infiniband->tryReceive(driver->qp, &buffer->infAddress);
    } catch (...) {
        driver->packetBufPool.destroy(buffer);
        throw;
    }

    if (bd == NULL) {
        driver->packetBufPool.destroy(buffer);
        return;
    }

    if (bd->messageBytes < 40) {
        LOG(ERROR, "received packet without GRH!");
        driver->packetBufPool.destroy(buffer);
    } else {
        LOG(DEBUG, "received %u byte packet (not including GRH) from %s",
            bd->messageBytes - 40,
            buffer->infAddress->toString().c_str());

        // copy from the infiniband buffer into our dynamically allocated
        // buffer.
        memcpy(buffer->payload, bd->buffer + 40, bd->messageBytes - 40);

        driver->packetBufsUtilized++;
        Received received;
        received.payload = buffer->payload;
        received.len = bd->messageBytes - 40;
        received.sender = buffer->infAddress.get();
        received.driver = driver;
        (*driver->incomingPacketHandler)(&received);
    }

    // post the original infiniband buffer back to the receive queue
    driver->infiniband->postReceive(driver->qp, bd);
}

/**
 * See docs in the ``Driver'' class.
 */
template<typename Infiniband>
ServiceLocator
InfUdDriver<Infiniband>::getServiceLocator()
{
    return ServiceLocator(locatorString);
}

template class InfUdDriver<RealInfiniband>;

} // namespace RAMCloud
