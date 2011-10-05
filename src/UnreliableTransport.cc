/* Copyright (c) 2010-2011 Stanford University
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

#include "CycleCounter.h"
#include "RawMetrics.h"
#include "ServiceManager.h"
#include "UnreliableTransport.h"

namespace RAMCloud {

UnreliableTransport::UnreliableTransport(Driver* driver)
    : driver(driver)
    , clientPendingList()
    , serverRpcPool()
    , serverPendingList()
#ifdef INFINIBAND
    , infinibandAddressHandleCache()
#endif
{
    struct IncomingPacketHandler : Driver::IncomingPacketHandler {
        explicit IncomingPacketHandler(UnreliableTransport& t) : t(t) {}
        void operator()(Driver::Received* received) {
            CycleCounter<RawMetric> _(&metrics->transport.receive.ticks);
            ++metrics->transport.receive.packetCount;
            ++metrics->transport.receive.iovecCount;
            metrics->transport.receive.byteCount += received->len;
            Header* header = received->getOffset<Header>(0);
            if (header == NULL)
                return;
            if (header->getClientToServer()) {
                UnreliableServerRpc* rpc = NULL;
                foreach (UnreliableServerRpc& i, t.serverPendingList) {
                    if (i.nonce == header->getNonce()) {
                        rpc = &i;
                        break;
                    }
                }
                if (rpc == NULL) {
                    rpc = t.serverRpcPool.construct(t, header->getNonce(),
                                                    received->sender);
                    t.serverPendingList.push_back(*rpc);
                }
                uint32_t len;
                char* data = received->steal(&len);
                Driver::PayloadChunk::appendToBuffer(&rpc->requestPayload,
                                 data + sizeof(Header),
                                 len - downCast<uint32_t>(sizeof(Header)),
                                 t.driver.get(),
                                 data);
                if (!header->getMoreWillFollow()) {
                    erase(t.serverPendingList, *rpc);
                    Context::get().serviceManager->handleRpc(rpc);
                }
            } else {
                foreach (UnreliableClientRpc& rpc, t.clientPendingList) {
                    if (rpc.nonce == header->getNonce()) {
                        uint32_t len;
                        char* data = received->steal(&len);
                        Driver::PayloadChunk::appendToBuffer(rpc.response,
                             data + sizeof(Header),
                             len - downCast<uint32_t>(sizeof(Header)),
                             t.driver.get(),
                             data);
                        if (!header->getMoreWillFollow()) {
                            rpc.markFinished();
                            erase(t.clientPendingList, rpc);
                        }
                        break;
                    }
                }
            }
        }
        UnreliableTransport& t;
    };
    driver->connect(new IncomingPacketHandler(*this));
}

UnreliableTransport::~UnreliableTransport()
{
    clientPendingList.clear();
    serverPendingList.clear();
}

string
UnreliableTransport::getServiceLocator()
{
    return driver->getServiceLocator();
}

Transport::SessionRef
UnreliableTransport::getSession(const ServiceLocator& serviceLocator)
{
    return new UnreliableSession(*this, serviceLocator);
}

void
UnreliableTransport::sendPacketized(const Driver::Address* recipient,
                                    Header headerTemplate, Buffer& payload)
{
    Header header = headerTemplate;
    const uint32_t totalLength = payload.getTotalLength();
    const uint32_t maxPayload =
        driver->getMaxPacketSize() - downCast<uint32_t>(sizeof(header));
    for (uint32_t sent = 0; sent < totalLength; sent += maxPayload) {
        header.setMoreWillFollow((totalLength - sent) > maxPayload);
        Buffer::Iterator slice(payload, sent, maxPayload);
        CycleCounter<RawMetric> counter;
        driver->sendPacket(recipient, &header, sizeof(header), &slice);
        metrics->transport.transmit.ticks += counter.stop();
        ++metrics->transport.transmit.packetCount;
    }
}

UnreliableTransport::UnreliableServerRpc::UnreliableServerRpc(
                                        UnreliableTransport& t,
                                        uint32_t nonce,
                                        const Driver::Address* clientAddress)
    : t(t)
    , nonce(nonce)
    , clientAddress(clientAddress)
    , listEntries()
{
}

void
UnreliableTransport::UnreliableServerRpc::sendReply()
{
#ifdef INFINIBAND
    // Hack to cache Infiniband address handles in a hash table. Otherwise we
    // have to create a new address handle on each response, and that's
    // currently an expensive task (it adds about 50 us to an RTT).
    const RealInfiniband::Address* infAddress =
        dynamic_cast<const RealInfiniband::Address*>(clientAddress); // NOLINT
    if (infAddress != NULL) {
        union {
            struct {
                uint16_t physicalPort;
                uint16_t lid;
                uint32_t qpn;
            };
            uint64_t bulk;
        } key;
        static_assert(sizeof(key) == sizeof(key.bulk), "packing fail");
        key.physicalPort = downCast<uint16_t>(infAddress->physicalPort);
        key.lid = infAddress->lid;
        key.qpn = infAddress->qpn;
        ibv_ah*& ah = t.infinibandAddressHandleCache[key.bulk];
        if (ah != NULL)
            infAddress->ah = ah;
        else
            ah = infAddress->getHandle();
    }
#endif

    Header header = { 0, 0, nonce };
    t.sendPacketized(clientAddress, header, replyPayload);

#ifdef INFINIBAND
    if (infAddress != NULL)
        infAddress->ah = NULL; // leak address handles since they are cached
#endif

    t.serverRpcPool.destroy(this);
    ++metrics->transport.transmit.messageCount;
    metrics->transport.transmit.iovecCount += replyPayload.getNumberChunks();
    metrics->transport.transmit.byteCount += replyPayload.getTotalLength();
}


UnreliableTransport::UnreliableSession::UnreliableSession(
                                        UnreliableTransport& t,
                                        const ServiceLocator& serviceLocator)
    : t(t)
    , serverAddress(t.driver->newAddress(serviceLocator))
{
}

Transport::ClientRpc*
UnreliableTransport::UnreliableSession::clientSend(Buffer* request,
                                                   Buffer* response)
{
    ++metrics->transport.transmit.messageCount;
    metrics->transport.transmit.iovecCount += request->getNumberChunks();
    metrics->transport.transmit.byteCount += request->getTotalLength();
    return new(response, MISC) UnreliableClientRpc(t, request, response,
                                                   *serverAddress.get());
}

void
UnreliableTransport::UnreliableSession::release()
{
    delete this;
}

UnreliableTransport::UnreliableClientRpc::UnreliableClientRpc(
                                            UnreliableTransport& t,
                                            Buffer* request,
                                            Buffer* response,
                                            Driver::Address& serverAddress)
    : Transport::ClientRpc(request, response)
    , t(t)
    , nonce(static_cast<uint32_t>(generateRandom() & Header::NONCE_MASK))
    , listEntries()
{
    t.clientPendingList.push_back(*this);
    Header header = { 1, 0, nonce };
    t.sendPacketized(&serverAddress, header, *request);
}

} // namespace RAMCloud
