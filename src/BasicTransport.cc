/* Copyright (c) 2015-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "BasicTransport.h"
#include "Service.h"
#include "ServiceLocator.h"
#include "TimeTrace.h"
#include "WorkerManager.h"

namespace RAMCloud {

/**
 * Construct a new BasicTransport.
 * 
 * \param context
 *      Shared state about various RAMCloud modules.
 * \param locator
 *      Service locator that contains parameters for this transport.
 * \param driver
 *      Used to send and receive packets. This transport becomes owner
 *      of the driver and will free it in when this object is deleted.
 * \param clientId
 *      Identifier that identifies us in outgoing RPCs: must be unique across
 *      all servers and clients.
 */
BasicTransport::BasicTransport(Context* context, const ServiceLocator* locator,
        Driver* driver, uint64_t clientId)
    : context(context)
    , driver(driver)
    , maxDataPerPacket(driver->getMaxPacketSize() - sizeof32(DataHeader))
    , clientId(clientId)
    , nextSequenceNumber(1)
    , serverRpcPool()
    , clientRpcPool()
    , outgoingRpcs()
    , incomingRpcs()
    , serverTimerList()
    , roundTripBytes(getRoundTripBytes(locator))
    , grantIncrement(5*maxDataPerPacket)
    , timer(this, context->dispatch)
    , timerInterval(0)
    , timeoutIntervals(100)
    , pingIntervals(5)
{
    driver->connect(new IncomingPacketHandler(this));

    // Set up the timer to trigger at 2 ms intervals. We use this choice
    // (as of 11/2015) because the Linux kernel appears to buffer packets
    // for up to about 1 ms before delivering them to applications. Shorter
    // intervals result in unnecessary retransmissions.
    timerInterval = Cycles::fromMicroseconds(2000);
    timer.start(Cycles::rdtsc() + timerInterval);

    LOG(NOTICE, "BasicTransport parameters: maxDataPerPacket %u, "
            "roundTripBytes %u, grantIncrement %u, pingIntervals %d, "
            "timeoutIntervals %d, timerInterval %.2f ms",
            maxDataPerPacket, roundTripBytes,
            grantIncrement, pingIntervals, timeoutIntervals,
            Cycles::toSeconds(timerInterval)*1e3);
}

/**
 * Destructor for BasicTransports.
 */
BasicTransport::~BasicTransport()
{
    // This cleanup is mostly for the benefit of unit tests: in production,
    // this destructor is unlikely ever to get called.
    timer.stop();

    // Reclaim all of the RPC objects.
    for (ServerRpcMap::iterator it = incomingRpcs.begin();
            it != incomingRpcs.end(); ) {
        ServerRpc* serverRpc = it->second;

        // Advance iterator; otherwise it will get invalidated by
        // deleteServerRpc.
        it++;
        deleteServerRpc(serverRpc);
    }
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); it++) {
        ClientRpc* clientRpc = it->second;
        clientRpcPool.destroy(clientRpc);
    }
    delete driver;
}

// See Transport::getServiceLocator().
string
BasicTransport::getServiceLocator()
{
    return driver->getServiceLocator();
}

/*
 * When we are finished processing an incoming RPC, this method is
 * invoked to delete the RPC object and remove it from all existing
 * data structures.
 * 
 * \param serverRpc
 *      An RPC that has either completed normally or should be
 *      aborted.
 */
void
BasicTransport::deleteServerRpc(ServerRpc* serverRpc)
{
    TEST_LOG("RpcId (%lu, %lu)", serverRpc->rpcId.clientId,
            serverRpc->rpcId.sequence);
    incomingRpcs.erase(serverRpc->rpcId);
    if ((serverRpc->transmitOffset > 0) || !serverRpc->requestComplete) {
        erase(serverTimerList, *serverRpc);
    }
    serverRpcPool.destroy(serverRpc);
}

/**
 * Parse option values in a service locator to determine how many bytes
 * of data must be sent to cover the round-trip latency of a connection.
 *
 * \param locator
 *      Service locator that may contain "gbs" and "rttMicros" options.
 *      If NULL, or if any of the options  are missing, then defaults
 *      are supplied.
 */
uint32_t
BasicTransport::getRoundTripBytes(const ServiceLocator* locator)
{
    uint32_t gBitsPerSec = 10;
    uint32_t roundTripMicros = 25;

    if (locator  != NULL) {
        if (locator->hasOption("gbs")) {
            char* end;
            uint32_t value = downCast<uint32_t>(strtoul(
                    locator->getOption("gbs").c_str(), &end, 10));
            if ((*end == 0) && (value != 0)) {
                gBitsPerSec = value;
            } else {
                LOG(ERROR, "Bad BasicTransport gbs option value '%s' "
                        "(expected positive integer); ignoring option",
                        locator->getOption("gbs").c_str());
            }
        }
        if (locator->hasOption("rttMicros")) {
            char* end;
            uint32_t value = downCast<uint32_t>(strtoul(
                    locator->getOption("rttMicros").c_str(), &end, 10));
            if ((*end == 0) && (value != 0)) {
                roundTripMicros = value;
            } else {
                LOG(ERROR, "Bad BasicTransport rttMicros option value '%s' "
                        "(expected positive integer); ignoring option",
                        locator->getOption("rttMicros").c_str());
            }
        }
    }

    // Compute round-trip time in terms of full packets (round up).
    uint32_t roundTripBytes = (roundTripMicros*gBitsPerSec*1000)/8;
    roundTripBytes = ((roundTripBytes+maxDataPerPacket-1)/maxDataPerPacket)
            * maxDataPerPacket;
    return roundTripBytes;
}

/**
 * Return a printable symbol for the opcode field from a packet.
 * \param opcode
 *     Opcode field from a packet.
 * \return
 *     The result is a static string, which may change on the next
 *
 */
string
BasicTransport::opcodeSymbol(uint8_t opcode) {
    switch (opcode) {
        case BasicTransport::PacketOpcode::ALL_DATA:
            return "ALL_DATA";
        case BasicTransport::PacketOpcode::DATA:
            return "DATA";
        case BasicTransport::PacketOpcode::GRANT:
            return "GRANT";
        case BasicTransport::PacketOpcode::RESEND:
            return "RESEND";
        case BasicTransport::PacketOpcode::PING:
            return "PING";
        case BasicTransport::PacketOpcode::RETRY:
            return "RETRY";
    }

    return format("opcode %d", opcode);
}

/**
 * This method takes care of packet sizing and transmitting message data,
 * both for requests and for responses. When a method returns, the given
 * range of data will have been queued for the NIC but may not actually
 * have been transmitted yet.
 * 
 * \param address
 *      Identifies the destination for the message.
 * \param rpcId
 *      Unique identifier for the RPC.
 * \param message
 *      Contains the entire message.
 * \param offset
 *      Offset in bytes of the first byte to be transmitted.
 * \param length
 *      Total number of bytes to transmit. If offset + length exceeds
 *      the message length, then all of the remaining bytes in message,
 *      starting at offset, will be transmitted.
 * \param flags
 *      Extra flags to set in packet headers, such as FROM_CLIENT or
 *      RETRANSMISSION. Must at least specify either FROM_CLIENT or
 *      FROM_SERVER.
 */
void
BasicTransport::sendBytes(const Driver::Address* address, RpcId rpcId,
        Buffer* message, int offset, int length, uint8_t flags)
{
    flags |= NEED_GRANT;
    int messageSize = downCast<int>(message->size());
    if (length >= (messageSize - offset)) {
        length = messageSize - offset;
        flags = downCast<uint8_t>(flags & ~NEED_GRANT);
    }
    if (length <= 0) {
        return;
    }

    if ((offset == 0) && (length <= maxDataPerPacket) &&
            (length == messageSize)) {
        // Message fits entirely in a single packet.
        AllDataHeader header(rpcId, flags, downCast<uint16_t>(length));
        Buffer::Iterator iter(message, 0, length);
        driver->sendPacket(address, &header, &iter);
    } else {
        // Send multiple packets.
        while (length > 0) {
            DataHeader header(rpcId, message->size(), offset, flags);
            int bytesThisPacket = std::min(length, maxDataPerPacket);
            Buffer::Iterator iter(message, offset, bytesThisPacket);
            driver->sendPacket(address, &header, &iter);
            offset += bytesThisPacket;
            length -= bytesThisPacket;
        }
    }
}

/**
 * Given a pointer to a BasicTransport packet, return a humanreturn-readable
 * string describing the information in its header.
 * 
 * \param packet
 *      Address of the first bite of the packet header, which must be
 *      contiguous in memory.
 * \param packetLength
 *      Size of the header, in bytes.
 */
string
BasicTransport::headerToString(const void* packet, uint32_t packetLength)
{
    string result;
    const BasicTransport::CommonHeader* common =
            static_cast<const BasicTransport::CommonHeader*>(packet);
    uint32_t headerLength = sizeof32(BasicTransport::CommonHeader);
    if (packetLength < headerLength) {
        goto packetTooShort;
    }
    result += BasicTransport::opcodeSymbol(common->opcode);
    if (common->flags & BasicTransport::FROM_CLIENT) {
        result += " FROM_CLIENT";
    } else {
        result += " FROM_SERVER";
    }
    result += format(", rpcId %lu.%lu",
            common->rpcId.clientId, common->rpcId.sequence);
    switch (common->opcode) {
        case BasicTransport::PacketOpcode::ALL_DATA:
            headerLength = sizeof32(BasicTransport::AllDataHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
        case BasicTransport::PacketOpcode::DATA: {
            headerLength = sizeof32(BasicTransport::DataHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const BasicTransport::DataHeader* data =
                    static_cast<const BasicTransport::DataHeader*>(packet);
            result += format(", totalLength %u, offset %u%s%s",
                    data->totalLength, data->offset,
                    common->flags & BasicTransport::NEED_GRANT
                            ? ", NEED_GRANT" : "",
                    common->flags & BasicTransport::RETRANSMISSION
                            ? ", RETRANSMISSION" : "");
            break;
        }
        case BasicTransport::PacketOpcode::GRANT: {
            headerLength = sizeof32(BasicTransport::GrantHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const BasicTransport::GrantHeader* grant =
                    static_cast<const BasicTransport::GrantHeader*>(packet);
            result += format(", offset %u", grant->offset);
            break;
        }
        case BasicTransport::PacketOpcode::RESEND: {
            headerLength = sizeof32(BasicTransport::ResendHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const BasicTransport::ResendHeader* resend =
                    static_cast<const BasicTransport::ResendHeader*>(
                    packet);
            result += format(", offset %u, length %u%s",
                    resend->offset, resend->length,
                    common->flags & BasicTransport::RESTART
                            ? ", RESTART" : "");
            break;
        }
        case BasicTransport::PacketOpcode::PING:
            headerLength = sizeof32(BasicTransport::PingHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
        case BasicTransport::PacketOpcode::RETRY:
            headerLength = sizeof32(BasicTransport::RetryHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
    }
    return result;

  packetTooShort:
    if (!result.empty()) {
        result += ", ";
    }
    result += format("packet too short (got %u bytes, need at least %u)",
            packetLength, headerLength);
    return result;
}

/**
 * Construct a new client session.
 *
 * \throw TransportException
 *      The service locator couldn't be parsed (a log message will
 *      have been generated already).
 */
BasicTransport::Session::Session(BasicTransport* t,
        const ServiceLocator* locator, uint32_t timeoutMs)
    : t(t)
    , serverAddress(NULL)
    , roundTripBytes(t->getRoundTripBytes(locator))
    , aborted(false)
{
    try {
        serverAddress = t->driver->newAddress(locator);
    }
    catch (const Exception& e) {
        LOG(NOTICE, "%s", e.message.c_str());
        throw TransportException(HERE,
                "BasicTransport couldn't parse service locator");
    }
    setServiceLocator(locator->getOriginalString());
}

/*
 * Destructor for client sessions.
 */
BasicTransport::Session::~Session()
{
    abort();
    delete serverAddress;
}

// See Transport::Session::abort for docs.
void
BasicTransport::Session::abort()
{
    aborted = true;
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); ) {
        uint64_t sequence = it->first;
        ClientRpc* clientRpc = it->second;
        it++;
        if (clientRpc->session == this) {
            t->clientRpcPool.destroy(clientRpc);
            t->outgoingRpcs.erase(sequence);
        }
    }
}

// See Transport::Session::cancelRequest for docs.
void
BasicTransport::Session::cancelRequest(RpcNotifier* notifier)
{
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); it++) {
        ClientRpc* clientRpc = it->second;
        if (clientRpc->notifier == notifier) {
            t->outgoingRpcs.erase(it);
            t->clientRpcPool.destroy(clientRpc);

            // It's no longer safe to use "it", but at this point we're
            // done (the RPC can't exist in the list twice).
            return;
        }
    }
}

// See Transport::Session::getRpcInfo for docs.
string
BasicTransport::Session::getRpcInfo()
{
    string result;
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); it++) {
        ClientRpc* clientRpc = it->second;
        if (clientRpc->session != this) {
            continue;
        }
        if (result.size() != 0) {
            result += ", ";
        }
        result += WireFormat::opcodeSymbol(clientRpc->request);
    }
    if (result.empty())
        result = "no active RPCs";
    result += " to server at ";
    result += getServiceLocator();
    return result;
}

// See Transport::Session::sendRequest for docs.
void
BasicTransport::Session::sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier)
{
    response->reset();
    if (aborted) {
        notifier->failed();
        return;
    }
    ClientRpc *clientRpc = t->clientRpcPool.construct(this, request,
            response, notifier);
    RpcId id(t->clientId, t->nextSequenceNumber);
    t->nextSequenceNumber++;
    t->outgoingRpcs[id.sequence] = clientRpc;
    t->sendBytes(serverAddress, id, request, 0, roundTripBytes, FROM_CLIENT);
    clientRpc->transmitOffset = roundTripBytes;
}

/**
 * This method is invoked by the driver whenever a packet arrives.
 * It is the top-level dispatching method for dealing with incoming
 * packets, both for requests and responses.
 * 
 * \param received
 *      Information about the new packet.
 */
void
BasicTransport::IncomingPacketHandler::handlePacket(Driver::Received* received)
{
    // The following method retrieves a header from a packet
    CommonHeader* common = received->getOffset<CommonHeader>(0);
    if (common == NULL) {
        RAMCLOUD_CLOG(WARNING, "packet from %s too short (%u bytes)",
                received->sender->toString().c_str(), received->len);
        return;
    }

    if (!(common->flags & FROM_CLIENT)) {
        // This packet was sent by the server, and it pertains to an RPC
        // for which we are the client.
        ClientRpcMap::iterator it = t->outgoingRpcs.find(
                common->rpcId.sequence);
        if (it == t->outgoingRpcs.end()) {
            LOG(NOTICE, "Received packet from %s for %s RPC: %s",
                    received->sender->toString().c_str(),
                    (common->rpcId.sequence < t->nextSequenceNumber)
                    ? "completed" : "unknown",
                    headerToString(common, received->len).c_str());
            return;
        }
        ClientRpc* clientRpc = it->second;
        clientRpc->silentIntervals = 0;
        switch (common->opcode) {
            case PacketOpcode::ALL_DATA: {
                // This RPC is now finished.
                AllDataHeader* header = received->getOffset<AllDataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                uint32_t length;
                char *payload = received->steal(&length);
                uint32_t requiredLength =
                        downCast<uint32_t>(header->messageLength) +
                        sizeof32(AllDataHeader);
                if (length < requiredLength) {
                    t->driver->release(payload);
                    RAMCLOUD_CLOG(WARNING, "ALL_DATA response from %s too "
                            "short (got %u bytes, expected %u)",
                            received->sender->toString().c_str(),
                            received->len, requiredLength);
                    return;
                }
                Driver::PayloadChunk::appendToBuffer(clientRpc->response,
                        payload + sizeof32(AllDataHeader),
                        header->messageLength, t->driver, payload);
                t->outgoingRpcs.erase(header->common.rpcId.sequence);
                clientRpc->notifier->completed();
                t->clientRpcPool.destroy(clientRpc);
                return;
            }

            case PacketOpcode::DATA: {
                DataHeader* header = received->getOffset<DataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                if (!clientRpc->accumulator) {
                    clientRpc->accumulator.construct(t, clientRpc->response);
                }
                clientRpc->accumulator->addPacket(received, header);
                if (clientRpc->response->size() >= header->totalLength) {
                    // Response complete.
                    if (clientRpc->response->size()
                            > header->totalLength) {
                        // We have more bytes than we want. This can happen
                        // if the last packet gets padded by the network
                        // layer to meet minimum size requirements. Just
                        // truncate the response.
                        clientRpc->response->truncate(header->totalLength);
                    }
                    t->outgoingRpcs.erase(header->common.rpcId.sequence);
                    clientRpc->notifier->completed();
                    t->clientRpcPool.destroy(clientRpc);
                } else {
                    // See if we need to output a GRANT.
                    if ((header->common.flags & NEED_GRANT) &&
                            (clientRpc->grantOffset <
                            (clientRpc->response->size() +
                            clientRpc->session->roundTripBytes)) &&
                            (clientRpc->grantOffset < header->totalLength)) {
                        clientRpc->grantOffset = clientRpc->response->size()
                                + clientRpc->session->roundTripBytes
                                + t->grantIncrement;
                        GrantHeader grant(header->common.rpcId,
                                clientRpc->grantOffset, FROM_CLIENT);
                        t->driver->sendPacket(clientRpc->session->serverAddress,
                                &grant, NULL);
                    }
                }
                if ((header->offset < clientRpc->resendLimit) &&
                        !(header->common.flags & RETRANSMISSION)) {
                    LOG(NOTICE, "Original data arrived from server %s after "
                            "RESEND: sequence %lu, offset %u, "
                            "resendLimit %u",
                            received->sender->toString().c_str(),
                            common->rpcId.sequence, header->offset,
                            clientRpc->resendLimit);
                }
                return;
            }

            case PacketOpcode::GRANT: {
                GrantHeader* header = received->getOffset<GrantHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                if (header->offset > clientRpc->transmitOffset) {
                    t->sendBytes(clientRpc->session->serverAddress,
                            header->common.rpcId, clientRpc->request,
                            clientRpc->transmitOffset,
                            header->offset - clientRpc->transmitOffset,
                            FROM_CLIENT);
                    clientRpc->transmitOffset = header->offset;
                }
                return;
            }

            case PacketOpcode::RESEND: {
                ResendHeader* header = received->getOffset<ResendHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                if (header->common.flags & RESTART) {
                    clientRpc->transmitOffset = 0;
                    clientRpc->grantOffset = 0;
                    clientRpc->resendLimit = 0;
                    clientRpc->accumulator.destroy();
                }
                t->sendBytes(clientRpc->session->serverAddress,
                        header->common.rpcId, clientRpc->request,
                        header->offset, header->length,
                        FROM_CLIENT|RETRANSMISSION);
                uint32_t resendEnd = header->offset + header->length;
                if (resendEnd > clientRpc->transmitOffset) {
                    clientRpc->transmitOffset = resendEnd;
                }
                return;
            }

            case PacketOpcode::RETRY: {
                LOG(NOTICE, "Client received RETRY from %s for sequence %lu",
                        received->sender->toString().c_str(),
                        common->rpcId.sequence);
                Service::prepareRetryResponse(clientRpc->response,
                        0, 0, "BasicTransport suffered packet loss after "
                        "server freed its state");
                t->outgoingRpcs.erase(common->rpcId.sequence);
                clientRpc->notifier->completed();
                t->clientRpcPool.destroy(clientRpc);
                return;
            }

            default:
            RAMCLOUD_CLOG(WARNING,
                    "unexpected opcode %s received from server %s",
                    opcodeSymbol(common->opcode).c_str(),
                    received->sender->toString().c_str());
            return;
        }
    } else {
        // This packet was sent by the client; it relates to an RPC
        // for which we are the server.

        // Find the record for this RPC, if one exists.
        ServerRpc* serverRpc = NULL;
        ServerRpcMap::iterator it = t->incomingRpcs.find(common->rpcId);
        if (it != t->incomingRpcs.end()) {
            serverRpc = it->second;
            serverRpc->silentIntervals = 0;
        }

        switch (common->opcode) {
            case PacketOpcode::ALL_DATA: {
                // Common case: the entire request fit in a single packet.

                AllDataHeader* header = received->getOffset<AllDataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                if (serverRpc != NULL) {
                    // This shouldn't normally happen: it means this packet is
                    // a duplicate, so we can just discard it.
                    return;
                }
                uint32_t length;
                char *payload = received->steal(&length);
                uint32_t requiredLength =
                        downCast<uint32_t>(header->messageLength) +
                        sizeof32(AllDataHeader);
                if (length < requiredLength) {
                    t->driver->release(payload);
                    RAMCLOUD_CLOG(WARNING, "ALL_DATA request from %s too "
                            "short (got %u bytes, expected %u)",
                            received->sender->toString().c_str(),
                            received->len, requiredLength);
                    return;
                }
                serverRpc = t->serverRpcPool.construct(t, received->sender,
                        header->common.rpcId);
                t->incomingRpcs[header->common.rpcId] = serverRpc;
                Driver::PayloadChunk::appendToBuffer(&serverRpc->requestPayload,
                        payload + sizeof32(AllDataHeader),
                        header->messageLength, t->driver, payload);
                serverRpc->requestComplete = true;
                t->context->workerManager->handleRpc(serverRpc);
                return;
            }

            case PacketOpcode::DATA: {
                DataHeader* header = received->getOffset<DataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                if (serverRpc == NULL) {
                    serverRpc = t->serverRpcPool.construct(t,
                            received->sender, header->common.rpcId);
                    t->incomingRpcs[header->common.rpcId] = serverRpc;
                    serverRpc->accumulator.construct(t,
                            &serverRpc->requestPayload);
                    t->serverTimerList.push_back(*serverRpc);
                } else if (serverRpc->requestComplete) {
                    // We've already received the full message, so
                    // ignore this packet.
                    TEST_LOG("ignoring extraneous packet");
                    goto serverDataDone;
                }
                serverRpc->accumulator->addPacket(received, header);
                if (serverRpc->requestPayload.size() >= header->totalLength) {
                    // Message complete; start servicing the RPC.
                    if (serverRpc->requestPayload.size()
                            > header->totalLength) {
                        // We have more bytes than we want. This can happen
                        // if the last packet gets padded by the network
                        // layer to meet minimum size requirements. Just
                        // truncate the request.
                        serverRpc->requestPayload.truncate(header->totalLength);
                    }
                    erase(t->serverTimerList, *serverRpc);
                    serverRpc->requestComplete = true;
                    t->context->workerManager->handleRpc(serverRpc);
                } else {
                    // See if we need to output a GRANT.
                    if ((header->common.flags & NEED_GRANT) &&
                            (serverRpc->grantOffset <
                            (serverRpc->requestPayload.size()
                            + t->roundTripBytes)) &&
                            (serverRpc->grantOffset < header->totalLength)) {
                        serverRpc->grantOffset =
                                serverRpc->requestPayload.size()
                                + t->roundTripBytes + t->grantIncrement;
                        GrantHeader grant(header->common.rpcId,
                                serverRpc->grantOffset, FROM_SERVER);
                        t->driver->sendPacket(serverRpc->clientAddress,
                                &grant, NULL);
                    }
                }
                serverDataDone:
                if ((header->offset < serverRpc->resendLimit) &&
                        !(header->common.flags & RETRANSMISSION)) {
                    LOG(NOTICE, "Original data arrived from client %s after "
                            "RESEND: sequence %lu, offset %u, "
                            "resendLimit %u",
                            received->sender->toString().c_str(),
                            common->rpcId.sequence, header->offset,
                            serverRpc->resendLimit);
                }
                return;
            }

            case PacketOpcode::GRANT: {
                GrantHeader* header = received->getOffset<GrantHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                if ((serverRpc == NULL) || (serverRpc->transmitOffset == 0)) {
                    RAMCLOUD_LOG(WARNING, "unexpected GRANT from client %s, "
                            "id (%lu,%lu), grantOffset %u",
                            received->sender->toString().c_str(),
                            header->common.rpcId.clientId,
                            header->common.rpcId.sequence, header->offset);
                    return;
                }
                if (header->offset > serverRpc->transmitOffset) {
                    t->sendBytes(serverRpc->clientAddress,
                            header->common.rpcId, &serverRpc->replyPayload,
                            serverRpc->transmitOffset,
                            header->offset - serverRpc->transmitOffset,
                            FROM_SERVER);
                    serverRpc->transmitOffset = header->offset;
                    if (serverRpc->transmitOffset >=
                            serverRpc->replyPayload.size()) {
                        // See "Note A" in sendReply.
                        t->deleteServerRpc(serverRpc);

                    }
                }
                return;
            }

            case PacketOpcode::RESEND: {
                ResendHeader* header = received->getOffset<ResendHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                if (serverRpc == NULL) {
                    // This situation can happen if a packet of the response
                    // got lost but we have already freed the ServerRpc. We
                    // no longer have the missing data, so ask the client to
                    // restart the RPC from scratch.
                    RAMCLOUD_CLOG(WARNING, "received RESEND from client %s, "
                            "but RPC state no longer exists",
                            received->sender->toString().c_str());
                    RetryHeader retry(header->common.rpcId, FROM_SERVER);
                    t->driver->sendPacket(received->sender, &retry, NULL);
                    return;
                }
                if (serverRpc->transmitOffset == 0) {
                    // We haven't started transmitting the result yet, so
                    // we shouldn't have received this packet; ignored.
                    RAMCLOUD_CLOG(WARNING, "unexpected RESEND from client %s",
                            received->sender->toString().c_str());
                    return;
                }
                t->sendBytes(serverRpc->clientAddress,
                        serverRpc->rpcId, &serverRpc->replyPayload,
                        header->offset, header->length,
                        RETRANSMISSION|FROM_SERVER);
                uint32_t resendEnd = header->offset + header->length;
                if (resendEnd > serverRpc->transmitOffset) {
                    serverRpc->transmitOffset = resendEnd;
                    if (serverRpc->transmitOffset >=
                            serverRpc->replyPayload.size()) {
                        // See "Note A" in sendReply.
                        t->deleteServerRpc(serverRpc);
                    }
                }
                return;
            }

            case PacketOpcode::PING: {
                if (serverRpc == NULL) {
                    // No record of this RPC. Either all of the packets got
                    // lost or we already sent a response and deleted our
                    // state. Ask the client to restart transmission.
                    ResendHeader resend(common->rpcId, 0, t->roundTripBytes,
                            FROM_SERVER|RESTART);
                    t->driver->sendPacket(received->sender, &resend, NULL);
                    RAMCLOUD_LOG(NOTICE, "Unexpected PING from %s for sequence"
                            "%lu (packets lost?); requesting retransmit of"
                            "bytes %u-%u",
                            received->sender->toString().c_str(),
                            common->rpcId.sequence,
                            0, t->roundTripBytes);
                    return;
                }
                if (serverRpc->transmitOffset == 0) {
                    // Either we haven't received the whole request message yet
                    // or we're still working on executing the RPC. In either
                    // case, just send a dummy GRANT back to the client,
                    // whose only purpose is to let the client know we're
                    // still alive. If there's a problem receiving the request,
                    // handleTimerEvent will take care of that.
                    GrantHeader grant(common->rpcId, serverRpc->grantOffset,
                            FROM_SERVER);
                    t->driver->sendPacket(serverRpc->clientAddress,
                            &grant, NULL);
                    if (!serverRpc->requestComplete) {
                        LOG(NOTICE, "PING request from %s after receiving %u "
                                "bytes for sequence %lu, %lu fragments",
                                received->sender->toString().c_str(),
                                serverRpc->requestPayload.size(),
                                common->rpcId.sequence,
                                serverRpc->accumulator->fragments.size());
                    }
                } else {
                    // We have started sending the response message. It's
                    // possible that all of the response packets got lost,
                    // so we have to retransmit something. On the other hand,
                    // it's also possible that the PING arrived just after
                    // we started sending a response, so no packets have
                    // actually been lost. This is the more likely case, so
                    // only resent one packet's worth of data (as opposed to
                    // retransmitting everything we've already sent).
                    t->sendBytes(serverRpc->clientAddress,
                            serverRpc->rpcId, &serverRpc->replyPayload,
                            0, t->maxDataPerPacket, FROM_SERVER|RETRANSMISSION);
                    LOG(NOTICE, "PING request from %s while sending reply, "
                            "%u bytes already sent",
                            received->sender->toString().c_str(),
                            serverRpc->transmitOffset);
                }
                return;
            }

            default:
                RAMCLOUD_CLOG(WARNING,
                        "unexpected opcode %s received from client %s",
                        opcodeSymbol(common->opcode).c_str(),
                        received->sender->toString().c_str());
                return;
        }
    }

    packetLengthError:
    RAMCLOUD_CLOG(WARNING, "packet of type %s from %s too short (%u bytes)",
            opcodeSymbol(common->opcode).c_str(),
            received->sender->toString().c_str(),
            received->len);

}

/**
 * Returns a string containing human-readable information about the client
 * that initiated this RPC. Right now this isn't formatted as a service
 * locator; it just describes a Driver::Address.
 */
string
BasicTransport::ServerRpc::getClientServiceLocator()
{
    return clientAddress->toString();
}

/**
 * This method is invoked when a server has finished processing an RPC.
 * It begins transmitting the response back to the client, but returns
 * before that process is complete.
 */
void
BasicTransport::ServerRpc::sendReply()
{
    t->sendBytes(clientAddress, rpcId, &replyPayload, 0, t->roundTripBytes,
            FROM_SERVER);
    if (t->roundTripBytes >= replyPayload.size()) {
        // Note A: delete the ServerRpc object as soon as we have transmitted
        // the last byte. This has the disadvantage that if some of this data
        // is lost we won't be able to retransmit it (the whole RPC will be
        // retried), but this approach is simpler and faster in the common
        // case where data isn't lost.
        t->deleteServerRpc(this);
    } else {
        transmitOffset = t->roundTripBytes;
        t->serverTimerList.push_back(*this);
    }
}

/**
 * Construct a MessageAccumulator.
 *
 * \param t
 *      Overall information about the transport.
 * \param buffer
 *      The complete message will be assembled here; caller should ensure
 *      that this is initially empty. The caller owns the storage for this
 *      and must ensure that it persists as long as this object persists.
 */
BasicTransport::MessageAccumulator::MessageAccumulator(BasicTransport* t,
        Buffer* buffer)
    : t(t)
    , buffer(buffer)
    , fragments()
    , grantOffset(0)
{ }

/**
 * Destructor for MessageAccumulators.
 */
BasicTransport::MessageAccumulator::~MessageAccumulator()
{
    // If there are any unassembled fragments, then we must release
    // them back to the driver.
    for (FragmentMap::iterator it = fragments.begin();
            it != fragments.end(); it++) {
        MessageFragment fragment = it->second;
        t->driver->release(fragment.payload);
    }
    fragments.clear();
}

/**
 * This method is invoked whenever a new DATA packet arrives for a partially
 * complete message. It saves information about the new fragment and
 * (eventually) combines all of the fragments into a complete message.
 * 
 * \param received
 *      Information about a DATA packet, as provided by the driver. Note:
 *      we will "steal" this from the driver and ensure that it is
 *      eventually returned to the driver.
 * \param header
 *      Pointer to header data in the packet. Caller must have ensured that
 *      the packet is large enough to hold a complete header.
 */
void
BasicTransport::MessageAccumulator::addPacket(Driver::Received* received,
        DataHeader *header)
{
    uint32_t length;
    char *payload = received->steal(&length);
    if (header->offset > buffer->size()) {
        // Can't append this packet into the buffer because some prior
        // data is missing. Save the packet for later.
        fragments[header->offset] = MessageFragment(payload, length);
        return;
    }

    // Append this fragment to the assembled message buffer, then see
    // if some of the unappended fragments can now be appended as well.
    appendFragment(payload, header->offset, length);
    while (true) {
        FragmentMap::iterator it = fragments.begin();
        if (it == fragments.end()) {
            break;
        }
        uint32_t offset = it->first;
        if (offset > buffer->size()) {
            break;
        }
        MessageFragment fragment = it->second;
        appendFragment(fragment.payload, offset, fragment.length);
        fragments.erase(it);
    }
}

/**
 * This method is invoked to append a fragment to a partially-assembled
 * message. It handles the special cases where part or all of the
 * fragment is already in the assembled message.
 * 
 * \param payload
 *      Address of the first byte of a DATA packet, as returned by
 *      Driver::steal. The caller must already have stolen this packet
 *      from the driver so we now own it.
 * \param offset
 *      Offset in the message corresponding to the first byte of message
 *      data at payload.  Must be no greater than the current length of
 *      buffer.
 * \param length
 *      Total size of the packet at *payload (including header).
 */
void
BasicTransport::MessageAccumulator::appendFragment(char* payload,
        uint32_t offset, uint32_t length)
{
    uint32_t bytesToSkip = buffer->size() - offset;
    length -= sizeof32(DataHeader);
    if (bytesToSkip >= length) {
        // This entire fragment is redundant; drop it.
        t->driver->release(payload);
        return;
    }
    Driver::PayloadChunk::appendToBuffer(buffer,
            payload + sizeof32(DataHeader) + bytesToSkip,
            length - bytesToSkip, t->driver, payload);
}

/**
 * This method is invoked to issue a RESEND packet when it appears that
 * packets have been lost. It is used by both servers and clients.
 * 
 * \param t
 *      Overall information about the transport.
 * \param address
 *      Network address to which the RESEND should be sent.
 * \param rpcId
 *      Unique identifier for the RPC in question.
 * \param grantOffset
 *      Largest grantOffset that has been sent for this message (i.e.
 *      this is how many total bytes we should have received already).
 *      May be 0 if the client never requested a grant (meaning that it
 *      planned to transmit the entire message unilaterally).
 * \param roundTripBytes
 *      Number of bytes that can be transmitted during the time it takes
 *      for a round-trip latency.
 * \param whoFrom
 *      Must be either FROM_CLIENT, indicating that we are the client, or
 *      FROM_SERVER, indicating that we are the server.
 *
 * \return
 *      The offset of the byte just after the last one whose retransmission
 *      was requested.
 */
uint32_t
BasicTransport::MessageAccumulator::requestRetransmission(BasicTransport *t,
        const Driver::Address* address, RpcId rpcId, uint32_t grantOffset,
        uint32_t roundTripBytes, uint8_t whoFrom)
{
    if ((reinterpret_cast<uint64_t>(&fragments) < 0x1000lu)) {
        DIE("Bad fragment pointer: %p", &fragments);
    }
    uint32_t endOffset;
    FragmentMap::iterator it = fragments.begin();

    // Compute the end of the retransmission range.
    if (it != fragments.end()) {
        // Retransmit the entire gap up to the first fragment.
        endOffset = it->first;
    } else if (grantOffset > 0) {
        // Retransmit everything that we've asked the sender to send:
        // we don't seem to have received any of it.
        endOffset = grantOffset;
    } else {
        // We haven't issued a GRANT for this message; just retransmit
        // one round-trip's worth of data. Once this data arrives, the
        // normal grant mechanism should kick in if it's still needed.
        endOffset = buffer->size() + roundTripBytes;
    }
    assert(endOffset > buffer->size());
    ResendHeader resend(rpcId, buffer->size(), endOffset - buffer->size(),
            whoFrom);
    t->driver->sendPacket(address, &resend, NULL);
    RAMCLOUD_LOG(NOTICE, "requested retransmit of %s bytes %u-%u from %s, "
            "sequence %lu, grantOffset %u (packets lost?)",
            (rpcId.clientId == t->clientId) ? "response" : "request",
            buffer->size(), endOffset, address->toString().c_str(),
            rpcId.sequence, grantOffset);
    return endOffset;
}

/**
 * Constructor for a transport's timer. Note: this method does not
 * actually start the timer.
 * 
 * \param t
 *      The transport on whose behalf this timer operates.
 * \param dispatch
 *      Dispatch object that will be used to invoke the timer.
 */
BasicTransport::Timer::Timer(BasicTransport* t, Dispatch* dispatch)
    : Dispatch::Timer(dispatch)
    , t(t)
{}

/**
 * This method is invoked by the dispatcher at intervals determined
 * by t->timerInterval. It implements all of the timer-related functionality
 * for both clients and servers, such as requesting packet retransmission
 * and aborting RPCs.
 */
void
BasicTransport::Timer::handleTimerEvent()
{
    // First, restart the timer.
    start(Cycles::rdtsc() + t->timerInterval);

    // Scan all of the ClientRpc objects.
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); ) {
        uint64_t sequence = it->first;
        ClientRpc* clientRpc = it->second;
        clientRpc->silentIntervals++;

        // Advance the iterator here, so that it won't get invalidated if
        // we delete the ClientRpc below.
        it++;

        assert(t->timeoutIntervals > 2*t->pingIntervals);
        if (clientRpc->silentIntervals >= t->timeoutIntervals) {
            // A long time has elapsed with no communication whatsoever
            // from the server, so abort the RPC.
            RAMCLOUD_LOG(WARNING, "aborting %s RPC to server %s, "
                    "sequence %lu: timeout",
                    WireFormat::opcodeSymbol(clientRpc->request),
                    clientRpc->session->serverAddress->toString().c_str(),
                    sequence);
            t->outgoingRpcs.erase(sequence);
            clientRpc->notifier->failed();
            t->clientRpcPool.destroy(clientRpc);
            continue;
        }
        if (clientRpc->silentIntervals == (t->pingIntervals+2)) {
            // We have sent a PING, but didn't get a timely response.
            RAMCLOUD_LOG(NOTICE, "slow PING response from server %s "
                    "for %s RPC, sequence %lu",
                    clientRpc->session->serverAddress->toString().c_str(),
                    WireFormat::opcodeSymbol(clientRpc->request),
                    sequence);
        }

        if (clientRpc->response->size() == 0) {
            // We haven't received any part of the response message.
            // Send occasional PING packets, which should produce some
            // response from the server, so that we know it's still alive
            // and working. Note: the wait time for this ping is longer
            // than the server's wait time to request retransmission (first
            // give the server a chance to handle the problem).
            if ((clientRpc->silentIntervals % t->pingIntervals) == 0) {
                PingHeader ping(RpcId(t->clientId, sequence), FROM_CLIENT);
                t->driver->sendPacket(clientRpc->session->serverAddress,
                        &ping, NULL);
            }
        } else {
            // We have received part of the response. If the server has gone
            // silent, this must mean packets were lost, so request
            // retransmission.
            assert(clientRpc->accumulator);
            if (clientRpc->silentIntervals >= 2) {
                clientRpc->resendLimit =
                        clientRpc->accumulator->requestRetransmission(t,
                        clientRpc->session->serverAddress,
                        RpcId(t->clientId, sequence),
                        clientRpc->grantOffset,
                        clientRpc->session->roundTripBytes, FROM_CLIENT);
            }
        }
    }

    // Scan all of the ServerRpc objects for which network I/O is in
    // progress (either for the request or the response).
    for (ServerTimerList::iterator it = t->serverTimerList.begin();
            it != t->serverTimerList.end(); ) {
        ServerRpc* serverRpc = &(*it);
        serverRpc->silentIntervals++;

        // Advance the iterator now, so it won't get invalidated if we
        // delete the ServerRpc below.
        it++;

        // If a long time has elapsed with no communication whatsoever
        // from the client, then abort the RPC. Note: this code should
        // only be executed when we're waiting to transmit or receive
        // (never if we're waiting for the RPC to execute locally).
        assert(serverRpc->transmitOffset > 0 || !serverRpc->requestComplete);
        if (serverRpc->silentIntervals >= t->timeoutIntervals) {
            RAMCLOUD_LOG(WARNING,
                    "aborting %s RPC from client %s: %u request bytes "
                    "assembled, %lu unassembled fragments, request %s, "
                    "%u response bytes transmitted",
                    WireFormat::opcodeSymbol(&serverRpc->requestPayload),
                    serverRpc->clientAddress->toString().c_str(),
                    serverRpc->requestPayload.size(),
                    serverRpc->accumulator ?
                        serverRpc->accumulator->fragments.size() : 0,
                    serverRpc->requestComplete ? "complete" : "incomplete",
                    serverRpc->transmitOffset);
            t->deleteServerRpc(serverRpc);
            continue;
        }

        // See if we need to request retransmission for part of the request
        // message.
        if ((serverRpc->silentIntervals >= 2) && !serverRpc->requestComplete) {
            serverRpc->resendLimit =
                    serverRpc->accumulator->requestRetransmission(t,
                    serverRpc->clientAddress, serverRpc->rpcId,
                    serverRpc->grantOffset, t->roundTripBytes, FROM_SERVER);
        }
    }
}

}  // namespace RAMCloud
