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

#include "FastTransport.h"

namespace RAMCloud {

// --- FastTransport ---

// - public -

/**
 * Create a FastTransport attached to a particular Driver
 *
 * \param driver
 *      The lower-level driver (presumably to an unreliable mechanism) to
 *      send/receive fragments on. The transport takes ownership of this driver
 *      and will delete it in the destructor.
 */
FastTransport::FastTransport(Driver* driver)
    : driver(driver)
    , clientSessions(this)
    , serverSessions(this)
    , serverReadyQueue()
    , timerList()
{
}

FastTransport::~FastTransport()
{
    // Sessions must be destroyed before the driver
    // since they might hold driver memory.
    serverSessions.clear();
    clientSessions.clear();
    delete driver;
}

// See Transport::getSession().
Transport::SessionRef
FastTransport::getSession(const ServiceLocator& serviceLocator)
{
    clientSessions.expire();
    ClientSession* session = clientSessions.get();
    session->init(serviceLocator);
    return session;
}

// See Transport::serverRecv().
FastTransport::ServerRpc*
FastTransport::serverRecv()
{
    poll();
    if (serverReadyQueue.empty())
        return NULL;
    ServerRpc* rpc = &serverReadyQueue.front();
    serverReadyQueue.pop_front();
    return rpc;
}

// - private -

/**
 * Schedule Timer::onTimerFired() to be called when the system TSC reaches when.
 *
 * If timer is already scheduled it will be rescheduled for when.
 *
 * \param timer
 *      The Timer on which to call onTimerFired().
 * \param now
 *      Roughly the current TSC.
 * \param cyclesToWait
 *      onTimerFired() is called when rdtsc() is now + cyclesToWait or beyond.
 */
void
FastTransport::addTimer(Timer* timer, uint64_t now, uint64_t cyclesToWait)
{
    timer->startTime = now;
    timer->when = now + cyclesToWait;
    if (!timer->listEntries.is_linked())
        timerList.push_front(*timer);
}

/**
 * \return
 *      Number of bytes of RPC data that can fit in a fragment (including the
 *      RPC headers).
 */
uint32_t
FastTransport::dataPerFragment()
{
    return driver->getMaxPacketSize() - sizeof(Header);
}

/**
 * Invoke onTimerFired() on any expired, scheduled Timer after removing it
 * from the timer event queue.
 *
 * Any timer that would like to be fired again later needs to
 * reschedule itself.
 */
void
FastTransport::fireTimers()
{
    uint64_t now = rdtsc();
    TimerList::iterator iter(timerList.begin());
    while (iter != timerList.end()) {
        Timer& timer(*iter);
        if (timer.when && timer.when <= now) {
            iter = timerList.erase(iter);
            timer.onTimerFired(now);
        } else {
            ++iter;
        }
    }
}

/**
 * Number of fragments that would be required to send dataBuffer over
 * this transport.
 *
 * \param dataBuffer
 *      A Buffer intended for transmission over this transport.
 * \return
 *      See method description.
 */
uint32_t
FastTransport::numFrags(const Buffer* dataBuffer)
{
    uint32_t perFragment = dataPerFragment();
    return (dataBuffer->getTotalLength() + perFragment - 1) /
            perFragment;
}

/**
 * Deschedule a Timer.
 *
 * \param timer
 *      The Timer to deschedule.
 */
void
FastTransport::removeTimer(Timer* timer)
{
    timer->when = 0;
    if (timer->listEntries.is_linked())
        timerList.erase(timerList.iterator_to(*timer));
}

/**
 * Try to get fragments from the Driver and accumulate an rpc request,
 * dispatching ready timer events in between.
 *
 * This method returns when the Driver no longer has incoming fragments
 * queued.
 *
 * This is only called by the server.
 */
void
FastTransport::poll()
{
    while (tryProcessPacket())
        fireTimers();
    fireTimers();
}

/**
 * Return a packet indicating BAD_SESSION.
 *
 * \param header
 *      Header from the incoming packet that indicated a bad session.
 * \param address
 *      Indicates where to send the error packet. 
 */
void
FastTransport::sendBadSessionError(Header *header,
        const Driver::Address* address)
{
    Header replyHeader;
    replyHeader.sessionToken = header->sessionToken;
    replyHeader.rpcId = header->rpcId;
    replyHeader.clientSessionHint = header->clientSessionHint;
    replyHeader.serverSessionHint = header->serverSessionHint;
    replyHeader.channelId = header->channelId;
    replyHeader.payloadType = Header::BAD_SESSION;
    replyHeader.direction = Header::SERVER_TO_CLIENT;
    sendPacket(address, &replyHeader, NULL);
}

/**
 * Send a fragment through the transport's driver.
 *
 * Randomly augments fragments with pleaseDrop bit for testing.
 * See Driver::sendPacket().
 */
void
FastTransport::sendPacket(const Driver::Address* address,
                          Header* header,
                          Buffer::Iterator* payload)
{
#if TESTING
    // both sides have +1 to silence gcc when PACKET_LOSS_PERCENTAGE is 0
    header->pleaseDrop = ((generateRandom() % 100) + 1 <
                          PACKET_LOSS_PERCENTAGE + 1);
#endif
    driver->sendPacket(address, header, sizeof(*header), payload);
}

/**
 * Get a single packet from the Driver and dispatch it to the appropriate
 * handler.
 *
 * \retval false
 *      If the Driver didn't have a packet ready.
 * \retval true
 *      A packet was processed and more may be waiting.
 */
bool
FastTransport::tryProcessPacket()
{
    Driver::Received received;
    if (!driver->tryRecvPacket(&received)) {
        TEST_LOG("no packet ready");
        return false;
    }

    Header* header = received.getOffset<Header>(0);
    if (header == NULL) {
        LOG(WARNING,
            "packet too short (%d bytes)",
            received.len);
        return true;
    }
    if (header->pleaseDrop) {
        TEST_LOG("dropped");
        return true;
    }

    if (header->getDirection() == Header::CLIENT_TO_SERVER) {
        // Packet is from the client being processed on the server; find
        // an existing session or open a new one.
        if (header->serverSessionHint >= serverSessions.size()) {
            if (header->getPayloadType() == Header::SESSION_OPEN) {
                // Start a new session on this server for the client.
                LOG(DEBUG, "opening session %d", header->clientSessionHint);
                serverSessions.expire();
                ServerSession* session = serverSessions.get();
                session->startSession(received.sender,
                                      header->clientSessionHint);
            } else {
                LOG(WARNING, "bad session hint %d", header->serverSessionHint);
                sendBadSessionError(header, received.sender);
            }
            return true;
        }
        ServerSession* session = serverSessions[header->serverSessionHint];
        if (session->getToken() == header->sessionToken) {
            TEST_LOG("calling ServerSession::processInboundPacket");
            session->processInboundPacket(&received);
            return true;
        } else {
            LOG(WARNING, "bad session token (0x%lx in session %d, "
                "0x%lx in packet)", session->getToken(),
                header->serverSessionHint, header->sessionToken);
            sendBadSessionError(header, received.sender);
            return true;
        }
    } else {
        // Packet is from the server being processed on the client.
        if (header->clientSessionHint < clientSessions.size()) {
            ClientSession* session = clientSessions[header->clientSessionHint];
            TEST_LOG("client session processing packet");
            if (session->getToken() == header->sessionToken ||
                header->getPayloadType() == Header::SESSION_OPEN) {
                session->processInboundPacket(&received);
            } else {
                LOG(WARNING, "bad fragment token (0x%lx in session %d, "
                    "0x%lx in packet), client dropping", session->getToken(),
                    header->clientSessionHint, header->sessionToken);
            }
        } else {
            LOG(WARNING, "bad client session hint %d",
                header->clientSessionHint);
        }
    }
    return true;
}

// --- ClientRpc ---

/**
 * Create an RPC over a Transport to a Service with a specific request
 * payload and a destination Buffer for response.
 *
 * \param transport
 *      The Transport this RPC is to be emitted on.
 * \param request
 *      The request payload including RPC headers.
 * \param[out] response
 *      The response payload including the RPC headers.
 */
FastTransport::ClientRpc::ClientRpc(FastTransport* transport,
                                    Buffer* request,
                                    Buffer* response)
    : requestBuffer(request)
    , responseBuffer(response)
    , state(IN_PROGRESS)
    , transport(transport)
    , channelQueueEntries()
{
}

/**
 * Blocks until the response buffer associated with this RPC is valid and
 * populated.
 *
 * This method must be called for each RPC before its result can be used.
 *
 * \throws TransportException
 *      If the RPC aborted.
 */
void
FastTransport::ClientRpc::getReply()
{
    // No need to "delete this;" on our way out of the method
    // it is handled by the response buffer destructor since this
    // ClientRpc is in it's MISC memory.

    uint8_t i = 0;
    while (true) {
        switch (state) {
        case IN_PROGRESS:
        default:
            transport->poll();
            break;
        case COMPLETED:
            return;
        case ABORTED:
            throw TransportException("RPC aborted");
        }
        if (++i == 0) // On machines with a small number of cores,
            yield();  // give other tasks a chance to run.
    }
}

/// Change state to ABORTED.  Internal to FastTransport.
void
FastTransport::ClientRpc::abort()
{
    state = ABORTED;
}

/// Change state to COMPLETED.  Internal to FastTransport.
void
FastTransport::ClientRpc::complete()
{
    state = COMPLETED;
}

// --- ServerRpc ---

/**
 * Create a ServerRpc. Used to allocate ServerRpc as part of ServerChannel, see
 * setup() for per RPC initialization.
 */
FastTransport::ServerRpc::ServerRpc()
    : session(NULL)
    , channelId(0)
    , readyQueueEntries()
{
}

/// Make sure this RPC isn't still in a list.  Only happens during testing.
FastTransport::ServerRpc::~ServerRpc()
{
    maybeDequeue();
}

/**
 * Reset a ServerRpc to a unused state.
 */
void
FastTransport::ServerRpc::reset()
{
    maybeDequeue();
    recvPayload.reset();
    replyPayload.reset();
    session = NULL;
    channelId = 0;
}

/**
 * If queued in FastTransport::serverReadyQueue then dequeue it.
 *
 * Used internally to ensure this RPC isn't in a list still after reset()
 * or delete.
 */
void
FastTransport::ServerRpc::maybeDequeue()
{
    if (session) {
        ServerReadyQueue* q = &session->transport->serverReadyQueue;
        if (readyQueueEntries.is_linked())
            q->erase(q->iterator_to(*this));
    }
}

/**
 * Initialize a ServerRpc to a ServerSession on a particular channel.
 *
 * \param session
 *      The ServerSession this RPC is associated with.
 * \param channelId
 *      The channel in session on which to handle this RPC.
 */
void
FastTransport::ServerRpc::setup(ServerSession* session,
                                uint8_t channelId)
{
    reset();
    this->session = session;
    this->channelId = channelId;
}

/**
 * Begin sending the RPC response.
 */
void
FastTransport::ServerRpc::sendReply()
{
    session->beginSending(channelId);
}

// --- PayloadChunk ---

/**
 * Append a subregion of payload data which releases the memory to the
 * Driver that allocated it when it's containing Buffer is destroyed.
 *
 * \param buffer
 *      The Buffer to append the data to.
 * \param data
 *      The address of the data to appear in the Buffer.  This must be
 *      inside the payload range specified later in the arguments.  The
 *      idea is that if there is some data at the front or end of the
 *      payload region that should be "stripped" before placing it in
 *      the Buffer that can be done here (i.e. Header).
 * \param dataLength
 *      The length in bytes of the region starting at data that is a
 *      subregion of the payload.
 * \param driver
 *      The Driver to release() this payload to on Buffer destruction.
 * \param payload
 *      The address to release() to the Driver on destruction.
 * \param payloadLength
 *      The length of the resources starting at payload to release.
 */
FastTransport::PayloadChunk*
FastTransport::PayloadChunk::prependToBuffer(Buffer* buffer,
                                             char* data,
                                             uint32_t dataLength,
                                             Driver* driver,
                                             char* payload,
                                             uint32_t payloadLength)
{
    PayloadChunk* chunk =
        new(buffer, CHUNK) PayloadChunk(data,
                                        dataLength,
                                        driver,
                                        payload,
                                        payloadLength);
    Buffer::Chunk::prependChunkToBuffer(buffer, chunk);
    return chunk;
}

/**
 * Prepend a subregion of payload data which releases the memory to the
 * Driver that allocated it when it's containing Buffer is destroyed.
 *
 * \param buffer
 *      The Buffer to prepend the data to.
 * \param data
 *      The address of the data to appear in the Buffer.  This must be
 *      inside the payload range specified later in the arguments.  The
 *      idea is that if there is some data at the front or end of the
 *      payload region that should be "stripped" before placing it in
 *      the Buffer that can be done here (i.e. Header).
 * \param dataLength
 *      The length in bytes of the region starting at data that is a
 *      subregion of the payload.
 * \param driver
 *      The Driver to release() this payload to on Buffer destruction.
 * \param payload
 *      The address to release() to the Driver on destruction.
 * \param payloadLength
 *      The length of the resources starting at payload to release.
 */
FastTransport::PayloadChunk*
FastTransport::PayloadChunk::appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            Driver* driver,
                                            char* payload,
                                            uint32_t payloadLength)
{
    PayloadChunk* chunk =
        new(buffer, CHUNK) PayloadChunk(data,
                                        dataLength,
                                        driver,
                                        payload,
                                        payloadLength);
    Buffer::Chunk::appendChunkToBuffer(buffer, chunk);
    return chunk;
}

/// Returns memory to the Driver once the Chunk is discarded.
FastTransport::PayloadChunk::~PayloadChunk()
{
    if (driver)
        driver->release(payload, payloadLength);
}

/**
 * Construct a PayloadChunk which will release it's resources to the
 * Driver that allocated it when it's containing Buffer is destroyed.
 *
 * \param data
 *      The address of the data to appear in the Buffer.  This must be
 *      inside the payload range specified later in the arguments.  The
 *      idea is that if there is some data at the front or end of the
 *      payload region that should be "stripped" before placing it in
 *      the Buffer that can be done here (i.e. Header).
 * \param dataLength
 *      The length in bytes of the region starting at data that is a
 *      subregion of the payload.
 * \param driver
 *      The Driver to release() this payload to on Buffer destruction.
 * \param payload
 *      The address to release() to the Driver on destruction.
 * \param payloadLength
 *      The length of the resources starting at payload to release.
 */
FastTransport::PayloadChunk::PayloadChunk(void* data,
                                          uint32_t dataLength,
                                          Driver* driver,
                                          char* const payload,
                                          uint32_t payloadLength)
    : Buffer::Chunk(data, dataLength)
    , driver(driver)
    , payload(payload)
    , payloadLength(payloadLength)
{
}

// --- InboundMessage ---

/**
 * Construct an InboundMessage which is NOT yet ready to use.
 *
 * NOTE: until setup() and init() have been called this instance
 * is not ready to receive fragments.
 */
FastTransport::InboundMessage::InboundMessage()
    : transport(NULL)
    , session(NULL)
    , channelId(0)
    , totalFrags(0)
    , firstMissingFrag(0)
    , dataStagingWindow()
    , dataBuffer(NULL)
    , timer(this)
{
    // The staging window always starts with the packet *after*
    // firstMissingFrag.
    dataStagingWindow.advance();
}

/**
 * Cleanup an InboundMessage, releasing any unaccounted for packet
 * data back to the Driver.
 */
FastTransport::InboundMessage::~InboundMessage()
{
    reset();
}

/**
 * One-time initialization that permanently attaches this instance to
 * a particular Session, channelId, and timer status.
 *
 * This method is necessary since the Channels in which they are contained
 * are allocated as an array (hence with the default constructor) requiring
 * additional post-constructor setup.
 *
 * \param transport
 *      The FastTranport this message is associated with.
 * \param session
 *      The Session belonging to this message.
 * \param channelId
 *      The ID of the channel this message belongs to.
 * \param useTimer
 *      Whether this message should respond to timer events.
 */
void
FastTransport::InboundMessage::setup(FastTransport* transport,
                                     Session* session,
                                     uint32_t channelId,
                                     bool useTimer)
{
    this->transport = transport;
    this->session = session;
    this->channelId = channelId;
    if (timer.useTimer)
        transport->removeTimer(&timer);
    timer.when = 0;
    timer.startTime = 0;
    timer.useTimer = useTimer;
}

/**
 * Creates and transmits an ACK decribing which fragments are still missing.
 */
void
FastTransport::InboundMessage::sendAck()
{
    Header header;
    session->fillHeader(&header, channelId);
    header.payloadType = Header::ACK;
    Buffer payloadBuffer;
    AckResponse *ackResponse =
        new(&payloadBuffer, APPEND) AckResponse(firstMissingFrag);
    for (uint32_t i = 0; i < dataStagingWindow.getLength(); i++) {
        std::pair<char*, uint32_t> elt =
            dataStagingWindow[firstMissingFrag + 1 + i];
        if (elt.first)
            ackResponse->stagingVector |= (1 << i);
    }
    Buffer::Iterator iter(payloadBuffer);
    transport->sendPacket(session->getAddress(), &header, &iter);
}

/**
 * Cleans up an InboundMessage and marks it inactive.
 *
 * A subsequent call to init() will set it up to be reused.  This call
 * also returns any memory held in the incoming window to the Driver and
 * removes any timer events associated with the message.
 */
void
FastTransport::InboundMessage::reset()
{
    for (uint32_t i = 0; i < dataStagingWindow.getLength(); i++) {
        std::pair<char*, uint32_t> elt =
            dataStagingWindow[firstMissingFrag + 1 + i];
        if (elt.first)
            transport->driver->release(elt.first, elt.second);
    }
    totalFrags = 0;
    dataStagingWindow.reset();
    dataStagingWindow.advance();
    firstMissingFrag = 0;
    dataBuffer = NULL;
    timer.startTime = 0;
    if (timer.useTimer)
         transport->removeTimer(&timer);
}

/**
 * Initialize a previously reset InboundMessage for use.
 *
 * This must be called before a previously inactive InboundMessage is ready
 * to receive fragments.
 *
 * \param totalFrags
 *      The total number of incoming fragments the message should expect.
 * \param[out] dataBuffer
 *      The buffer to fill with the data from this message.
 */
void
FastTransport::InboundMessage::init(uint16_t totalFrags,
                                    Buffer* dataBuffer)
{
    reset();
    this->totalFrags = totalFrags;
    this->dataBuffer = dataBuffer;
    if (timer.useTimer)
        transport->addTimer(&timer, rdtsc(), timeoutCycles());
}

/**
 * Take a single fragment and incorporate it as part of this message.
 * Additionally, send an ACK if this packet was marked with an ACK request.
 *
 * \param received
 *      A single fragment wrapped in a Driver::Received.
 * \return
 *      True if the full message has been received and the dataBuffer is now
 *      complete and valid.
 */
bool
FastTransport::InboundMessage::processReceivedData(Driver::Received* received)
{
    assert(received->len >= sizeof(Header));
    Header *header = reinterpret_cast<Header*>(received->payload);

    if (header->totalFrags != totalFrags) {
        // If the fragment header disagrees on the total length of the message
        // with the value set on init() the packet is ignored.
        LOG(WARNING, "header->totalFrags (%d) != totalFrags (%d)",
            header->totalFrags, totalFrags);
        return firstMissingFrag == totalFrags;
    }

    if (header->fragNumber == firstMissingFrag) {
        // If the fragNumber matches the firstMissingFrag of this message then
        // it is concatenated to the message's buffer along with any other
        // packets following this fragments in the dataStagingWindow that are
        // contiguous and have no other missing fragments preceding them in
        // the message.
        uint32_t length;
        // Take responsibility for returning the memory to the Driver.
        char *payload = received->steal(&length);
        // Give that responsibility to dataBuffer's destructor.
        PayloadChunk::appendToBuffer(dataBuffer,
                                     payload + sizeof(Header),
                                     length - sizeof(Header),
                                     transport->driver,
                                     payload,
                                     length);

        // Advance the staging window (and firstMissingFrag) to restore the
        // invariants:
        //  - firstMissingFrag refers to the first fragment we have not yet
        //    received.
        //  - the first slot in dataStagingWindow refers to the fragment
        //    *after* firstMissingFrag.
        while (true) {
            std::pair<char*, uint32_t> pair =
                dataStagingWindow[firstMissingFrag + 1];
            char* payload = pair.first;
            uint32_t length = pair.second;
            dataStagingWindow.advance();
            firstMissingFrag++;
            if (!payload)
                break;
            // Give that responsibility to dataBuffer's destructor, notice
            // this responsibility was stolen on a prior method invocation
            // in the else block below.
            PayloadChunk::appendToBuffer(dataBuffer,
                                         payload + sizeof(Header),
                                         length - sizeof(Header),
                                         transport->driver,
                                         payload,
                                         length);
        }
    } else if (header->fragNumber > firstMissingFrag) {
        // If the fragNumber exceeds the firstMissingFrag of this message then
        // it is stored in the dataStagingWindow to be appended by the above
        // block on a future call to this method.
        if ((header->fragNumber - firstMissingFrag) >
            MAX_STAGING_FRAGMENTS) {
            LOG(WARNING, "fragNumber %d out of range (last OK = %d)",
                header->fragNumber, firstMissingFrag + MAX_STAGING_FRAGMENTS);
        } else {
            if (!dataStagingWindow[header->fragNumber].first) {
                uint32_t length;
                // Take responsibility for returning the memory to the Driver.
                // Notice responsibilty for the memory will be transferred to
                // dataBuffer when this fragment is eventually appended on
                // a future method invocation in other half of this if-else.
                char* payload = received->steal(&length);
                dataStagingWindow[header->fragNumber] =
                    std::pair<char*, uint32_t>(payload, length);
            } else {
                LOG(WARNING, "duplicate fragment %d received",
                    header->fragNumber);
            }
        }
    } else { // header->fragNumber < firstMissingFrag
        // stale, no-op
    }

    if (header->requestAck)
        sendAck();
    if (timer.useTimer)
         transport->addTimer(&timer, rdtsc(), timeoutCycles());

    return firstMissingFrag == totalFrags;
}

// --- InboundMessage::Timer ---

/**
 * Used to declare one Timer per InboundMessage.  Otherwise unused.
 *
 * \param inboundMsg
 *      The InboundMessage this timer will close or send an ACK for
 *      when the timer trips.
 */
FastTransport::InboundMessage::Timer::Timer(InboundMessage* const inboundMsg)
    : useTimer(false)
    , inboundMsg(inboundMsg)
{
}

/**
 * If this message is taking too long then close it (timeoutCycles() *
 * TIMEOUTS_UNTIL_ABORTING), otherwise send an AckResponse.
 *
 * This prevents the connection from stalling if incoming fragments
 * with Header::requestAck set are lost.
 *
 * See FastTransport::Timer for general information on timers.
 */
void
FastTransport::InboundMessage::Timer::onTimerFired(uint64_t now)
{
    // NOTE: Kills the entire session because one message is stalled.
    //       We could make this less aggressive if we think they might recover.
    if (now - startTime > sessionTimeoutCycles()) {
        inboundMsg->session->close();
    } else {
        inboundMsg->transport->addTimer(this, now, timeoutCycles());
        inboundMsg->sendAck();
    }
}

// --- OutboundMessage ---

/**
 * Construct an OutboundMessage which is NOT yet ready to use.
 *
 * NOTE: until setup() has been called this instance
 * is not ready to send fragments.
 */
FastTransport::OutboundMessage::OutboundMessage()
    : transport(NULL)
    , session(NULL)
    , channelId(0)
    , sendBuffer(0)
    , firstMissingFrag(0)
    , totalFrags(0)
    , packetsSinceAckReq(0)
    , sentTimes()
    , numAcked(0)
    , timer(this)
{
}

FastTransport::OutboundMessage::~OutboundMessage()
{
    if (timer.useTimer)
        transport->removeTimer(&timer);
}

/**
 * One-time initialization that permanently attaches this instance to
 * a particular Session, channelId, and timer status.
 *
 * This method is necessary since the Channels in which they are contained
 * are allocated as an array (hence with the default constructor) requiring
 * additional post-constructor setup.
 *
 * \param transport
 *      The FastTranport this message is associated with.
 * \param session
 *      The Session this message is associated with.
 * \param channelId
 *      The ID of the channel this message belongs to.
 * \param useTimer
 *      Whether this message should respond to timer events.
 */
void
FastTransport::OutboundMessage::setup(FastTransport* transport,
                                      Session* session,
                                      uint32_t channelId,
                                      bool useTimer)
{
    this->transport = transport;
    this->session = session;
    this->channelId = channelId;
    reset();
    timer.useTimer = useTimer;
}

/**
 * Cleans up an OutboundMessage and marks it inactive.
 *
 * This must be called before an actively used instance can be recycled
 * by calling beginSending() on it.
 */
void
FastTransport::OutboundMessage::reset()
{
    sendBuffer = NULL;
    firstMissingFrag = 0;
    totalFrags = 0;
    packetsSinceAckReq = 0;
    sentTimes.reset();
    numAcked = 0;
    if (timer.useTimer)
        transport->removeTimer(&timer);
    timer.when = 0;
    timer.startTime = 0;
}

/**
 * Begin sending a buffer, sending as many fragments as permitted by
 * the protocol.  Requires the message to be inactive (clear() was
 * called on it).
 *
 * \param dataBuffer
 *      Buffer of data to send.
 */
void
FastTransport::OutboundMessage::beginSending(Buffer* dataBuffer)
{
    assert(!sendBuffer);
    sendBuffer = dataBuffer;
    totalFrags = transport->numFrags(sendBuffer);
    send();
}

/**
 * Send out additional data fragments and update timestamps/status in
 * sentTimes as much as permitted by the current state of the world.
 * This method is invoked by beginSending and then again later whenever
 * something has occurred that may permit additional data fragments
 * to be sent (such as the arrival of an ACK or the passage of time).
 *
 * Pre-conditions:
 *  - beginSending() must have been called since the last call to reset().
 */
void
FastTransport::OutboundMessage::send()
{
    /*
     * If a packet is retransmitted due to a timeout it is sent with a request
     * for ACK and no further packets are transmitted until the next event
     * (either an additional timeout or an ACK is processed).  If no packet
     * is retransmitted then the call will send as many fresh data packets as
     * the window allows with every REQ_ACK_AFTER th packet marked as request
     * for ACK.
     *
     * Side-effects:
     *  - sentTimes is updated to reflect any sent packets.
     *  - If timers are enabled for this message then the timer is scheduled
     *    to fire when the next packet retransmit timeout occurs.
     */
    uint64_t now = rdtsc();

    // First, decide on candidate range of packets to send/resend
    // Only fragments less than stop will be considered for (re-)send

    // Can't send beyond the last fragment
    uint32_t stop = totalFrags;
    // Can't send beyond the window
    stop = std::min(stop, numAcked + WINDOW_SIZE);
    // Can't send beyond what the receiver is willing to accept
    stop = std::min(stop, firstMissingFrag + MAX_STAGING_FRAGMENTS + 1);

    // Send frags from candidate range
    for (uint32_t fragNumber = firstMissingFrag; fragNumber < stop;
            fragNumber++) {
        uint64_t sentTime = sentTimes[fragNumber];
        // skip if ACKED or if already sent but not yet timed out
        if ((sentTime == ACKED) ||
            (sentTime != 0 && sentTime + timeoutCycles() >= now))
            continue;
        // isRetransmit if already sent and timed out (guaranteed by if above)
        bool isRetransmit = sentTime != 0;
        // requestAck if retransmit or
        // haven't asked for ack in awhile and this is not the last frag
        bool requestAck = isRetransmit ||
            (packetsSinceAckReq == REQ_ACK_AFTER - 1 &&
             fragNumber != totalFrags - 1);
        sendOneData(fragNumber, requestAck);
        sentTimes[fragNumber] = now;
        if (isRetransmit)
            break;
    }

    // find the packet that will cause timeout the earliest and
    // schedule a timer just after that
    if (timer.useTimer) {
        uint64_t oldest = ~(0lu);
        for (uint32_t fragNumber = firstMissingFrag; fragNumber < stop;
                fragNumber++) {
            uint64_t sentTime = sentTimes[fragNumber];
            // if we reach a not-sent, the rest must be not-sent
            if (!sentTime)
                break;
            if (sentTime != ACKED && sentTime > 0)
                if (sentTime < oldest)
                    oldest = sentTime;
        }
        if (oldest != ~(0lu))
            transport->addTimer(&timer, oldest, timeoutCycles());
    }
}

/**
 * Process an AckResponse and advance window, if possible.
 *
 * Notice this function calls send() to try to send additional fragments.
 *
 * Side-effects:
 *  - firstMissingFrag and sentTimes may advance.
 *  - fragments may be marked as ACKED.
 *  - send() may be freed up to send further packets.
 *
 * \param received
 *      Data received from a sender containing a packet header and a valid
 *      AckResponse payload.
 * \return
 *      true if the entire message is complete (has been acked).
 */
bool
FastTransport::OutboundMessage::processReceivedAck(Driver::Received* received)
{
    if (!sendBuffer)
        return false;

    if (received->len < sizeof(Header) + sizeof(AckResponse)) {
        LOG(WARNING, "ACK packet too short (%d bytes)", received->len);
        return false;
    }
    AckResponse *ack =
        received->getOffset<AckResponse>(sizeof(Header));

    if (ack->firstMissingFrag < firstMissingFrag) {
        LOG(WARNING, "stale ACK (ack->firstmissing: %d, firstMissingFrag: %d)",
            ack->firstMissingFrag, firstMissingFrag);
    } else if (ack->firstMissingFrag > totalFrags) {
        LOG(WARNING, "invalid ACK (firstMissingFrag %d > totalFrags %d)",
            ack->firstMissingFrag, totalFrags);
    } else if (ack->firstMissingFrag >
             (firstMissingFrag + sentTimes.getLength())) {
        LOG(WARNING, "invalid ACK (firstMissingFrag %d beyond end "
            "of window %d)", ack->firstMissingFrag,
            firstMissingFrag + sentTimes.getLength());
    } else {
        sentTimes.advance(ack->firstMissingFrag - firstMissingFrag);
        firstMissingFrag = ack->firstMissingFrag;
        numAcked = ack->firstMissingFrag;
        for (uint32_t i = 0; i < sentTimes.getLength() - 1; i++) {
            bool acked = (ack->stagingVector >> i) & 1;
            if (acked) {
                sentTimes[firstMissingFrag + i + 1] = ACKED;
                numAcked++;
            }
        }
    }
    send();
    return firstMissingFrag == totalFrags;
}

/**
 * Send out a single data fragment drawn from sendBuffer.
 *
 * \param fragNumber
 *      The fragment number to place in the packet header and which fragment
 *      of the sendBuffer to transmit.
 * \param requestAck
 *      The packet header will have the request ACK bit set.
 */
void
FastTransport::OutboundMessage::sendOneData(uint32_t fragNumber,
                                            bool requestAck)
{
    Header header;
    session->fillHeader(&header, channelId);
    header.fragNumber = fragNumber;
    header.totalFrags = totalFrags;
    header.requestAck = requestAck;
    header.payloadType = Header::DATA;
    uint32_t dataPerFragment = transport->dataPerFragment();
    Buffer::Iterator iter(*sendBuffer,
                          fragNumber * dataPerFragment,
                          dataPerFragment);
    transport->sendPacket(session->getAddress(), &header, &iter);

    if (requestAck)
        packetsSinceAckReq = 0;
    else
        packetsSinceAckReq++;
}

// -- OutboundMessage::Timer ---

/**
 * Used to declare one Timer per OutboundMessage.  Otherwise unused.
 *
 * \param outboundMsg
 *      The OutboundMessage this timer will close or resend fragments for
 *      when the timer trips.
 */
FastTransport::OutboundMessage::Timer::Timer(OutboundMessage* const outboundMsg)
    : useTimer(false)
    , outboundMsg(outboundMsg)
{
}

/**
 * If this message is taking too long then close it (timeoutCycles() *
 * TIMEOUTS_UNTIL_ABORTING), otherwise resend unacked packets
 * that were sent awhile ago.
 *
 * See FastTransport::Timer for general information on timers.
 */
void
FastTransport::OutboundMessage::Timer::onTimerFired(uint64_t now)
{
    if (now - startTime > sessionTimeoutCycles()) {
        LOG(DEBUG, "closing session due to timeout");
        outboundMsg->session->close();
    } else {
        outboundMsg->send();
    }
}

// -- Session ---

const uint64_t FastTransport::Session::INVALID_TOKEN = 0xcccccccccccccccclu;

// --- ServerSession ---

const uint32_t FastTransport::ServerSession::INVALID_HINT = 0xccccccccu;

/**
 * Create a session associated with a particular transport.
 *
 * \param transport
 *      The FastTransport with which this Session is associated.
 * \param sessionId
 *      This session's offset in FastTransport::serverSessionTable.  This
 *      is used as the serverSessionHint in packets to the client so the
 *      client can return the hint and get fast access to this ServerSession.
 */
FastTransport::ServerSession::ServerSession(FastTransport* transport,
                                            uint32_t sessionId)
    : Session(transport, sessionId)
    , nextFree(FastTransport::SessionTable<ServerSession>::NONE)
    , clientAddress()
    , clientSessionHint(INVALID_HINT)
    , lastActivityTime(0)
{
    for (uint32_t i = 0; i < NUM_CHANNELS_PER_SESSION; i++)
        channels[i].setup(transport, this, i);
}

FastTransport::ServerSession::~ServerSession()
{
    assert(expire());
}

/**
 * Switch from PROCESSING to SENDING_WAITING and initiate transfer of the
 * RPC response from the server to the client.
 *
 * Preconditions:
 *  - The caller must ensure that the indicated channel is PROCESSING.
 *
 * \param channelId
 *      Send the response on this channelId, which is currently PROCESSING.
 */
void
FastTransport::ServerSession::beginSending(uint8_t channelId)
{
    ServerChannel* channel = &channels[channelId];
    assert(channel->state == ServerChannel::PROCESSING);
    channel->state = ServerChannel::SENDING_WAITING;
    Buffer* responseBuffer = &channel->currentRpc.replyPayload;
    channel->outboundMsg.beginSending(responseBuffer);
    lastActivityTime = rdtsc();
}

/// This shouldn't ever be called.
void
FastTransport::ServerSession::close()
{
    LOG(WARNING, "should never be called");
}

// See Session::expire().
bool
FastTransport::ServerSession::expire()
{
    if (lastActivityTime == 0)
        return true;

    for (uint32_t i = 0; i < NUM_CHANNELS_PER_SESSION; i++) {
        if (channels[i].state == ServerChannel::PROCESSING)
            return false;
    }

    for (uint32_t i = 0; i < NUM_CHANNELS_PER_SESSION; i++) {
        if (channels[i].state == ServerChannel::IDLE)
            continue;
        channels[i].state = ServerChannel::IDLE;
        channels[i].rpcId = ~0U;
        channels[i].currentRpc.reset();
        channels[i].inboundMsg.reset();
        channels[i].outboundMsg.reset();
    }

    token = INVALID_TOKEN;
    clientSessionHint = INVALID_HINT;
    lastActivityTime = 0;
    clientAddress.reset();

    return true;
}

// See Session::fillHeader().
void
FastTransport::ServerSession::fillHeader(Header* const header,
                                         uint8_t channelId) const
{
    header->rpcId = channels[channelId].rpcId;
    header->channelId = channelId;
    header->direction = Header::SERVER_TO_CLIENT;
    header->clientSessionHint = clientSessionHint;
    header->serverSessionHint = id;
    header->sessionToken = token;
}

// See Session::getAddress().
const Driver::Address*
FastTransport::ServerSession::getAddress()
{
    return clientAddress.get();
}

// See Session::getLastActivityTime().
uint64_t
FastTransport::ServerSession::getLastActivityTime()
{
    return lastActivityTime;
}

/**
 * Dispatch an incoming packet to the correct action for this session.
 *
 * \param received
 *      A packet wrapped up in a Driver::Received.
 */
void
FastTransport::ServerSession::processInboundPacket(Driver::Received* received)
{
    lastActivityTime = rdtsc();
    Header* header = received->getOffset<Header>(0);
    if (header->channelId >= NUM_CHANNELS_PER_SESSION) {
        LOG(WARNING, "invalid channel id %d", header->channelId);
        return;
    }

    ServerChannel* channel = &channels[header->channelId];
    if (channel->rpcId == header->rpcId) {
        // Incoming packet is part of the current RPC
        switch (header->getPayloadType()) {
        case Header::DATA:
            TEST_LOG("processReceivedData");
            processReceivedData(channel, received);
            break;
        case Header::ACK:
            TEST_LOG("processReceivedAck");
            processReceivedAck(channel, received);
            break;
        default:
            LOG(WARNING, "current rpcId has bad packet type %d",
                header->getPayloadType());
        }
    } else if (channel->rpcId + 1 == header->rpcId) {
        TEST_LOG("start a new RPC");
        // Incoming packet is part of the next RPC
        // reset everything out and setup for next RPC
        switch (header->getPayloadType()) {
        case Header::DATA: {
            channel->state = ServerChannel::RECEIVING;
            channel->rpcId = header->rpcId;
            channel->inboundMsg.reset();
            channel->outboundMsg.reset();
            channel->currentRpc.setup(this, header->channelId);
            Buffer* recvBuffer = &channel->currentRpc.recvPayload;
            channel->inboundMsg.init(header->totalFrags, recvBuffer);
            TEST_LOG("processReceivedData");
            processReceivedData(channel, received);
            break;
        }
        default:
            LOG(WARNING, "new rpcId has bad type %d", header->getPayloadType());
        }
    } else {
        LOG(WARNING, "packet from old RPC (packet rpcId: %d, channel rpcId: %d",
            header->rpcId, channel->rpcId);
    }
}

/**
 * Create a new session and send the SessionOpenResponse to the client.
 *
 * \param clientAddress
 *      Address of the session initiator.
 * \param clientSessionHint
 *      Hint that the client uses to find informataion about this session;
 *      must be included in messages to the client.
 */
void
FastTransport::ServerSession::startSession(
                                       const Driver::Address* clientAddress,
                                       uint32_t clientSessionHint)
{
    this->clientAddress.reset(clientAddress->clone());
    this->clientSessionHint = clientSessionHint;
    token = ((generateRandom() << 32) | generateRandom());

    // send session open response
    Header header;
    header.direction = Header::SERVER_TO_CLIENT;
    header.clientSessionHint = clientSessionHint;
    header.serverSessionHint = id;
    header.sessionToken = token;
    header.rpcId = 0;
    header.channelId = 0;
    header.payloadType = Header::SESSION_OPEN;

    Buffer payload;
    SessionOpenResponse* sessionOpen;
    sessionOpen = new(&payload, APPEND) SessionOpenResponse;
    sessionOpen->numChannels = NUM_CHANNELS_PER_SESSION;
    Buffer::Iterator payloadIter(payload);
    transport->sendPacket(this->clientAddress.get(), &header, &payloadIter);
    lastActivityTime = rdtsc();
}

// - private -

/**
 * Process an ACK on a particular channel.
 *
 * Side-effect:
 *  - This may free some window and transmit more packets.
 *
 * \param channel
 *      The channel to process the ACK on.
 * \param received
 *      The ACK packet encapsulated in a Driver::Received.
 */
void
FastTransport::ServerSession::processReceivedAck(ServerChannel* channel,
                                                 Driver::Received* received)
{
    if (channel->state == ServerChannel::SENDING_WAITING)
        channel->outboundMsg.processReceivedAck(received);
}

/**
 * Process a data fragment on a particular channel.
 *
 * Routing the packet to the correct handler is a function of the current
 * state of the channel.
 *
 * Side-effect:
 *  - channel->state will transition from RECEIVING to PROCESSING if the
 *    full request has been received.
 *
 * \param channel
 *      The channel to process the data on.
 * \param received
 *      The data packet encapsulated in a Driver::Received.
 */
void
FastTransport::ServerSession::processReceivedData(ServerChannel* channel,
                                                  Driver::Received* received)
{
    Header* header = received->getOffset<Header>(0);
    switch (channel->state) {
    case ServerChannel::IDLE:
        LOG(WARNING, "data packet arrived for IDLE channel");
        break;
    case ServerChannel::RECEIVING:
        if (channel->inboundMsg.processReceivedData(received)) {
            transport->serverReadyQueue.push_back(channel->currentRpc);
            channel->state = ServerChannel::PROCESSING;
        }
        break;
    case ServerChannel::PROCESSING:
        if (header->requestAck)
            channel->inboundMsg.sendAck();
        break;
    case ServerChannel::SENDING_WAITING:
        /*
         * This is an extremely subtle and racy case.  This can happen when
         * the sender believes a fragment didn't make it to the receiver
         * and resends when in reality the receiver simply hasn't received
         * the earlier transmission.  With low timeouts this can occur
         * consistently when CPUs are contended because the kernel
         * scheduler has a rather long period.
         */
        if (received->len < sizeof(Header))
            LOG(DEBUG, "extraneous packet too small to contain Header");
        else
            LOG(DEBUG, "extraneous packet Header: %s",
                Header::headerToString(received->payload,
                                       sizeof(Header)).c_str());
        // Ignore the incoming packet and continue to send the response.
        // Hopefully this will appease the sender spamming us.
        channel->outboundMsg.send();
        break;
    }
}

// --- ClientSession ---

const uint32_t FastTransport::ClientSession::INVALID_HINT = 0xccccccccu;

/**
 * Create a session associated with a particular transport.
 *
 * \param transport
 *      The FastTransport with which this Session is associated.
 * \param sessionId
 *      This session's offset in FastTransport::serverSessionTable.  This
 *      is used as the serverSessionHint in packets to the client so the
 *      client can return the hint and get fast access to this ServerSession.
 */
FastTransport::ClientSession::ClientSession(FastTransport* transport,
                                            uint32_t sessionId)
    : FastTransport::Session(transport, sessionId)
    , nextFree(FastTransport::SessionTable<ClientSession>::NONE)
    , channels(0)
    , channelQueue()
    , lastActivityTime(0)
    , numChannels(0)
    , serverAddress()
    , serverSessionHint(INVALID_HINT)
    , timer(this)
{
}

FastTransport::ClientSession::~ClientSession()
{
    assert(expire());
}

// See Session::close().
void
FastTransport::ClientSession::close()
{
    LOG(DEBUG, "closing session");
    for (uint32_t i = 0; i < numChannels; i++) {
        if (channels[i].currentRpc)
            channels[i].currentRpc->abort();
    }
    ChannelQueue::iterator iter(channelQueue.begin());
    while (iter != channelQueue.end()) {
        ClientRpc& rpc(*iter);
        iter = channelQueue.erase(iter);
        rpc.abort();
    }
    resetChannels();
    serverSessionHint = INVALID_HINT;
    token = INVALID_TOKEN;
    if (timer.listEntries.is_linked())
        transport->removeTimer(&timer);
}

// See Transport::Session::clientSend().
FastTransport::ClientRpc*
FastTransport::ClientSession::clientSend(Buffer* request, Buffer* response)
{
    ClientRpc* rpc = new(request, MISC) ClientRpc(transport,
                                                  request, response);

    // rpc will be performed immediately on the first available channel or
    // queued until a channel is idle if none are currently available.
    lastActivityTime = rdtsc();
    if (!isConnected()) {
        connect();
        LOG(DEBUG, "queueing RPC");
        channelQueue.push_back(*rpc);
    } else {
        ClientChannel* channel = getAvailableChannel();
        if (!channel) {
            LOG(DEBUG, "queueing RPC");
            channelQueue.push_back(*rpc);
        } else {
            assert(channel->state == ClientChannel::IDLE);
            channel->state = ClientChannel::SENDING;
            channel->currentRpc = rpc;
            channel->outboundMsg.beginSending(rpc->requestBuffer);
        }
    }

    return rpc;
}

/**
 * Send a session open request if one isn't currently "in flight" to
 * serverAddress and establish an open ServerSession on the remote end.
 */
void
FastTransport::ClientSession::connect()
{
    if (timer.sessionOpenRequestInFlight)
        return;

    sendSessionOpenRequest();

    timer.start();
}

// See Session::expire().
bool
FastTransport::ClientSession::expire()
{
    if (refCount > 0)
        return false;
    for (uint32_t i = 0; i < numChannels; i++) {
        if (channels[i].currentRpc)
            return false;
    }
    if (!channelQueue.empty())
        return false;
    close();
    return true;
}

// See Session::fillHeader().
void
FastTransport::ClientSession::fillHeader(Header* const header,
                                         uint8_t channelId) const
{
    header->rpcId = channels[channelId].rpcId;
    header->channelId = channelId;
    header->direction = Header::CLIENT_TO_SERVER;
    header->clientSessionHint = id;
    header->serverSessionHint = serverSessionHint;
    header->sessionToken = token;
}

// See Session::getAddress().
const Driver::Address*
FastTransport::ClientSession::getAddress()
{
    return serverAddress.get();
}

// See Session::getLastActivityTime().
uint64_t
FastTransport::ClientSession::getLastActivityTime()
{
    return lastActivityTime;
}

/**
 * Set the remote address on a client session.
 */
void
FastTransport::ClientSession::init(const ServiceLocator& serviceLocator)
{
    serverAddress.reset(transport->driver->newAddress(serviceLocator));
}

/**
 * Return whether this session is currently connect to a remote endpoint.
 *
 * \return
 *      See method description.
 */
bool
FastTransport::ClientSession::isConnected()
{
    return (numChannels != 0);
}

/**
 * Dispatch an incoming packet to the correct action for this session.
 *
 * \param received
 *      A packet wrapped up in a Driver::Received.  The caller has
 *      checked that the packet matches this session and that it is
 *      a server-to-client packet.
 */
void
FastTransport::ClientSession::processInboundPacket(Driver::Received* received)
{
    lastActivityTime = rdtsc();
    Header* header = received->getOffset<Header>(0);
    if (header->channelId >= numChannels) {
        if (header->getPayloadType() == Header::SESSION_OPEN)
            processSessionOpenResponse(received);
        else
            LOG(WARNING, "invalid channel id %d", header->channelId);
        return;
    }

    ClientChannel* channel = &channels[header->channelId];
    if (channel->rpcId == header->rpcId) {
        switch (header->getPayloadType()) {
        case Header::DATA:
            processReceivedData(channel, received);
            break;
        case Header::ACK:
            processReceivedAck(channel, received);
            break;
        case Header::BAD_SESSION:
            // The server does not believe it has a matching session
            // (perhaps it rebooted?).  Requeue any current RPCs and
            // try to reconnect.
            for (uint32_t i = 0; i < numChannels; i++) {
                if (channels[i].currentRpc)
                    channelQueue.push_back(*channels[i].currentRpc);
            }
            resetChannels();
            serverSessionHint = INVALID_HINT;
            token = INVALID_TOKEN;
            connect();
            break;
        default:
            LOG(WARNING, "bad payload type %d", header->getPayloadType());
        }
    } else {
        if (header->getPayloadType() == Header::DATA &&
            header->requestAck) {
            LOG(WARNING, "TODO: fake a full ACK response");
        } else {
            LOG(WARNING, "out-of-order packet (got rpcId %d, "
                "current rpcId %d)", header->rpcId, channel->rpcId);
        }
    }
}

/**
 * Send a SessionOpenRequest packet.
 */
void
FastTransport::ClientSession::sendSessionOpenRequest()
{
    Header header;
    header.direction = Header::CLIENT_TO_SERVER;
    header.clientSessionHint = id;
    header.serverSessionHint = serverSessionHint;
    header.sessionToken = token;
    header.rpcId = 0;
    header.channelId = 0;
    header.requestAck = 0;
    header.payloadType = Header::SESSION_OPEN;
    transport->sendPacket(serverAddress.get(),
                          &header, NULL);

    // Schedule the timer to resend if no response.
    uint64_t now = rdtsc();
    transport->addTimer(&timer, now, timeoutCycles());
    lastActivityTime = now;
}

// - private -

/**
 * Allocates numChannels worth of Channels in this Session.
 *
 * This is separated out so that testing methods can allocate Channels
 * without having to mock out a SessionOpenResponse.
 */
void
FastTransport::ClientSession::allocateChannels()
{
    channels = new ClientChannel[numChannels];
    for (uint32_t i = 0; i < numChannels; i++)
        channels[i].setup(transport, this, i);
}

/**
 * Reset this session to 0 channels and free associated resources.
 */
void
FastTransport::ClientSession::resetChannels()
{
    numChannels = 0;
    delete[] channels;
    channels = 0;
}

/**
 * Return an IDLE channel which can be used to service an RPC or 0
 * if none are IDLE.
 *
 * \return
 *      See method description.
 */
FastTransport::ClientSession::ClientChannel*
FastTransport::ClientSession::getAvailableChannel()
{
    for (uint32_t i = 0; i < numChannels; i++) {
        if (channels[i].state == ClientChannel::IDLE)
            return &channels[i];
    }
    return 0;
}

/**
 * Process an ACK on a particular channel.
 *
 * Side-effect:
 *  - This may free some window and transmit more packets.
 *
 * \param channel
 *      The channel to process the ACK on.
 * \param received
 *      The ACK packet encapsulated in a Driver::Received.
 */
void
FastTransport::ClientSession::processReceivedAck(ClientChannel* channel,
                                                 Driver::Received* received)
{
    if (channel->state == ClientChannel::SENDING)
        channel->outboundMsg.processReceivedAck(received);
}

/**
 * Process a data fragment on a particular channel.
 *
 * Side-effects:
 *  - If data is received while the channel is SENDING it transitions to
 *    RECEIVING.
 *  - If the channel completes its RPC it goes onto the available channel
 *    queue.
 *
 * \param channel
 *      The channel to process the ACK on.
 * \param received
 *      The data packet encapsulated in a Driver::Received.  The caller
 *      must have verified that the packet belongs to the current RPC for
 *      channel and that it is a data packet.
 */
void
FastTransport::ClientSession::processReceivedData(ClientChannel* channel,
                                                  Driver::Received* received)
{
    Header* header = received->getOffset<Header>(0);
    // Discard if idle
    if (channel->state == ClientChannel::IDLE) {
        LOG(WARNING, "packet arrived on IDLE channel (rpcId %d)",
            header->rpcId);
        return;
    }
    // If sending end sending and start receiving
    if (channel->state == ClientChannel::SENDING) {
        channel->outboundMsg.reset();
        channel->inboundMsg.init(header->totalFrags,
                                 channel->currentRpc->responseBuffer);
        channel->state = ClientChannel::RECEIVING;
    }
    if (channel->inboundMsg.processReceivedData(received)) {
        // InboundMsg has gotten its last fragment
        channel->currentRpc->complete();
        channel->rpcId += 1;
        channel->outboundMsg.reset();
        channel->inboundMsg.reset();
        if (channelQueue.empty()) {
            channel->state = ClientChannel::IDLE;
            channel->currentRpc = 0;
        } else {
            ClientRpc* rpc = &channelQueue.front();
            channelQueue.pop_front();
            channel->state = ClientChannel::SENDING;
            channel->currentRpc = rpc;
            channel->outboundMsg.beginSending(rpc->requestBuffer);
        }
    }
}

/**
 * Establishes a connected session and begins any queued RPCs on as many
 * channels as are available.
 *
 * \param received
 *      A Driver::Received wrapping an RPC packet with a SessionOpenResponse
 *      contained within.
 */
void
FastTransport::ClientSession::processSessionOpenResponse(
        Driver::Received* received)
{
    if (numChannels > 0)
        return;

    // Clear the SessionOpenRequest retransmit timer
    if (timer.listEntries.is_linked())
        transport->removeTimer(&timer);

    Header* header = received->getOffset<Header>(0);
    SessionOpenResponse* response =
        received->getOffset<SessionOpenResponse>(sizeof(*header));
    serverSessionHint = header->serverSessionHint;
    token = header->sessionToken;
    LOG(DEBUG, "response numChannels: %u", response->numChannels);
    numChannels = response->numChannels;
    if (MAX_NUM_CHANNELS_PER_SESSION < numChannels)
        numChannels = MAX_NUM_CHANNELS_PER_SESSION;
    LOG(DEBUG, "session open response: numChannels: %u", numChannels);
    allocateChannels();
    for (uint32_t i = 0; i < numChannels; i++) {
        if (channelQueue.empty())
            break;
        LOG(DEBUG, "assigned RPC to channel: %u", i);
        ClientRpc* rpc = &channelQueue.front();
        channelQueue.pop_front();
        channels[i].state = ClientChannel::SENDING;
        channels[i].currentRpc = rpc;
        channels[i].outboundMsg.beginSending(rpc->requestBuffer);
    }
}

// --- ClientSession::Timer ---
FastTransport::ClientSession::Timer::Timer(ClientSession* session)
    : session(session)
    , sessionOpenRequestInFlight(false)
{
}

void
FastTransport::ClientSession::Timer::onTimerFired(uint64_t now)
{
    if (now - startTime > sessionTimeoutCycles()) {
        sessionOpenRequestInFlight = false;
        session->close();
    } else {
        session->sendSessionOpenRequest();
    }
}

void
FastTransport::ClientSession::Timer::start()
{
    sessionOpenRequestInFlight = true;
    startTime = rdtsc();
}

} // end RAMCloud
