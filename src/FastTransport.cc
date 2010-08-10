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
 * Implementation of #RAMCloud::FastTransport.
 */

#include "FastTransport.h"
#include "MockDriver.h"

namespace RAMCloud {

/**
 * Create a FastTransport attached to a particular Driver
 *
 * \param driver
 *      The driver over which packets should be sent.
 */
FastTransport::FastTransport(Driver* driver)
    : driver(driver),
      serverSessions(this),
      clientSessions(this),
      serverReadyQueue(),
      timerList()
{
    TAILQ_INIT(&serverReadyQueue);
    LIST_INIT(&timerList);
}

// TODO(stutsman) Why is this public?
void
FastTransport::poll()
{
    while (tryProcessPacket())
        fireTimers();
    fireTimers();
}

// TODO(stutsman) What should the actual interface be for sessions?
FastTransport::ClientSession*
FastTransport::getClientSession()
{
    clientSessions.expire();
    return clientSessions.get();
}

// See Transport::clientSend().
FastTransport::ClientRPC*
FastTransport::clientSend(Service* service,
                          Buffer* request,
                          Buffer* response)
{
    ClientRPC* rpc = new(request, MISC) ClientRPC(this, service,
                                                  request, response);
    rpc->start();
    return rpc;
}

FastTransport::ServerRPC*
FastTransport::serverRecv()
{
    ServerRPC* rpc;
    while ((rpc = TAILQ_FIRST(&serverReadyQueue)) == NULL)
        poll();
    TAILQ_REMOVE(&serverReadyQueue, rpc, readyQueueEntries);
    return rpc;
}

bool
FastTransport::tryProcessPacket()
{
    Driver::Received received;
    if (!driver->tryRecvPacket(&received))
        return false;

    Header* header = received.getOffset<Header>(0);
    if (header == NULL) {
        LOG(DEBUG, "packet too small");
        return true;
    }
    if (header->pleaseDrop)
        return true;

    if (header->getDirection() == Header::CLIENT_TO_SERVER) {
        if (header->serverSessionHint < serverSessions.size()) {
            ServerSession* session = serverSessions[header->serverSessionHint];
            if (session->getToken() == header->sessionToken) {
                session->processInboundPacket(&received);
                return true;
            } else {
                LOG(DEBUG, "bad token");
            }
        }
        switch (header->getPayloadType()) {
        case Header::SESSION_OPEN: {
            LOG(DEBUG, "session open");
            serverSessions.expire();
            ServerSession* session = serverSessions.get();
            session->startSession(&received.addr,
                                  received.addrlen,
                                  header->clientSessionHint);
            break;
        }
        default: {
            LOG(DEBUG, "bad session");
            Header replyHeader;
            replyHeader.sessionToken = header->sessionToken;
            replyHeader.rpcId = header->rpcId;
            replyHeader.clientSessionHint = header->clientSessionHint;
            replyHeader.serverSessionHint = header->serverSessionHint;
            replyHeader.channelId = header->channelId;
            replyHeader.payloadType = Header::BAD_SESSION;
            replyHeader.direction = Header::SERVER_TO_CLIENT;
            sendPacket(&received.addr, received.addrlen,
                       &replyHeader, NULL);
            break;
        }
        }
    } else {
        if (header->clientSessionHint < clientSessions.size()) {
            ClientSession* session = clientSessions[header->clientSessionHint];
            session->processInboundPacket(&received);
        } else {
            LOG(DEBUG, "Bad client session hint");
        }
    }
    return true;
}

void
FastTransport::addTimer(Timer* timer, uint64_t when)
{
    timer->when = when;
    if (!LIST_IS_IN(timer, listEntries)) {
        LIST_INSERT_HEAD(&timerList, timer, listEntries);
    }
}

void
FastTransport::removeTimer(Timer* timer)
{
    timer->when = 0;
    if (LIST_IS_IN(timer, listEntries)) {
        LIST_REMOVE(timer, listEntries);
    }
}

void
FastTransport::fireTimers()
{
    uint64_t now = rdtsc();
    Timer* timer;
    LIST_FOREACH(timer, &timerList, listEntries) {
        if (timer->when && timer-> when < now) {
            //LOG(DEBUG, "Timer fired: %lu", now - timer->when);
            timer->fireTimer(now);
            if (timer->when < now) {
                timer->when = 0;
                removeTimer(timer);
            }
       }
    }
}

void
FastTransport::sendPacket(const sockaddr* address,
           socklen_t addressLength,
           Header* header,
           Buffer::Iterator* payload)
{
    header->pleaseDrop = (random() % 100) < PACKET_LOSS_PERCENTAGE;
    driver->sendPacket(address, addressLength,
                       header, sizeof(*header),
                       payload);
}

uint32_t
FastTransport::dataPerFragment()
{
    return driver->getMaxPayloadSize() - sizeof(Header);
}

uint32_t
FastTransport::numFrags(const Buffer* dataBuffer)
{
    return ((dataBuffer->getTotalLength() + dataPerFragment() - 1) /
             dataPerFragment());
}

// --- ClientRPC ---

FastTransport::ClientRPC::ClientRPC(FastTransport* transport,
                                    Service* service,
                                    Buffer* request,
                                    Buffer* response)
    : requestBuffer(request),
      responseBuffer(response),
      state(IDLE),
      transport(transport),
      service(service),
      serverAddress(),
      serverAddressLen(0),
      channelQueueEntries()
{
    sockaddr_in *addr = const_cast<sockaddr_in*>(
        reinterpret_cast<const sockaddr_in*>(&serverAddress));
    addr->sin_family = AF_INET;
    addr->sin_port = htons(service->getPort());
    if (inet_aton(service->getIp(), &addr->sin_addr) == 0)
        throw Exception("inet_aton failed");
    serverAddressLen = sizeof(*addr);
}

void
FastTransport::ClientRPC::getReply()
{
    while (true) {
        switch (state) {
        case IDLE:
            assert(0);
        case IN_PROGRESS:
        default:
            transport->poll();
            break;
        case COMPLETED:
            return;
        case ABORTED:
            throw TransportException("RPC aborted");
        }
    }
}

void
FastTransport::ClientRPC::aborted()
{
    state = ABORTED;
}

void
FastTransport::ClientRPC::completed()
{
    state = COMPLETED;
}

void
FastTransport::ClientRPC::start()
{
    assert(state == IDLE);
    state = IN_PROGRESS;
    ClientSession* session =
        static_cast<ClientSession*>(service->getSession());
    if (!session)
        session = transport->clientSessions.get();
    if (!session->isConnected())
        session->connect(&serverAddress, serverAddressLen);
    service->setSession(session);
    LOG(DEBUG, "Using session id %u", session->getId());
    session->startRpc(this);
}

// --- ServerRPC ---

FastTransport::ServerRPC::ServerRPC(ServerSession* session,
                                    uint8_t channelId)
    : session(session),
      channelId(channelId),
      readyQueueEntries()
{
}

void
FastTransport::ServerRPC::sendReply()
{
    session->beginSending(channelId);
    // don't forget to delete(this) eventually
}

// --- PayloadChunk ---

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

FastTransport::PayloadChunk::~PayloadChunk()
{
    if (driver)
        driver->release(payload, payloadLength);
}

FastTransport::PayloadChunk::PayloadChunk(void* data,
                                          uint32_t dataLength,
                                          Driver* driver,
                                          char* const payload,
                                          uint32_t payloadLength)
    : Buffer::Chunk(data, dataLength),
      driver(driver),
      payload(payload),
      payloadLength(payloadLength)
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
    : transport(0),
      session(0),
      channelId(0),
      totalFrags(0),
      firstMissingFrag(0),
      dataStagingRing(),
      dataBuffer(NULL),
      timer(false, this)
{
}

/**
 * Cleanup an InboundMessage, releasing any unaccounted for packet
 * data back to the Driver.
 */
FastTransport::InboundMessage::~InboundMessage()
{
    clear();
}

/**
 * One-time initialization that permanently attaches this instance to
 * a particular Session, channelId, and timer status.
 *
 * This method is necessary since the Channels in which they are contained
 * are allocated as an arrray (hence with the default constructor) requiring
 * additional post-constructor setup.
 *
 * \param session
 *      The Session belonging to this message.
 * \param channelId
 *      The ID of the channel this message belongs to.
 * \param useTimer
 *      Whether this message should respond to timer events.
 */
void
FastTransport::InboundMessage::setup(Session* session,
                                     uint32_t channelId,
                                     bool useTimer)
{
    this->session = session;
    this->transport = session->transport;
    this->channelId = channelId;
    if (timer.useTimer)
        transport->removeTimer(&timer);
    timer.when = 0;
    timer.numTimeouts = 0;
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
    for (uint32_t i = 0; i < dataStagingRing.getLength(); i++) {
        std::pair<char*, uint32_t> elt = dataStagingRing[i];
        if (elt.first)
            ackResponse->stagingVector |= (1 << i);
    }
    socklen_t addrlen;
    const sockaddr* addr = session->getAddress(&addrlen);
    Buffer::Iterator iter(payloadBuffer);
    transport->sendPacket(addr,
                          addrlen,
                          &header,
                          &iter);
}

/**
 * Cleans up an InboundMessage and marks it inactive.
 *
 * A subsequent call to init() will set it up to be reused.  This call
 * also returns any memory held in the incoming window to the Driver and
 * removes any timer events associated with the message.
 */
void
FastTransport::InboundMessage::clear()
{
    totalFrags = 0;
    firstMissingFrag = 0;
    dataBuffer = NULL;
    for (uint32_t i = 0; i < dataStagingRing.getLength(); i++) {
        std::pair<char*, uint32_t> elt = dataStagingRing[i];
        if (elt.first)
            transport->driver->release(elt.first, elt.second);
    }
    dataStagingRing.clear();
    timer.numTimeouts = 0;
    if (timer.useTimer)
         transport->removeTimer(&timer);
}

/**
 * Initialize a previously cleared InboundMessage for use.
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
    clear();
    this->totalFrags = totalFrags;
    this->dataBuffer = dataBuffer;
    if (timer.useTimer)
        transport->addTimer(&timer, rdtsc() + TIMEOUT_NS);
}

/**
 * Take a single fragment and incorporate it as part of this message.
 *
 * If the fragment header disagrees on the total length of the message
 * with the value set on init() the packet is ignored.
 *
 * If the fragNumber matches the firstMissingFrag of this message then
 * it is concatenated to the message's buffer along with any other packets
 * following this fragments in the dataStagingRing that are contiguous and
 * have no other missing fragments preceding them in the message.
 *
 * If the fragNumber exceeds the firstMissingFrag of this message then
 * it is stored in the dataStagingRing to be appended according to the
 * case outlined in the paragraph above.
 *
 * Any packet data that will be returned as part of the Buffer is "stolen"
 * from the Driver::Received object, and the Buffer's Chunk object is
 * responsible for returning the memory to the Driver later.  Any packet data
 * that goes unused (that which is not stolen) will be returned to the driver
 * when the Driver::Received is deleted.
 *
 * This code also notices if the incoming fragment has sendAck set.  If it
 * does then sendAck is called directly after handling this packet as above.
 *
 * \param received
 *      A single fragment wrapped in a Driver::Received.
 * \return
 *      Whether the full message has been received and the dataBuffer is now
 *      complete and valid.
 */
bool
FastTransport::InboundMessage::processReceivedData(Driver::Received* received)
{
    assert(received->len >= sizeof(Header));
    Header *header = reinterpret_cast<Header*>(received->payload);

    if (header->totalFrags != totalFrags) {
        // What's wrong with the other end?
        LOG(DEBUG, "header->totalFrags != totalFrags");
        return firstMissingFrag == totalFrags;
    }

    if (header->fragNumber == firstMissingFrag) {
        uint32_t length;
        char *payload = received->steal(&length);
        PayloadChunk::appendToBuffer(dataBuffer,
                                     payload + sizeof(Header),
                                     length - sizeof(Header),
                                     transport->driver,
                                     payload,
                                     length);
        firstMissingFrag++;
        while (true) {
            // TODO ring serializer for unit test assertions
            std::pair<char*, uint32_t> pair = dataStagingRing[0];
            char* payload = pair.first;
            uint32_t length = pair.second;
            dataStagingRing.advance(1);
            if (!payload)
                break;
            PayloadChunk::appendToBuffer(dataBuffer,
                                         payload + sizeof(Header),
                                         length - sizeof(Header),
                                         transport->driver,
                                         payload,
                                         length);
            firstMissingFrag++;
        }
    } else if (header->fragNumber > firstMissingFrag) {
        if ((header->fragNumber - firstMissingFrag) >
            MAX_STAGING_FRAGMENTS) {
            LOG(DEBUG, "fragNumber too big");
        } else {
            uint32_t i = header->fragNumber - firstMissingFrag - 1;
            if (!dataStagingRing[i].first) {
                uint32_t length;
                char* payload = received->steal(&length);
                dataStagingRing[i] =
                    std::pair<char*, uint32_t>(payload, length);
            } else {
                LOG(DEBUG, "duplicate fragment %d received",
                    header->fragNumber);
            }
        }
    } else { // header->fragNumber < firstMissingFrag
        // stale, no-op
    }

    // TODO(ongaro): Have caller call this->sendAck() instead.
    if (header->requestAck)
        sendAck();
    if (timer.useTimer)
         transport->addTimer(&timer, rdtsc() + TIMEOUT_NS);

    return firstMissingFrag == totalFrags;
}

// --- OutboundMessage ---
FastTransport::OutboundMessage::OutboundMessage()
    : transport(0),
      session(0),
      channelId(0),
      sendBuffer(0),
      firstMissingFrag(0),
      totalFrags(0),
      packetsSinceAckReq(0),
      sentTimes(),
      numAcked(0),
      timer(false, this)
{
}

void
FastTransport::OutboundMessage::setup(Session* session,
                                      uint32_t channelId,
                                      bool useTimer)
{
    this->session = session;
    this->transport = session->transport;
    this->channelId = channelId;
    clear();
    timer.useTimer = useTimer;
}

void
FastTransport::OutboundMessage::clear()
{
    sendBuffer = NULL;
    firstMissingFrag = 0;
    totalFrags = 0;
    packetsSinceAckReq = 0;
    sentTimes.clear();
    numAcked = 0;
    if (timer.useTimer)
        transport->removeTimer(&timer);
    timer.when = 0;
    timer.numTimeouts = 0;
}

void
FastTransport::OutboundMessage::beginSending(Buffer* dataBuffer)
{
    assert(!sendBuffer);
    sendBuffer = dataBuffer;
    totalFrags = transport->numFrags(sendBuffer);
    send();
}

void
FastTransport::OutboundMessage::send()
{
    if (!sendBuffer)
        return;

    uint64_t now = rdtsc();

    // number of fragments to be sent
    uint32_t sendCount = 0;

    // whether any of the fragments are being sent because they are
    // expired
    bool forceAck = false;

    // the fragment number of the last fragment to be sent
    uint32_t lastToSend = ~0;

    // can't send beyond the last fragment
    uint32_t stop = totalFrags;
    // can't send beyond the window
    stop = std::min(stop, numAcked + WINDOW_SIZE);
    // can't send beyond what the receiver is willing to accept
    stop = std::min(stop, firstMissingFrag + MAX_STAGING_FRAGMENTS + 1);

    // Figure out which frags to send;
    // flag them with sentTime of TO_SEND
    for (uint32_t fragNumber = firstMissingFrag;
         fragNumber < stop;
         fragNumber++) {
        uint32_t i = fragNumber - firstMissingFrag;
        uint64_t sentTime = sentTimes[i];
        if (sentTime == 0) {
            sentTimes[i] = TO_SEND;
            sendCount++;
            lastToSend = fragNumber;
        } else if (sentTime != ACKED && sentTime + TIMEOUT_NS < now) {
            forceAck = true;
            sentTimes[i] = TO_SEND;
            sendCount++;
            lastToSend = fragNumber;
        }
    }

    forceAck =
        forceAck &&
            (packetsSinceAckReq + sendCount < REQ_ACK_AFTER - 1 ||
             lastToSend == totalFrags);

    // Send the fragments
    for (uint32_t i = 0; i < sentTimes.getLength(); i++) {
        uint64_t sentTime = sentTimes[i];
        if (sentTime == TO_SEND) {
            uint32_t fragNumber = firstMissingFrag + i;
            sendOneData(fragNumber,
                        (forceAck && lastToSend == fragNumber));
        }
    }

    // Update sentTimes
    now = rdtsc();
    for (uint32_t i = 0; i < sentTimes.getLength(); i++) {
        uint64_t sentTime = sentTimes[i];
        if (sentTime == TO_SEND)
            sentTimes[i] = now;
    }

    if (timer.useTimer) {
        uint64_t oldest = ~(0lu);
        for (uint32_t i = 0; i < sentTimes.getLength(); i++) {
            uint64_t sentTime = sentTimes[i];
            if (sentTime != ACKED && sentTime > 0)
                if (sentTime < oldest)
                    oldest = sentTime;
        }
        if (oldest != ~(0lu))
            transport->addTimer(&timer, oldest + TIMEOUT_NS);
    }

}

bool
FastTransport::OutboundMessage::processReceivedAck(Driver::Received* received)
{
    if (!sendBuffer)
        return false;
    //LOG(DEBUG, "OutboundMessage processReceivedAck");

    assert(received->len >= sizeof(Header) + sizeof(AckResponse));
    AckResponse *ack =
        received->getOffset<AckResponse>(sizeof(Header));

    if (ack->firstMissingFrag < firstMissingFrag) {
        LOG(DEBUG, "OutboundMessage dropped stale ACK");
    } else if (ack->firstMissingFrag > totalFrags) {
        LOG(DEBUG, "OutboundMessage dropped invalid ACK"
                   "(shouldn't happen)");
    } else if (ack->firstMissingFrag >
             (firstMissingFrag + sentTimes.getLength())) {
        LOG(DEBUG, "OutboundMessage dropped ACK that advanced too far "
                   "(shouldn't happen)");
    } else {
        //LOG(DEBUG, "OutboundMessage legitimate ACK - take action: "
                   //"ack fmf %u", ack->firstMissingFrag);
        sentTimes.advance(ack->firstMissingFrag - firstMissingFrag);
        firstMissingFrag = ack->firstMissingFrag;
        numAcked = ack->firstMissingFrag;
        for (uint32_t i = 0; i < sentTimes.getLength() - 1; i++) {
            bool acked = (ack->stagingVector >> i) & 1;
            if (acked) {
                //LOG(DEBUG, "Now acked: %d", firstMissingFrag + i + 1);
                sentTimes[i + 1] = ACKED;
                numAcked++;
            } else {
                //LOG(DEBUG, "Not acked still: %d", firstMissingFrag + i + 1);
            }
        }
    }
    send();
    return firstMissingFrag == totalFrags;
}

void
FastTransport::OutboundMessage::sendOneData(uint32_t fragNumber,
                                            bool forceRequestAck)
{
    bool requestAck =
        forceRequestAck ||
        (packetsSinceAckReq == REQ_ACK_AFTER - 1 &&
         fragNumber != totalFrags - 1);

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

    socklen_t addrlen;
    const sockaddr* addr = session->getAddress(&addrlen);
    transport->sendPacket(addr, addrlen, &header, &iter);

    if (requestAck)
        packetsSinceAckReq = 0;
    else
        packetsSinceAckReq++;
}

// --- ServerSession ---

FastTransport::ServerSession::ServerSession(FastTransport* transport,
                                            uint32_t sessionId)
    : Session(transport),
      token(0xcccccccccccccccc),
      clientSessionHint(0xcccccccc),
      lastActivityTime(0),
      clientAddress(),
      clientAddressLen(0),
      id(sessionId),
      nextFree(FastTransport::SessionTable<ServerSession>::NONE)
{
    memset(&clientAddress, 0xcc, sizeof(clientAddress));
    for (uint32_t i = 0; i < NUM_CHANNELS_PER_SESSION; i++)
        channels[i].setup(this, i);
}

uint64_t
FastTransport::ServerSession::getToken()
{
    return token;
}

const sockaddr*
FastTransport::ServerSession::getAddress(socklen_t *len)
{
    *len = clientAddressLen;
    return &clientAddress;
}

uint64_t
FastTransport::ServerSession::getLastActivityTime()
{
    return lastActivityTime;
}

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

void
FastTransport::ServerSession::startSession(const sockaddr *clientAddress,
                                           socklen_t clientAddressLen,
                                           uint32_t clientSessionHint)
{
    assert(sizeof(this->clientAddress) >= clientAddressLen);
    memcpy(&this->clientAddress, clientAddress, clientAddressLen);
    this->clientAddressLen = clientAddressLen;
    this->clientSessionHint = clientSessionHint;
    token = ((random() << 32) | random());

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
    sessionOpen->maxChannelId = NUM_CHANNELS_PER_SESSION - 1;
    Buffer::Iterator payloadIter(payload);
    transport->sendPacket(&this->clientAddress, this->clientAddressLen,
                          &header, &payloadIter);
    lastActivityTime = rdtsc();
}

void
FastTransport::ServerSession::processInboundPacket(Driver::Received* received)
{
    lastActivityTime = rdtsc();
    Header* header = received->getOffset<Header>(0);
    if (header->channelId >= NUM_CHANNELS_PER_SESSION) {
        LOG(DEBUG, "drop due to invalid channel");
        return;
    }

    ServerChannel* channel = &channels[header->channelId];
    if (channel->rpcId == header->rpcId) {
        switch (header->getPayloadType()) {
        case Header::DATA:
            processReceivedData(channel, received);
            break;
        case Header::ACK:
            processReceivedAck(channel, received);
            break;
        default:
            LOG(DEBUG, "drop current rpcId with bad type");
        }
    } else if (channel->rpcId + 1 == header->rpcId) {
        switch (header->getPayloadType()) {
        case Header::DATA: {
            channel->state = ServerChannel::RECEIVING;
            channel->rpcId = header->rpcId;
            channel->inboundMsg.clear();
            channel->outboundMsg.clear();
            delete channel->currentRpc;
            channel->currentRpc = new ServerRPC(this,
                                                header->channelId);
            Buffer* recvBuffer = &channel->currentRpc->recvPayload;
            channel->inboundMsg.init(header->totalFrags, recvBuffer);
            processReceivedData(channel, received);
            break;
        }
        default:
            LOG(DEBUG, "drop new rpcId with bad type");
        }
    } else {
        LOG(DEBUG, "drop old packet");
    }
}

void
FastTransport::ServerSession::beginSending(uint8_t channelId)
{
    ServerChannel* channel = &channels[channelId];
    assert(channel->state == ServerChannel::PROCESSING);
    channel->state = ServerChannel::SENDING_WAITING;
    Buffer* responseBuffer = &channel->currentRpc->replyPayload;
    channel->outboundMsg.beginSending(responseBuffer);
    lastActivityTime = rdtsc();
}

void
FastTransport::ServerSession::close()
{
    LOG(WARNING, "ServerSession::close should never be called");
}

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
        delete channels[i].currentRpc;
        channels[i].currentRpc = NULL;
        channels[i].inboundMsg.clear();
        channels[i].outboundMsg.clear();
    }

    token = 0xcccccccccccccccc;
    clientSessionHint = 0xcccccccc;
    lastActivityTime = 0;
    memset(&clientAddress, 0xcc, sizeof(clientAddress));

    return true;
}

void
FastTransport::ServerSession::processReceivedData(ServerChannel* channel,
                                                  Driver::Received* received)
{
    Header* header = received->getOffset<Header>(0);
    switch (channel->state) {
    case ServerChannel::IDLE:
        break;
    case ServerChannel::RECEIVING:
        if (channel->inboundMsg.processReceivedData(received)) {
            TAILQ_INSERT_TAIL(&transport->serverReadyQueue,
                              channel->currentRpc,
                              readyQueueEntries);
            channel->state = ServerChannel::PROCESSING;
        }
        break;
    case ServerChannel::PROCESSING:
        if (header->requestAck)
            channel->inboundMsg.sendAck();
        break;
    case ServerChannel::SENDING_WAITING:
        channel->outboundMsg.send();
        break;
    }
}

void
FastTransport::ServerSession::processReceivedAck(ServerChannel* channel,
                                                 Driver::Received* received)
{
    if (channel->state == ServerChannel::SENDING_WAITING)
        channel->outboundMsg.processReceivedAck(received);
}

// --- ClientSession ---

void
FastTransport::ClientSession::processSessionOpenResponse(
        Driver::Received* received)
{
    if (numChannels > 0)
        return;
    Header* header = received->getOffset<Header>(0);
    SessionOpenResponse* response =
        received->getOffset<SessionOpenResponse>(sizeof(*header));
    serverSessionHint = header->serverSessionHint;
    token = header->sessionToken;
    LOG(DEBUG, "response max avail: %u", response->maxChannelId);
    numChannels = response->maxChannelId + 1;
    if (MAX_NUM_CHANNELS_PER_SESSION < numChannels)
        numChannels = MAX_NUM_CHANNELS_PER_SESSION;
    LOG(DEBUG, "Session open response: numChannels: %u", numChannels);
    allocateChannels();
    for (uint32_t i = 0; i < numChannels; i++) {
        if (TAILQ_EMPTY(&channelQueue))
            break;
        LOG(DEBUG, "Assigned RPC to channel: %u", i);
        ClientRPC* rpc = TAILQ_FIRST(&channelQueue);
        TAILQ_REMOVE(&channelQueue, rpc, channelQueueEntries);
        channels[i].state = ClientChannel::SENDING;
        channels[i].currentRpc = rpc;
        channels[i].outboundMsg.beginSending(rpc->requestBuffer);
    }
}

/**
 * Allocates numChannels worth of Channels in this Session.
 *
 * This is separated out so that testing methods can allocate Channels
 * without having to mock out a SessionOpenResponse.
 */
inline void
FastTransport::ClientSession::allocateChannels()
{
    channels = new ClientChannel[numChannels];
    for (uint32_t i = 0; i < numChannels; i++) {
        channels[i].setup(this, i);
        channels[i].state = ClientChannel::IDLE;
        channels[i].rpcId = 0;
        channels[i].currentRpc = NULL;
    }
}

void
FastTransport::ClientSession::processReceivedData(ClientChannel* channel,
                                                  Driver::Received* received)
{
    if (channel->state == ClientChannel::IDLE)
        return;
    Header* header = received->getOffset<Header>(0);
    if (channel->state == ClientChannel::SENDING) {
        channel->outboundMsg.clear();
        channel->inboundMsg.init(header->totalFrags,
                                 channel->currentRpc->responseBuffer);
        channel->state = ClientChannel::RECEIVING;
    }
    if (channel->inboundMsg.processReceivedData(received)) {
        channel->currentRpc->completed();
        channel->rpcId += 1;
        channel->outboundMsg.clear();
        channel->inboundMsg.clear();
        if (TAILQ_EMPTY(&channelQueue)) {
            channel->state = ClientChannel::IDLE;
            channel->currentRpc = NULL;
        } else {
            ClientRPC *rpc = TAILQ_FIRST(&channelQueue);
            assert(rpc->channelQueueEntries.tqe_prev);
            TAILQ_REMOVE(&channelQueue, rpc, channelQueueEntries);
            channel->state = ClientChannel::SENDING;
            channel->currentRpc = rpc;
            channel->outboundMsg.beginSending(rpc->requestBuffer);
        }
    }
}

void
FastTransport::ClientSession::processReceivedAck(ClientChannel* channel,
                                                 Driver::Received* received)
{
    if (channel->state == ClientChannel::SENDING)
        channel->outboundMsg.processReceivedAck(received);
}

FastTransport::ClientSession::ClientChannel*
FastTransport::ClientSession::getAvailableChannel()
{
    for (uint32_t i = 0; i < numChannels; i++) {
        if (channels[i].state == ClientChannel::IDLE)
            return &channels[i];
    }
    return NULL;
}

void
FastTransport::ClientSession::clearChannels()
{
    numChannels = 0;
    delete[] channels;
    channels = NULL;
}

FastTransport::ClientSession::ClientSession(FastTransport* transport,
                                            uint32_t sessionId)
    : Session(transport),
      channels(NULL),
      numChannels(0),
      token(0xcccccccccccccccc),
      serverSessionHint(0xcccccccc),
      lastActivityTime(0),
      serverAddress(),
      serverAddressLen(0),
      channelQueue(),
      id(sessionId),
      nextFree(FastTransport::SessionTable<ClientSession>::NONE)
{
    memset(&serverAddress, 0xcc, sizeof(serverAddress));
    TAILQ_INIT(&channelQueue);
}

bool
FastTransport::ClientSession::isConnected()
{
    return (numChannels != 0);
}

void
FastTransport::ClientSession::connect(const sockaddr* serverAddress,
                                      socklen_t serverAddressLen)
{
    if (serverAddress != NULL) {
        assert(sizeof(this->serverAddress) >= serverAddressLen);
        memcpy(&this->serverAddress, serverAddress, serverAddressLen);
        this->serverAddressLen = serverAddressLen;
    }

    // TODO(ongaro): Would it be possible to open a session like other
    // RPCs?

    Header header;
    header.direction = Header::CLIENT_TO_SERVER;
    header.clientSessionHint = id;
    header.serverSessionHint = serverSessionHint;
    header.sessionToken = token;
    header.rpcId = 0;
    header.channelId = 0;
    header.payloadType = Header::SESSION_OPEN;
    transport->sendPacket(&this->serverAddress,
                          this->serverAddressLen,
                          &header, NULL);
    lastActivityTime = rdtsc();
}

const sockaddr*
FastTransport::ClientSession::getAddress(socklen_t *len)
{
    *len = serverAddressLen;
    return &serverAddress;
}

uint64_t
FastTransport::ClientSession::getLastActivityTime()
{
    return lastActivityTime;
}

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

void
FastTransport::ClientSession::processInboundPacket(Driver::Received* received)
{
    lastActivityTime = rdtsc();
    Header* header = received->getOffset<Header>(0);
    if (header->channelId >= numChannels) {
        if (header->getPayloadType() == Header::SESSION_OPEN)
            processSessionOpenResponse(received);
        else
            LOG(DEBUG, "drop due to invalid channel");
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
            for (uint32_t i = 0; i < numChannels; i++) {
                if (channels[i].currentRpc != NULL) {
                    TAILQ_INSERT_TAIL(&channelQueue,
                                      channels[i].currentRpc,
                                      channelQueueEntries);
                }
            }
            clearChannels();
            serverSessionHint = 0xcccccccc;
            token = 0xccccccccccccccc;
            connect(NULL, 0);
            break;
        default:
            LOG(DEBUG, "drop current rpcId with bad type");
        }
    } else {
        if (header->getPayloadType() == Header::DATA &&
            header->requestAck) {
            LOG(DEBUG, "TODO: fake a full ACK response");
        } else {
            LOG(DEBUG, "drop old packet");
        }
    }
}

void
FastTransport::ClientSession::startRpc(ClientRPC* rpc)
{
    lastActivityTime = rdtsc();
    ClientChannel* channel = getAvailableChannel();
    if (channel == NULL) {
        LOG(DEBUG, "Queueing RPC");
        TAILQ_INSERT_TAIL(&channelQueue, rpc, channelQueueEntries);
    } else {
        LOG(DEBUG, "RPC proceeding on channel %p", channel);
        assert(channel->state == ClientChannel::IDLE);
        channel->state = ClientChannel::SENDING;
        channel->currentRpc = rpc;
        channel->outboundMsg.beginSending(rpc->requestBuffer);
    }
}

void
FastTransport::ClientSession::close()
{
    LOG(DEBUG, "Closing session");
    for (uint32_t i = 0; i < numChannels; i++) {
        if (channels[i].currentRpc != NULL)
            channels[i].currentRpc->aborted();
    }
    while (!TAILQ_EMPTY(&channelQueue)) {
        ClientRPC* rpc = TAILQ_FIRST(&channelQueue);
        TAILQ_REMOVE(&channelQueue, rpc, channelQueueEntries);
        rpc->aborted();
    }
    clearChannels();
    serverSessionHint = 0xcccccccc;
    token = 0xcccccccccccccccc;
}

bool
FastTransport::ClientSession::expire()
{
    for (uint32_t i = 0; i < numChannels; i++) {
        if (channels[i].currentRpc != NULL)
            return false;
    }
    if (!TAILQ_EMPTY(&channelQueue))
        return false;
    clearChannels();
    serverSessionHint = 0xcccccccc;
    token = 0xcccccccccccccccc;
    return true;
}

} // end RAMCloud
