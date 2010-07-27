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

#ifndef RAMCLOUD_FASTTRANSPORT_H
#define RAMCLOUD_FASTTRANSPORT_H

#include <sys/socket.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <queue.h>
#include <Common.h>
#include <Transport.h>
#include <Driver.h>
#include <Ring.h>

#undef CURRENT_LOG_MODULE
#define CURRENT_LOG_MODULE TRANSPORT_MODULE

#define DOSTR(x) #x
#define STR(x) DOSTR(x)
#define AT __FILE__ ":" STR(__LINE__)

namespace RAMCloud {

class FastTransport : public Transport {
    class Session;
    class ServerSession;
  public:
    class ServerRPC;

    explicit FastTransport(Driver* driver)
        : driver(driver), serverSession(this, 0), clientSession(this, 0),
        serverReadyQueue() {
        TAILQ_INIT(&serverReadyQueue);
    }

    void poll() {
        while (tryProcessPacket())
            fireTimers();
        fireTimers();
    }

    class ClientRPC : public Transport::ClientRPC {
      public:
          void getReply() {
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

      private:
        ClientRPC(FastTransport* transport, const Service* service,
                  Buffer* request, Buffer* response)
            : state(IDLE), transport(transport),
            serverAddress(), serverAddressLen(0),
            requestBuffer(request), responseBuffer(response),
            channelQueueEntries() {

            sockaddr_in *addr = const_cast<sockaddr_in*>(
                reinterpret_cast<const sockaddr_in*>(&serverAddress));
            addr->sin_family = AF_INET;
            addr->sin_port = htons(service->getPort());
            if (inet_aton(service->getIp(), &addr->sin_addr) == 0)
                throw Exception("inet_aton failed");
            serverAddressLen = sizeof(*addr);
        }

        void start() {
            assert(state == IDLE);
            state = IN_PROGRESS;
            ClientSession* session = &transport->clientSession;
            if (!session->isConnected())
                session->connect(&serverAddress, serverAddressLen);
            session->startRpc(this);
        }

        void aborted() {
            state = ABORTED;
        }

        void completed() {
            state = COMPLETED;
        }

        enum {
            IDLE,
            IN_PROGRESS,
            COMPLETED,
            ABORTED,
        } state;

        FastTransport* const transport;
        const sockaddr serverAddress;
        socklen_t serverAddressLen;
      public:
        Buffer* const requestBuffer;
        Buffer* const responseBuffer;
      private:
        TAILQ_ENTRY(ClientRPC) channelQueueEntries;
        friend class FastTransport;
        friend class FastTransportTest;
        DISALLOW_COPY_AND_ASSIGN(ClientRPC);
    };

    ClientRPC* clientSend(const Service* service,
                          Buffer* request, Buffer* response) {
        ClientRPC* rpc = new(request, MISC) ClientRPC(this, service,
                                                      request, response);
        rpc->start();
        return rpc;
    }

    class ServerRPC : public Transport::ServerRPC {
      public:
        ServerRPC(ServerSession* session, uint8_t channelId)
            : session(session), channelId(channelId), readyQueueEntries() {
        }
        void sendReply() {
            session->beginSending(channelId);
            // don't forget to delete(this) eventually
        }
      private:
        ServerSession* const session;
        const uint8_t channelId;
        TAILQ_ENTRY(ServerRPC) readyQueueEntries;
        friend class FastTransport;
        friend class FastTransportTest;
        DISALLOW_COPY_AND_ASSIGN(ServerRPC);
    };

    ServerRPC* serverRecv() {
        ServerRPC* rpc;
        while ((rpc = TAILQ_FIRST(&serverReadyQueue)) == NULL)
            poll();
        TAILQ_REMOVE(&serverReadyQueue, rpc, readyQueueEntries);
        return rpc;
    }

  private:
    enum { NUM_CHANNELS_PER_SESSION = 8 };
    enum { MAX_NUM_CHANNELS_PER_SESSION = 8 };
    enum { PACKET_LOSS_PERCENTAGE = 0 };
    enum { MAX_STAGING_FRAGMENTS = 32 };
    enum { WINDOW_SIZE = 10 };
    enum { REQ_ACK_AFTER = 5 };
    enum { TIMEOUT_NS = 10 * 1000 * 1000 }; // 10 ms

    struct PayloadChunk : public Buffer::Chunk {
        static PayloadChunk* prependToBuffer(Buffer* buffer,
                                             char* data,
                                             uint32_t dataLength,
                                             Driver* driver,
                                             char* payload,
                                             uint32_t payloadLength) {
            PayloadChunk* chunk =
                new(buffer, CHUNK) PayloadChunk(data,
                                                dataLength,
                                                driver,
                                                payload,
                                                payloadLength);
            Buffer::Chunk::prependChunkToBuffer(buffer, chunk);
            return chunk;
        }
        static PayloadChunk* appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            Driver* driver,
                                            char* payload,
                                            uint32_t payloadLength) {
            PayloadChunk* chunk =
                new(buffer, CHUNK) PayloadChunk(data,
                                                dataLength,
                                                driver,
                                                payload,
                                                payloadLength);
            Buffer::Chunk::prependChunkToBuffer(buffer, chunk);
            return chunk;
        }
        ~PayloadChunk() {
            driver->release(payload, payloadLength);
        }
      private:
        PayloadChunk(void* data, uint32_t dataLength,
                     Driver* driver,
                     char* const payload, uint32_t payloadLength)
            : Buffer::Chunk(data, dataLength),
              driver(driver), payload(payload), payloadLength(payloadLength)
        {
        }
        Driver* const driver;
        char* const payload;
        const uint32_t payloadLength;
        DISALLOW_COPY_AND_ASSIGN(PayloadChunk);
    };

    struct Header {
        enum PayloadType {
            DATA         = 0,
            ACK          = 1,
            SESSION_OPEN = 2,
            RESERVED1    = 3,
            BAD_SESSION  = 4,
            RESERVED2    = 5,
            RESERVED3    = 6,
            RESERVED4    = 7
        };
        enum Direction {
            CLIENT_TO_SERVER = 0,
            SERVER_TO_CLIENT = 1
        };
        uint64_t sessionToken;
        uint32_t rpcId;
        uint32_t clientSessionHint;
        uint32_t serverSessionHint;
        uint16_t fragNumber;
        uint16_t totalFrags;
        uint8_t channelId;
        uint8_t direction:1;
        uint8_t requestAck:1;
        uint8_t pleaseDrop:1;
        uint8_t reserved1:1;
        uint8_t payloadType:4;
        PayloadType getPayloadType() {
            return static_cast<PayloadType>(payloadType);
        };
        Direction getDirection() {
            return static_cast<Direction>(direction);
        };
    } __attribute__((packed));

    struct SessionOpenResponse {
        uint8_t maxChannelId;
    } __attribute__((packed));

    struct AckResponse {
        AckResponse(uint16_t firstMissingFrag,
                    uint32_t stagingVector = 0)
            : firstMissingFrag(firstMissingFrag),
              stagingVector(stagingVector) {}
        uint16_t firstMissingFrag;
        uint32_t stagingVector;
    } __attribute__((packed));

    class InboundMessage {
      public:
        InboundMessage(FastTransport* transport,
                       Session* session,
                       uint32_t channelId,
                       bool useTimer)
            : transport(transport),
              session(session),
              channelId(channelId),
              totalFrags(0),
              firstMissingFrag(0),
              dataStagingRing(),
              dataBuffer(NULL)
              // TODO(stutsman) timer
        {
        }
        ~InboundMessage()
        {
            clear();
        }
        void init(Session* session, uint32_t channelId, bool useTimer) {
            this->session = session;
            this->transport = session->transport;
            this->channelId = channelId;
            // TODO(stutsman) useTimer
        }
        void sendAck() {
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
        void clear() {
            totalFrags = 0;
            firstMissingFrag = 0;
            dataBuffer = NULL;
            for (uint32_t i = 0; i < dataStagingRing.getLength(); i++) {
                std::pair<char*, uint32_t> elt = dataStagingRing[i];
                if (elt.first)
                    transport->driver->release(elt.first, elt.second);
            }
            dataStagingRing.clear();
            // TODO(stutsman)
            // timer.numTimeouts = 0;
            // if timer.useTimer
            //     transport->removeTimer(timer);
        }
        void init(uint16_t totalFrags, Buffer* dataBuffer) {
            clear();
            this->totalFrags = totalFrags;
            this->dataBuffer = dataBuffer;
            firstMissingFrag = 0;
            // TODO(stutsman)
            // if timer.useTimer
            //     transport->addTimer(timer, gettime() + TIMEOUT_NS);
        }
        /**
         * \return
         *      Whether the full message has been received and added
         *      to the dataBuffer.
         */
        bool processReceivedData(Driver::Received* received) {
            assert(received->len >= sizeof(Header));
            Header *header = reinterpret_cast<Header*>(received->payload);

            if (header->totalFrags != totalFrags) {
                // What's wrong with the other end?
                LOG(DEBUG, "header->totalFrags != totalFrags");
                return firstMissingFrag == totalFrags;
            }

            LOG(DEBUG, "%d/%d received", header->fragNumber, totalFrags - 1);

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
            // TODO(stutsman)
            // if (timer->useTimer) {
            //     transport->addTimer(timer, gettime() + TIMEOUT_NS)
            // }

            return firstMissingFrag == totalFrags;
        }
      private:
        FastTransport* transport;
        Session* session;
        uint32_t channelId;
        uint32_t totalFrags;
        uint32_t firstMissingFrag;
        Ring<std::pair<char*, uint32_t>,
             MAX_STAGING_FRAGMENTS> dataStagingRing;
        Buffer* dataBuffer;
        DISALLOW_COPY_AND_ASSIGN(InboundMessage);
    };

    class OutboundMessage {
        static const uint64_t TO_SEND = ~(0lu);
        static const uint64_t ACKED = ~(0lu) - 1;
      public:
        OutboundMessage(FastTransport* transport,
                         Session* session,
                         uint32_t channelId,
                         bool useTimer)
            : transport(transport),
              session(session),
              channelId(channelId),
              sendBuffer(NULL),
              firstMissingFrag(0),
              totalFrags(0),
              packetsSinceAckReq(0),
              sentTimes(),
              numAcked(0)
              // TODO(stutsman) timer
        {
        }
        void init(Session* session, uint32_t channelId, bool useTimer) {
            this->session = session;
            this->transport = session->transport;
            this->channelId = channelId;
            // TODO(stutsman) useTimer
        }
        void clear() {
            sendBuffer = NULL;
            firstMissingFrag = 0;
            totalFrags = 0;
            packetsSinceAckReq = 0;
            sentTimes.clear();
            numAcked = 0;
            // TODO(stutsman)
            // transport->removeTimer(timer);
            // timer->numTimeouts = 0;
        }
        void beginSending(Buffer* dataBuffer) {
            assert(!sendBuffer);
            sendBuffer = dataBuffer;
            totalFrags = transport->numFrags(sendBuffer);
            send();
        }
        void send() {
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

            // TODO(stutsman)
            // if (timer->useTimer) {
            //      uint32_t oldset = sentTimes.getLength();
            //      for (uint32_t i = 0; i < sentTimes.getLength(); i++) {
            //          uint32_t sentTime = sentTimes[i];
            //          if (sentTime != ACKED || sentTime < oldest)
            //              oldest = sentTime;
            //      }
            //      if (oldest != sentTimes.getLength())
            //          transport->addTimer(timer, oldtest + TIMEOUT_NS);
            // }

        }
        bool processReceivedAck(Driver::Received* received) {
            if (!sendBuffer)
                return false;

            assert(received->len >= sizeof(Header) + sizeof(AckResponse));
            AckResponse *ack =
                received->getOffset<AckResponse>(sizeof(Header));

            if (ack->firstMissingFrag < firstMissingFrag) {
                LOG(DEBUG, "OutboundMessage dropped stable ACK");
            } else if (ack->firstMissingFrag > totalFrags) {
                LOG(DEBUG, "OutboundMessage dropped invalid ACK"
                           "(shouldn't happen)");
            } else if (ack->firstMissingFrag >
                     (firstMissingFrag + sentTimes.getLength())) {
                LOG(DEBUG, "OutboundMessage dropped ACK that advanced too far "
                           "(shouldn't happen)");
            } else {
                sentTimes.advance(ack->firstMissingFrag - firstMissingFrag);
                firstMissingFrag = ack->firstMissingFrag;
                numAcked = ack->firstMissingFrag;
                for (uint32_t i = 0; i < sentTimes.getLength() - 1; i++) {
                    bool acked = (ack->stagingVector >> i) & 1;
                    if (acked) {
                        sentTimes[i + 1] = ACKED;
                        numAcked++;
                    }
                }
            }
            send();
            return firstMissingFrag == totalFrags;
        }
      private:
        void sendOneData(uint32_t fragNumber, bool forceRequestAck = false) {
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
        FastTransport* transport;
        Session* session;
        uint32_t channelId;
        Buffer* sendBuffer;
        uint32_t firstMissingFrag;
        uint32_t totalFrags;
        uint32_t packetsSinceAckReq;
        Ring<uint64_t, MAX_STAGING_FRAGMENTS + 1> sentTimes;
        uint32_t numAcked;

        DISALLOW_COPY_AND_ASSIGN(OutboundMessage);
    };

    class Session {
      public:
        virtual void fillHeader(Header* header, uint8_t channelId) = 0;
        virtual const sockaddr* getAddress(socklen_t *len) = 0;
        virtual uint64_t getLastActivityTime() = 0;
        virtual bool expire() = 0;
        virtual ~Session() {}
        explicit Session(FastTransport* transport)
            : transport(transport) {}
        FastTransport* const transport;
      private:
        DISALLOW_COPY_AND_ASSIGN(Session);
    };

    class ServerSession : public Session {
        struct ServerChannel {
          public:
            /// This creates broken in/out messages that are reinitialized
            /// by init()
            ServerChannel()
                : state(IDLE),
                  rpcId(~0U),
                  currentRpc(NULL),
                  inboundMsg(NULL, NULL, 0, false),
                  outboundMsg(NULL, NULL, 0, false)
            {
            }
            void init(Session* session, uint32_t channelId) {
                inboundMsg.init(session, channelId, false);
                outboundMsg.init(session, channelId, false);
            }
            enum {
                IDLE,
                RECEIVING,
                PROCESSING,
                SENDING_WAITING,
            } state;
            uint32_t rpcId;
            ServerRPC* currentRpc;
            InboundMessage inboundMsg;
            OutboundMessage outboundMsg;
          private:
            DISALLOW_COPY_AND_ASSIGN(ServerChannel);
        };

        ServerChannel channels[NUM_CHANNELS_PER_SESSION];

        uint64_t token;
        const uint32_t id;
        uint32_t clientSessionHint;
        uint64_t lastActivityTime;
        sockaddr clientAddress;
        socklen_t clientAddressLen;

      public:
        ServerSession(FastTransport* transport, uint32_t sessionId)
            : Session(transport),
              token(0xcccccccccccccccc),
              id(sessionId),
              clientSessionHint(0xcccccccc),
              lastActivityTime(0),
              clientAddress(),
              clientAddressLen(0) {
            memset(&clientAddress, 0xcc, sizeof(clientAddress));
            for (uint32_t i = 0; i < NUM_CHANNELS_PER_SESSION; i++)
                channels[i].init(this, i);
        }

        uint64_t getToken() {
            return token;
        }

        const sockaddr* getAddress(socklen_t *len) {
            *len = clientAddressLen;
            return &clientAddress;
        }

        uint64_t getLastActivityTime() {
            return lastActivityTime;
        }

        void fillHeader(Header* header, uint8_t channelId) {
            header->rpcId = channels[channelId].rpcId;
            header->channelId = channelId;
            header->direction = Header::SERVER_TO_CLIENT;
            header->clientSessionHint = clientSessionHint;
            header->serverSessionHint = id;
            header->sessionToken = token;
        }

        void startSession(const sockaddr *clientAddress,
                          socklen_t clientAddressLen,
                          uint32_t clientSessionHint) {
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

        void processInboundPacket(Driver::Received* received) {
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

        void beginSending(uint8_t channelId) {
            ServerChannel* channel = &channels[channelId];
            assert(channel->state == ServerChannel::PROCESSING);
            channel->state = ServerChannel::SENDING_WAITING;
            Buffer* responseBuffer = &channel->currentRpc->replyPayload;
            channel->outboundMsg.beginSending(responseBuffer);
            lastActivityTime = rdtsc();
        }


        bool expire() {
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

      private:

        void processReceivedData(ServerChannel* channel,
                                 Driver::Received* received) {
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

        void processReceivedAck(ServerChannel* channel,
                                Driver::Received* received) {
            if (channel->state == ServerChannel::SENDING_WAITING)
                channel->outboundMsg.processReceivedAck(received);
        }

        DISALLOW_COPY_AND_ASSIGN(ServerSession);
    };

    class ClientSession : public Session {
        struct ClientChannel {
          public:
            /// This creates broken in/out messages that are reinitialized
            /// by init()
            ClientChannel()
                : state(IDLE),
                  rpcId(~0U),
                  currentRpc(NULL),
                  outboundMsg(NULL, NULL, 0, false),
                  inboundMsg(NULL, NULL, 0, false)
            {
            }
            void init(Session* session, uint32_t channelId) {
                outboundMsg.init(session, channelId, false);
                inboundMsg.init(session, channelId, false);
            }
            enum {
                IDLE,
                SENDING,
                RECEIVING,
            } state;
            uint32_t rpcId;
            ClientRPC* currentRpc;
            OutboundMessage outboundMsg;
            InboundMessage inboundMsg;
          private:
            DISALLOW_COPY_AND_ASSIGN(ClientChannel);
        };

        // TODO(ongaro): session open timer

        ClientChannel *channels;
        uint32_t numChannels;

        uint64_t token;
        const uint32_t id;
        uint32_t serverSessionHint;
        uint64_t lastActivityTime;
        sockaddr serverAddress;
        socklen_t serverAddressLen;

        TAILQ_HEAD(ChannelQueueHead, ClientRPC) channelQueue;

        void processSessionOpenResponse(Driver::Received* received) {
            if (numChannels > 0)
                return;
            Header* header = received->getOffset<Header>(0);
            SessionOpenResponse* response =
                received->getOffset<SessionOpenResponse>(sizeof(*header));
            serverSessionHint = header->serverSessionHint;
            token = header->sessionToken;
            numChannels = std::min(response->maxChannelId + 1,
                                   static_cast<int>(
                                       MAX_NUM_CHANNELS_PER_SESSION));
            channels = new ClientChannel[numChannels];
            for (uint32_t i = 0; i < numChannels; i++) {
                channels[i].state = ClientChannel::IDLE;
                channels[i].rpcId = 0;
                channels[i].currentRpc = NULL;
                channels[i].init(this, i);
            }
            for (uint32_t i = 0; i < numChannels; i++) {
                ClientRPC* rpc = TAILQ_FIRST(&channelQueue);
                if (rpc == NULL)
                    break;
                TAILQ_REMOVE(&channelQueue, rpc, channelQueueEntries);
                channels[i].state = ClientChannel::SENDING;
                channels[i].currentRpc = rpc;
                channels[i].outboundMsg.beginSending(rpc->requestBuffer);
            }
        }

        void processReceivedData(ClientChannel* channel,
                                 Driver::Received* received) {
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
                ClientRPC *rpc = TAILQ_FIRST(&channelQueue);
                if (rpc == NULL) {
                    channel->state = ClientChannel::IDLE;
                    channel->currentRpc = NULL;
                } else {
                    TAILQ_REMOVE(&channelQueue, rpc, channelQueueEntries);
                    channel->state = ClientChannel::SENDING;
                    channel->currentRpc = rpc;
                    channel->outboundMsg.beginSending(rpc->requestBuffer);
                }
            }
        }

        void processReceivedAck(ClientChannel* channel,
                                Driver::Received* received) {
            if (channel->state == ClientChannel::SENDING)
                channel->outboundMsg.processReceivedAck(received);
        }

        ClientChannel* getAvailableChannel() {
            for (uint32_t i = 0; i < numChannels; i++) {
                if (channels[i].state == ClientChannel::IDLE)
                    return &channels[i];
            }
            return NULL;
        }

        void clearChannels() {
            numChannels = 0;
            delete[] channels;
            channels = NULL;
        }

      public:
        ClientSession(FastTransport* transport, uint32_t sessionId)
            : Session(transport),
              channels(NULL),
              numChannels(0),
              token(0xcccccccccccccccc),
              id(sessionId),
              serverSessionHint(0xcccccccc),
              lastActivityTime(0),
              serverAddress(),
              serverAddressLen(0),
              channelQueue() {
            memset(&serverAddress, 0xcc, sizeof(serverAddress));
            TAILQ_INIT(&channelQueue);
        }

        bool isConnected() {
            return (numChannels != 0);
        }

        void connect(const sockaddr* serverAddress,
                     socklen_t serverAddressLen) {
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

        const sockaddr* getAddress(socklen_t *len) {
            *len = serverAddressLen;
            return &serverAddress;
        }

        uint64_t getLastActivityTime() {
            return lastActivityTime;
        }

        void fillHeader(Header* header, uint8_t channelId) {
            header->rpcId = channels[channelId].rpcId;
            header->channelId = channelId;
            header->direction = Header::CLIENT_TO_SERVER;
            header->clientSessionHint = id;
            header->serverSessionHint = serverSessionHint;
            header->sessionToken = token;
        }

        void processInboundPacket(Driver::Received* received) {
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

        void startRpc(ClientRPC* rpc) {
            lastActivityTime = rdtsc();
            ClientChannel* channel = getAvailableChannel();
            if (channel == NULL) {
                TAILQ_INSERT_TAIL(&channelQueue, rpc, channelQueueEntries);
            } else {
                assert(channel->state == ClientChannel::IDLE);
                channel->state = ClientChannel::SENDING;
                channel->currentRpc = rpc;
                channel->outboundMsg.beginSending(rpc->requestBuffer);
            }
        }

        void close() {
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

        bool expire() {
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

      private:
        DISALLOW_COPY_AND_ASSIGN(ClientSession);
    };

    bool tryProcessPacket() {
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
            if (header->serverSessionHint < 1) {
                ServerSession* session = &serverSession;
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
                ServerSession* session = &serverSession;
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
            if (header->clientSessionHint < 1) {
                ClientSession* session = &clientSession;
                session->processInboundPacket(&received);
            } else {
                LOG(DEBUG, "Bad client session hint");
            }
        }
        return true;
    }

    void fireTimers() {
    }

    void sendPacket(const sockaddr* address, socklen_t addressLength,
                    Header* header, Buffer::Iterator* payload) {
        header->pleaseDrop = (random() < ~0U * PACKET_LOSS_PERCENTAGE / 100);
        driver->sendPacket(address, addressLength,
                           header, sizeof(*header),
                           payload);
    }

    uint32_t dataPerFragment() {
        return driver->getMaxPayloadSize() - sizeof(Header);
    }

    uint32_t numFrags(const Buffer* dataBuffer) {
        return ((dataBuffer->getTotalLength() + dataPerFragment() - 1) /
                 dataPerFragment());
    }

    Driver* const driver;
    ServerSession serverSession;
    ClientSession clientSession;
    TAILQ_HEAD(ServerReadyQueueHead, ServerRPC) serverReadyQueue;

    friend class FastTransportTest;
    DISALLOW_COPY_AND_ASSIGN(FastTransport);
};

}  // namespace RAMCloud

#endif
