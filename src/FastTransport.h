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
#include <queue.h>
#include <Common.h>
#include <Transport.h>
#include <Driver.h>

#undef CURRENT_LOG_MODULE
#define CURRENT_LOG_MODULE TRANSPORT_MODULE

namespace RAMCloud {

class FastTransport : public Transport {
    class ServerSession;
  public:
    class ServerRPC;

    explicit FastTransport(Driver* driver)
        : driver(driver), serverSession(this, 0), serverReadyQueue() {
        LIST_INIT(&serverReadyQueue);
    }

    void poll() {
        while (tryProcessPacket())
            fireTimers();
        fireTimers();
    }

    class ClientRPC : public Transport::ClientRPC {
        friend class FastTransport;
        friend class FastTransportTest;
        DISALLOW_COPY_AND_ASSIGN(ClientRPC);
    };

    ClientRPC* clientSend(const Service* service, Buffer* request,
                          Buffer* response) {
        throw UnrecoverableTransportException("Not implemented");
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
        LIST_ENTRY(ServerRPC) readyQueueEntries;
        friend class FastTransport;
        friend class FastTransportTest;
        DISALLOW_COPY_AND_ASSIGN(ServerRPC);
    };

    ServerRPC* serverRecv() {
        ServerRPC* rpc;
        while ((rpc = LIST_FIRST(&serverReadyQueue)) == NULL)
            poll();
        LIST_REMOVE(rpc, readyQueueEntries);
        return rpc;
    }

  private:
    enum { NUM_CHANNELS_PER_SESSION = 1 };
    enum { PACKET_LOSS_PERCENTAGE = 0 };

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
        uint16_t firstMissingFrag;
        uint32_t stagingVector;
    } __attribute__((packed));

    class InboundMessage {
      public:
        void clear() {}
        void init(uint16_t totalFrags, Buffer* dataBuffer) {}
        bool processReceivedData(Driver::Received* received) {
            throw new UnrecoverableTransportException("not implemented");
        }
        void sendAck() {
            throw new UnrecoverableTransportException("not implemented");
        }
    };

    class OutboundMessage {
      public:
        void clear() {}
        void beginSending(Buffer* dataBuffer) {
            throw new UnrecoverableTransportException("not implemented");
        }
        void send() {
            throw new UnrecoverableTransportException("not implemented");
        }
        void processReceivedAck(Driver::Received* received) {
            throw new UnrecoverableTransportException("not implemented");
        }
    };

    class ServerSession {
        struct ServerChannel {
          public:
            ServerChannel()
                : state(IDLE),
                  rpcId(~0U),
                  currentRpc(NULL),
                  inboundMsg(),
                  outboundMsg() {
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

        FastTransport* const transport;
        uint64_t token;
        const uint32_t id;
        uint32_t clientSessionHint;
        uint64_t lastActivityTime;
        sockaddr clientAddress;
        socklen_t clientAddressLen;

      public:
        ServerSession(FastTransport* transport, uint32_t sessionId)
            : transport(transport),
              token(0xcccccccccccccccc),
              id(sessionId),
              clientSessionHint(0xcccccccc),
              lastActivityTime(0),
              clientAddress(),
              clientAddressLen(0) {
            memset(&clientAddress, 0xcc, sizeof(clientAddress));
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

        void startSession(sockaddr *clientAddress,
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

            for (ServerChannel* channel = &channels[0];
                 channel < &channels[NUM_CHANNELS_PER_SESSION];
                 channel++) {
                if (channel->state == ServerChannel::PROCESSING)
                    return false;
            }

            for (ServerChannel* channel = &channels[0];
                 channel < &channels[NUM_CHANNELS_PER_SESSION];
                 channel++) {
                if (channel->state == ServerChannel::IDLE)
                    continue;
                channel->state = ServerChannel::IDLE;
                channel->rpcId = ~0U;
                delete channel->currentRpc;
                channel->currentRpc = NULL;
                channel->inboundMsg.clear();
                channel->outboundMsg.clear();
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
                    LIST_INSERT_HEAD(&transport->serverReadyQueue,
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



    };

    bool tryProcessPacket() {
        Driver::Received received;
        if (!driver->tryRecvPacket(&received))
            return false;

        LOG(DEBUG, "received");

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
                if (session->getToken() == header->sessionToken)
                    session->processInboundPacket(&received);
                else
                    LOG(DEBUG, "bad token");
            switch (header->getPayloadType()) {
            case Header::SESSION_OPEN: {
                ServerSession* session = &serverSession;
                session->startSession(&received.addr,
                                      received.addrlen,
                                      header->clientSessionHint);
                break;
            }
            default: {
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
            }
        } else {
            throw UnrecoverableTransportException("Not implemented");
        }
        return true;
    }

    void fireTimers() {
    }

    void sendPacket(sockaddr* address, socklen_t addressLength,
                    Header* header, Buffer::Iterator* payload) {
        header->pleaseDrop = (random() < ~0U * PACKET_LOSS_PERCENTAGE / 100);
        driver->sendPacket(address, addressLength,
                           header, sizeof(*header),
                           payload);
    }

    Driver* const driver;
    ServerSession serverSession;
    LIST_HEAD(ServerReadyQueueHead, ServerRPC) serverReadyQueue;

    friend class FastTransportTest;
    DISALLOW_COPY_AND_ASSIGN(FastTransport);
};

}  // namespace RAMCloud

#endif
