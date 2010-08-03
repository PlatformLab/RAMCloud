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
 * Declaration of #RAMCloud::FastTransport.
 */

#ifndef RAMCLOUD_FASTTRANSPORT_H
#define RAMCLOUD_FASTTRANSPORT_H

#include <sys/socket.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <queue.h>

#include <vector>

#include "Common.h"
#include "Transport.h"
#include "Driver.h"
#include "Ring.h"

#undef CURRENT_LOG_MODULE
#define CURRENT_LOG_MODULE TRANSPORT_MODULE

namespace RAMCloud {

class FastTransport : public Transport {
    class Session;
    class ServerSession;
    class ClientSession;
  public:
    explicit FastTransport(Driver* driver);
    void poll();
    ClientSession* getClientSession();

    class ClientRPC : public Transport::ClientRPC {
      public:
        void getReply();

        Buffer* const requestBuffer;
        Buffer* const responseBuffer;

      private:
        ClientRPC(FastTransport* transport, const Service* service,
                  Buffer* request, Buffer* response);
        void aborted();
        void completed();
        void start();

        enum {
            IDLE,
            IN_PROGRESS,
            COMPLETED,
            ABORTED,
        } state;

        FastTransport* const transport;
        const sockaddr serverAddress;
        socklen_t serverAddressLen;
        TAILQ_ENTRY(ClientRPC) channelQueueEntries;
        friend class FastTransport;
        friend class FastTransportTest;
        DISALLOW_COPY_AND_ASSIGN(ClientRPC);
    };

    ClientRPC* clientSend(const Service* service,
                          Buffer* request, Buffer* response);

    class ServerRPC : public Transport::ServerRPC {
      public:
        ServerRPC(ServerSession* session, uint8_t channelId);
        void sendReply();
      private:
        ServerSession* const session;
        const uint8_t channelId;
        TAILQ_ENTRY(ServerRPC) readyQueueEntries;
        friend class FastTransport;
        friend class FastTransportTest;
        DISALLOW_COPY_AND_ASSIGN(ServerRPC);
    };

    ServerRPC* serverRecv();

  private:
    enum { NUM_CHANNELS_PER_SESSION = 8 };
    enum { MAX_NUM_CHANNELS_PER_SESSION = 8 };
    enum { PACKET_LOSS_PERCENTAGE = 5 };
    enum { MAX_STAGING_FRAGMENTS = 32 };
    enum { WINDOW_SIZE = 10 };
    enum { REQ_ACK_AFTER = 5 };
    enum { TIMEOUT_NS = 10 * 1000 * 1000 }; // 10 ms
    enum { TIMEOUTS_UNTIL_ABORTING = 500 }; // >= 5 s
    enum { SESSION_TIMEOUT_NS = 60lu * 60 * 1000 * 1000 * 1000 }; // 30 min

    class Timer {
      public:
          Timer() : when(0), listEntries() {}
          virtual void fireTimer(uint64_t now) = 0;
          virtual ~Timer() {}
          uint64_t when;
          LIST_ENTRY(Timer) listEntries;
      private:
        DISALLOW_COPY_AND_ASSIGN(Timer);
    };

    class PayloadChunk : public Buffer::Chunk {
      public:
        static PayloadChunk* prependToBuffer(Buffer* buffer,
                                             char* data,
                                             uint32_t dataLength,
                                             Driver* driver,
                                             char* payload,
                                             uint32_t payloadLength);
        static PayloadChunk* appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            Driver* driver,
                                            char* payload,
                                            uint32_t payloadLength);
        ~PayloadChunk();
      private:
        PayloadChunk(void* data,
                     uint32_t dataLength,
                     Driver* driver,
                     char* const payload,
                     uint32_t payloadLength);
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
                       bool useTimer);
        ~InboundMessage();
        void init(Session* session, uint32_t channelId, bool useTimer);
        void sendAck();
        void clear();
        void init(uint16_t totalFrags, Buffer* dataBuffer);
        /**
         * \return
         *      Whether the full message has been received and added
         *      to the dataBuffer.
         */
        bool processReceivedData(Driver::Received* received);
      private:
        FastTransport* transport;
        Session* session;
        uint32_t channelId;
        uint32_t totalFrags;
        uint32_t firstMissingFrag;
        Ring<std::pair<char*, uint32_t>,
             MAX_STAGING_FRAGMENTS> dataStagingRing;
        Buffer* dataBuffer;
        class Timer : public FastTransport::Timer {
          public:
            Timer(bool useTimer, InboundMessage* const inboundMsg)
                : useTimer(useTimer), numTimeouts(0),
                  inboundMsg(inboundMsg)
            {
            }
            virtual void fireTimer(uint64_t now) {
                numTimeouts++;
                if (numTimeouts == TIMEOUTS_UNTIL_ABORTING) {
                    LOG(DEBUG, "Closing session due to timeout");
                    inboundMsg->session->close();
                } else {
                    //LOG(DEBUG, "Timer fired; resending ACK");
                    inboundMsg->transport->addTimer(this,
                                                    rdtsc() + TIMEOUT_NS);
                    inboundMsg->sendAck();
                }
            }
            bool useTimer;
            uint32_t numTimeouts;
          private:
            InboundMessage* const inboundMsg;
            DISALLOW_COPY_AND_ASSIGN(Timer);
        };
        Timer timer;
        DISALLOW_COPY_AND_ASSIGN(InboundMessage);
    };

    class OutboundMessage {
        static const uint64_t TO_SEND = ~(0lu);
        static const uint64_t ACKED = ~(0lu) - 1;
      public:
        OutboundMessage(FastTransport* transport,
                         Session* session,
                         uint32_t channelId,
                         bool useTimer);
        void init(Session* session, uint32_t channelId, bool useTimer);
        void clear();
        void beginSending(Buffer* dataBuffer);
        void send();
        bool processReceivedAck(Driver::Received* received);
      private:
        void sendOneData(uint32_t fragNumber, bool forceRequestAck = false);
        FastTransport* transport;
        Session* session;
        uint32_t channelId;
        Buffer* sendBuffer;
        uint32_t firstMissingFrag;
        uint32_t totalFrags;
        uint32_t packetsSinceAckReq;
        Ring<uint64_t, MAX_STAGING_FRAGMENTS + 1> sentTimes;
        uint32_t numAcked;
        class Timer : public FastTransport::Timer {
          public:
            Timer(bool useTimer, OutboundMessage* const outboundMsg)
                : useTimer(useTimer), numTimeouts(0),
                  outboundMsg(outboundMsg)
            {
            }
            virtual void fireTimer(uint64_t now) {
                numTimeouts++;
                if (numTimeouts == TIMEOUTS_UNTIL_ABORTING) {
                    LOG(DEBUG, "Closing session due to timeout");
                    outboundMsg->session->close();
                } else {
                    //LOG(DEBUG, "Timer fired; resending");
                    outboundMsg->send();
                }
            }
            bool useTimer;
            uint32_t numTimeouts;
          private:
            OutboundMessage* const outboundMsg;
            DISALLOW_COPY_AND_ASSIGN(Timer);
        };
        Timer timer;

        DISALLOW_COPY_AND_ASSIGN(OutboundMessage);
    };

    class Session {
      public:
        virtual void fillHeader(Header* header, uint8_t channelId) = 0;
        virtual const sockaddr* getAddress(socklen_t *len) = 0;
        virtual uint64_t getLastActivityTime() = 0;
        virtual bool expire() = 0;
        virtual void close() = 0;
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
        uint32_t clientSessionHint;
        uint64_t lastActivityTime;
        sockaddr clientAddress;
        socklen_t clientAddressLen;

      public:
        // TODO(stutsman) template friend doesn't work - no idea why
        const uint32_t id;
        uint32_t nextFree;
        ServerSession(FastTransport* transport, uint32_t sessionId);
        uint64_t getToken();
        const sockaddr* getAddress(socklen_t *len);
        uint64_t getLastActivityTime();
        void fillHeader(Header* header, uint8_t channelId);
        void startSession(const sockaddr *clientAddress,
                          socklen_t clientAddressLen,
                          uint32_t clientSessionHint);
        void processInboundPacket(Driver::Received* received);
        void beginSending(uint8_t channelId);
        void close();
        bool expire();

      private:
        void processReceivedData(ServerChannel* channel,
                                 Driver::Received* received);
        void processReceivedAck(ServerChannel* channel,
                                Driver::Received* received);
        // TODO(stutsman) template friend doesn't work - no idea why
        template <typename T> friend class SessionTable;

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
                bool useTimer = true;
                outboundMsg.init(session, channelId, useTimer);
                inboundMsg.init(session, channelId, useTimer);
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
        uint32_t serverSessionHint;
        uint64_t lastActivityTime;
        sockaddr serverAddress;
        socklen_t serverAddressLen;

        TAILQ_HEAD(ChannelQueueHead, ClientRPC) channelQueue;

        void processSessionOpenResponse(Driver::Received* received);
        void processReceivedData(ClientChannel* channel,
                                 Driver::Received* received);
        void processReceivedAck(ClientChannel* channel,
                                Driver::Received* received);
        ClientChannel* getAvailableChannel();
        void clearChannels();
      public:
        // TODO(stutsman) template friend doesn't work - no idea why
        const uint32_t id;
        uint32_t nextFree;
        ClientSession(FastTransport* transport, uint32_t sessionId);
        bool isConnected();
        void connect(const sockaddr* serverAddress,
                     socklen_t serverAddressLen);
        const sockaddr* getAddress(socklen_t *len);
        uint64_t getLastActivityTime();
        void fillHeader(Header* header, uint8_t channelId);
        void processInboundPacket(Driver::Received* received);
        void startRpc(ClientRPC* rpc);
        void close();
        bool expire();

      private:
        template <typename T> friend class SessionTable;
        DISALLOW_COPY_AND_ASSIGN(ClientSession);
    };

    template <typename T>
    class SessionTable {
      public:
        /// A Session with this as nextFree is not itself free.
        static const uint32_t NONE = ~0u;
        /// A Session with this as nextFree is last Session in the free list.
        static const uint32_t TAIL = ~0u - 1;
        explicit SessionTable(FastTransport* transport)
            : transport(transport),
              sessions(),
              firstFree(TAIL),
              lastCleanedIndex(0)
        {
        }

        T* operator[](uint32_t sessionHint) const
        {
            return sessions[sessionHint];
        }

        T* get()
        {
            uint32_t sessionHint = firstFree;
            if (sessionHint >= sessions.size()) {
                // Invalid, no free sessions, so create a new one
                sessionHint = sessions.size();
                T* session = new T(transport, sessionHint);
                session->nextFree = TAIL;
                sessions.push_back(session);
            }
            T* session = sessions[sessionHint];
            firstFree = session->nextFree;
            session->nextFree = NONE;
            return sessions[sessionHint];
        }

        void put(T* session)
        {
            session->nextFree = firstFree;
            firstFree = session->id;
        }

        void expire(uint32_t sessionsToCheck = 5)
        {
            uint64_t now = rdtsc();
            for (uint32_t i = 0; i < sessionsToCheck; i++) {
                lastCleanedIndex++;
                if (lastCleanedIndex >= sessions.size()) {
                    lastCleanedIndex = 0;
                    if (sessions.size() == 0)
                        break;
                }
                T* session = sessions[lastCleanedIndex];
                if (session->nextFree == NONE &&
                    (session->getLastActivityTime() +
                     SESSION_TIMEOUT_NS < now)) {
                    if (session->expire())
                        put(session);
                }
            }
        }

        uint32_t size()
        {
            return sessions.size();
        }

      private:
        FastTransport* const transport;
        std::vector<T*> sessions;
        uint32_t firstFree;
        uint32_t lastCleanedIndex;

        friend class SessionTableTest;
        DISALLOW_COPY_AND_ASSIGN(SessionTable);
    };

    bool tryProcessPacket();
    void addTimer(Timer* timer, uint64_t when);
    void removeTimer(Timer* timer);
    void fireTimers();
    void sendPacket(const sockaddr* address, socklen_t addressLength,
                    Header* header, Buffer::Iterator* payload);
    uint32_t dataPerFragment();
    uint32_t numFrags(const Buffer* dataBuffer);

    Driver* const driver;
    SessionTable<ServerSession> serverSessions;
    SessionTable<ClientSession> clientSessions;
    TAILQ_HEAD(ServerReadyQueueHead, ServerRPC) serverReadyQueue;
    LIST_HEAD(TimerListHead, Timer) timerList;

    friend class FastTransportTest;
    friend class SessionTableTest;
    friend class Services;
    DISALLOW_COPY_AND_ASSIGN(FastTransport);
};

}  // namespace RAMCloud

#endif
