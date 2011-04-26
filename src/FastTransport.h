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

#ifndef RAMCLOUD_FASTTRANSPORT_H
#define RAMCLOUD_FASTTRANSPORT_H

#include <sys/socket.h>
#include <netinet/ip.h>
#include <arpa/inet.h>

#include <vector>

#include "BenchUtil.h"
#include "BoostIntrusive.h"
#include "Common.h"
#include "Dispatch.h"
#include "Driver.h"
#include "Transport.h"
#include "Window.h"

#undef CURRENT_LOG_MODULE
#define CURRENT_LOG_MODULE RAMCloud::TRANSPORT_MODULE

namespace RAMCloud {

/*
 * Control flow in/under FastTransport can be rather hairy.  Below a rough
 * stack trace is given for when a packet follows through the FastTransport
 * layer just to give a developer a rough idea of where to look.
 *
 * - Client Outbound
 *  - ClientSession::clientSend
 *   - ClientSession::getAvailableChannel
 *  - OutboundMessage::beginSending
 *  - OutboundMessage::send
 *  - OutboundMessage::sendOneData
 *
 * - Server Inbound
 *  - Dispatch::poll
 *  - Driver
 *  - FastTransport::handleIncomingPacket
 *  - ServerSession::processInboundPacket
 *  - ServerSession::processReceivedData
 *  - InboundMessage::processReceivedData
 *
 * - Server Outbound
 *  - ServerRpc::sendReply
 *  - ServerSession::beginSending
 *  - OutboundMessage::beginSending
 *  - OutboundMessage::send
 *
 * - Client Inbound
 *  - Dispatch::poll
 *  - Driver
 *  - FastTransport::handleIncomingPacket
 *  - ClientSession::processReceivedData
 *  - ClientSession::processReceivedData
 *  - InboundMessage::processReceivedData
 */

/**
 * A Transport which supports simple, but reliable RPCs using unreliable
 * datagram protocols (Drivers).  See Transport for more information.
 */
class FastTransport : public Transport {
    class Session;
    class ServerSession;
    class ClientSession;
  public:
    explicit FastTransport(Driver* driver);
    ~FastTransport();

    void dumpStats() {
        driver->dumpStats();
    }
    VIRTUAL_FOR_TESTING void handleIncomingPacket(Driver::Received *received);

    ServiceLocator getServiceLocator();
    void registerMemory(void* base, size_t bytes) {}

    /**
     * Manages an entire request/response cycle from the client perspective.
     *
     * Once the RPC is created start() will initiate the RPC and wait()
     * will block until the response is complete and valid.
     */
    class ClientRpc : public Transport::ClientRpc {
      private:
        ClientRpc(FastTransport* transport,
                  Buffer* request, Buffer* response);

        /// Contains an RPC request payload including the RPC header.
        Buffer* const requestBuffer;

        /// The destination Buffer for the RPC response.
        Buffer* const responseBuffer;

        /// The Transport on which to send/receive the RPC.
        FastTransport* const transport;

        /// Entries to allow this RPC to be placed in a channel queue.
        IntrusiveListHook channelQueueEntries;

      private:
        friend class FastTransport;
        friend class ClientSession;
        friend class FastTransportTest;
        friend class ClientRpcTest;
        friend class ClientSessionTest;
        DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

    virtual Transport::SessionRef
    getSession(const ServiceLocator& serviceLocator);

    /**
     * Serves as a unit of storage allocation for per Rpc server state.
     */
    class ServerRpc : public Transport::ServerRpc {
      public:
        ServerRpc();
        virtual ~ServerRpc();
        void reset();
        void setup(ServerSession* session, uint8_t channelId);
        void sendReply();

      private:
        void maybeDequeue();

        /// The ServerSession this RPC is being handled on, const after setup().
        ServerSession* session;

        /**
         * The channel number in session this RPC is being handled on,
         * const after setup().
         */
        uint8_t channelId;

        friend class FastTransport;
        friend class ServerSessionTest;
        friend class FastTransportTest;
        DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

  private:
    /**
     * Max number of concurrent RPCs per Session.
     *
     * A fixed number of channels which the server-side allocates
     * and returns in session open responses.
     */
    enum { NUM_CHANNELS_PER_SESSION = 8 };

    /**
     * Max number of channels to take advantage of on the client if they
     * are available on the server.
     *
     * If the server is prepared to handle RPCs on more channels than this
     * just use this number and leave the rest unused.  The client
     * dynamically allocates up to this number of channels after receiving a
     * session open response. See ClientSession::processSessionOpenResponse().
     */
    enum { MAX_NUM_CHANNELS_PER_SESSION = 8 };

    /// Simulate uniform packet loss with this percentage.
    enum { PACKET_LOSS_PERCENTAGE = 0 };

    /**
     * firstMissingFrag + MAX_STAGING_FRAGMENTS - 1 is the highest fragNumber
     * that can be received without being thrown on the floor.  Indirectly
     * limits the highest packet number that can be acked.
     *
     * This prevents the sender/receiver from having to buffer an unbounded
     * number of fragments before they can be appended to a buffer allowing
     * for static allocation of fragment and ackresponse storage.
     */
    enum { MAX_STAGING_FRAGMENTS = 32 };

    /// Maximum number of in-flight non-acked fragments.
    enum { WINDOW_SIZE = 10 };

    /// Sender requests ack every REQ_ACK_AFTERth packet.
    enum { REQ_ACK_AFTER = 5 };

    /// Time after last send for fragment before assuming lost/resending in ns.
    enum { TIMEOUT_NS = 10 * 1000 * 1000 }; // 10 ms

    /// Periods of TIMEOUT_NS that can pass in a row before failing this RPC.
    enum { TIMEOUTS_UNTIL_ABORTING = 500 }; // >= 5 s

    /// Time after which a Session is considered dead if no activity is seen.
    enum { SESSION_TIMEOUT_NS = 60lu * 60 * 1000 * 1000 * 1000 }; // 30 min

    /**
     * Number of cycles this CPU's TSC should increment before considering
     * a fragment to be timed out.
     */
    static uint64_t
    timeoutCycles()
    {
#if TESTING
        if (timeoutCyclesOverride)
            return timeoutCyclesOverride;
#endif
        static uint64_t value = 0;
        if (value == 0)
            value = nanosecondsToCycles(TIMEOUT_NS);
        return value;
    }

    /**
     * Number of cycles this CPU's TSC should increment before considering
     * a session to be timed out.
     */
    static uint64_t
    sessionTimeoutCycles()
    {
#if TESTING
        if (sessionTimeoutCyclesOverride)
            return sessionTimeoutCyclesOverride;
#endif
        static uint64_t value = 0;
        if (value == 0)
            value = nanosecondsToCycles(SESSION_TIMEOUT_NS);
        return value;
    }

    /**
     * Wire-format for FastTransport fragment Headers.
     */
    struct Header {
        Header()
            : sessionToken(0)
            , rpcId(0)
            , clientSessionHint(0)
            , serverSessionHint(0)
            , fragNumber(0)
            , totalFrags(0)
            , channelId(0)
            , direction(CLIENT_TO_SERVER)
            , requestAck(0)
            , pleaseDrop(0)
            , reserved1(0)
            , payloadType(DATA)
        {
        }

        /**
         * Legal values for payloadType field (a 4-bit field).
         */
        enum PayloadType {
            DATA         = 0,   ///< Payload contains user data.
            ACK          = 1,   ///< Payload contains AckResponse.
            SESSION_OPEN = 2,   ///< SessionOpenRequest or SessionOpenResponse
            RESERVED1    = 3,
            BAD_SESSION  = 4,   ///< Payload invalid, requested session bad.
            RESERVED2    = 5,
            RESERVED3    = 6,
            RESERVED4    = 7
        };

        /// Values for direction field.
        enum Direction {
            CLIENT_TO_SERVER = 0,   ///< Fragment is part of a client request.
            SERVER_TO_CLIENT = 1    ///< Fragment is part of a server response.
        };

        /**
         * Authentication token initially provided to a client by a server on
         * SessionOpenResponse.  The client must provide this token on each
         * following request reusing the same session.
         */
        uint64_t sessionToken;

        /**
         * Strictly increasing identifier to disambiguate (along with
         * sessionToken and channelId) which rpc this fragment is a part of.
         */
        uint32_t rpcId;

        /**
         * Offset into clientSessionTable of the ClientSession of this rpc.
         * Used for fast fragment to session association.  This must be
         * correct or the client will drop the packet after mismatching
         * the sessionToken.
         */
        uint32_t clientSessionHint;

        /**
         * Offset into serverSessionTable of the ServerSession of this rpc.
         * Used for fast fragment-to-session association.  This must be
         * correct or the server will drop the packet after mismatching
         * the sessionToken.  However: if payloadType is SESSION_OPEN then
         * this must be INVALID_TOKEN.
         */
        uint32_t serverSessionHint;

        /**
         * Position of this fragment in the rpc request or response starting
         * from 0 (for both request and response).
         */
        uint16_t fragNumber;

        /**
         * Total number of fragments the receiver should expect.  If
         * fragNumber == (totalFrags - 1) then this is the last fragment
         * in the request or response.
         */
        uint16_t totalFrags;

        /**
         * Which "channel" this rpc is being handled on.  Channels represent
         * concurrent rpcs being handled at a particular endpoint.
         */
        uint8_t channelId;

        /// Indicates whether going to the server or client.  See Direction.
        uint8_t direction:1;

        /// If this is set the receiver should emit an AckResponse.
        uint8_t requestAck:1;

        /**
         * This fragment should be dropped for testing and simulation purposes.
         *
         * FastTransport still sends this packet, but the recipient throws it
         * on the floor without processing it.  This allows debugging tools like
         * Wireshark to capture the packet still as compared to dropping it
         * before the packet is sent.
         */
        uint8_t pleaseDrop:1;

        /// Padding
        uint8_t reserved1:1;

        /**
         * Describes how the bytes in this fragment following this header
         * should be interpreted.  See PayloadType.
         */
        uint8_t payloadType:4;

        /// See payloadType.
        PayloadType getPayloadType()
        {
            return static_cast<PayloadType>(payloadType);
        }

        /// See direction.
        Direction getDirection()
        {
            return static_cast<Direction>(direction);
        }

        /**
         * For unit testing; convert a header in a buffer to a string.
         */
        static string headerToString(const void* header, uint32_t headerLen)
        {
            assert(headerLen == sizeof(Header));
            const Header* self = static_cast<const Header*>(header);
            return format(
                     "{ sessionToken:%lx rpcId:%u "
                     "clientSessionHint:%x serverSessionHint:%x "
                     "%u/%u frags "
                     "channel:%u "
                     "dir:%u reqACK:%u drop:%u "
                     "payloadType:%u }",
                     self->sessionToken, self->rpcId,
                     self->clientSessionHint, self->serverSessionHint,
                     self->fragNumber, self->totalFrags,
                     self->channelId,
                     self->direction, self->requestAck, self->pleaseDrop,
                     self->payloadType);
        }

        /// For unit testing; convert this header to a string.
        string toString()
        {
            return headerToString(this, sizeof(*this));
        }
    } __attribute__((packed));

    /**
     * Wire-format for response with PayloadType SESSION_OPEN.
     */
    struct SessionOpenResponse {
        /**
         * The number of channels that the client is allowed to use in
         * future RPCs on this session (channel ids must be in the
         * range [0..numChannels-1]).
         */
        uint8_t numChannels;
    } __attribute__((packed));

    /**
     * Wire-format for the ACK response payload.
     */
    struct AckResponse {
        /**
         * Convenience constructor to create a complete ACK response payload.
         *
         * \param firstMissingFrag
         *      See member documentation.
         * \param stagingVector
         *      See member documentation.
         */
        AckResponse(uint16_t firstMissingFrag,
                    uint32_t stagingVector = 0)
            : firstMissingFrag(firstMissingFrag)
            , stagingVector(stagingVector)
        {
        }

        /// See InboundMessage::firstMissingFrag.
        uint16_t firstMissingFrag;

        /**
         * Bit vector describing which frags beyond firstMissingFrag
         * have been received.  The first bit corresponds to the
         * (firstMissingFrag + 1)th fragment.
         */
        uint32_t stagingVector;
    } __attribute__((packed));

    /**
     * InboundMessage accumulates and assembles fragments into a complete
     * incoming message.
     *
     * An incoming message must first be associated (permanently) with a
     * particular Session, Channel, and timer configuration using setup().
     * To start assembling a message init() is first called to tell the
     * message object how many fragments to expect and where to place the
     * resulting data.
     *
     * From here a Channel hands fragments still wrapped by the Driver as
     * a Driver::Received to processReceivedData which takes care of the
     * details.
     *
     * Once an instance is no longer is use reset() must be called to allow
     * future reuse of the instance.
     */
    class InboundMessage {
      public:
        InboundMessage();
        ~InboundMessage();
        void setup(FastTransport* transport, Session* session,
                   uint32_t channelId, bool useTimer);
        void sendAck();
        void reset();
        void init(uint16_t totalFrags, Buffer* dataBuffer);
        bool processReceivedData(Driver::Received* received);
      private:
        /// The transport to which this message belongs.  Set by setup().
        FastTransport* transport;

        /// The session to which this message belongs.  Set by setup().
        Session* session;

        /// The channel ID to which this message belongs.  Set by setup().
        uint32_t channelId;

        /// Total number of fragments that make up this message. Set by init().
        uint32_t totalFrags;

        /// fragNumber of the earliest fragment that is still missing from
        /// the inbound message.
        uint32_t firstMissingFrag;

        /**
         * Structure to hold received fragments that can't be added to the
         * buffer yet because fragments preceding them are still missing.
         *
         * This structure holds both a pointer to the data and the length.
         * The first element in the window corresponds to the
         * (firstMissingFrag + 1)th fragment.
         *
         * The memory region covered by each pair has been stolen from the
         * Driver and responsibility is passed to higher levels by eventually
         * wrapping it as a PayloadChunk.
         *
         * If this message has reset() called on it it will return any
         * memory pointed to by these pairs to the Driver.
         */
        Window<std::pair<char*, uint32_t>,
             MAX_STAGING_FRAGMENTS> dataStagingWindow;

        /**
         * The place to accumulate the result message.  Valid and available
         * for use at higher levels once processReceivedData returns true.
         *
         * It contains fragNumbers 0 up to but not including firstMissingFrag.
         */
        Buffer* dataBuffer;

        /**
         * When invoked by the dispatcher this timer will timeout the session if
         * it is idle for too long, otherwise it will just transmit an ACK.
         */
        class Timer : public Dispatch::Timer {
          public:
            explicit Timer(InboundMessage* const inboundMsg);
            virtual void handleTimerEvent();
          private:
            /// The InboundMessage this timer sendAcks on or resets if fired.
            InboundMessage* const inboundMsg;

            DISALLOW_COPY_AND_ASSIGN(Timer);
        };

        /// Handles idle session cleanup and ACKs due to timeout.
        Timer timer;

        /// False means we don't use the timer (typically means we're the
        /// server side and timeouts are handled by the client)
        bool useTimer;

        friend class FastTransportTest;
        friend class InboundMessageTest;
        DISALLOW_COPY_AND_ASSIGN(InboundMessage);
    };

    /**
     * OutboundMessage handles sending an RPC from the initiator's side.
     *
     * An OutboundMessage must first be associated (permanently) with a
     * particular Session, Channel, and timer configuration using setup().
     *
     * Users of OutboundMessage call beginSending() and funnel subsequest ACK
     * responses to the message using processReceivedAck().  Between the ACK
     * responses and internal timers the message will ensure reliable delivery
     * of the RPC request.  If the OutboundMessage represents a request once a
     * data fragment is received from the RPC destination it should be assumed
     * that the RPC response has begun and this OutboundMessage is done.
     * The message must be reset by calling reset() to allow future calls to
     * beginSending().
     */
    class OutboundMessage {
      public:
        OutboundMessage();
        ~OutboundMessage();
        void setup(FastTransport* transport, Session* session,
                   uint32_t channelId, bool useTimer);
        void reset();
        void beginSending(Buffer* dataBuffer);
        void send();
        bool processReceivedAck(Driver::Received* received);

      private:
        void sendOneData(uint32_t fragNumber, bool forceRequestAck = false);

        /// Transport this message is associated with.
        FastTransport* transport;

        /// Session this message is associated with.
        Session* session;

        /// ID of the Channel this message is associated with.
        uint32_t channelId;

        /// The data this message is sending.
        Buffer* sendBuffer;

        /**
         * The number before which the receiving end has acknowledged receipt of
         * every fragment, in the range [0, totalFrags].
         */
        uint32_t firstMissingFrag;

        /// The total number of fragments in the message to send.
        uint32_t totalFrags;

        /**
         * The number of data packets sent on the wire since the last ACK
         * request. This is used to determine when to request the next ACK.
         */
         uint32_t packetsSinceAckReq;

        /**
         * Magic "timestamp" value used in sentTimes to indicate that the
         * corresponding fragment never needs to be retransmitted.
         */
        static const uint64_t ACKED = ~(0lu);

        /**
         * A record of when unacknowledged fragments were sent, which is useful
         * for retransmission.
         * A Window of MAX_STAGING_FRAGMENTS + 1 timestamps, where each entry
         * contains the TSC of the (firstMissingFrag + i)th packet was sent
         * (0 if it has never been sent), or ACKED if it has already been
         * acknowledged by the receiving end.
         * The 0th entry is implicitly ACKED when an AckResponse advances
         * the firstMissingFrag.  The 1th through 33th entries correspond
         * to each in the ACK bit vector the AckResponse.
         */
        Window<uint64_t, MAX_STAGING_FRAGMENTS + 1> sentTimes;

        /**
         * The total number of fragments the receiving end has acknowledged, in
         * the range [0, totalFrags]. This is used for flow control, as the
         * sender guarantees to send only fragments whose numbers are below
         * numAcked + WINDOW_SIZE.
         */
        uint32_t numAcked;

        /**
         * When invoked by the FastTransport timer code this timer will
         * timeout the session if it is idle for too long, otherwise it
         * call send() to resend packets which were sent awhile ago but
         * haven't been ACKed yet.
         */
        class Timer : public Dispatch::Timer {
          public:
            explicit Timer(OutboundMessage* const outboundMsg);
            virtual void handleTimerEvent();
          private:
            /// Message this timer resends packets for or closes when fired.
            OutboundMessage* const outboundMsg;

            DISALLOW_COPY_AND_ASSIGN(Timer);
        };

        /// Handles idle session cleanup and retransmits due to timeout.
        Timer timer;

        /// False means we don't use the timer (typically means we're the
        /// server side and timeouts are handled by the client)
        bool useTimer;

        friend class ClientSessionTest;
        friend class OutboundMessageTest;
        DISALLOW_COPY_AND_ASSIGN(OutboundMessage);
    };

    /**
     * Manages RPCs between a particular client and server and caches state
     * to create a fastpath for future RPCs.
     *
     * Sessions have a number of channels that allow multiple ongoing RPCs
     * and work queues which allows scheduling future RPC requests.
     *
     * Base class for ClientSession and ServerSession; allows InboundMessage
     * and OutboundMessage to be agnostic about which type of Session it is
     * associated with.
     */
    class Session {
      protected:
        /// Used to trash the token field; shouldn't be seen on the wire.
        static const uint64_t INVALID_TOKEN;

        /**
         * Create a session and permanently associate it with a FastTransport.
         *
         * \param transport
         *      The FastTransport this Session is associated with.
         * \param id
         *      Permanently fixes this Session's id.  See Session::id.
         */
        explicit Session(FastTransport* transport, uint32_t id)
            : id(id)
            , lastActivityTime(dispatch->currentTime)
            , transport(transport)
            , token(INVALID_TOKEN)
        {}

      public:
        /// nop
        virtual ~Session() {}

        /**
         * Abort all ongoing and queued RPCs and reset the Session in to a
         * reusable state.
         */
        virtual void close() = 0;

        /**
         * Close this ClientSession if it is not servicing any RPC.
         *
         * Called periodically to try to reclaim inactive sessions, generally
         * just before Session is allocated (e.g. before the serverSessionTable
         * is expanded, or during getClientSession()).
         *
         * \return
         *      Whether the ClientSession is now closed.
         */
        virtual bool expire() = 0;

        /**
         * Populate a Header with all the fields necessary to send it to
         * the remote endpoint associated with this Session.
         *
         * The caller will want to set several additional fields in the
         * Header such as payloadType.
         *
         * \param[out] header
         *      The Header to populate.
         * \param channelId
         *      The id of the channel sending this message.
         */
        virtual void fillHeader(Header* const header,
                                uint8_t channelId) const = 0;

        /**
         * Return the address of remote endpoint associated with this session.
         *
         * \return
         *      See method description.
         */
        virtual const Driver::Address* getAddress() = 0;

        /**
         * Returns the authentication token the client needs to succesfully
         * reassociate with a ServerSession.
         *
         * \return
         *      See method description.
         */
        uint64_t
        getToken()
        {
            return token;
        }

        /**
         * This session's offset in FastTransport::serverSessions and 
         * FastTransport::clientSessions.
         * Used to provide the remote party with a hint enabling fast
         * association of a fragment with a Session.
         */
        const uint32_t id;

        /**
         * Later of (a) TSC when last packet arrived from the peer at the
         * other end and (b) TSC of the last time we started sending a new
         * RPC request or response.
         */
        uint64_t lastActivityTime;

        /// The FastTransport this session is associated with.
        FastTransport* const transport;

      protected:
        /**
         * Authentication token provided by the server. For ClientSession
         * this to identifes the client to the server, for ServerSession
         * it is used to identify the client.
         * Must be presented in all fragment headers that are associated
         * with a Session.
         */
        uint64_t token;

      private:
        friend class PollerTest;
        DISALLOW_COPY_AND_ASSIGN(Session);
    };

    /**
     * Manages RPCs between a particular client and server from
     * the server perspective.
     */
    class ServerSession : public Session {
      public:
        ServerSession(FastTransport* transport, uint32_t sessionId);
        ~ServerSession();
        void beginSending(uint8_t channelId);
        virtual void close();
        virtual bool expire();
        virtual void fillHeader(Header* const header, uint8_t channelId) const;
        virtual const Driver::Address* getAddress();
        void processInboundPacket(Driver::Received* received);
        void startSession(const Driver::Address* clientAddress,
                          uint32_t clientSessionHint);

        /// Used to trash the hint field; shouldn't be seen on the wire except
        /// when opening a new session.
        static const uint32_t INVALID_HINT;

        /**
         * Used to maintain a linked list of free session in a
         * SessionTable.  See SessionTable for more detail.
         *
         * \bug Only public because the template friend doesn't work.
         */
        uint32_t nextFree;

      private:
        /**
         * The state assoicated with an ongoing RPC.
         */
        struct ServerChannel {
          public:
            /// Trash for rpcId for when ServerChannel is IDLE.
            enum { INVALID_RPC_ID = ~(0u) };

            /**
             * This creates broken in/out messages that are reinitialized
             * by setup().
             */
            ServerChannel()
                : currentRpc()
                , inboundMsg()
                , outboundMsg()
                , rpcId(INVALID_RPC_ID)
                , state(IDLE)
            {
            }

            /**
             * Initialize a channel and make it ready for use.
             *
             * Resets the channel to an IDLE state and resets its
             * InboundMessage and OutboundMessage.
             *
             * \param transport
             *      The FastTranport this channel is associated with.
             * \param session
             *      The session this channel is associated with.
             * \param channelId
             *      The particular channel in session this channel represents.
             */
            void setup(FastTransport* transport,
                       ServerSession* session,
                       uint32_t channelId)
            {
                state = IDLE;
                rpcId = INVALID_RPC_ID;
                currentRpc.reset();
                inboundMsg.setup(transport, session, channelId, false);
                outboundMsg.setup(transport, session, channelId, false);
            }

            /// The RPC this channel is actively servicing, or invalid if none.
            ServerRpc currentRpc;

            /// The RPC request is accumulated here.
            InboundMessage inboundMsg;

            /// The RPC response is managed here.
            OutboundMessage outboundMsg;

            /**
             * Disambiguates fragments on subsequent rpcs to the same session
             * and channel.
             *
             * This increments each time a new RPC is started on a particular
             * channel for a particular session.
             */
            uint32_t rpcId;

            /// Current state of the channel.
            enum {
                IDLE,               ///< This channel has not received an RPC
                                    ///< since the session was opened.
                RECEIVING,          ///< InboundMessage is receiving.
                PROCESSING,         ///< Request complete, response not ready.
                SENDING_WAITING,    ///< OutboundMessage transmitting (or
                                    ///< response finished and waiting for next
                                    ///< RPC.
            } state;

          private:
            DISALLOW_COPY_AND_ASSIGN(ServerChannel);
        };

        void processReceivedAck(ServerChannel* channel,
                                Driver::Received* received);
        void processReceivedData(ServerChannel* channel,
                                 Driver::Received* received);

        /**
         * The channels for this session.
         * One RPC can be serviced at a time per channel.
         */
        ServerChannel channels[NUM_CHANNELS_PER_SESSION];

        Driver::AddressPtr clientAddress;     ///< Where to send the response.

        /**
         * A token provided by the client about this session. We return this to
         * the client in responses to help it locate its information about the
         * session.
         */
        uint32_t clientSessionHint;

        friend class FastTransportTest;
        friend class PollerTest;
        // template friend doesn't work - no idea why
        template <typename T> friend class SessionTable;
        friend class ServerSessionTest;
        DISALLOW_COPY_AND_ASSIGN(ServerSession);
    };

    /**
     * Manages RPCs between a particular client and server from
     * the client perspective.
     */
    class ClientSession : public Session, public Transport::Session {
      public:
        ClientSession(FastTransport* transport, uint32_t sessionId);
        ~ClientSession();

        ClientRpc* clientSend(Buffer* request, Buffer* response);

        void close();
        void connect();
        bool expire();
        void fillHeader(Header* const header, uint8_t channelId) const;
        const Driver::Address* getAddress();
        void init(const ServiceLocator& serviceLocator);
        bool isConnected();
        void processInboundPacket(Driver::Received* received);
        void release() { expire(); }
        void sendSessionOpenRequest();

        /// Used to trash the hint field; shouldn't be seen on the wire.
        static const uint32_t INVALID_HINT;

        /**
         * Used to maintain a linked list of free sessions in a
         * SessionTable.  See SessionTable for more detail.
         *
         * \bug Only public because the template friend doesn't work.
         */
        uint32_t nextFree;

      private:
        /**
         * The state associated with an ongoing RPC.
         */
        class ClientChannel {
          public:
            /**
             * This creates broken in/out messages that are reinitialized
             * by setup().
             */
            ClientChannel()
                : currentRpc(NULL)
                , inboundMsg()
                , outboundMsg()
                , rpcId(0)
                , state(IDLE)
            {
            }

            /**
             * Initialize a channel and make it ready for use.
             *
             * Resets the channel to an IDLE state and resets its
             * InboundMessage and OutboundMessage.
             *
             * \param transport
             *      The FastTranport this channel is associated with.
             * \param session
             *      The session this channel is associated with.
             * \param channelId
             *      The particular channel in session this channel represents.
             */
            void setup(FastTransport* transport,
                       FastTransport::Session* session,
                       uint32_t channelId)
            {
                state = IDLE;
                rpcId = 0;
                currentRpc = NULL;
                bool useTimer = true;
                outboundMsg.setup(transport, session, channelId, useTimer);
                inboundMsg.setup(transport, session, channelId, useTimer);
            }

            /// The RPC this channel is actively servicing.
            ClientRpc* currentRpc;

            /// The RPC request is accumulated here.
            InboundMessage inboundMsg;

            /// The RPC response is managed here.
            OutboundMessage outboundMsg;

            /**
             * The rpcId of the active RPC.
             *
             * This increments each time a new RPC is started on a particular
             * channel for a particular session.
             */
            uint32_t rpcId;

            /// Current state of the channel.
            enum {
                IDLE,       ///< Not handling an RPC.
                /**
                 * OutboundMessage transmitting, move to RECEIVING when the first
                 * response packet is received.
                 */
                SENDING,
                RECEIVING,  ///< InboundMessage is receiving.
            } state;

          private:
            DISALLOW_COPY_AND_ASSIGN(ClientChannel);
        };

        /**
         * Handles timeouts and retries in SessionOpenRequests.
         */
        class Timer : public Dispatch::Timer {
          public:
            explicit Timer(ClientSession* session);
            virtual void handleTimerEvent();
            /**
             * The ClientSession for which we're waiting for a response
             * to a SessionOpenRequest.
             */
            ClientSession* session;
          private:
            DISALLOW_COPY_AND_ASSIGN(Timer);
        };

        /**
         * The channels for this session.
         * One RPC can be serviced at a time per channel.
         */
        ClientChannel *channels;

        INTRUSIVE_LIST_TYPEDEF(ClientRpc, channelQueueEntries) ChannelQueue;

        /**
         * Queue of ClientRpcs currently awaiting service.  This session
         * pops Rpcs from this queue and processes them channels as they
         * become free.
         */
        ChannelQueue channelQueue;

        /// Number of concurrent RPCs allowed in this session.  Zero means
        /// this session isn't connected to a server.
        uint32_t numChannels;

        Driver::AddressPtr serverAddress;     ///< Where to send requests.
        uint32_t serverSessionHint; ///< Session offset in remote SessionTable

        Timer timer; ///< Tracks timeout for SessionOpenRequests.

        /// True means we have issued a SessionOpenRequest but have not yet
        /// received an acknowledgment
        bool sessionOpenRequestInFlight;

        void allocateChannels();
        void resetChannels();
        ClientChannel* getAvailableChannel();
        void processReceivedAck(ClientChannel* channel,
                                Driver::Received* received);
        void processReceivedData(ClientChannel* channel,
                                 Driver::Received* received);
        void processSessionOpenResponse(Driver::Received* received);

        template <typename T> friend class SessionTable;
        friend class FastTransportTest;
        friend class ClientSessionTest;
        friend class ClientRpcTest;
        friend class InboundMessageTest;
        friend class OutboundMessageTest;
        DISALLOW_COPY_AND_ASSIGN(ClientSession);
    };

    /**
     * Manages Sessions.
     *
     * Internally handles allocation and reuse of Sessions and also allows
     * fast access to sessions by offset into an indexed structure.
     *
     * \tparam T
     *      The type of Sessions this table manages.  Either ServerSession
     *      or ClientSession.
     */
    template <typename T>
    class SessionTable {
      public:
        enum {
            /// A Session with this as nextFree is not itself free.
            NONE = ~(0u),
            /// A Session with this as nextFree is last Session in the free list
            TAIL = ~(0u) - 1
        };

        /**
         * Create a SessionTable for a particular FastTransport.
         *
         * \param transport
         *      The FastTransport to associate all sessions this
         *      table creates with.
         */
        explicit SessionTable(FastTransport* transport)
            : firstFree(TAIL)
            , lastCleanedIndex(0)
            , sessions()
            , transport(transport)
        {
        }

        ~SessionTable() {
            for (uint32_t i = 0; i < sessions.size(); i++)
                delete sessions[i];
        }

        /**
         * Free all existing sessions and reset the table to its initial state.
         */
        void clear() {
            for (uint32_t i = 0; i < sessions.size(); i++)
                delete sessions[i];
            sessions.clear();
            lastCleanedIndex = 0;
            firstFree = TAIL;
        }

        /**
         * Fast access to a session by a sessionHint (which is simply an
         * offset into the SessionTable).
         *
         * Note: The caller is responsible for bounds checking this otherwise
         * the access is unsafe.
         *
         * \param sessionHint
         *      Which entry in the table to return.
         * \return
         *      A pointer to the Session corresponding to sessionHint.
         */
        T* operator[](uint32_t sessionHint) const
        {
            return sessions[sessionHint];
        }

        /**
         * Return a free Session, preferably reused.
         *
         * If all existing Sessions are currently busy the SessionTable is
         * extended and a new Session is returned.
         *
         * \return
         *      A pointer to a free Session.
         */
        T* get()
        {
            uint32_t sessionHint = firstFree;
            if (sessionHint >= size()) {
                // Invalid, no free sessions, so create a new one
                sessionHint = size();
                T* session = new T(transport, sessionHint);
                session->nextFree = TAIL;
                sessions.push_back(session);
            }
            T* session = sessions[sessionHint];
            firstFree = session->nextFree;
            session->nextFree = NONE;
            return sessions[sessionHint];
        }

        /**
         * Mark session as free, returning it to the stewardship of the
         * SessionTable.
         *
         * \param session
         *      The Session which the caller is no longer making use of.
         */
        void put(T* session)
        {
            session->nextFree = firstFree;
            firstFree = session->id;
        }

        /**
         * Scan the table looking for Session whose lastActivityTime is
         * beyond sessionTimeoutCycles(), calling Session::expire() on them
         * and returning them to the free list if possible.
         *
         * This implementation checks 5 sessions to see if they are ready
         * to be reused before returning.
         */
        void expire()
        {
            const uint32_t sessionsToCheck = 5;
            uint64_t now = dispatch->currentTime;
            for (uint32_t i = 0; i < sessionsToCheck; i++) {
                lastCleanedIndex++;
                if (lastCleanedIndex >= sessions.size()) {
                    lastCleanedIndex = 0;
                    if (sessions.size() == 0)
                        break;
                }
                T* session = sessions[lastCleanedIndex];
                if (session->nextFree == NONE &&
                    (session->lastActivityTime +
                     sessionTimeoutCycles() <= now)) {
                    if (session->expire())
                        put(session);
                }
            }
        }

        /**
         * Returns the number of Sessions in the table (either busy or free).
         *
         * \return
         *      See method description.
         */
        uint32_t size()
        {
            return downCast<uint32_t>(sessions.size());
        }

      private:
        /**
         * If firstFree == TAIL then no Sessions in the table are free,
         * otherwise firstFree is the offset of the first Session in the
         * table which is free.
         */
        uint32_t firstFree;

        /// Tracks where to resume expiry probing on calls to expire().
        uint32_t lastCleanedIndex;

        /// The actual table of session pointers.
        std::vector<T*> sessions;

        /// The transport to pass to the constructor of newly created Sessions.
        FastTransport* const transport;

        friend class SessionTableTest;
        DISALLOW_COPY_AND_ASSIGN(SessionTable);
    };

    uint32_t dataPerFragment();
    uint32_t numFrags(const Buffer* dataBuffer);
    void sendBadSessionError(Header *header, const Driver::Address* address);
    void sendPacket(const Driver::Address* address,
                    Header* header, Buffer::Iterator* payload);

    /// The Driver used to send/recv packets for this FastTransport.
    Driver* const driver;

    /// Contains state for all RPCs this transport participates in as client.
    SessionTable<ClientSession> clientSessions;

    /// Contains state for all RPCs this transport participates in as server.
    SessionTable<ServerSession> serverSessions;

    // If non-zero, overrides the value of timeoutCycles during tests.
    static uint64_t timeoutCyclesOverride;

    // If non-zero, overrides the value of sessionTimeoutCycles during tests.
    static uint64_t sessionTimeoutCyclesOverride;

    friend class FastTransportTest;
    friend class PollerTest;
    friend class ClientRpcTest;
    friend class SessionTableTest;
    friend class InboundMessageTest;
    friend class OutboundMessageTest;
    friend class ServerSessionTest;
    friend class ClientSessionTest;
    friend class MockReceived;
    friend class Services;
    friend class Poller;
    DISALLOW_COPY_AND_ASSIGN(FastTransport);
};

}  // namespace RAMCloud

#endif
