/* Copyright (c) 2015-2017 Stanford University
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

#ifndef RAMCLOUD_BASICTRANSPORT_H
#define RAMCLOUD_BASICTRANSPORT_H

#include <deque>

#if __cplusplus >= 201402L
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include "flat_hash_map.h"
#pragma GCC diagnostic pop
#endif
#include "BoostIntrusive.h"
#include "Buffer.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "Driver.h"
#include "ServerRpcPool.h"
#include "ServiceLocator.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * This class implements a simple transport that uses the Driver mechanism
 * for datagram-based packet delivery.
 */
class BasicTransport : public Transport {
  PRIVATE:
    struct DataHeader;

  public:
    explicit BasicTransport(Context* context, const ServiceLocator* locator,
            Driver* driver, bool driverOwner, uint64_t clientId);
    ~BasicTransport();

    string getServiceLocator();
    Transport::SessionRef getSession(const ServiceLocator* serviceLocator,
            uint32_t timeoutMs = 0) {
        // Note: DispatchLock not needed here, since this doesn't access
        // any transport or driver state.
        return new Session(this, serviceLocator, timeoutMs);
    }
    void registerMemory(void* base, size_t bytes) {
        driver->registerMemory(base, bytes);
    }

  PRIVATE:
    /// As of 08/17, std::unordered_map is significantly slower than
    /// ska::flat_hash_map. According to the Perf benchmark, inserting
    /// +deleting an entry and lookup in an ska::flat_hash_map take 15 ns
    /// and 4 ns, respectively (vs. 80 ns and 15 ns using std::unordered_map).
    template<typename K, typename V, typename H = std::hash<K>>
#if __cplusplus >= 201402L
    // ska::flat_hash_map requires c++14 features to compile.
    using HashMap = ska::flat_hash_map<K, V, H>;
#else
    using HashMap = std::unordered_map<K, V, H>;
#endif

    /**
     * A unique identifier for an RPC.
     */
    struct RpcId {
        uint64_t clientId;           // Uniquely identifies the client for
                                     // this request.
        uint64_t sequence;           // Sequence number for this RPC (unique
                                     // for clientId, monotonically increasing).

        RpcId(uint64_t clientId, uint64_t sequence)
            : clientId(clientId)
            , sequence(sequence)
        {}

        /**
         * Comparison function for RpcIds, for use in std::maps etc.
         */
        bool operator<(RpcId other) const
        {
            return (clientId < other.clientId)
                    || ((clientId == other.clientId)
                    && (sequence < other.sequence));
        }

        /**
         * Equality function for RpcIds, for use in std::unordered_maps etc.
         */
        bool operator==(RpcId other) const
        {
            return ((clientId == other.clientId)
                    && (sequence == other.sequence));
        }

        /**
         * This class computes a hash of an RpcId, so that RpcIds can
         * be used as keys in unordered_maps.
         */
        struct Hasher {
            std::size_t operator()(const RpcId& rpcId) const {
                std::size_t h1 = std::hash<uint64_t>()(rpcId.clientId);
                std::size_t h2 = std::hash<uint64_t>()(rpcId.sequence);
                return h1 ^ (h2 << 1);
            }
        };
    } __attribute__((packed));

    /**
     * This class represents the client side of the connection between a
     * particular client in a particular server. Each session can support
     * multiple outstanding RPCs.
     */
    class Session : public Transport::Session {
      public:
        virtual ~Session();
        void abort();
        void cancelRequest(RpcNotifier* notifier);
        string getRpcInfo();
        virtual void sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier);

      PRIVATE:
        // Transport associated with this session.
        BasicTransport* t;

        // Address of the target server to which RPCs will be sent. This is
        // dynamically allocated and must be freed by the session.
        Driver::Address* serverAddress;

        // True means the abort method has been invoked, sso this session
        // is no longer usable.
        bool aborted;

        Session(BasicTransport* t, const ServiceLocator* locator,
                uint32_t timeoutMs);

        friend class BasicTransport;
        DISALLOW_COPY_AND_ASSIGN(Session);
    };

    /**
     * An object of this class stores the state for a multi-packet
     * message for which at least one packet has been received. It is
     * used both for request messages on the server and for response
     * messages on the client. Not used for single-packet messages.
     */
    class MessageAccumulator {
      public:
        MessageAccumulator(BasicTransport* t, Buffer* buffer,
                uint32_t totalLength);
        ~MessageAccumulator();
        bool addPacket(DataHeader *header, uint32_t length);
        uint32_t requestRetransmission(BasicTransport *t,
                const Driver::Address* address, RpcId rpcId,
                uint32_t grantOffset, uint8_t whoFrom);

        /// Transport that is managing this object.
        BasicTransport* t;

        /// Used to assemble the complete message. It holds all of the
        /// data that has been received for the message so far, up to the
        /// first byte that has not yet been received.
        Buffer* buffer;

        /// Describes a portion of an incoming message.
        struct MessageFragment {
            /// Address of first byte of a DATA packet, as returned by
            /// Driver::steal.
            DataHeader *header;

            /// # bytes of message data available at payload.
            uint32_t length;

            MessageFragment()
                    : header(NULL), length(0)
            {}
            MessageFragment(DataHeader *header, uint32_t length)
                    : header(header), length(length)
            {}
        };

        /// This map stores information about packets that have been
        /// received but cannot yet be added to buffer because one or
        /// more preceding packets have not yet been received. Each
        /// key is an offset in the message; each value describes the
        /// corresponding fragment, which is a stolen Driver::Received.
        typedef HashMap<uint32_t, MessageFragment> FragmentMap;
        FragmentMap fragments;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(MessageAccumulator);
    };

    /**
     * An object of this class stores the state for an incoming message that
     * requires scheduling. It is used both for request messages on the server
     * and for response messages on the client. Not used for messages that can
     * be sent unilaterally.
     */
    class ScheduledMessage {
      public:
        ScheduledMessage(RpcId rpcId, MessageAccumulator* accumulator,
                uint32_t unscheduledBytes, const Driver::Address* senderAddress,
                uint32_t totalLength, uint8_t whoFrom);
        ~ScheduledMessage();

        /// Holds state of partially-received multi-packet messages.
        const MessageAccumulator* const accumulator;

        /// Offset from the most recent GRANT packet we have sent for this
        /// incoming message, or # unscheduled bytes in this message if we
        /// haven't sent any GRANTs.
        uint32_t grantOffset;

        /// Unique identifier for the RPC this message belongs to.
        RpcId rpcId;

        /// Network address of the message sender.
        const Driver::Address* senderAddress;

        /// Total # bytes in the message.
        const uint32_t totalLength;

        /// Must be either FROM_CLIENT, indicating that this message is the
        /// request from a client, or FROM_SERVER, indicating that this is
        /// the response from a server.
        const uint8_t whoFrom;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ScheduledMessage);
    };

    class ClientRpc;
    class ServerRpc;
    /**
     * An outgoing message that is either the request of a ClientRpc or the
     * response of a ServerRpc.
     */
    class OutgoingMessage {
      public:
        /// True if we have not transmitted every byte up to transmitLimit
        /// (and this object is linked in t->activeOutgoingMessages).
        bool active;

        /// Holds the contents of the message.
        Buffer* buffer;

        /// This message is part of either a request or a response. Exactly
        /// one of the two variables below will be non-NULL; it refers to data
        /// about the containing RPC.
        ClientRpc* clientRpc;
        ServerRpc* serverRpc;

        /// Offset within the message of the next byte we should transmit to
        /// the recipient; all preceding bytes have already been sent.
        uint32_t transmitOffset;

        /// The number of bytes in the message that it's OK for us to transmit.
        /// Bytes after this cannot be transmitted until we receive a GRANT for
        /// them. Must be always smaller than or equal to the message size.
        uint32_t transmitLimit;

        /// Cycles::rdtsc time of the most recent time that we transmitted
        /// data bytes of the message.
        uint64_t lastTransmitTime;

        /// Used to link this object into t->activeOutgoingMessages.
        IntrusiveListHook outgoingMessageLinks;

        /// # bytes that can be sent unilaterally.
        uint32_t unscheduledBytes;

        OutgoingMessage(ClientRpc* clientRpc, ServerRpc* serverRpc,
                BasicTransport* t, Buffer* buffer)
            : active(false)
            , buffer(buffer)
            , clientRpc(clientRpc)
            , serverRpc(serverRpc)
            , transmitOffset(0)
            , transmitLimit()
            , lastTransmitTime(0)
            , outgoingMessageLinks()
            , unscheduledBytes(t->roundTripBytes)
        {}

        virtual ~OutgoingMessage() {}
        void activate(BasicTransport* t);

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(OutgoingMessage);
    };

    /**
     * One object of this class exists for each outgoing RPC; it is used
     * to track the RPC through to completion.
     */
    class ClientRpc {
      public:
        /// The ClientSession on which to send/receive the RPC.
        Session* session;

        /// Request message for the RPC.
        OutgoingMessage request;

        /// Will eventually hold the response for the RPC.
        Buffer* response;

        /// Use this object to report completion.
        RpcNotifier* notifier;

        /// Unique identifier for this RPC.
        RpcId rpcId;

        /// Number of times that the transport timer has fired since we
        /// received any packets from the server.
        uint32_t silentIntervals;

        /// True means that the request message is in the process of being
        /// transmitted (and this object is linked on t->outgoingRequests).
        bool transmitPending;

        /// Holds state of partially-received multi-packet responses. Empty
        /// if the response is single-packet.
        Tub<MessageAccumulator> accumulator;

        /// Holds state of response messages that require scheduling. Empty
        /// if the response is unscheduled.
        Tub<ScheduledMessage> scheduledMessage;

        /// Used to link this object into t->outgoingRequests.
        IntrusiveListHook outgoingRequestLinks;

        ClientRpc(Session* session, uint64_t sequence, Buffer* request,
                Buffer* response, RpcNotifier* notifier)
            : session(session)
            , request(this, NULL, session->t, request)
            , response(response)
            , notifier(notifier)
            , rpcId(session->t->clientId, sequence)
            , silentIntervals(0)
            , transmitPending(true)
            , accumulator()
            , scheduledMessage()
            , outgoingRequestLinks()
        {}

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

    /**
     * Holds server-side state for an RPC.
     */
    class ServerRpc : public Transport::ServerRpc {
      public:
        void sendReply();
        string getClientServiceLocator();

        /// The transport that will be used to deliver the response when
        /// the RPC completes.
        BasicTransport* t;

        /// Uniquely identifies this RPC among all RPCs ever received by
        /// this server. This is the *server's* sequence number; the client's
        /// sequence number is in rpcId.
        uint64_t sequence;

        /// True if the RPC has been cancelled by the client. This is only
        /// neccessary for the case where an RPC cannot be deleted immediately
        /// (e.g., it's being executed); we use this flag to indicate that
        /// this RPC should be removed at the server's earliest convenience.
        bool cancelled;

        /// Address of the client to which the RPC response will be sent.
        /// Not owned by this class.
        const Driver::Address* clientAddress;

        /// Unique identifier for this RPC.
        RpcId rpcId;

        /// Number of times that the transport timer has fired since we
        /// received any packets from the client.
        uint32_t silentIntervals;

        /// True means we have received the entire request message, so either
        /// we're processing the request or we're sending the response now.
        bool requestComplete;

        /// True means we have finished processing the request and are trying
        /// to send a response.
        bool sendingResponse;

        /// Holds state of partially-received multi-packet requests. Empty if
        /// the request is single-packet.
        Tub<MessageAccumulator> accumulator;

        /// Response message for the RPC.
        OutgoingMessage response;

        /// Holds state of the request message that requires scheduling. Empty
        /// if the request is unscheduled.
        Tub<ScheduledMessage> scheduledMessage;

        /// Used to link this object into t->serverTimerList.
        IntrusiveListHook timerLinks;

        /// Used to link this object into t->outgoingResponses.
        IntrusiveListHook outgoingResponseLinks;

        ServerRpc(BasicTransport* transport, uint64_t sequence,
                const Driver::Address* clientAddress, RpcId rpcId)
            : t(transport)
            , sequence(sequence)
            , cancelled(false)
            , clientAddress(clientAddress)
            , rpcId(rpcId)
            , silentIntervals(0)
            , requestComplete(false)
            , sendingResponse(false)
            , accumulator()
            , response(NULL, this, transport, &replyPayload)
            , scheduledMessage()
            , timerLinks()
            , outgoingResponseLinks()
        {}

        DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    /**
     * This enum defines the opcode field values for packets. See the
     * xxxHeader class definitions below for more information about each
     * kind of packet
     */
    enum PacketOpcode {
        ALL_DATA               = 20,
        DATA                   = 21,
        GRANT                  = 22,
        LOG_TIME_TRACE         = 23,
        RESEND                 = 24,
        BUSY                   = 25,
        ABORT                  = 26,
        BOGUS                  = 27,      // Used only in unit tests.
        // If you add a new opcode here, you must also do the following:
        // * Change BOGUS so it is the highest opcode
        // * Add support for the new opcode in opcodeSymbol and headerToString
    };

    /**
     * Describes the wire format for header fields that are common to all
     * packet types.
     */
    struct CommonHeader {
        uint8_t opcode;              // One of the values of PacketOpcode.
        RpcId rpcId;                 // RPC associated with this packet.
        uint8_t flags;               // ORed competition a flag bits; see
                                     // below for definitions.
        CommonHeader(PacketOpcode opcode, RpcId rpcId, uint8_t flags)
            : opcode(opcode), rpcId(rpcId), flags(flags) {}
    } __attribute__((packed));

    // Bit values for CommonHeader flags; not all flags are valid for all
    // opcodes.
    // FROM_CLIENT:              Valid for all opcodes. Non-zero means this
    //                           packet was sent from client to server; zero
    //                           means it was sent from server to client.
    // FROM_SERVER:              Opposite of FROM_CLIENT; provided to make
    //                           code more readable.
    // RETRANSMISSION:           Used only in DATA packets. Nonzero means
    //                           this packet is being sent in response to a
    //                           RESEND request (it has already been sent
    //                           previously).
    // RESTART:                  Used only in RESEND packets: indicates that
    //                           the server has no knowledge of this request,
    //                           so the client should reset its state to
    //                           indicate that everything needs to be resent.
    static const uint8_t FROM_CLIENT =    1;
    static const uint8_t FROM_SERVER =    0;
    static const uint8_t RETRANSMISSION = 2;
    static const uint8_t RESTART =        4;

    /**
     * Describes the wire format for an ALL_DATA packet, which contains an
     * entire request or response message.
     */
    struct AllDataHeader {
        CommonHeader common;         // Common header fields.
        uint16_t messageLength;      // Total # bytes in the message (i.e.,
                                     // the bytes following this header).

        // The remaining packet bytes after the header constitute the
        // entire request or response message.

        explicit AllDataHeader(RpcId rpcId, uint8_t flags,
                uint16_t messageLength)
            : common(PacketOpcode::ALL_DATA, rpcId, flags)
            , messageLength(messageLength) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for a DATA packet, which contains a
     * portion of a request or response message
     */
    struct DataHeader {
        CommonHeader common;         // Common header fields.
        uint32_t totalLength;        // Total # bytes in the message (*not*
                                     // this packet!).
        uint32_t offset;             // Offset within the message of the first
                                     // byte of data in this packet.
        uint32_t unscheduledBytes;   // # unscheduled bytes in the message.

        // The remaining packet bytes after the header constitute message
        // data starting at the given offset.

        DataHeader(RpcId rpcId, uint32_t totalLength, uint32_t offset,
                uint32_t unscheduledBytes, uint8_t flags)
            : common(PacketOpcode::DATA, rpcId, flags)
            , totalLength(totalLength)
            , offset(offset)
            , unscheduledBytes(unscheduledBytes)
        {}
    } __attribute__((packed));

    /**
     * Describes the wire format for GRANT packets. A GRANT is sent by
     * the receiver back to the sender to indicate that it is now safe
     * for the sender to transmit a given range of bytes in the message.
     * This packet type is used for flow control.
     */
    struct GrantHeader {
        CommonHeader common;         // Common header fields.
        uint32_t offset;             // Byte offset within the message; the
                                     // sender should now transmit all data up
                                     // to (but not including) this offset, if
                                     // it hasn't already.

        GrantHeader(RpcId rpcId, uint32_t offset, uint8_t flags)
            : common(PacketOpcode::GRANT, rpcId, flags), offset(offset) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for RESEND packets. A RESEND is sent by
     * the receiver back to the sender when it believes that some of the
     * message data was lost in transmission. The receiver should resend
     * the specified portion of the message, even if it already sent
     * it before.
     */
    struct ResendHeader {
        CommonHeader common;         // Common header fields.
        uint32_t offset;             // Byte offset within the message of the
                                     // first byte of data that should be
                                     // retransmitted.
        uint32_t length;             // Number of bytes of data to retransmit;
                                     // this could specify a range longer than
                                     // the total message size.

        ResendHeader(RpcId rpcId, uint32_t offset, uint32_t length,
                uint8_t flags)
            : common(PacketOpcode::RESEND, rpcId, flags), offset(offset),
              length(length) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for LOG_TIME_TRACE packets. These packets
     * are only used for debugging and performance analysis: the recipient
     * will write its time trace to the log.
     */
    struct LogTimeTraceHeader {
        CommonHeader common;         // Common header fields.

        explicit LogTimeTraceHeader(RpcId rpcId, uint8_t flags)
            : common(PacketOpcode::LOG_TIME_TRACE, rpcId, flags) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for BUSY packets. These packets are used
     * to let the recipient know that the sender is still alive; they
     * don't trigger any actions on the receiver except resetting timers.
     */
    struct BusyHeader {
        CommonHeader common;         // Common header fields.

        explicit BusyHeader(RpcId rpcId, uint8_t flags)
            : common(PacketOpcode::BUSY, rpcId, flags) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for ABORT packets. These packets are used
     * to let the server know that the client has cancelled the RPC. They
     * are necessary to avoid spurious warning messages in the log.
     */
    struct AbortHeader {
        CommonHeader common;         // Common header fields.

        explicit AbortHeader(RpcId rpcId)
            : common(PacketOpcode::ABORT, rpcId, FROM_CLIENT) {}
    } __attribute__((packed));

    /**
     * Causes BasicTransport to be invoked during each iteration through
     * the dispatch poller loop.
     */
    class Poller : public Dispatch::Poller {
      public:
        explicit Poller(Context* context, BasicTransport* t)
            : Dispatch::Poller(context->dispatch, "BasicTransport(" +
                    t->driver->getServiceLocator() + ")::Poller")
            , t(t)
            , lastPollTime(0) { }
        virtual int poll();
      private:
        /// Transport on whose behalf this poller operates.
        BasicTransport* t;

        /// The most recent time that we polled the receive queue of the NIC,
        /// in rdtsc ticks. Only used for diagnostic time tracing.
        uint64_t lastPollTime;
        DISALLOW_COPY_AND_ASSIGN(Poller);
    };

  PRIVATE:
    void checkTimeouts();
    void deleteClientRpc(ClientRpc* clientRpc);
    void deleteServerRpc(ServerRpc* serverRpc);
    uint32_t getRoundTripBytes(const ServiceLocator* locator);
    void handlePacket(Driver::Received* received);
    static string headerToString(const void* header, uint32_t headerLength);
    static string opcodeSymbol(uint8_t opcode);
    uint32_t sendBytes(const Driver::Address* address, RpcId rpcId,
            Buffer* message, uint32_t offset, uint32_t maxBytes,
            uint32_t unscheduedBytes, uint8_t flags);
    template<typename T>
    void sendControlPacket(const Driver::Address* recipient, const T* packet);
    uint32_t tryToTransmitData();

    /// Shared RAMCloud information.
    Context* context;

    /// The Driver used to send and receive packets.
    Driver* driver;

    /// Is this transport the owner of #driver? If yes, free the driver upon
    /// destruction.
    bool driverOwner;

    /// Service locator string of this transport.
    string locatorString;

    /// Allows us to get invoked during the dispatch polling loop.
    Poller poller;

    /// Maximum # bytes of message data that can fit in one packet.
    CONST uint32_t maxDataPerPacket;

    /// Maximum # bytes of a message that we consider as small. For small
    /// messages, the tryToTransmitData mechanism takes more time then just
    /// transmitting the packet. In order to be efficient on workloads with
    /// lots of short messages, we only use tryToTransmitData for messages
    /// of nontrivial length.
    CONST uint32_t smallMessageThreshold;

    /// Unique identifier for this client (used to provide unique
    /// identification for RPCs).
    uint64_t clientId;

    /// The sequence number to use in the next outgoing RPC (i.e., one
    /// higher than the highest number ever used in the past).
    uint64_t nextClientSequenceNumber;

    /// The sequence number to use for the next incoming RPC (i.e., one
    /// higher than the highest number ever used in the past).
    uint64_t nextServerSequenceNumber;

    /// Holds incoming packets received from the driver. This is only
    /// used temporarily during the poll method, but it's allocated here
    /// so that we only pay the cost for storage allocation once (don't
    /// want to reallocate space in every call to poll). Always empty,
    /// except when the poll method is executing.
    std::vector<Driver::Received> receivedPackets;

    /// Holds incoming messages we are about to grant. Always empty, except
    /// when the poll method is receiving and processing incoming packets.
    std::vector<ScheduledMessage*> messagesToGrant;

    /// Pool allocator for our ServerRpc objects.
    ServerRpcPool<ServerRpc> serverRpcPool;

    /// Pool allocator for ClientRpc objects.
    ObjectPool<ClientRpc> clientRpcPool;

    /// Holds RPCs for which we are the client, and for which a
    /// response has not yet been completely received (we have sent
    /// at least part of the request, but not necessarily the entire
    /// request yet). Keys are RPC sequence numbers.
    typedef HashMap<uint64_t, ClientRpc*> ClientRpcMap;
    ClientRpcMap outgoingRpcs;

    /// Holds RPCs for which we are the client, and for which the
    /// request message has not yet been completely transmitted (once
    /// the last byte of the request has been transmitted for the first
    /// time, the RPC is removed from this map). An RPC may be in both
    /// this list and outgoingRpcs simultaneously.
    INTRUSIVE_LIST_TYPEDEF(ClientRpc, outgoingRequestLinks)
            OutgoingRequestList;
    OutgoingRequestList outgoingRequests;

    /// List of active outgoing messages (i.e., those with grants available)
    /// sorted in ascending order of remaining bytes.
    INTRUSIVE_LIST_TYPEDEF(OutgoingMessage, outgoingMessageLinks)
            OutgoingMessageList;
    OutgoingMessageList activeOutgoingMessages;

    /// An RPC is in this map if (a) is one for which we are the server,
    /// (b) at least one byte of the request message has been received, and
    /// (c) the last byte of the response message has not yet been passed
    /// to the driver.  Note: this map could get large if the server gets
    /// backed up, so that there are a large number of RPCs that have been
    /// received but haven't yet been assigned to worker threads.
    typedef HashMap<RpcId, ServerRpc*, RpcId::Hasher> ServerRpcMap;
    ServerRpcMap incomingRpcs;

    /// Holds RPCs for which we are the server, and whose response is
    /// partially transmitted (the response is ready to be sent, but the
    /// last byte has not yet been sent). An RPC may be in both
    /// this list and incomingRpcs simultaneously.
    INTRUSIVE_LIST_TYPEDEF(ServerRpc, outgoingResponseLinks)
            OutgoingResponseList;
    OutgoingResponseList outgoingResponses;

    /// Subset of the objects in incomingRpcs that require monitoring by
    /// the timer. We keep this as a separate list so that the timer doesn't
    /// have to consider RPCs currently being executed (which could be a
    /// very large number if the server is overloaded).
    INTRUSIVE_LIST_TYPEDEF(ServerRpc, timerLinks) ServerTimerList;
    ServerTimerList serverTimerList;

    /// The number of bytes corresponding to a round-trip time between
    /// two machines.  This serves two purposes. First, senders may
    /// transmit this many initial bytes without receiving a GRANT; this
    /// hides the round-trip time for receiving a GRANT, thereby minimizing
    /// latency. Second, the receiver uses this to pace GRANT messages (it
    /// tries to keep at least this many bytes of unreceived data granted
    /// at all times, in order to utilize the full network bandwidth).
    uint32_t roundTripBytes;

    /// How many bytes to extend the granted range in each new GRANT;
    /// a larger value avoids the overhead of sending and receiving
    /// GRANTS, but it can result in additional buffering in the network.
    uint32_t grantIncrement;

    /// Specifies the interval between calls to checkTimeouts, in units
    /// of rdtsc ticks.
    uint64_t timerInterval;

    /// At this Cycles::rdtsc time we'll start looking for a convenient
    /// opportunity to call checkTimeouts.
    uint64_t nextTimeoutCheck;

    /// At this Cycles::rdtsc time we'll call checkTimeouts even if it's
    /// not convenient. 0 means we haven't yet set a deadline (because we
    /// haven't reached nextTimeoutCheck).
    uint64_t timeoutCheckDeadline;

    /// If either client or server experiences this many timer wakeups without
    /// receiving any packets from the other end, then it will abort the
    /// request.
    uint32_t timeoutIntervals;

    /// If a client experiences this many timer wakeups without receiving
    /// any packets from the server for particular RPC, then it sends a
    /// RESEND request, assuming the response was lost.
    uint32_t pingIntervals;

    DISALLOW_COPY_AND_ASSIGN(BasicTransport);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_BASICTRANSPORT_H
