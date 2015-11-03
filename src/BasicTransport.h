/* Copyright (c) 2015 Stanford University
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

#include "BoostIntrusive.h"
#include "Buffer.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "Driver.h"
#include "ServerRpcPool.h"
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
    explicit BasicTransport(Context* context, Driver* driver,
            uint64_t clientId);
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
        MessageAccumulator(BasicTransport* t, Buffer* buffer);
        ~MessageAccumulator();
        void addPacket(Driver::Received* received, DataHeader *header);
        void appendFragment(char* payload, uint32_t offset, uint32_t length);
        void requestRetransmission(BasicTransport *t,
                const Driver::Address* address, RpcId rpcId,
                uint32_t limit);

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
            char* payload;

            /// # of bytes of message data available at payload.
            uint32_t length;

            MessageFragment()
                    : payload(NULL), length(0)
            {}
            MessageFragment(char* payload, uint32_t length)
                    : payload(payload), length(length)
            {}
        };

        /// This map stores information about packets that have been
        /// received but cannot yet be added to buffer because one or
        /// more preceding packets have not yet been received. Each
        /// key is an offset in the message; each value describes the
        /// corresponding fragment, which is a stolen Driver::Received.
        typedef std::map<uint32_t, MessageFragment>FragmentMap;
        FragmentMap fragments;

        /// Offset into the message of the most recent GRANT packet
        /// we have sent (i.e., we've already authorized the sender to
        /// transmit bytes up to this point in the message).
        uint32_t grantOffset;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(MessageAccumulator);
    };

    /**
     * One object of this class exists for each outgoing RPC; it is used
     * to track the RPC through to completion.
     */
    struct ClientRpc {
        /// The ClientSession on which to send/receive the RPC.
        Session* session;

        /// Request message for the RPC.
        Buffer* request;

        /// Will eventually hold the response for the RPC.
        Buffer* response;

        /// Use this object to report completion.
        RpcNotifier* notifier;

        /// Offset within the request message of the next byte we should
        /// transmit to the server; all preceding bytes have already
        /// been sent.
        uint32_t transmitOffset;

        /// Offset from the most recent GRANT packet we have sent for
        /// this message, or 0 if we haven't sent any GRANTs.
        uint32_t grantOffset;

        /// Number of times that the transport timer has fired since we
        /// received any packets from the server.
        uint32_t silentIntervals;

        /// Holds state of partially-received multi-packet responses.
        Tub<MessageAccumulator> accumulator;

        ClientRpc(Session* session, Buffer* request,
                Buffer* response, RpcNotifier* notifier)
            : session(session)
            , request(request)
            , response(response)
            , notifier(notifier)
            , transmitOffset(0)
            , grantOffset(0)
            , silentIntervals(0)
            , accumulator()
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

        /// Where to send the response once the RPC has executed.
        const Driver::Address* clientAddress;

        /// Unique identifier for this RPC.
        RpcId rpcId;

        /// Offset within the response message of the next byte we should
        /// transmit to the client; all preceding bytes have already
        /// been sent. 0 means we have not yet started sending the response.
        uint32_t transmitOffset;

        /// Offset from the most recent GRANT packet we have sent for
        /// the request message, or 0 if we haven't sent any GRANTs.
        uint32_t grantOffset;

        /// Number of times that the transport timer has fired since we
        /// received any packets from the client.
        uint32_t silentIntervals;

        /// True means we have received the entire request message, so either
        /// we're processing the request or we're sending the response now.
        bool requestComplete;

        /// Holds state of partially-received multi-packet requests.
        Tub<MessageAccumulator> accumulator;

        /// Used to link this object into t->serverTimerList.
        IntrusiveListHook timerLinks;

        ServerRpc(BasicTransport* transport,
                const Driver::Address* clientAddress, RpcId rpcId)
            : t(transport)
            , clientAddress(clientAddress)
            , rpcId(rpcId)
            , transmitOffset(0)
            , grantOffset(0)
            , silentIntervals(0)
            , requestComplete(false)
            , accumulator()
            , timerLinks()
        {}

        DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    /**
     * An object of this class is passed to the driver and used by the
     * driver to deliver incoming packets to us.
     */
    class IncomingPacketHandler : public Driver::IncomingPacketHandler {
      public:
        explicit IncomingPacketHandler(BasicTransport* t) : t(t) {}
        void handlePacket(Driver::Received* received);
        BasicTransport* t;
        DISALLOW_COPY_AND_ASSIGN(IncomingPacketHandler);
    };

    /**
     * One object of this class is created for each BasicTransport; it
     * is invoked at regular intervals to handle timer-driven issues such
     * as requests for retransmission and aborts after timeouts.
     */
    class Timer : public Dispatch::Timer {
      public:
        explicit Timer(BasicTransport* t, Dispatch* dispatch);
        virtual ~Timer() {}
        virtual void handleTimerEvent();

        // The transport on whose behalf this timer operates.
        BasicTransport* t;

        DISALLOW_COPY_AND_ASSIGN(Timer);
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
        RESEND                 = 23,
        PING                   = 24,
        RETRY                  = 25,
    };

    /**
     * Describes the wire format for header fields that are common to all
     * packet types.
     */
    struct CommonHeader {
        uint8_t opcode;              // One of the values of PacketOpcode.
        RpcId rpcId;                 // RPC associated with this packet.
        CommonHeader(PacketOpcode opcode, RpcId rpcId)
            : opcode(opcode), rpcId(rpcId) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for an ALL_DATA packet, which contains an
     * entire request or response message.
     */
    struct AllDataHeader {
        CommonHeader common;         // Common header fields.

        // The remaining packet bytes after the header constitute the
        // entire request or response message.

        explicit AllDataHeader(RpcId rpcId)
            : common(PacketOpcode::ALL_DATA, rpcId) {}
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
        uint8_t needGrant;           // Zero means the sender is transmitting
                                     // the entire message unilaterally;
                                     // nonzero means the last part of the
                                     // message won't be sent without a
                                     // GRANT from the server.

        // The remaining packet bytes after the header constitute message
        // data starting at the given offset.

        DataHeader(RpcId rpcId, uint32_t totalLength, uint32_t offset,
                uint8_t needGrant)
            : common(PacketOpcode::DATA, rpcId),
            totalLength(totalLength), offset(offset), needGrant(needGrant) {}
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

        GrantHeader(RpcId rpcId, uint32_t offset)
            : common(PacketOpcode::GRANT, rpcId), offset(offset) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for RESEND packets. A RESEND is sent by
     * the receiver back to the sender when it believes that some of the
     * message data was lost in transmission. The receiver should resend
     * the specified portion of the message, even if it is already sent
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

        ResendHeader(RpcId rpcId, uint32_t offset, uint32_t length)
            : common(PacketOpcode::RESEND, rpcId), offset(offset),
              length(length) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for PING packets. A PING is sent by a client
     * if a long time elapses and the client has not received any portion of
     * the response for an RPC. If the server cannot yet send a response,
     * it should respond with a STILL_WORKING packet.
     */
    struct PingHeader {
        CommonHeader common;         // Common header fields.

        explicit PingHeader(RpcId rpcId)
            : common(PacketOpcode::PING, rpcId) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for RETRY packets. A RETRY is sent by
     * the server to the client; it indicates that the client should restart
     * the RPC from scratch (throw away any existing state about the
     * transmission of the request and/or response). It can happen under
     * various conditions, such as when a client requests retransmission
     * of the result message but the server has garbage-collected the RPC
     * and discarded that information.
     */
    struct RetryHeader {
        CommonHeader common;         // Common header fields.

        explicit RetryHeader(RpcId rpcId)
            : common(PacketOpcode::RETRY, rpcId) {}
    } __attribute__((packed));

  PRIVATE:
    void deleteServerRpc(ServerRpc* serverRpc);
    static string opcodeSymbol(uint8_t opcode);
    void sendBytes(const Driver::Address* address, RpcId rpcId,
            Buffer* message, int offset, int length);

    /// Shared RAMCloud information.
    Context* context;

    /// The Driver used to send and receive packets.
    Driver* driver;

    /// Maximum # bytes of message data that can fit in one packet.
    int maxDataPerPacket;

    /// Unique identifier for this client (used to provide unique
    /// identification for RPCs).
    uint64_t clientId;

    /// The sequence number to use in the next RPC (i.e., one higher than
    /// the highest number ever used in the past).
    uint64_t nextSequenceNumber;

    /// Pool allocator for our ServerRpc objects.
    ServerRpcPool<ServerRpc> serverRpcPool;

    /// Pool allocator for ClientRpc objects.
    ObjectPool<ClientRpc> clientRpcPool;

    /// Holds RPCs for which we are the client, and for which a
    /// response has not yet been completely received (we have sent
    /// at least part of the request, but not necessarily the entire
    /// request yet). Keys are RPC sequence numbers. Note: as of
    /// 10/2015, maps are faster than unordered_maps if they hold
    /// fewer than about 20 objects.
    typedef std::map<uint64_t, ClientRpc*> ClientRpcMap;
    ClientRpcMap outgoingRpcs;

    /// An RPC is in this map if (a) is one for which we are the server,
    /// (b) at least one byte of the request message has been received, and
    /// (c) the last byte of the response message has not yet been passed
    /// to the driver.  Note: this map could get large if the server gets
    /// backed up, so that there are a large number of RPCs that have been
    /// receive but haven't yet been assigned to worker threads.
    typedef std::unordered_map<RpcId, ServerRpc*, RpcId::Hasher> ServerRpcMap;
    ServerRpcMap incomingRpcs;

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

    /// Used to implement functionality triggered by time, such as retries
    /// when packets are lost.
    Timer timer;

    /// Specifies the interval between timer wakeups, in units of rdtsc ticks.
    uint64_t timerInterval;

    /// If either client or server experiences this many timer wakeups without
    /// receiving any packets from the other end, then it will abort the
    /// request.
    uint32_t timeoutIntervals;

    /// If a client experiences this many timer wakeups without receiving
    /// any packets from the server for particular RPC, then it sends a
    /// PING request.
    uint32_t pingIntervals;

    DISALLOW_COPY_AND_ASSIGN(BasicTransport);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_BASICTRANSPORT_H
