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

/**
 * \file
 * This file defines an implementation of Transport for Infiniband
 * using reliable connected queue-pairs (RC).
 */

#include <time.h>
#include <string>
#include <boost/unordered_map.hpp>
#include <vector>

#include "BoostIntrusive.h"
#include "Common.h"
#include "Dispatch.h"
#include "IpAddress.h"
#include "Tub.h"
#include "Segment.h"
#include "Transport.h"
#include "Infiniband.h"

#ifndef RAMCLOUD_INFRCTRANSPORT_H
#define RAMCLOUD_INFRCTRANSPORT_H

namespace RAMCloud {

/**
 * Transport mechanism that uses Infiniband's reliable connections.
 * This class is templated in order to simplify replacing some of the
 * Infiniband guts for testing.  The "Infiniband" type name corresponds
 * to various low-level Infiniband facilities used both here and in
 * InfUdDriver.  "RealInfiniband" (the only instantiation that currently
 * exists) corresponds to the actual Infiniband driver facilities in
 * Infiniband.cc.
 */
template<typename Infiniband = RealInfiniband>
class InfRcTransport : public Transport {
    // forward declarations
    class InfRCSession;
    typedef typename Infiniband::BufferDescriptor BufferDescriptor;
    typedef typename Infiniband::QueuePair QueuePair;
    typedef typename Infiniband::QueuePairTuple QueuePairTuple;

  public:
    explicit InfRcTransport(const ServiceLocator* sl = NULL);
    ~InfRcTransport();
    ServerRpc* serverRecv() __attribute__((warn_unused_result));
    SessionRef getSession(const ServiceLocator& sl) {
        return new InfRCSession(this, sl);
    }
    ServiceLocator getServiceLocator();
    void dumpStats() {
        infiniband->dumpStats();
    }
    uint32_t getMaxRpcSize() const;
    void registerMemory(void* base, size_t bytes)
    {
        assert(logMemoryRegion == NULL);
        logMemoryRegion = ibv_reg_mr(infiniband->pd.pd, base, bytes,
            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
        assert(logMemoryRegion != NULL);
        logMemoryBase = reinterpret_cast<uintptr_t>(base);
        logMemoryBytes = bytes;
        LOG(NOTICE, "Registered %Zd bytes at %p", bytes, base);
    }

  private:
    class ServerRpc : public Transport::ServerRpc {
        public:
            explicit ServerRpc(InfRcTransport* transport, QueuePair* qp,
                               uint64_t nonce);
            void sendReply();
        private:
            InfRcTransport* transport;
            QueuePair*      qp;
            /// Uniquely identifies the RPC.
            uint64_t        nonce;
            DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    class ClientRpc : public Transport::ClientRpc {
        public:
            explicit ClientRpc(InfRcTransport* transport,
                               InfRCSession* session,
                               Buffer* request,
                               Buffer* response,
                               uint64_t nonce);
            void sendOrQueue();

        private:
            bool
            tryZeroCopy(Buffer* request);

            InfRcTransport*     transport;
            InfRCSession*       session;
            Buffer*             request;
            Buffer*             response;
            /// Uniquely identifies the RPC.
            uint64_t            nonce;
            enum {
                PENDING,
                REQUEST_SENT,
                RESPONSE_RECEIVED,
            } state;
        public:
            IntrusiveListHook   queueEntries;
            friend class InfRCSession;
            friend class InfRcTransport;
            DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

    /**
     * A header that goes at the start of every RPC request and response.
     */
    struct Header {
        explicit Header(uint64_t nonce) : nonce(nonce) {}
        /// Uniquely identifies the RPC.
        uint64_t nonce;
    };

    // maximum RPC size we'll permit. we'll use the segment size plus a
    // little extra for header overhead, etc.
    static const uint32_t MAX_RPC_SIZE = Segment::SEGMENT_SIZE + 4096;
    static const uint32_t MAX_SHARED_RX_QUEUE_DEPTH = 32;
    static const uint32_t MAX_SHARED_RX_SGE_COUNT = 8;
    static const uint32_t MAX_TX_QUEUE_DEPTH = 16;
    static const uint32_t MAX_TX_SGE_COUNT = 8;
    static const uint32_t QP_EXCHANGE_USEC_TIMEOUT = 50000;
    static const uint32_t QP_EXCHANGE_MAX_TIMEOUTS = 10;

    INTRUSIVE_LIST_TYPEDEF(ClientRpc, queueEntries) ClientRpcList;

    class InfRCSession : public Session {
      public:
        explicit InfRCSession(InfRcTransport *transport,
            const ServiceLocator& sl);
        Transport::ClientRpc* clientSend(Buffer* request, Buffer* response)
            __attribute__((warn_unused_result));
        void release();

      private:
        InfRcTransport *transport;
        QueuePair* qp;
        friend class ClientRpc;
        DISALLOW_COPY_AND_ASSIGN(InfRCSession);
    };

    /**
     * A Buffer::Chunk that is comprised of memory for incoming packets,
     * owned by the HCA but loaned to us during the processing of an
     * incoming RPC so the message doesn't have to be copied.
     *
     * PayloadChunk behaves like any other Buffer::Chunk except it returns
     * its memory to the HCA when the Buffer is deleted.
     */
    class PayloadChunk : public Buffer::Chunk {
      public:
        static PayloadChunk* prependToBuffer(Buffer* buffer,
                                             char* data,
                                             uint32_t dataLength,
                                             InfRcTransport* transport,
                                             ibv_srq* srq,
                                             BufferDescriptor* bd);
        static PayloadChunk* appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            InfRcTransport* transport,
                                            ibv_srq* srq,
                                            BufferDescriptor* bd);
        ~PayloadChunk();

      private:
        PayloadChunk(void* data,
                     uint32_t dataLength,
                     InfRcTransport* transport,
                     ibv_srq* srq,
                     BufferDescriptor* bd);

        InfRcTransport* transport;

        /// Return the PayloadChunk memory here.
        ibv_srq* srq;
        BufferDescriptor* const bd;

        DISALLOW_COPY_AND_ASSIGN(PayloadChunk);
    };

    // misc helper functions
    void setNonBlocking(int fd);

    // Extend Infiniband::postSrqReceive by issuing queued up transmissions
    void postSrqReceiveAndKickTransmit(ibv_srq* srq, BufferDescriptor *bd);

    // Grab a transmit buffer from our free list, or wait for completions if
    // necessary.
    BufferDescriptor* getTransmitBuffer();

    // queue pair connection setup helpers
    QueuePair* clientTrySetupQueuePair(IpAddress& address);
    bool       clientTryExchangeQueuePairs(struct sockaddr_in *sin,
                                           QueuePairTuple *outgoingQpt,
                                           QueuePairTuple *incomingQpt,
                                           uint32_t usTimeout);

    /// See #infiniband.
    Tub<Infiniband> realInfiniband;

    /**
     * Used by this class to make all Infiniband verb calls.  In normal
     * production use it points to #realInfiniband; for testing it points to a
     * mock object.
     */
    Infiniband* infiniband;

    BufferDescriptor*   serverRxBuffers[MAX_SHARED_RX_QUEUE_DEPTH];
    BufferDescriptor*   clientRxBuffers[MAX_SHARED_RX_QUEUE_DEPTH];

    vector<BufferDescriptor*> txBuffers;

    ibv_srq*     serverSrq;         // shared receive work queue for server
    ibv_srq*     clientSrq;         // shared receive work queue for client
    ibv_cq*      serverRxCq;        // completion queue for serverRecv
    ibv_cq*      clientRxCq;        // completion queue for client wait
    ibv_cq*      commonTxCq;        // common completion queue for all transmits
    int          ibPhysicalPort;    // physical port number on the HCA
    int          lid;               // local id for this HCA and physical port
    int          serverSetupSocket; // UDP socket for incoming setup requests;
                                    // -1 means we're not a server
    int          clientSetupSocket; // UDP socket for outgoing setup requests

    // ibv_wc.qp_num to QueuePair* lookup used to look up the QueuePair given
    // a completion event on the shared receive queue
    boost::unordered_map<uint32_t, QueuePair*> queuePairMap;

    /**
     * RPCs which are waiting for a receive buffer to become available before
     * their request can be sent. See #ClientRpc::sendOrQueue().
     */
    ClientRpcList clientSendQueue;

    /**
     * The number of client receive buffers that are in use, either from
     * outstandingRpcs or from RPC responses that have borrowed these buffers
     * and will return them with the PayloadChunk mechanism.
     * Invariant: numUsedClientSrqBuffers <= MAX_SHARED_RX_QUEUE_DEPTH.
     */
    uint32_t numUsedClientSrqBuffers;

    /// RPCs which are awaiting their responses from the network.
    ClientRpcList outstandingRpcs;

    Tub<CycleCounter<Metric>> clientRpcsActiveTime;

    /// ServiceLocator string. May be empty if a NULL ServiceLocator was
    /// passed to the constructor. Since InfRcTransport bootstraps over
    /// UDP, this could in the future contain a dynamic UDP port number.
    string locatorString;

    /**
     * This class (and its instance below) connect with the dispatcher's
     * polling mechanism so that we get invoked each time through the polling
     * loop to check for incoming packets.
     */
    class Poller : public Dispatch::Poller {
      public:
        explicit Poller(InfRcTransport* transport) : transport(transport) {}
        virtual bool operator() ();

      private:
        /// Check this transport for packets every time we are invoked.
        InfRcTransport* transport;
        DISALLOW_COPY_AND_ASSIGN(Poller);
    };
    Poller poller;

    /**
     * An event handler used on servers to respond to incoming packets
     * from clients that are requesting new connections.
     */
    class ServerConnectHandler : public Dispatch::File {
      public:
        ServerConnectHandler(int fd, InfRcTransport* transport)
                : Dispatch::File(fd, Dispatch::FileEvent::READABLE),
                fd(fd), transport(transport) { }
        virtual void operator() ();
      private:
        // The following variables are just copies of constructor arguments.
        int fd;
        InfRcTransport* transport;
        DISALLOW_COPY_AND_ASSIGN(ServerConnectHandler);
    };
    Tub<ServerConnectHandler> serverConnectHandler;

    // Hack for 0-copy from Log
    // This must go away after SOSP
    uintptr_t logMemoryBase;
    size_t logMemoryBytes;
    ibv_mr* logMemoryRegion;

    DISALLOW_COPY_AND_ASSIGN(InfRcTransport);
};

extern template class InfRcTransport<RealInfiniband>;

}  // namespace RAMCloud

#endif
