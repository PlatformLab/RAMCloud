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
 * This file defines an implementation of Transport for Infiniband
 * using reliable connected queue-pairs (RC).
 */

#include <time.h>
#include <string>
#include <boost/unordered_map.hpp>
#include <vector>

#include "BoostIntrusive.h"
#include "Common.h"
#include "IpAddress.h"
#include "Segment.h"
#include "Transport.h"
#include "Infiniband.h"

#ifndef RAMCLOUD_INFRCTRANSPORT_H
#define RAMCLOUD_INFRCTRANSPORT_H

namespace RAMCloud {

template<typename Infiniband = RealInfiniband>
class InfRcTransport : public Transport {
    // forward declarations
    class InfRCSession;
    typedef typename Infiniband::BufferDescriptor BufferDescriptor;
    typedef typename Infiniband::QueuePair QueuePair;
    typedef typename Infiniband::QueuePairTuple QueuePairTuple;

  public:
    explicit InfRcTransport(const ServiceLocator* sl = NULL);
    ~InfRcTransport() { }
    ServerRpc* serverRecv() __attribute__((warn_unused_result));
    SessionRef getSession(const ServiceLocator& sl) {
        return new InfRCSession(this, sl);
    }
    ServiceLocator getServiceLocator();
    void dumpStats() {
        LOG(NOTICE, "InfRcTransport totalClientSendCopyTime: %lu",
            totalClientSendCopyTime);
        LOG(NOTICE, "InfRcTransport totalClientSendCopyBytes: %lu",
            totalClientSendCopyBytes);
        LOG(NOTICE, "InfRcTransport totalSendReplyCopyTime: %lu",
            totalSendReplyCopyTime);
        LOG(NOTICE, "InfRcTransport totalSendReplyCopyBytes: %lu",
            totalSendReplyCopyBytes);
        totalClientSendCopyTime = 0;
        totalClientSendCopyBytes = 0;
        totalSendReplyCopyTime = 0;
        totalSendReplyCopyBytes = 0;
        infiniband->dumpStats();
    }
    uint32_t getMaxRpcSize() const;

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
            bool isReady();
            void sendOrQueue();
            void wait();

        private:
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
    static const uint32_t MAX_SHARED_RX_QUEUE_DEPTH = 16;
    static const uint32_t MAX_SHARED_RX_SGE_COUNT = 8;
    static const uint32_t MAX_TX_QUEUE_DEPTH = 8;
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

    void poll();

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
    void       serverTrySetupQueuePair();

    /// See #infiniband.
    ObjectTub<Infiniband> realInfiniband;

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
    int          serverSetupSocket; // UDP socket for incoming setup requests
    int          clientSetupSocket; // UDP socket for outgoing setup requests

    // ibv_wc.qp_num to QueuePair* lookup used to look up the QueuePair given
    // a completion event on the shared receive queue
    boost::unordered_map<uint32_t, QueuePair*> queuePairMap;

    /// For tracking stats on how much time is spent memcpying on request TX.
    static uint64_t totalClientSendCopyTime;
    /// For tracking stats on how much data is memcpyed on request TX.
    static uint64_t totalClientSendCopyBytes;
    /// For tracking stats on how much time is spent memcpying on reply TX.
    static uint64_t totalSendReplyCopyTime;
    /// For tracking stats on how much data is memcpyed on reply TX.
    static uint64_t totalSendReplyCopyBytes;

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

    /// ServiceLocator string. May be empty if a NULL ServiceLocator was
    /// passed to the constructor. Since InfRcTransport bootstraps over
    /// UDP, this could in the future contain a dynamic UDP port number.
    string locatorString;

    DISALLOW_COPY_AND_ASSIGN(InfRcTransport);
};

extern template class InfRcTransport<RealInfiniband>;

}  // namespace RAMCloud

#endif
