/* Copyright (c) 2010-2014 Stanford University
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
#include <unordered_map>
#include <vector>

#include "BoostIntrusive.h"
#include "Common.h"
#include "Dispatch.h"
#include "IpAddress.h"
#include "Tub.h"
#include "Segment.h"
#include "ServerRpcPool.h"
#include "ShortMacros.h"
#include "SessionAlarm.h"
#include "PortAlarm.h"
#include "Transport.h"
#include "TransportManager.h"
#include "Infiniband.h"
#include "CycleCounter.h"
#include "RawMetrics.h"
#include "PerfCounter.h"

#ifndef RAMCLOUD_INFRCTRANSPORT_H
#define RAMCLOUD_INFRCTRANSPORT_H

namespace RAMCloud {

/**
 * Transport mechanism that uses Infiniband reliable queue pairs.
 */
class InfRcTransport : public Transport {
    typedef RAMCloud::Perf::ReadRequestHandle_MetricSet
        ReadRequestHandle_MetricSet;
    // forward declarations
  PRIVATE:
    class InfRcSession;
    class Poller;
    class InfRcServerPort;
    typedef Infiniband::BufferDescriptor BufferDescriptor;
    typedef Infiniband::QueuePair QueuePair;
    typedef Infiniband::QueuePairTuple QueuePairTuple;
    typedef Infiniband::RegisteredBuffers RegisteredBuffers;

  public:
    explicit InfRcTransport(Context* context, const ServiceLocator* sl = NULL);
    ~InfRcTransport();
    SessionRef getSession(const ServiceLocator& sl, uint32_t timeoutMs = 0) {
        return new InfRcSession(this, sl, timeoutMs);
    }
    string getServiceLocator();
    void dumpStats() {
        infiniband->dumpStats();
    }
    uint32_t getMaxRpcSize() const;

    /**
     * Register a memory region with the HCA for zero-copy transmission.
     * After registration Buffer::Chunks sent in client RPC requests can
     * be given directly to the HCA without copying into a transmit
     * buffer first. Callers must collectively ensure this function is only
     * once. It is possible to extend this function to support multiple
     * regions, but for the moment we only use it to register the
     * Log Seglets.
     *
     * \param base
     *      Starting address of the region to be registered with the HCA.
     * \param bytes
     *      Length of the region starting at \a base to register with the HCA.
     */
    void registerMemory(void* base, size_t bytes)
    {
        assert(logMemoryRegion == NULL);
        logMemoryRegion = ibv_reg_mr(infiniband->pd.pd, base, bytes,
            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
        if (logMemoryRegion == NULL) {
            LOG(ERROR, "ibv_reg_mr failed to register %Zd bytes at %p",
                bytes, base);
            throw TransportException(HERE, "ibv_reg_mr failed");
        }
        logMemoryBase = reinterpret_cast<uintptr_t>(base);
        logMemoryBytes = bytes;
        RAMCLOUD_LOG(NOTICE, "Registered %Zd bytes at %p", bytes, base);
    }
    static void setName(const char* name);

  PRIVATE:
    class ServerRpc : public Transport::ServerRpc {
        public:
            explicit ServerRpc(InfRcTransport* transport, QueuePair* qp,
                               uint64_t nonce);
            void sendReply();
            string getClientServiceLocator();
            ReadRequestHandle_MetricSet::Interval rpcServiceTime;
        private:
            InfRcTransport* transport;
            QueuePair*      qp;
            /// Uniquely identifies the RPC.
            uint64_t        nonce;
            DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    class ClientRpc {
        public:
            explicit ClientRpc(InfRcTransport* transport,
                               InfRcSession* session,
                               Buffer* request,
                               Buffer* response,
                               RpcNotifier* notifier,
                               uint64_t nonce);
            void sendOrQueue();

        PRIVATE:
            InfRcTransport*     transport;
            InfRcSession*       session;

            // Buffers for request and response messages.
            Buffer*             request;
            Buffer*             response;

            /// Use this object to report completion.
            RpcNotifier*        notifier;

            /// Uniquely identifies the RPC.
            uint64_t            nonce;

            /// If the RPC couldn't immediately be sent because there
            /// weren't enough client receive buffers available, this
            /// records the start of the waiting time, so we can print
            /// a message if the wait is long.
            uint64_t waitStart;

            enum {
                PENDING,
                REQUEST_SENT,
                RESPONSE_RECEIVED,
            } state;
        public:
            IntrusiveListHook   queueEntries;
            friend class InfRcSession;
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

    static const uint32_t MAX_SHARED_RX_QUEUE_DEPTH = 32;

    // Since we always use at most 1 SGE per receive request, there is no need
    // to set this parameter any higher. In fact, larger values for this
    // parameter result in increased descriptor size, which means that the
    // Infiniband controller needs to fetch more data from host memory,
    // which results in a higher number of on-controller cache misses.
    static const uint32_t MAX_SHARED_RX_SGE_COUNT = 1;
    static const uint32_t MAX_TX_QUEUE_DEPTH = 16;
    // With 64 KB seglets 1 MB is fractured into 16 or 17 pieces, plus we
    // need an entry for the headers.
    enum { MAX_TX_SGE_COUNT = 24 };
    static const uint32_t QP_EXCHANGE_USEC_TIMEOUT = 50000;
    static const uint32_t QP_EXCHANGE_MAX_TIMEOUTS = 10;

    INTRUSIVE_LIST_TYPEDEF(ClientRpc, queueEntries) ClientRpcList;

    class InfRcSession : public Session {
      public:
        explicit InfRcSession(InfRcTransport *transport,
            const ServiceLocator& sl, uint32_t timeoutMs);
        ~InfRcSession();
        virtual void abort();
        virtual void cancelRequest(RpcNotifier* notifier);
        virtual string getRpcInfo();
        virtual void sendRequest(Buffer* request, Buffer* response,
            RpcNotifier* notifier);

      PRIVATE:
        // Transport that manages this session.
        InfRcTransport *transport;
        // Connection to the server; NULL means this socket has been aborted.
        QueuePair* qp;
        // Used to detect server timeouts on the client port.
        SessionAlarm sessionAlarm;

        friend class ClientRpc;
        friend class Poller;
        DISALLOW_COPY_AND_ASSIGN(InfRcSession);
    };

    /**
     * One ServerPort instance is created at queue pair generation to
     * maintain the port liveness watchdog information.
     *
     * When associated client queue pair is destroyed, eg. through
     * deletion of RamCloud instance, the other side of queue 
     * pair on server and its resource need to be deleted.
     * The liveness watchdog cleans up the server side queue pair.
     *
     * SeverPort has to be dynamically instanciated to avoid
     * 'double free' error, since each instance is freed with
     * 'delete self' at the watch dog timeout.
     **/
    class InfRcServerPort : public ServerPort {
    public:
        explicit InfRcServerPort(InfRcTransport *transport,
                                 QueuePair* qp);
        ~InfRcServerPort();
        void close(); // close and shutdown this port
        const string getPortName() const;
    PRIVATE:
        // Transport that manages this port
        InfRcTransport *transport;
        // Listening queue pair
        QueuePair*  qp;
        // Used to detect client timeouts on the server port for listening
        PortAlarm  portAlarm;

        friend class InfRcTransport;
        friend class ServerConnectHandler;
        friend class Poller;
        DISALLOW_COPY_AND_ASSIGN(InfRcServerPort);
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

        friend class Buffer;      // allocAux must call private constructor.
        DISALLOW_COPY_AND_ASSIGN(PayloadChunk);
    };

    // misc helper functions
    void sendZeroCopy(Buffer* message, QueuePair* qp);
    void setNonBlocking(int fd);

    // Extend Infiniband::postSrqReceive by issuing queued up transmissions
    void postSrqReceiveAndKickTransmit(ibv_srq* srq, BufferDescriptor *bd);

    // Grab a transmit buffer from our free list, or wait for completions if
    // necessary.
    BufferDescriptor* getTransmitBuffer();

    // Pull TX buffers from completion queue and add to freeTxBuffers.
    int reapTxBuffers();

    // queue pair connection setup helpers
    QueuePair* clientTrySetupQueuePair(IpAddress& address);
    bool       clientTryExchangeQueuePairs(struct sockaddr_in *sin,
                                           QueuePairTuple *outgoingQpt,
                                           QueuePairTuple *incomingQpt,
                                           uint32_t usTimeout);

    /// Shared RAMCloud information.
    Context* context;

    /// See #infiniband.
    Tub<Infiniband> realInfiniband;

    /**
     * Used by this class to make all Infiniband verb calls.  In normal
     * production use it points to #realInfiniband; for testing it points to a
     * mock object.
     */
    Infiniband* infiniband;

    /// Infiniband receive buffers, written directly by the HCA.
    Tub<RegisteredBuffers> rxBuffers;

    /// Infiniband transmit buffers.
    Tub<RegisteredBuffers> txBuffers;
    vector<BufferDescriptor*> freeTxBuffers;

    ibv_srq*     serverSrq;         // shared receive work queue for server
    ibv_srq*     clientSrq;         // shared receive work queue for client
    ibv_cq*      serverRxCq;        // completion queue for incoming requests
    ibv_cq*      clientRxCq;        // completion queue for client wait
    ibv_cq*      commonTxCq;        // common completion queue for all transmits
    int          ibPhysicalPort;    // physical port number on the HCA
    int          lid;               // local id for this HCA and physical port
    int          serverSetupSocket; // UDP socket for incoming setup requests;
                                    // -1 means we're not a server
    int          clientSetupSocket; // UDP socket for outgoing setup requests

    // Map ibv_wc.qp_num/qp.LocalQpNumber to InfRcServerPort*.
    // InfRcServePort contains QueuePair* and Alarm* for the port
    std::unordered_map<uint32_t, InfRcServerPort*> serverPortMap;

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

    /**
     * Number of server receive buffers that are currently available for
     * new requests.
     */
    uint32_t numFreeServerSrqBuffers;

    /// RPCs which are awaiting their responses from the network.
    ClientRpcList outstandingRpcs;

    Tub<CycleCounter<RawMetric>> clientRpcsActiveTime;

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
        explicit Poller(InfRcTransport* transport)
            : Dispatch::Poller(transport->context->dispatch,
                               "InfRcTransport::Poller")
            , transport(transport) {}
        virtual void poll();

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
            : Dispatch::File(transport->context->dispatch, fd,
                             Dispatch::FileEvent::READABLE)
            , fd(fd)
            , transport(transport) { }
        virtual void handleFileEvent(int events);
      private:
        // The following variables are just copies of constructor arguments.
        int fd;
        InfRcTransport* transport;
        DISALLOW_COPY_AND_ASSIGN(ServerConnectHandler);
    };
    Tub<ServerConnectHandler> serverConnectHandler;

    /// Starting address of the region registered with the HCA for zero-copy
    /// transmission, if any. If no region is registered then 0.
    /// See registerMemory().
    uintptr_t logMemoryBase;

    /// Length of the region starting at #logMemoryBase which is registered
    /// with the HCA for zero-copy transmission. If no region is registered
    /// then 0. See registerMemory().
    size_t logMemoryBytes;

    /// Infiniband memory region of the region registered with the HCA for
    /// zero-copy transmission. If no region is registered then NULL.
    /// See registerMemory().
    ibv_mr* logMemoryRegion;

    // CycleCounter that's constructed when TX goes active and is destroyed
    // when all TX buffers have been reclaimed. Counts are added to metrics.
    Tub<CycleCounter<uint64_t>> transmitCycleCounter;

    /// Pool allocator for our ServerRpc objects.
    ServerRpcPool<ServerRpc> serverRpcPool;

    /// Allocator for ClientRpc objects.
    ObjectPool<ClientRpc> clientRpcPool;

    /// Name for this machine/application (passed from clients to servers so
    /// servers know who they are talking to).
    static char name[50];

    /// This variable gets around what appears to be a bug in Infiniband: as
    /// of 8/2012, if a queue pair is closed when transmit buffers are active
    /// on it, the transmit buffers never get returned via commonTxCq.  To
    /// work around this problem, don't delete queue pairs immediately. Instead,
    /// save them in this vector and delete them at a safe time, when there are
    /// no outstanding transmit buffers to be lost.
    vector<QueuePair*> deadQueuePairs;

    /// Used during unit testing; if set then don't actually transmit during
    /// sendZeroCopy(), just log sges instead.
    bool testingDontReallySend;

    DISALLOW_COPY_AND_ASSIGN(InfRcTransport);
};

}  // namespace RAMCloud

#endif
