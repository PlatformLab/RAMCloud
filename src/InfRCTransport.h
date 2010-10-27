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

// RAMCloud pragma [CPPLINT=0]

/**
 * \file
 * This file defines an implementation of Transport for Infiniband
 * using reliable connected queue-pairs (RC).
 */

#include <string>

#include "Common.h"
#include "Segment.h"
#include "Transport.h"
#include <infiniband/verbs.h>
#include <boost/unordered_map.hpp>

#ifndef RAMCLOUD_INFRCTRANSPORT_H
#define RAMCLOUD_INFRCTRANSPORT_H

namespace RAMCloud {

class InfRCTransport : public Transport {
    // forward declarations
    struct BufferDescriptor;
    class  QueuePair;

  public:
    explicit InfRCTransport(const ServiceLocator* sl = NULL);
    ~InfRCTransport() { }
    ServerRpc* serverRecv() __attribute__((warn_unused_result));
    SessionRef getSession(const ServiceLocator& sl) {
        return new InfRCSession(this, sl);
    }
    uint32_t getMaxRpcSize() const;

    class ServerRpc : public Transport::ServerRpc {
        public:
            explicit ServerRpc(InfRCTransport* transport, QueuePair* qp);
            void sendReply();
        private:
            InfRCTransport* transport;
            QueuePair*      qp;
            DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    class ClientRpc : public Transport::ClientRpc {
        public:
            explicit ClientRpc(InfRCTransport* transport,
                               QueuePair* qp,
                               Buffer* response);
            void getReply();
        private:
            InfRCTransport*     transport;
            QueuePair*          qp;
            Buffer*             response;
            DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

  private:
    // maximum RPC size we'll permit. we'll use the segment size plus a
    // little extra for header overhead, etc.
    static const uint32_t MAX_RPC_SIZE = Segment::SEGMENT_SIZE + 4096;
    static const uint32_t MAX_SHARED_RX_QUEUE_DEPTH = 4;
    static const uint32_t MAX_SHARED_RX_SGE_COUNT = 8;
    static const uint32_t MAX_TX_QUEUE_DEPTH = 64;
    static const uint32_t MAX_TX_SGE_COUNT = 8;

    class InfRCSession : public Session {
      public:
        explicit InfRCSession(InfRCTransport *transport,
            const ServiceLocator& sl);
        Transport::ClientRpc* clientSend(Buffer* request, Buffer* response)
            __attribute__((warn_unused_result));
        void release();

      private:
        InfRCTransport *transport;
        QueuePair* qp;
        DISALLOW_COPY_AND_ASSIGN(InfRCSession);
    };

    // wrap an RX or TX buffer registered with the HCA
    struct BufferDescriptor {
        char*           buffer;         // buf of getMaxPayloadSize() bytes
        ibv_mr*         mr;             // memory region of the buffer
        int             id;             // unique descriptor id
        bool            inUse;          // non-0 => Infiniband HCA owns `buffer'

        BufferDescriptor(char *buffer, ibv_mr *mr, uint32_t id) :
            buffer(buffer), mr(mr), id(id), inUse(false)
        {
        }
        BufferDescriptor() : buffer(NULL), mr(NULL), id(0), inUse(false) {}
    };

    // this class exists simply for passing queue pair handshake information
    // back and forth.
    class QueuePairTuple {
      public:
        QueuePairTuple() : qpn(0), psn(0), lid(0) {}
        QueuePairTuple(uint16_t lid, uint32_t qpn, uint32_t psn) :
            qpn(qpn), psn(psn), lid(lid) {}
        uint16_t getLid() const { return lid; }
        uint32_t getQpn() const { return qpn; }
        uint32_t getPsn() const { return psn; }

      private:
        uint32_t qpn;            // queue pair number
        uint32_t psn;            // initial packet sequence number
        uint16_t lid;            // infiniband address: "local id"

        DISALLOW_COPY_AND_ASSIGN(QueuePairTuple);
    } __attribute__((packed));

    // this class encapsulates the creation, use, and destruction of an RC
    // queue pair.
    //
    // the constructor will create a qp and bring it to the INIT state. 
    // after obtaining the lid, qpn, and psn of a remote queue pair, one
    // must call plumb() to bring the queue pair to the RTS state.
    class QueuePair {
      public:
        QueuePair(int ibPhysicalPort, ibv_pd *pd, ibv_srq *srq, ibv_cq *txcq,
            ibv_cq *rxcq);
       ~QueuePair();
        uint32_t getInitialPsn() const;
        uint32_t getLocalQpNumber() const;
        uint32_t getRemoteQpNumber() const;
        uint16_t getRemoteLid() const;
        void     plumb(QueuePairTuple *qpt);

      //private: XXXXX- move send/recv functionality into the queue pair shit
        int         ibPhysicalPort; // physical port number of the HCA
        ibv_pd*     pd;             // protection domain
        ibv_srq*    srq;            // shared receive queue
        ibv_qp*     qp;             // infiniband verbs QP handle
        ibv_cq*     txcq;           // transmit completion queue
        ibv_cq*     rxcq;           // receive completion queue
        uint32_t    initialPsn;     // initial packet sequence number

        DISALLOW_COPY_AND_ASSIGN(QueuePair);
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
                                             InfRCTransport* transport, 
                                             BufferDescriptor* bd);
        static PayloadChunk* appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            InfRCTransport* transport,
                                            BufferDescriptor* bd);
        ~PayloadChunk();

      private:
        PayloadChunk(void* data,
                     uint32_t dataLength,
                     InfRCTransport* transport,
                     BufferDescriptor* bd);

        InfRCTransport* transport;

        /// Return the PayloadChunk memory here.
        BufferDescriptor* const bd;

        DISALLOW_COPY_AND_ASSIGN(PayloadChunk);
    };

    // infiniband helper functions
    ibv_device* ibFindDevice(const char *name);
    int ibGetLid();
    void ibPostSrqReceive(BufferDescriptor *bd);
    void ibPostSend(QueuePair* qp, BufferDescriptor* bd, uint32_t length);
    void ibPostSendAndWait(QueuePair* qp, BufferDescriptor* bd,uint32_t length);
    BufferDescriptor allocateBufferDescriptorAndRegister();

    // queue pair connection setup helpers
    QueuePair* clientTrySetupQueuePair(const char* ip, int port);
    void       serverTrySetupQueuePair();

    BufferDescriptor    rxBuffers[MAX_SHARED_RX_QUEUE_DEPTH];
    int                 currentRxBuffer;

    BufferDescriptor    txBuffers[MAX_TX_QUEUE_DEPTH];
    int                 currentTxBuffer;

    ibv_srq*     srq;               // shared receive work queue
    ibv_device*  dev;               // infiniband HCA device we're using
    ibv_context* ctxt;              // HCA device context (handle) 
    ibv_pd*      pd;                // protection domain for registered memory
    ibv_cq*      rxcq;              // common completion queue for all receives
    ibv_cq*      txcq;              // common completion queue for all transmits
    int          ibPhysicalPort;    // physical port number on the HCA
    int          udpListenPort;     // UDP port number for server's setupSocket
    int          setupSocket;       // UDP socket for connection setup

    // ibv_wc.qp_num to QueuePair* lookup used to look up the QueuePair given
    // a completion event on the shared receive queue
    boost::unordered_map<uint32_t, QueuePair*> queuePairMap;

    DISALLOW_COPY_AND_ASSIGN(InfRCTransport);
};

}  // namespace RAMCloud

#endif
