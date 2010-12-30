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
 * This file contains a collection of Infiniband helper functions and classes,
 * which can be shared across different Infiniband transports and drivers.
 */

#include <infiniband/verbs.h>

#include "Common.h"
#include "Transport.h"
#include "InfAddress.h"

#ifndef RAMCLOUD_INFINIBAND_H
#define RAMCLOUD_INFINIBAND_H

namespace RAMCloud {

class Infiniband {
  public:

    // this class exists simply for passing queue pair handshake information
    // back and forth.
    class QueuePairTuple {
      public:
        QueuePairTuple() : qpn(0), psn(0), lid(0), nonce(0)
        {
            static_assert(sizeof(QueuePairTuple) == 18,
                              "QueuePairTuple has unexpected size");
        }
        QueuePairTuple(uint16_t lid, uint32_t qpn, uint32_t psn,
            uint64_t nonce) : qpn(qpn), psn(psn), lid(lid), nonce(nonce) {}
        uint16_t getLid() const { return lid; }
        uint32_t getQpn() const { return qpn; }
        uint32_t getPsn() const { return psn; }
        uint64_t getNonce() const { return nonce; }

      private:
        uint32_t qpn;            // queue pair number
        uint32_t psn;            // initial packet sequence number
        uint16_t lid;            // infiniband address: "local id"
        uint64_t nonce;          // random nonce used to confirm replies are
                                 // for received requests

        DISALLOW_COPY_AND_ASSIGN(QueuePairTuple); //NOLINT
    } __attribute__((packed));

    // this class encapsulates the creation, use, and destruction of an RC
    // queue pair.
    //
    // the constructor will create a qp and bring it to the INIT state.
    // after obtaining the lid, qpn, and psn of a remote queue pair, one
    // must call plumb() to bring the queue pair to the RTS state.
    class QueuePair {
      public:
        QueuePair(ibv_qp_type type,
                  ibv_context *ctxt,
                  int ibPhysicalPort,
                  ibv_pd *pd,
                  ibv_srq *srq,
                  ibv_cq *txcq,
                  ibv_cq *rxcq,
                  uint32_t maxSendWr,
                  uint32_t maxRecvWr,
                  uint32_t QKey = 0);
       ~QueuePair();
        uint32_t getInitialPsn() const;
        uint32_t getLocalQpNumber() const;
        uint32_t getRemoteQpNumber() const;
        uint16_t getRemoteLid() const;
        int      getState() const;
        void     plumb(QueuePairTuple *qpt);
        void     activate();

      //private: XXXXX- move send/recv functionality into the queue pair shit
        int          type;           // QP type (IBV_QPT_RC, etc.)
        ibv_context* ctxt;           // device context of the HCA to use
        int          ibPhysicalPort; // physical port number of the HCA
        ibv_pd*      pd;             // protection domain
        ibv_srq*     srq;            // shared receive queue
        ibv_qp*      qp;             // infiniband verbs QP handle
        ibv_cq*      txcq;           // transmit completion queue
        ibv_cq*      rxcq;           // receive completion queue
        uint32_t     initialPsn;     // initial packet sequence number

        DISALLOW_COPY_AND_ASSIGN(QueuePair);
    };

    // wrap an RX or TX buffer registered with the HCA
    struct BufferDescriptor {
        char *          buffer;         // buf of ``bytes'' length
        uint32_t        bytes;          // length of buffer in bytes
        uint32_t        messageBytes;   // byte length of message in the buffer
        ibv_mr *        mr;             // memory region of the buffer

        BufferDescriptor(char *buffer, uint64_t bytes, ibv_mr *mr) :
            buffer(buffer), bytes(bytes), messageBytes(0), mr(mr)
        {
        }
        BufferDescriptor() : buffer(NULL), bytes(0), messageBytes(0),
            mr(NULL) {}
    };

    static const char*  wcStatusToString(int status);
    static ibv_context* openDevice(const char *name);
    static int          getLid(ibv_context *ctxt,
                               int port);
    static BufferDescriptor* tryReceive(QueuePair *qp,
                                        InfAddress *sourceAddress = NULL);
    static BufferDescriptor* receive(QueuePair *qp,
                                     InfAddress *sourceAddress = NULL);
    static void              postReceive(QueuePair *qp, BufferDescriptor *bd);
    static void              postSrqReceive(ibv_srq* srq,
                                            BufferDescriptor *bd);
    static void         postSend(QueuePair* qp,
                                 BufferDescriptor* bd,
                                 uint32_t length,
                                 ibv_ah *ah = NULL,
                                 uint32_t remoteQpn = 0,
                                 uint32_t remoteQKey = 0);
    static void         postSendAndWait(QueuePair* qp,
                                        BufferDescriptor* bd,
                                        uint32_t length,
                                        ibv_ah *ah = NULL,
                                        uint32_t remoteQpn = 0,
                                        uint32_t remoteQKey = 0);
    static BufferDescriptor  allocateBufferDescriptorAndRegister(ibv_pd *pd,
                                                                 size_t bytes);

    // the following are straight up wrappers that can be overridden for testing
    static ibv_pd* allocateProtectionDomain(ibv_context* ctxt);
    static ibv_cq* createCompletionQueue(ibv_context* ctxt,
                                         int minimumEntries);
    static ibv_ah* createAddressHandle(ibv_pd *pd,
                                       ibv_ah_attr* attr);
    static void destroyAddressHandle(ibv_ah *ah);
    static ibv_srq* createSharedReceiveQueue(ibv_pd *pd,
                                             ibv_srq_init_attr *attr);
    static int pollCompletionQueue(ibv_cq *cq,
                                   int numEntries,
                                   ibv_wc *retWcArray);

  private:
    static const uint32_t MAX_INLINE_DATA = 400;
};

} // namespace

#endif // !RAMCLOUD_INFINIBAND_H
