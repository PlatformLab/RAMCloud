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
 * This file contains a mock Infiniband subclass, used for testing infiniband
 * trnasports and drivers.
 */

#include "Common.h"
#include "Infiniband.h"

#ifndef RAMCLOUD_MOCKINFINIBAND_H
#define RAMCLOUD_MOCKINFINIBAND_H

namespace RAMCloud {

class MockInfiniband : public Infiniband {
  public:
    MockInfiniband();
    ~MockInfiniband();

    // this class encapsulates the creation, use, and destruction of an RC
    // queue pair.
    //
    // the constructor will create a qp and bring it to the INIT state.
    // after obtaining the lid, qpn, and psn of a remote queue pair, one
    // must call plumb() to bring the queue pair to the RTS state.
    class MockQueuePair : public Infiniband::QueuePair {
      public:
        MockQueuePair(ibv_qp_type type,
                      ibv_context *ctxt,
                      int ibPhysicalPort,
                      ibv_pd *pd,
                      ibv_srq *srq,
                      ibv_cq *txcq,
                      ibv_cq *rxcq,
                      uint32_t maxSendWr,
                      uint32_t maxRecvWr,
                      uint32_t QKey = 0);
        ~MockQueuePair();
        uint32_t getInitialPsn() const;
        uint32_t getLocalQpNumber() const;
        uint32_t getRemoteQpNumber() const;
        uint16_t getRemoteLid() const;
        int      getState() const;
        void     plumb(QueuePairTuple *qpt);
        void     activate();

        int          type;           // QP type (IBV_QPT_RC, etc.)
        ibv_context* ctxt;           // device context of the HCA to use
        int          ibPhysicalPort; // physical port number of the HCA
        ibv_pd*      pd;             // protection domain
        ibv_srq*     srq;            // shared receive queue
        ibv_qp*      qp;             // infiniband verbs QP handle
        ibv_cq*      txcq;           // transmit completion queue
        ibv_cq*      rxcq;           // receive completion queue
        uint32_t     initialPsn;     // initial packet sequence number

        DISALLOW_COPY_AND_ASSIGN(MockQueuePair);
    };

    // factory overload to spit out MockQueuePair objects
    QueuePair* createQueuePair(ibv_qp_type type,
                               ibv_context *ctxt,
                               int ibPhysicalPort,
                               ibv_pd *pd,
                               ibv_srq *srq,
                               ibv_cq *txcq,
                               ibv_cq *rxcq,
                               uint32_t maxSendWr,
                               uint32_t maxRecvWr,
                               uint32_t QKey = 0);

    const char*  wcStatusToString(int status);
    ibv_context* openDevice(const char *name);
    int          getLid(ibv_context *ctxt,
                            int port);
    BufferDescriptor* tryReceive(QueuePair *qp,
                                     InfAddress *sourceAddress = NULL);
    BufferDescriptor* receive(QueuePair *qp,
                                  InfAddress *sourceAddress = NULL);
    void              postReceive(QueuePair *qp,
                                      BufferDescriptor *bd);
    void              postSrqReceive(ibv_srq* srq,
                                         BufferDescriptor *bd);
    void              postSend(QueuePair* qp,
                                   BufferDescriptor* bd,
                                   uint32_t length,
                                   ibv_ah *ah = NULL,
                                   uint32_t remoteQpn = 0,
                                   uint32_t remoteQKey = 0);
    void              postSendAndWait(QueuePair* qp,
                                          BufferDescriptor* bd,
                                          uint32_t length,
                                          ibv_ah *ah = NULL,
                                          uint32_t remoteQpn = 0,
                                          uint32_t remoteQKey = 0);
    BufferDescriptor  allocateBufferDescriptorAndRegister(ibv_pd *pd,
                                                              size_t bytes);
    ibv_pd*  allocateProtectionDomain(ibv_context* ctxt);
    ibv_cq*  createCompletionQueue(ibv_context* ctxt,
                                       int minimumEntries);
    ibv_ah*  createAddressHandle(ibv_pd *pd,
                                     ibv_ah_attr* attr);
    void     destroyAddressHandle(ibv_ah *ah);
    ibv_srq* createSharedReceiveQueue(ibv_pd *pd,
                                          ibv_srq_init_attr *attr);
    int      pollCompletionQueue(ibv_cq *cq,
                                     int numEntries,
                                     ibv_wc *retWcArray);
};

} // namespace

#endif // !RAMCLOUD_MOCKINFINIBAND_H
