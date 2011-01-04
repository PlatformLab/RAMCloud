/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Transport.h"
#include "MockInfiniband.h"

namespace RAMCloud {

/**
 * Construct a MockInfiniband object.
 */
MockInfiniband::MockInfiniband()
{
}

/**
 * Destroy a MockInfiniband object.
 */
MockInfiniband::~MockInfiniband()
{
}

/**
 * Factory for MockQueuePairs instead of QueuePairs.
 * See Infiniband::createQueuePair.
 */
Infiniband::QueuePair*
MockInfiniband::createQueuePair(ibv_qp_type type, ibv_context *ctxt,
    int ibPhysicalPort, ibv_pd *pd, ibv_srq *srq, ibv_cq *txcq, ibv_cq *rxcq,
    uint32_t maxSendWr, uint32_t maxRecvWr, uint32_t QKey)
{
    return new MockQueuePair(type, ctxt, ibPhysicalPort, pd, srq, txcq, rxcq,
        maxSendWr, maxRecvWr, QKey);
}

/**
 * See Infiniband::openDevice.
 */
ibv_context*
MockInfiniband::openDevice(const char *name)
{
    return NULL;
}

/**
 * See Infiniband::getLid.
 */
int
MockInfiniband::getLid(ibv_context *ctxt, int port)
{
    return 12;
}

/**
 * See Infiniband::tryReceive.
 */
Infiniband::BufferDescriptor*
MockInfiniband::tryReceive(QueuePair *qp, InfAddress *sourceAddress)
{
    return NULL;
}

/**
 * See Infiniband::receive.
 */
Infiniband::BufferDescriptor *
MockInfiniband::receive(QueuePair *qp, InfAddress *sourceAddress)
{
    return NULL;    // XXX not permitted
}

/**
 * See Infiniband::postReceive.
 */
void
MockInfiniband::postReceive(QueuePair *qp, BufferDescriptor *bd)
{
}

/**
 * See Infiniband::postSrqReceive.
 */
void
MockInfiniband::postSrqReceive(ibv_srq* srq, BufferDescriptor *bd)
{
}

/**
 * See Infiniband::postSend.
 */
void
MockInfiniband::postSend(QueuePair* qp, BufferDescriptor *bd, uint32_t length,
    ibv_ah *ah, uint32_t remoteQpn, uint32_t remoteQKey)
{
}

/**
 * See Infiniband::postSendAndWait.
 */
void
MockInfiniband::postSendAndWait(QueuePair* qp, BufferDescriptor *bd,
    uint32_t length, ibv_ah *ah, uint32_t remoteQpn, uint32_t remoteQKey)
{
}

/**
 * See Infiniband::allocateBufferDescriptorAndRegister.
 */
Infiniband::BufferDescriptor
MockInfiniband::allocateBufferDescriptorAndRegister(ibv_pd *pd, size_t bytes)
{
    return BufferDescriptor(NULL, bytes, NULL); /// XXX
}

/**
 * See Infiniband::allocateProtectionDomain.
 */
ibv_pd*
MockInfiniband::allocateProtectionDomain(ibv_context* ctxt)
{
    return NULL;
}

/**
 * See Infiniband::createCompletionQueue.
 */
ibv_cq*
MockInfiniband::createCompletionQueue(ibv_context* ctxt, int minimumEntries)
{
    return NULL;
}

/**
 * See Infiniband::createAddressHandle.
 */
ibv_ah*
MockInfiniband::createAddressHandle(ibv_pd *pd, ibv_ah_attr* attr)
{
    return NULL;
}

/**
 * See Infiniband::destroyAddressHandle.
 */
void
MockInfiniband::destroyAddressHandle(ibv_ah *ah)
{
}

/**
 * See Infiniband::createSharedReceiveQueue.
 */
ibv_srq*
MockInfiniband::createSharedReceiveQueue(ibv_pd *pd, ibv_srq_init_attr *attr)
{
    return NULL;
}

/**
 * See Infiniband::pollCompletionQueue.
 */
int
MockInfiniband::pollCompletionQueue(ibv_cq *cq, int numEntries,
    ibv_wc *retWcArray)
{
    return 0;
}

//-------------------------------------
// MockInfiniband::QueuePair class
//-------------------------------------

/**
 * See Infiniband::QueuePair::QueuePair.
 */
MockInfiniband::MockQueuePair::MockQueuePair(ibv_qp_type type,
    ibv_context *ctxt, int ibPhysicalPort, ibv_pd *pd, ibv_srq *srq,
    ibv_cq *txcq, ibv_cq *rxcq, uint32_t maxSendWr, uint32_t maxRecvWr,
    uint32_t QKey)
    : type(type),
      ctxt(ctxt),
      ibPhysicalPort(ibPhysicalPort),
      pd(pd),
      srq(srq),
      qp(NULL),
      txcq(txcq),
      rxcq(rxcq),
      initialPsn(generateRandom() & 0xffffff)
{
}

/**
 * See Infiniband::QueuePair::~QueuePair.
 */
MockInfiniband::MockQueuePair::~MockQueuePair()
{
}

/**
 * See Infiniband::QueuePair::plumb.
 */
void
MockInfiniband::MockQueuePair::plumb(QueuePairTuple *qpt)
{
}

/**
 * See Infiniband::QueuePair::activate.
 */
void
MockInfiniband::MockQueuePair::activate()
{
}

/**
 * See Infiniband::QueuePair::getInitialPsn.
 */
uint32_t
MockInfiniband::MockQueuePair::getInitialPsn() const
{
    return initialPsn;
}

/**
 * See Infiniband::QueuePair::getLocalQpNumber.
 */
uint32_t
MockInfiniband::MockQueuePair::getLocalQpNumber() const
{
    return 32;
}

/**
 * See Infiniband::QueuePair::getRemoteQpNumber.
 */
uint32_t
MockInfiniband::MockQueuePair::getRemoteQpNumber() const
{
    return 91;
}

/**
 * See Infiniband::QueuePair::getRemoteLid.
 */
uint16_t
MockInfiniband::MockQueuePair::getRemoteLid() const
{
    return 7;
}

/**
 * See Infiniband::QueuePair::getState.
 */
int
MockInfiniband::MockQueuePair::getState() const
{
    return IBV_QPS_RTS;
}

} // namespace
