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

/*
 * XXX- This file is used by both Transports and Drivers, but throws
 *      TransportExceptions. What should we do? Nix the static methods and
 *      Template it on the exception type?
 */

#include "Transport.h"
#include "Infiniband.h"

namespace RAMCloud {

/**
 * Given a string representation of the `status' field from Verbs
 * struct `ibv_wc'.
 *
 * \param[in] status
 *      The integer status obtained in ibv_wc.status.
 * \return
 *      A string corresponding to the given status.
 */
const char*
Infiniband::wcStatusToString(int status)
{
    static const char *lookup[] = {
        "SUCCESS",
        "LOC_LEN_ERR",
        "LOC_QP_OP_ERR",
        "LOC_EEC_OP_ERR",
        "LOC_PROT_ERR",
        "WR_FLUSH_ERR",
        "MW_BIND_ERR",
        "BAD_RESP_ERR",
        "LOC_ACCESS_ERR",
        "REM_INV_REQ_ERR",
        "REM_ACCESS_ERR",
        "REM_OP_ERR",
        "RETRY_EXC_ERR",
        "RNR_RETRY_EXC_ERR",
        "LOC_RDD_VIOL_ERR",
        "REM_INV_RD_REQ_ERR",
        "REM_ABORT_ERR",
        "INV_EECN_ERR",
        "INV_EEC_STATE_ERR",
        "FATAL_ERR",
        "RESP_TIMEOUT_ERR",
        "GENERAL_ERR"
    };

    if (status < IBV_WC_SUCCESS || status > IBV_WC_GENERAL_ERR)
        return "<status out of range!>";
    return lookup[status];
}

/**
 * Find an installed infiniband device by name and open it.
 *
 * \param[in] name
 *      The string name of the interface to look for. If NULL,
 *      open the first one returned by the Verbs library.
 * \return
 *      NULL if the device wasn't found or couldn't be opened, else
 *      a valid ibv_context pointer.
 */
ibv_context*
Infiniband::openDevice(const char *name)
{
    ibv_device **devices;

    devices = ibv_get_device_list(NULL);
    if (devices == NULL)
        return false;

    ibv_device *dev = NULL;

    if (name == NULL) {
        dev = devices[0];
    } else {
        for (int i = 0; devices[i] != NULL; i++) {
            if (strcmp(devices[i]->name, name) == 0) {
                dev = devices[i];
                break;
            }
        }
    }

    ibv_context *ctxt = NULL;
    if (dev != NULL)
        ctxt = ibv_open_device(dev);

    ibv_free_device_list(devices);

    return ctxt;
}

/**
 * Obtain the infiniband "local ID" of the device corresponding to
 * the provided context and port number.
 *
 * \param[in] ctxt
 *      Context of the device whose local ID we're looking up.
 * \param[in] port
 *      Port on the device whose local ID we're looking up. This value
 *      is typically 1, except on adapters with multiple physical ports.
 * \return
 *      The local ID corresponding to the given parameters.
 * \throw
 *      TransportException if the port cannot be queried.
 */
int
Infiniband::getLid(ibv_context *ctxt, int port)
{
    ibv_port_attr ipa;
    int ret = ibv_query_port(ctxt, port, &ipa);
    if (ret) {
        LOG(ERROR, "ibv_query_port failed on port %u\n", port);
        throw TransportException(HERE, ret);
    }
    return ipa.lid;
}

/**
 * Add the given BufferDescriptor to the given shared receive queue.
 *
 * \param[in] srq
 *      The shared receive queue on which to enqueue this BufferDescriptor.
 * \param[in] bd
 *      The BufferDescriptor to enqueue.
 * \throw
 *      TransportException if posting to the queue fails.
 */
void
Infiniband::postSrqReceive(ibv_srq* srq, BufferDescriptor *bd)
{
    ibv_sge isge = {
        reinterpret_cast<uint64_t>(bd->buffer),
        bd->bytes,
        bd->mr->lkey
    };
    ibv_recv_wr rxWorkRequest;

    memset(&rxWorkRequest, 0, sizeof(rxWorkRequest));
    rxWorkRequest.wr_id = reinterpret_cast<uint64_t>(bd);// stash descriptor ptr
    rxWorkRequest.next = NULL;
    rxWorkRequest.sg_list = &isge;
    rxWorkRequest.num_sge = 1;

    ibv_recv_wr *badWorkRequest;
    int ret = ibv_post_srq_recv(srq, &rxWorkRequest, &badWorkRequest);
    if (ret) {
        throw TransportException(HERE, ret);
    }
}

/**
 * Asychronously transmit the packet described by 'bd' on queue pair 'qp'.
 * This function returns immediately. 
 *
 * \param[in] qp
 *      The QueuePair on which to transmit the packet.
 * \param[in] bd
 *      The BufferDescriptor that contains the data to be transmitted.
 * \param[in] length
 *      The number of bytes used by the packet in the given BufferDescriptor.
 */
void
Infiniband::postSend(QueuePair* qp, BufferDescriptor *bd, uint32_t length)
{
    ibv_sge isge = {
        reinterpret_cast<uint64_t>(bd->buffer),
        length,
        bd->mr->lkey
    };
    ibv_send_wr txWorkRequest;

    memset(&txWorkRequest, 0, sizeof(txWorkRequest));
    txWorkRequest.wr_id = reinterpret_cast<uint64_t>(bd);// stash descriptor ptr
    txWorkRequest.next = NULL;
    txWorkRequest.sg_list = &isge;
    txWorkRequest.num_sge = 1;
    txWorkRequest.opcode = IBV_WR_SEND;
    txWorkRequest.send_flags = IBV_SEND_SIGNALED;

    // We can get a substantial latency improvement (nearly 2usec less per RTT)
    // by inlining data with the WQE for small messages. The Verbs library
    // automatically takes care of copying from the SGEs to the WQE.
    if (length <= MAX_INLINE_DATA)
        txWorkRequest.send_flags |= IBV_SEND_INLINE;

    ibv_send_wr *bad_txWorkRequest;
    if (ibv_post_send(qp->qp, &txWorkRequest, &bad_txWorkRequest)) {
        fprintf(stderr, "ibv_post_send failed!\n");
        exit(1);
    }
}

/**
 * Synchronously transmit the packet described by 'bd' on queue pair 'qp'.
 * This function waits to the HCA to return a completion status before
 * returning.
 *
 * \param[in] qp
 *      The QueuePair on which to transmit the packet.
 * \param[in] bd
 *      The BufferDescriptor that contains the data to be transmitted.
 * \param[in] length
 *      The number of bytes used by the packet in the given BufferDescriptor.
 * \param[in] cq
 *      The completion queue to poll.
 * \throw
 *      TransportException is the send does not result in success
 *      (IBV_WC_SUCCESS).
 */
void
Infiniband::postSendAndWait(QueuePair* qp, BufferDescriptor *bd,
    uint32_t length, ibv_cq *cq)
{
    postSend(qp, bd, length);

    ibv_wc wc;
    while (ibv_poll_cq(cq, 1, &wc) < 1) {}
    if (wc.status != IBV_WC_SUCCESS) {
        LOG(ERROR, "%s: wc.status(%d:%s) != IBV_WC_SUCCESS", __func__,
            wc.status, wcStatusToString(wc.status));
        throw TransportException(HERE, "ibPostSend failed");
    }
}

/**
 * Allocate a BufferDescriptor and register the backing memory with the
 * HCA. Note that the memory will be wired (i.e. cannot be swapped out)!
 *
 * \param[in] pd
 *      The protection domain to register this memory with.
 * \param[in] bytes
 *      Number of bytes to allocate.
 * \return
 *      A BufferDescriptor corresponding to the allocated memory.
 * \throw
 *      TransportException if allocation or registration failed.
 */
Infiniband::BufferDescriptor
Infiniband::allocateBufferDescriptorAndRegister(ibv_pd *pd, size_t bytes)
{
    void *p = xmemalign(4096, bytes);

    ibv_mr *mr = ibv_reg_mr(pd, p, bytes,
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
    if (mr == NULL)
        throw TransportException(HERE, "failed to register ring buffer");

    return BufferDescriptor(reinterpret_cast<char *>(p), bytes, mr);
}

//-------------------------------------
// Infiniband::QueuePair class
//-------------------------------------

/**
 * Construct a QueuePair. This object hides some of the ugly
 * initialisation of Infiniband "queue pairs", which are single-side
 * transmit and receive queues. This object can represent both reliable
 * connected (RC) and unreliable datagram (UD) queue pairs. Not all
 * methods are valid to all queue pair types.
 *
 * Somewhat confusingly, each communicating end has a QueuePair, which are
 * bound (one might say "paired", but that's even more confusing). This
 * object is somewhat analogous to a TCB in TCP. 
 *
 * After this method completes, the QueuePair will be in the INIT state.
 * A later call to #plumb() will transition it into the RTS state for
 * regular use with RC queue pairs.
 *
 * \param type
 *      The type of QueuePair to create. Currently valid values are
 *      IBV_QPT_RC for reliable QueuePairs and IBV_QPT_UD for
 *      unreliable ones.
 * \param ibPhysicalPort
 *      The physical port on the HCA we will use this QueuePair on.
 *      The default is 1, though some devices have multiple ports.
 * \param pd
 *      The Verbs protection domain this QueuePair will be associated
 *      with. Only memory registered under this domain can be handled
 *      by this QueuePair.
 * \param srq
 *      The Verbs shared receive queue to associate this QueuePair
 *      with. All writes received will use WQEs placed on the
 *      shared queue. If NULL, do not use a shared receive queue.
 * \param txcq
 *      The Verbs completion queue to be used for transmissions on
 *      this QueuePair.
 * \param rxcq
 *      The Verbs completion queue to be used for receives on this
 *      QueuePair.
 * \param maxSendWr
 *      Maximum number of outstanding send work requests allowed on
 *      this QueuePair.
 * \param maxRecvWr
 *      Maximum number of outstanding receive work requests allowed on
 *      this QueuePair.
 */
Infiniband::QueuePair::QueuePair(ibv_qp_type type, int ibPhysicalPort,
    ibv_pd *pd, ibv_srq *srq, ibv_cq *txcq, ibv_cq *rxcq,
    uint32_t maxSendWr, uint32_t maxRecvWr)
    : type(type),
      ibPhysicalPort(ibPhysicalPort),
      pd(pd),
      srq(srq),
      qp(NULL),
      txcq(txcq),
      rxcq(rxcq),
      initialPsn(generateRandom() & 0xffffff)
{
    if (type != IBV_QPT_RC && type != IBV_QPT_UD)
        throw TransportException(HERE, "invalid queue pair type");

    ibv_qp_init_attr qpia;
    memset(&qpia, 0, sizeof(qpia));
    qpia.send_cq = txcq;
    qpia.recv_cq = rxcq;
    qpia.srq = srq;                    // use the same shared receive queue
    qpia.cap.max_send_wr  = maxSendWr; // max outstanding send requests
    qpia.cap.max_recv_wr  = maxRecvWr; // max outstanding recv requests
    qpia.cap.max_send_sge = 1;         // max send scatter-gather elements
    qpia.cap.max_recv_sge = 1;         // max recv scatter-gather elements
    qpia.cap.max_inline_data =         // max bytes of immediate data on send q
        MAX_INLINE_DATA;
    qpia.qp_type = type;               // RC, UC, UD, or XRC
    qpia.sq_sig_all = 0;               // only generate CQEs on requested WQEs

    qp = ibv_create_qp(pd, &qpia);
    if (qp == NULL) {
        LOG(ERROR, "%s: ibv_create_qp failed", __func__);
        throw TransportException(HERE, "failed to create queue pair");
    }

    // move from RESET to INIT state
    ibv_qp_attr qpa;
    memset(&qpa, 0, sizeof(qpa));
    qpa.qp_state   = IBV_QPS_INIT;
    qpa.pkey_index = 0;
    qpa.port_num   = ibPhysicalPort;
    qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
    qpa.qkey       = UD_QKEY;

    int mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
    switch (type) {
    case IBV_QPT_RC:
        mask |= IBV_QP_ACCESS_FLAGS;
        break;
    case IBV_QPT_UD:
        mask |= IBV_QP_QKEY;
        break;
    default:
        assert(0);
    }

    int ret = ibv_modify_qp(qp, &qpa, mask);
    if (ret) {
        ibv_destroy_qp(qp);
        LOG(ERROR, "%s: failed to transition to INIT state", __func__);
        throw TransportException(HERE, ret);
    }
}

/**
 * Destroy the QueuePair by freeing the Verbs resources allocated.
 */
Infiniband::QueuePair::~QueuePair()
{
    ibv_destroy_qp(qp);
}

/**
 * Bring an newly created RC QueuePair into the RTS state, enabling
 * regular bidirectional communication. This is necessary before
 * the QueuePair may be used. Note that this only applies to
 * RC QueuePairs.
 *
 * \param qpt
 *      QueuePairTuple representing the remote QueuePair. The Verbs
 *      interface requires us to exchange handshaking information
 *      manually. This includes initial sequence numbers, queue pair
 *      numbers, and the HCA infiniband addresses.
 *
 * \throw TransportException
 *      An exception is thrown if this method is called on a QueuePair
 *      that is not of type IBV_QPT_RC, or if the QueuePair is not
 *      in the INIT state.
 */
void
Infiniband::QueuePair::plumb(QueuePairTuple *qpt)
{
    ibv_qp_attr qpa;
    int r;

    if (type != IBV_QPT_RC)
        throw TransportException(HERE, "plumb() called on wrong qp type");

    if (getState() != IBV_QPS_INIT) {
        LOG(ERROR, "%s: plumb() on qp in state %d", __func__, getState());
        throw TransportException(HERE, "plumb() on qp not in INIT state");
    }

    // now connect up the qps and switch to RTR
    memset(&qpa, 0, sizeof(qpa));
    qpa.qp_state = IBV_QPS_RTR;
    qpa.path_mtu = IBV_MTU_1024;
    qpa.dest_qp_num = qpt->getQpn();
    qpa.rq_psn = qpt->getPsn();
    qpa.max_dest_rd_atomic = 1;
    qpa.min_rnr_timer = 12;
    qpa.ah_attr.is_global = 0;
    qpa.ah_attr.dlid = qpt->getLid();
    qpa.ah_attr.sl = 0;
    qpa.ah_attr.src_path_bits = 0;

    r = ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                                IBV_QP_AV |
                                IBV_QP_PATH_MTU |
                                IBV_QP_DEST_QPN |
                                IBV_QP_RQ_PSN |
                                IBV_QP_MIN_RNR_TIMER |
                                IBV_QP_MAX_DEST_RD_ATOMIC);
    if (r) {
        LOG(ERROR, "%s: failed to transition to RTR state", __func__);
        throw TransportException(HERE, r);
    }

    // now move to RTS
    qpa.qp_state = IBV_QPS_RTS;
    qpa.timeout = 14;
    qpa.retry_cnt = 7;
    qpa.rnr_retry = 7;
    qpa.sq_psn = initialPsn;
    qpa.max_rd_atomic = 1;

    r = ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                                IBV_QP_TIMEOUT |
                                IBV_QP_RETRY_CNT |
                                IBV_QP_RNR_RETRY |
                                IBV_QP_SQ_PSN |
                                IBV_QP_MAX_QP_RD_ATOMIC);
    if (r) {
        LOG(ERROR, "%s: failed to transition to RTS state", __func__);
        throw TransportException(HERE, r);
    }

    // the queue pair should be ready to use once the client has finished
    // setting up their end.
    LOG(NOTICE, "%s infiniband qp plumbed: qpn 0x%x, ibPhysicalPort %u",
        __func__, qp->qp_num, ibPhysicalPort);
}

void
Infiniband::QueuePair::activate()
{
    ibv_qp_attr qpa;

    if (type != IBV_QPT_UD)
        throw TransportException(HERE, "activate() called on wrong qp type");

    if (getState() != IBV_QPS_INIT) {
        LOG(ERROR, "%s: activate() on qp in state %d", __func__, getState());
        throw TransportException(HERE, "activate() on qp not in INIT state");
    }

    // now switch to RTR
    memset(&qpa, 0, sizeof(qpa));
    qpa.qp_state = IBV_QPS_RTR;

    int ret = ibv_modify_qp(qp, &qpa, IBV_QP_STATE);
    if (ret) {
        LOG(ERROR, "failed to transition to RTR state");
        throw TransportException(HERE, ret);
    }

    // now move to RTS state
    qpa.qp_state = IBV_QPS_RTS;
    qpa.sq_psn = initialPsn;
    ret = ibv_modify_qp(qp, &qpa, IBV_QP_STATE | IBV_QP_SQ_PSN);
    if (ret) {
        LOG(ERROR, "failed to transition to RTS state");
        throw TransportException(HERE, ret);
    }

    LOG(NOTICE, "%s infiniband qp activated: qpn 0x%x, ibPhysicalPort %u",
        __func__, qp->qp_num, ibPhysicalPort);
}

/**
 * Get the initial packet sequence number for this QueuePair.
 * This is randomly generated on creation. It should not be confused
 * with the remote side's PSN, which is set in #plumb(). 
 */
uint32_t
Infiniband::QueuePair::getInitialPsn() const
{
    return initialPsn;
}

/**
 * Get the local queue pair number for this QueuePair.
 * QPNs are analogous to UDP/TCP port numbers.
 */
uint32_t
Infiniband::QueuePair::getLocalQpNumber() const
{
    return qp->qp_num;
}

/**
 * Get the remote queue pair number for this QueuePair, as set in #plumb().
 * QPNs are analogous to UDP/TCP port numbers.
 */
uint32_t
Infiniband::QueuePair::getRemoteQpNumber() const
{
    ibv_qp_attr qpa;
    ibv_qp_init_attr qpia;

    int r = ibv_query_qp(qp, &qpa, IBV_QP_DEST_QPN, &qpia);
    if (r) {
        // XXX log?!?
        throw TransportException(HERE, r);
    }

    return qpa.dest_qp_num;
}

/**
 * Get the remote infiniband address for this QueuePair, as set in #plumb().
 * LIDs are "local IDs" in infiniband terminology. They are short, locally
 * routable addresses.
 */
uint16_t
Infiniband::QueuePair::getRemoteLid() const
{
    ibv_qp_attr qpa;
    ibv_qp_init_attr qpia;

    int r = ibv_query_qp(qp, &qpa, IBV_QP_AV, &qpia);
    if (r) {
        // XXX log?!?
        throw TransportException(HERE, r);
    }

    return qpa.ah_attr.dlid;
}

int
Infiniband::QueuePair::getState() const
{
    ibv_qp_attr qpa;
    ibv_qp_init_attr qpia;

    int r = ibv_query_qp(qp, &qpa, IBV_QP_STATE, &qpia);
    if (r) {
        // XXX log?!?
        throw TransportException(HERE, r);
    }
    return qpa.qp_state;
}

} // namespace
