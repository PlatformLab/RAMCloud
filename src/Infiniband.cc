/* Copyright (c) 2010-2015 Stanford University
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

#include "CycleCounter.h"
#include "Infiniband.h"
#include "NetUtil.h"
#include "RawMetrics.h"
#include "ShortMacros.h"
#include "Transport.h"

namespace RAMCloud {


/**
 * Construct an Infiniband object.
 * \param[in] deviceName
 *      The string name of the installed interface to look for.
 *      If NULL, open the first one returned by the Verbs library.
 */
Infiniband::Infiniband(const char* deviceName)
    : device(deviceName)
    , pd(device)
    , ahMap()
    , totalAddressHandleAllocCalls()
    , totalAddressHandleAllocTime()
    , totalQpCreates(0)
    , totalQpDeletes(0)
{
}

/**
 * Destroy an Infiniband object.
 */
Infiniband::~Infiniband()
{
}

void
Infiniband::dumpStats()
{
    RAMCLOUD_LOG(NOTICE, "totalAddressHandleAllocCalls: %lu (count)",
        totalAddressHandleAllocCalls);
    RAMCLOUD_LOG(NOTICE, "totalAddressHandleAllocTime: %lu (ticks)",
        totalAddressHandleAllocTime);
    totalAddressHandleAllocCalls = 0;
    totalAddressHandleAllocTime = 0;
}

/**
 * Create a new QueuePair. This factory should be used in preference to
 * the QueuePair constructor directly, since this lets derivatives of
 * Infiniband, e.g. MockInfiniband (if it existed),
 * return mocked out QueuePair derivatives.
 *
 * See QueuePair::QueuePair for parameter documentation.
 */
Infiniband::QueuePair*
Infiniband::createQueuePair(ibv_qp_type type, int ibPhysicalPort, ibv_srq *srq,
                            ibv_cq *txcq, ibv_cq *rxcq, uint32_t maxSendWr,
                            uint32_t maxRecvWr, uint32_t maxSendSges,
                            uint32_t QKey)
{
    return new QueuePair(*this, type, ibPhysicalPort, srq, txcq, rxcq,
                         maxSendWr, maxRecvWr, maxSendSges, QKey);
}

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
 * Obtain the infiniband "local ID" of the device corresponding to
 * the provided context and port number.
 *
 * \param[in] port
 *      Port on the device whose local ID we're looking up. This value
 *      is typically 1, except on adapters with multiple physical ports.
 * \return
 *      The local ID corresponding to the given parameters.
 * \throw
 *      TransportException if the port cannot be queried.
 */
int
Infiniband::getLid(int port)
{
    ibv_port_attr ipa;
    int ret = ibv_query_port(device.ctxt, downCast<uint8_t>(port), &ipa);
    if (ret) {
        RAMCLOUD_LOG(ERROR, "ibv_query_port failed on port %u\n", port);
        throw TransportException(HERE, ret);
    }
    return ipa.lid;
}

/**
 * Obtain the link speed on this port to transmit and receive.
 *
 * \param[in] port
 *      Port on the device we're looking up. This value is typically 1, except
 *      on adapters with multiple physical ports.
 * \return
 *      The bandwidth, in Gbps.
 * \throw
 *      TransportException if the port cannot be queried.
 */
uint32_t
Infiniband::getBandwidthGbps(int port)
{
    ibv_port_attr ipa;
    int ret = ibv_query_port(device.ctxt, downCast<uint8_t>(port), &ipa);
    if (ret) {
        RAMCLOUD_LOG(ERROR, "ibv_query_port failed on port %d", port);
        throw TransportException(HERE, ret);
    }
    // The meaning of fields active_width and active_speed can be found at:
    // https://www.rdmamojo.com/2012/07/21/ibv_query_port/
    uint32_t gbps = 1;
    switch (ipa.active_width) {
        case 1: gbps = 1; break;
        case 2: gbps = 4; break;
        case 4: gbps = 8; break;
        case 8: gbps = 12; break;
        default:
            LOG(ERROR, "unexpected active width %u on port %d",
                    ipa.active_width, port);
    }
    switch (ipa.active_speed) {
        case 1:  gbps = gbps*5/2; break;
        case 2:  gbps *= 5; break;
        case 4:
        case 8:  gbps *= 10; break;
        case 16: gbps *= 14; break;
        case 32: gbps *= 25; break;
        default:
            LOG(ERROR, "unexpected active speed %u on port %d",
                    ipa.active_speed, port);
    }
    return gbps;
}

/**
 * Obtain the MTU enabled on this port to transmit and receive.
 *
 * \param[in] port
 *      Port on the device we're looking up. This value is typically 1, except
 *      on adapters with multiple physical ports.
 * \return
 *      The active MTU, in bytes.
 * \throw
 *      TransportException if the port cannot be queried.
 */
uint32_t
Infiniband::getMtu(int port)
{
    ibv_port_attr ipa;
    int ret = ibv_query_port(device.ctxt, downCast<uint8_t>(port), &ipa);
    if (ret) {
        RAMCLOUD_LOG(ERROR, "ibv_query_port failed on port %u\n", port);
        throw TransportException(HERE, ret);
    }
    switch (ipa.active_mtu) {
        case IBV_MTU_256:   return 256;
        case IBV_MTU_512:   return 512;
        case IBV_MTU_1024:  return 1024;
        case IBV_MTU_2048:  return 2048;
        case IBV_MTU_4096:  return 4096;
        default:
            DIE("Illegal enum ibv_mtu value %u", ipa.active_mtu);
    }
}

/**
 * Add the given BufferDescriptor to the receive queue for the given
 * QueuePair.
 *
 * \param[in] qp
 *      The QueuePair on whose receive queue we are to enqueue this
 *      BufferDescriptor.
 * \param[in] bd
 *      The BufferDescriptor to enqueue.
 * \throw
 *      TransportException if posting to the queue fails.
 */
void
Infiniband::postReceive(QueuePair *qp, BufferDescriptor *bd)
{
    ibv_sge isge = {
        (uint64_t)bd->buffer,
        bd->bytes,
        bd->mr->lkey
    };
    ibv_recv_wr rxWorkRequest;

    memset(&rxWorkRequest, 0, sizeof(rxWorkRequest));
    rxWorkRequest.wr_id   = reinterpret_cast<uint64_t>(bd);
    rxWorkRequest.next    = NULL;
    rxWorkRequest.sg_list = &isge;
    rxWorkRequest.num_sge = 1;

    ibv_recv_wr *badWorkRequest;
    int ret = ibv_post_recv(qp->qp, &rxWorkRequest, &badWorkRequest);
    if (ret) {
        throw TransportException(HERE, ret);
    }
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
 * \param[in] address
 *      UD queue pairs only. The address of the host to send to. 
 * \param[in] remoteQKey
 *      UD queue pairs only. The Q_Key of the remote pair to send to.
 * \throw TransportException
 *      if the send post fails.
 */
void
Infiniband::postSend(QueuePair* qp, BufferDescriptor *bd, uint32_t length,
                     const Address* address, uint32_t remoteQKey)
{
    if (qp->type == IBV_QPT_UD) {
        assert(address != NULL);
    } else {
        assert(address == NULL);
        assert(remoteQKey == 0);
    }

    ibv_sge isge = {
        reinterpret_cast<uint64_t>(bd->buffer),
        length,
        bd->mr->lkey
    };
    ibv_send_wr txWorkRequest;

    memset(&txWorkRequest, 0, sizeof(txWorkRequest));
    txWorkRequest.wr_id = reinterpret_cast<uint64_t>(bd);// stash descriptor ptr
    if (qp->type == IBV_QPT_UD) {
        txWorkRequest.wr.ud.ah = address->getHandle();
        txWorkRequest.wr.ud.remote_qpn = address->getQpn();
        txWorkRequest.wr.ud.remote_qkey = remoteQKey;
    }
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
        throw TransportException(HERE, "ibv_post_send failed");
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
 * \param[in] address
 *      UD queue pairs only. The address of the host to send to. 
 * \param[in] remoteQKey
 *      UD queue pairs only. The Q_Key of the remote pair to send to.
 * \throw
 *      TransportException if the send does not result in success
 *      (IBV_WC_SUCCESS).
 */
void
Infiniband::postSendAndWait(QueuePair* qp, BufferDescriptor *bd,
    uint32_t length, const Address* address, uint32_t remoteQKey)
{
    postSend(qp, bd, length, address, remoteQKey);
    CycleCounter<RawMetric> _(&metrics->transport.transmit.dmaTicks);

    ibv_wc wc;
    while (ibv_poll_cq(qp->txcq, 1, &wc) < 1) {}
    if (wc.status != IBV_WC_SUCCESS) {
        LOG(ERROR, "wc.status(%d:%s) != IBV_WC_SUCCESS",
            wc.status, wcStatusToString(wc.status));
        throw TransportException(HERE, "ibPostSend failed");
    }
}

/**
 * Create a completion queue. This simply wraps the verbs call.
 *
 * \param[in] minimumEntries
 *      The minimum number of completion entries this queue will support.
 * \return
 *      A valid ibv_cq pointer, or NULL on error.
 */
ibv_cq*
Infiniband::createCompletionQueue(int minimumEntries)
{
    return ibv_create_cq(device.ctxt, minimumEntries, NULL, NULL, 0);
}

/**
 * Create an address handle. This simply wraps the verbs call.
 *
 * \param[in] attr
 *      Pointer to an ibv_ah_attr struct describing the handle to
 *      create.
 * \return
 *      A valid ibv_ah pointer, or NULL on error.
 */
ibv_ah*
Infiniband::createAddressHandle(ibv_ah_attr* attr)
{
    return ibv_create_ah(pd.pd, attr);
}

/**
 * Destroy an address handle previously created with createAddressHandle().
 * This simply wraps the verbs call.
 *
 * \param[in] ah
 *      The address handle to destroy.
 */
void
Infiniband::destroyAddressHandle(ibv_ah *ah)
{
    ibv_destroy_ah(ah);
}

/**
 * Create a shared receive queue. This basically wraps the verbs call. 
 *
 * \param[in] maxWr
 *      The max number of outstanding work requests in the SRQ.
 * \param[in] maxSge
 *      The max number of scatter elements per WR.
 * \return
 *      A valid ibv_srq pointer, or NULL on error.
 */
ibv_srq*
Infiniband::createSharedReceiveQueue(uint32_t maxWr, uint32_t maxSge)
{
    ibv_srq_init_attr sia;
    memset(&sia, 0, sizeof(sia));
    sia.srq_context = device.ctxt;
    sia.attr.max_wr = maxWr;
    sia.attr.max_sge = maxSge;
    return ibv_create_srq(pd.pd, &sia);
}

/**
 * Poll a completion queue. This simply wraps the verbs call. 
 *
 * \param[in] cq 
 *      The completion queue to poll.
 * \param[in] numEntries 
 *      The maximum number of work completion entries to obtain.
 * \param[out] retWcArray
 *      Pointer to an array of ``numEntries'' ibv_wc structs.
 *      Completions are returned here.
 * \return
 *      The number of entries obtained. 0 if none, < 0 on error. Strictly less
 *      than ``numEntries'' means that ``cq'' has been emptied.
 */
int
Infiniband::pollCompletionQueue(ibv_cq *cq, int numEntries, ibv_wc *retWcArray)
{
    return ibv_poll_cq(cq, numEntries, retWcArray);
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
 * \param infiniband
 *      The #Infiniband object to associate this QueuePair with.
 * \param type
 *      The type of QueuePair to create. Currently valid values are
 *      IBV_QPT_RC for reliable QueuePairs and IBV_QPT_UD for
 *      unreliable ones.
 * \param ibPhysicalPort
 *      The physical port on the HCA we will use this QueuePair on.
 *      The default is 1, though some devices have multiple ports.
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
 * \param maxSendSges
 *      Maximum number of scatter-gather entries per work request allowed on
 *      this QueuePair.
 * \param QKey
 *      UD Queue Pairs only. The QKey for this pair. 
 */
Infiniband::QueuePair::QueuePair(Infiniband& infiniband, ibv_qp_type type,
    int ibPhysicalPort, ibv_srq *srq, ibv_cq *txcq, ibv_cq *rxcq,
    uint32_t maxSendWr, uint32_t maxRecvWr, uint32_t maxSendSges, uint32_t QKey)
    : infiniband(infiniband),
      type(type),
      ctxt(infiniband.device.ctxt),
      ibPhysicalPort(ibPhysicalPort),
      pd(infiniband.pd.pd),
      srq(srq),
      qp(NULL),
      txcq(txcq),
      rxcq(rxcq),
      initialPsn(generateRandom() & 0xffffff),
      handshakeSin(),
      peerLid(0)
{
    snprintf(peerName, sizeof(peerName), "?unknown?");
    if (type != IBV_QPT_RC && type != IBV_QPT_UD && type != IBV_QPT_RAW_PACKET)
        throw TransportException(HERE, "invalid queue pair type");

    ibv_qp_init_attr qpia;
    memset(&qpia, 0, sizeof(qpia));
    qpia.send_cq = txcq;
    qpia.recv_cq = rxcq;
    qpia.srq = srq;                    // use the same shared receive queue
    qpia.cap.max_send_wr  = maxSendWr; // max outstanding send requests
    qpia.cap.max_recv_wr  = maxRecvWr; // max outstanding recv requests
    qpia.cap.max_send_sge = maxSendSges; // max send scatter-gather elements
    qpia.cap.max_recv_sge = 1;         // max recv scatter-gather elements
    qpia.cap.max_inline_data =         // max bytes of immediate data on send q
        MAX_INLINE_DATA;
    qpia.qp_type = type;               // RC, UC, UD, or XRC
    qpia.sq_sig_all = 0;               // only generate CQEs on requested WQEs

    qp = ibv_create_qp(pd, &qpia);
    if (qp == NULL) {
        LOG(ERROR, "ibv_create_qp failed (%d prior creates, %d deletes): %s",
                infiniband.totalQpCreates, infiniband.totalQpDeletes,
                strerror(errno));
        throw TransportException(HERE, "failed to create queue pair");
    }
    infiniband.totalQpCreates++;

    // move from RESET to INIT state
    ibv_qp_attr qpa;
    memset(&qpa, 0, sizeof(qpa));
    qpa.qp_state   = IBV_QPS_INIT;
    qpa.pkey_index = 0;
    qpa.port_num   = downCast<uint8_t>(ibPhysicalPort);
    qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
    qpa.qkey       = QKey;

    int mask = IBV_QP_STATE | IBV_QP_PORT;
    switch (type) {
    case IBV_QPT_RC:
        mask |= IBV_QP_ACCESS_FLAGS;
        mask |= IBV_QP_PKEY_INDEX;
        break;
    case IBV_QPT_UD:
        mask |= IBV_QP_QKEY;
        mask |= IBV_QP_PKEY_INDEX;
        break;
    case IBV_QPT_RAW_PACKET:
        break;
    default:
        assert(0);
    }

    int ret = ibv_modify_qp(qp, &qpa, mask);
    if (ret) {
        ibv_destroy_qp(qp);
        LOG(ERROR, "failed to transition to INIT state errno %d", errno);
        throw TransportException(HERE, ret);
    }
}

/**
 * Destroy the QueuePair by freeing the Verbs resources allocated.
 */
Infiniband::QueuePair::~QueuePair()
{
    ibv_destroy_qp(qp);
    infiniband.totalQpDeletes++;
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
        LOG(ERROR, "plumb() on qp in state %d", getState());
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
    qpa.ah_attr.port_num = downCast<uint8_t>(ibPhysicalPort);

    r = ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                                IBV_QP_AV |
                                IBV_QP_PATH_MTU |
                                IBV_QP_DEST_QPN |
                                IBV_QP_RQ_PSN |
                                IBV_QP_MIN_RNR_TIMER |
                                IBV_QP_MAX_DEST_RD_ATOMIC);
    if (r) {
        LOG(ERROR, "failed to transition to RTR state");
        throw TransportException(HERE, r);
    }

    // now move to RTS
    qpa.qp_state = IBV_QPS_RTS;

    // How long to wait before retrying if packet lost or server dead.
    // Supposedly the timeout is 4.096us*2^timeout.  However, the actual
    // timeout appears to be 4.096us*2^(timeout+1), so the setting
    // below creates a 135ms timeout.
    qpa.timeout = 14;

    // How many times to retry after timeouts before giving up.
    qpa.retry_cnt = 7;

    // How many times to retry after RNR (receiver not ready) condition
    // before giving up. Occurs when the remote side has not yet posted
    // a receive request.
    qpa.rnr_retry = 7; // 7 is infinite retry.
    qpa.sq_psn = initialPsn;
    qpa.max_rd_atomic = 1;

    r = ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                                IBV_QP_TIMEOUT |
                                IBV_QP_RETRY_CNT |
                                IBV_QP_RNR_RETRY |
                                IBV_QP_SQ_PSN |
                                IBV_QP_MAX_QP_RD_ATOMIC);
    if (r) {
        LOG(ERROR, "failed to transition to RTS state");
        throw TransportException(HERE, r);
    }
    peerLid = qpt->getLid();

    // the queue pair should be ready to use once the client has finished
    // setting up their end.
}

void
Infiniband::QueuePair::activate(const Tub<MacAddress>& localMac)
{
    ibv_qp_attr qpa;
    if (type != IBV_QPT_UD && type != IBV_QPT_RAW_PACKET)
        throw TransportException(HERE, "activate() called on wrong qp type");

    if (getState() != IBV_QPS_INIT) {
        LOG(ERROR, "activate() on qp in state %d", getState());
        throw TransportException(HERE, "activate() on qp not in INIT state");
    }

    // now switch to RTR
    memset(&qpa, 0, sizeof(qpa));
    qpa.qp_state = IBV_QPS_RTR;
    int rtr_flags = IBV_QP_STATE;
    int ret = ibv_modify_qp(qp, &qpa, rtr_flags);
    if (ret) {
        LOG(ERROR, "failed to transition to RTR state: %s", strerror(errno));
        throw TransportException(HERE, ret);
    }

    // now move to RTS state
    qpa.qp_state = IBV_QPS_RTS;
    int rts_flags = IBV_QP_STATE;
    if (type != IBV_QPT_RAW_PACKET) {
        qpa.sq_psn = initialPsn;
        rts_flags |= IBV_QP_SQ_PSN;
    }
    ret = ibv_modify_qp(qp, &qpa, rts_flags);
    if (ret) {
        LOG(ERROR, "failed to transition to RTS state: %s", strerror(errno));
        throw TransportException(HERE, ret);
    }

    // For raw ethernet QP, register a flow steering rule that accepts all
    // RAMCloud packets (identified by EtherType field) addressed to us.
    if (type == IBV_QPT_RAW_PACKET) {
        const uint8_t* local_mac = localMac->address;
        struct raw_eth_flow_attr {
            struct ibv_flow_attr attr;
            struct ibv_flow_spec_eth spec_eth;
        } __attribute__((packed)) flow_attr = {
            .attr = {
                .comp_mask = 0,
                .type = IBV_FLOW_ATTR_NORMAL,
                .size = sizeof(flow_attr),
                .priority = 0,
                .num_of_specs = 1,
                .port = downCast<uint8_t>(ibPhysicalPort),
                .flags = 0,
            },
            .spec_eth = {
                .type = IBV_FLOW_SPEC_ETH,
                .size = sizeof(struct ibv_flow_spec_eth),
                .val = {
                    .dst_mac = { local_mac[0], local_mac[1], local_mac[2],
                                 local_mac[3], local_mac[4], local_mac[5] },
                    .src_mac = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
                    .ether_type = HTONS(NetUtil::EthPayloadType::RAMCLOUD),
                    .vlan_tag = 0,
                },
                .mask = {
                    .dst_mac = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF },
                    .src_mac = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
                    .ether_type = 0xFFFF,
                    .vlan_tag = 0,
                }
            }
        };

        struct ibv_flow *eth_flow;
        eth_flow = ibv_create_flow(qp, &flow_attr.attr);
        if (!eth_flow) {
            LOG(ERROR, "failed to attach steering flow: %s", strerror(errno));
            throw TransportException(HERE, ret);
        }
    }
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
 *
 * \throw
 *      TransportException is thrown if querying the queue pair
 *      fails.
 */
uint32_t
Infiniband::QueuePair::getRemoteQpNumber() const
{
    ibv_qp_attr qpa;
    ibv_qp_init_attr qpia;

    int r = ibv_query_qp(qp, &qpa, IBV_QP_DEST_QPN, &qpia);
    if (r) {
        // We should probably log something here.
        throw TransportException(HERE, r);
    }

    return qpa.dest_qp_num;
}

/**
 * Get the remote infiniband address for this QueuePair, as set in #plumb().
 * LIDs are "local IDs" in infiniband terminology; each LID provides a
 * unique identifier for a particular NIC.
 *
 * \throw
 *      TransportException is thrown if querying the queue pair
 *      fails.
 */
uint16_t
Infiniband::QueuePair::getRemoteLid() const
{
    return peerLid;
}

/**
 * Get the state of a QueuePair.
 *
 * \throw
 *      TransportException is thrown if querying the queue pair
 *      fails.
 */
int
Infiniband::QueuePair::getState() const
{
    ibv_qp_attr qpa;
    ibv_qp_init_attr qpia;

    int r = ibv_query_qp(qp, &qpa, IBV_QP_STATE, &qpia);
    if (r) {
        // We should probably log something here.
        throw TransportException(HERE, r);
    }
    return qpa.qp_state;
}

/**
 * Return true if the queue pair is in an error state, false otherwise.
 *
 * \throw
 *      TransportException is thrown if querying the queue pair
 *      fails.
 */
bool
Infiniband::QueuePair::isError() const
{
    ibv_qp_attr qpa;
    ibv_qp_init_attr qpia;

    int r = ibv_query_qp(qp, &qpa, -1, &qpia);
    if (r) {
        // We should probably log something here.
        throw TransportException(HERE, r);
    }
    return qpa.cur_qp_state == IBV_QPS_ERR;
}

/**
 * Provide information that can be used in log messages to identify the
 * other end of this connection.
 *
 * \param name
 *      Human-readable name for the application or machine at the other
 *      end of this connection.
 */
void
Infiniband::QueuePair::setPeerName(const char* name)
{
    snprintf(peerName, sizeof(peerName), "%s", name);
}

const char*
Infiniband::QueuePair::getPeerName() const
{
    return peerName;
}

/**
 * Get counter part Socket name of the Queue Pair
 **/
const string
Infiniband::QueuePair::getSinName() const
{
    return format("infrc:host=%s,port=%hu",
       inet_ntoa(handshakeSin.sin_addr), NTOHS(handshakeSin.sin_port));
}

/**
 * Construct an Address from the information in a ServiceLocator.
 * \param infiniband
 *      Infiniband instance under which this address is valid.
 * \param physicalPort
 *      The physical port number on the local device through which to send.
 * \param serviceLocator
 *      The "lid" and "qpn" options describe the desired address.
 * \throw BadAddress
 *      The serviceLocator couldn't be converted to an Address
 *      (e.g. a required option was missing, or the host name
 *      couldn't be parsed).
 */
Infiniband::Address::Address(Infiniband& infiniband,
                             int physicalPort,
                             const ServiceLocator* serviceLocator)
    : infiniband(infiniband)
    , physicalPort(physicalPort)
    , lid()
    , qpn()
    , ah(NULL)
{
    try {
        lid = serviceLocator->getOption<uint16_t>("lid");
    } catch (NoSuchKeyException &e) {
        throw BadAddressException(HERE,
            "Mandatory option ``lid'' missing from infiniband ServiceLocator.",
            serviceLocator);
    } catch (...) {
        throw BadAddressException(HERE,
            "Could not parse lid. Invalid or out of range.",
            serviceLocator);
    }

    try {
        qpn = serviceLocator->getOption<uint32_t>("qpn");
    } catch (NoSuchKeyException &e) {
        throw BadAddressException(HERE,
            "Mandatory option ``qpn'' missing from infiniband "
            "ServiceLocator.", serviceLocator);
    } catch (...) {
        throw BadAddressException(HERE,
            "Could not parse qpn. Invalid or out of range.",
            serviceLocator);
    }
}

Infiniband::Address::~Address() {
    // Don't call ibv_destroy_ah anymore: we keep address handles
    // forever in the ahMap cache.
}

uint64_t
Infiniband::Address::getHash() const
{
    uint64_t hash = lid;
    hash = (hash << 32) + qpn;
    return hash;
}

/**
 * Return a string describing the contents of this Address (host
 * address & port).
 */
string
Infiniband::Address::toString() const
{
    return format("%u:%u", lid, qpn);
}

/**
 * Return an Infiniband address handle for this Address.
 *
 * Performance note: The first time this is called for a particular lid, it
 * will allocate memory for the address handle, which is an expensive
 * operation.
 *
 * \throw TransportException
 *      if ibv_create_ah fails
 */
ibv_ah*
Infiniband::Address::getHandle() const
{
    if (ah != NULL) {
        return ah;
    }

    // See if we have a cached value.
    AddressHandleMap::iterator it = infiniband.ahMap.find(lid);
    if (it != infiniband.ahMap.end()) {
        ah = it->second;
        return ah;
    }

    // Must allocate a new address handle. See also:
    // https://www.rdmamojo.com/2012/09/22/ibv_create_ah/
    ibv_ah_attr attr = {
        .grh = {},
        .dlid = lid,
        .sl = 0,
        .src_path_bits = 0,
        .static_rate = 0,
        .is_global = 0,
        .port_num = downCast<uint8_t>(physicalPort)
    };
    infiniband.totalAddressHandleAllocCalls += 1;
    uint64_t start = Cycles::rdtsc();
    ah = ibv_create_ah(infiniband.pd.pd, &attr);
    infiniband.totalAddressHandleAllocTime += Cycles::rdtsc() - start;
    if (ah == NULL)
        throw TransportException(HERE, "failed to create ah", errno);
    infiniband.ahMap[lid] = ah;
    return ah;
}

} // namespace
