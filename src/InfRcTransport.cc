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

/**
 * \file
 * Implementation of an Infiniband reliable transport layer using reliable
 * connected queue pairs. Handshaking is done over IP/UDP and addressing
 * is based on that, i.e. addresses look like normal IP/UDP addresses
 * because the infiniband queue pair set up is bootstrapped over UDP.
 *
 * The transport uses a common pool of receive and transmit buffers that
 * are pre-registered with the HCA for direct access. All receive buffers
 * are placed on a shared receive queue, which avoids having to allocate
 * buffers to individual receive queues for each client queue pair (this would
 * be costly for many queue pairs, and wasteful if they're idle). The shared
 * receive queue can be associated with many queue pairs, each of which has
 * their own completion queue. This allows us to demultiplex receive events
 * for each client queue pair (ClientRpc::getReply()), as well as the server's
 * incoming RPC queue pair (serverRecv()). It also lets us block easily on a
 * synchronous getReply() without having to worry about dealing with incoming
 * server RPC requests that occur before the response comes back.
 *
 * In short, the receive path looks like the following:
 *  - As a server, we have just one completion queue for all client queue pairs.
 *  - As a client, we have N completion queues for N client queue pairs.
 *
 * For the transmit path, we have one completion queue for all cases, since
 * we currently do synchronous sends.
 *
 * Each receive and transmit buffer is sized large enough for the maximum
 * possible RPC size for simplicity. (XXX It's unclear to me what happens if
 * we send a buffer too large for the receiver.)
 *
 * To reference the buffer associated with each work queue element on the shared
 * receive queue, we stash pointers in the 64-bit `wr_id' field of the work
 * request.
 *
 * Connected queue pairs require some bootstrapping, which we do as follows:
 *  - The server maintains a UDP listen port.
 *  - Clients establish QPs by sending their tuples to the server as a request.
 *    Tuples are basically (address, queue pair number, sequence number),
 *    similar to TCP. Think of this as TCP's SYN packet.
 *  - Servers receive client tuples, create an associated queue pair, and
 *    reply via UDP with their QP's tuple. Think of this as TCP's SYN/ACK.
 *  - Clients receive the server's tuple reply and complete their queue pair
 *    setup. Communication over infiniband is ready to go. 
 *
 * Of course, using UDP means these things can get lost. We should have a
 * mechanism for cleaning up halfway-completed QPs that occur when clients
 * die before completing or never get the server's UDP response. Similarly,
 * clients right now block forever if the request is lost. They should time
 * out and retry, although at what level retries should occur isn't clear.
 */

#include <errno.h>
#include <fcntl.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "Common.h"
#include "Transport.h"
#include "InfRcTransport.h"
#include "ServiceLocator.h"

#define check_error_null(x, s)                              \
    do {                                                    \
        if ((x) == NULL) {                                  \
            LOG(ERROR, "%s: %s", __func__, s);              \
            throw TransportException(HERE, errno);          \
        }                                                   \
    } while (0)

namespace RAMCloud {

/**
 * Given a string representation of the `status' field from Verbs
 * struct `ibv_wc'.
 */
static const char*
wcStatusToString(int status)
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

//------------------------------
// InfRcTransport class
//------------------------------

/**
 * Construct a InfRcTransport.
 *
 * \param sl
 *      The ServiceLocator describing which HCA to use and the IP/UDP
 *      address and port numbers to use for handshaking. If NULL,
 *      the transport will be configured for client use only.
 */
InfRcTransport::InfRcTransport(const ServiceLocator *sl)
    : currentTxBuffer(0),
      srq(NULL),
      dev(NULL),
      ctxt(NULL),
      pd(NULL),
      serverRxCq(NULL),
      commonTxCq(NULL),
      ibPhysicalPort(1),
      udpListenPort(0),
      serverSetupSocket(-1),
      clientSetupSocket(-1),
      queuePairMap()
{
    static_assert(sizeof(InfRcTransport::QueuePairTuple) == 10);

    const char *ibDeviceName = NULL;

    if (sl != NULL) {
        try {
            ibDeviceName   = sl->getOption<const char *>("dev");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

        try {
            ibPhysicalPort = sl->getOption<int>("devport");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

        try {
            udpListenPort  = sl->getOption<int>("port");
        } catch (ServiceLocator::NoSuchKeyException& e) {}
    }

    // Do nothing if we're not specifically configured and there are no
    // Infiniband interfaces.
    //
    // XXX- TransportManager should probably take an exception and
    //      be sure to ignore this Transport in the future.
    if (sl == NULL && ibFindDevice(ibDeviceName) == NULL)
        return;

    // Step 1:
    //  Set up the udp sockets we use for out-of-band infiniband handshaking.

    // For clients, the kernel will automatically assign a dynamic port on
    // first use.
    clientSetupSocket = socket(PF_INET, SOCK_DGRAM, 0);
    if (clientSetupSocket == -1) {
        LOG(ERROR, "%s: failed to create client socket", __func__);
        throw TransportException(HERE, "client socket failed");
    }

    // If this is a server, create a server setup socket and bind it.
    if (sl != NULL) {
        struct sockaddr_in sin;
        sin.sin_family = PF_INET;
        sin.sin_port   = htons(udpListenPort);
        sin.sin_addr.s_addr = INADDR_ANY;

        serverSetupSocket = socket(PF_INET, SOCK_DGRAM, 0);
        if (serverSetupSocket == -1) {
            LOG(ERROR, "%s: failed ot create server socket", __func__);
            throw TransportException(HERE, "server socket failed");
        }

        if (bind(serverSetupSocket, reinterpret_cast<sockaddr *>(&sin),
          sizeof(sin))) {
            close(serverSetupSocket);
            LOG(ERROR, "%s: failed to bind socket", __func__);
            throw TransportException(HERE, "socket failed");
        }

        int flags = fcntl(serverSetupSocket, F_GETFL);
        if (flags == -1) {
            close(serverSetupSocket);
            LOG(ERROR, "%s: fcntl F_GETFL failed", __func__);
            throw TransportException(HERE, "fnctl failed");
        }
        if (fcntl(serverSetupSocket, F_SETFL, flags | O_NONBLOCK)) {
            close(serverSetupSocket);
            LOG(ERROR, "%s: fcntl F_GETFL failed", __func__);
            throw TransportException(HERE, "fnctl failed");
        }
    }

    // Step 2:
    //  Set up the initial verbs necessities: open the device, allocate
    //  protection domain, create shared receive queue, register buffers.

    dev = ibFindDevice(ibDeviceName);
    check_error_null(dev, "failed to find infiniband device");

    ctxt = ibv_open_device(dev);
    check_error_null(ctxt, "failed to open infiniband device");

    pd = ibv_alloc_pd(ctxt);
    check_error_null(pd, "failed to allocate infiniband pd");

    // create a shared receive queue. all queue pairs use this and we
    // post receive buffer work requests to this queue only. the motiviation
    // is to avoid having to post at least one buffer to every single queue
    // pair (we may have thousands of them with megabyte buffers).
    ibv_srq_init_attr sia;
    memset(&sia, 0, sizeof(sia));
    sia.srq_context = ctxt;
    sia.attr.max_wr = MAX_SHARED_RX_QUEUE_DEPTH;
    sia.attr.max_sge = MAX_SHARED_RX_SGE_COUNT;

    srq = ibv_create_srq(pd, &sia);
    check_error_null(srq, "failed to create shared receive queue");

    // XXX- for now we allocate TX and RX buffers and use them as a ring.
    for (uint32_t i = 0; i < MAX_SHARED_RX_QUEUE_DEPTH; i++) {
        rxBuffers[i] = allocateBufferDescriptorAndRegister();
        ibPostSrqReceive(&rxBuffers[i]);
    }
    for (uint32_t i = 0; i < MAX_TX_QUEUE_DEPTH; i++)
        txBuffers[i] = allocateBufferDescriptorAndRegister();

    // create completion queues for server receive and server/client transmit
    serverRxCq = ibv_create_cq(ctxt, MAX_SHARED_RX_QUEUE_DEPTH,
        NULL, NULL, 0);
    check_error_null(serverRxCq, "failed to create receive completion queue");

    commonTxCq = ibv_create_cq(ctxt, MAX_TX_QUEUE_DEPTH, NULL, NULL, 0);
    check_error_null(commonTxCq, "failed to create receive completion queue");
}

/**
 * Wait for an incoming request.
 *
 * The server polls the infiniband shared receive queue, as well as
 * the UDP setup socket. The former contains incoming RPCs, whereas
 * the latter is used to set up QueuePairs between clients and the
 * server, as an out-of-band handshake is needed.
 */
Transport::ServerRpc*
InfRcTransport::serverRecv()
{
    // query the infiniband adapter first. if there's nothing to process,
    // try to read a datagram from a connecting client.
    // in the future, this should occur in separate threads.
    while (1) {
        ibv_wc wc;

        if (ibv_poll_cq(serverRxCq, 1, &wc) >= 1) {
            if (queuePairMap.find(wc.qp_num) == queuePairMap.end()) {
                LOG(ERROR, "%s: failed to find qp_num in map", __func__);
                continue;
            }

            QueuePair *qp = queuePairMap[wc.qp_num];

            BufferDescriptor* bd =
                reinterpret_cast<BufferDescriptor*>(wc.wr_id);

            if (wc.status == IBV_WC_SUCCESS) {
                ServerRpc *r = new ServerRpc(this, qp);
                PayloadChunk::appendToBuffer(&r->recvPayload, bd->buffer,
                    wc.byte_len, this, bd);
                return r;
            }

            LOG(ERROR, "%s: failed to receive rpc!", __func__);
            ibPostSrqReceive(bd);
        } else {
            serverTrySetupQueuePair();
        }
    }
}

/**
 * Construct a Session object for the public #getSession() interface.
 *
 * \param transport
 *      The transport this Session will be associated with.
 * \param sl
 *      The ServiceLocator describing the server to communicate with.
 */
InfRcTransport::InfRCSession::InfRCSession(InfRcTransport *transport,
    const ServiceLocator& sl)
    : transport(transport),
      qp(NULL)
{
    const char *ip = sl.getOption<const char*>("host");
    int port = sl.getOption<uint16_t>("port");

    // create and set up a new queue pair for this client
    qp = transport->clientTrySetupQueuePair(ip, port);
}

/**
 * Destroy the Session.
 */
void
InfRcTransport::InfRCSession::release()
{
    delete this;
}

/**
 * Issue an RPC request using infiniband.
 *
 * \param request
 *      Contents of the request message.
 * \param[out] response
 *      When a response arrives, the response message will be made
 *      available via this Buffer.
 *
 * \return  A pointer to the allocated space or \c NULL if there is not enough
 *          space in this Allocation.
 */
Transport::ClientRpc*
InfRcTransport::InfRCSession::clientSend(Buffer* request, Buffer* response)
{
    InfRcTransport *t = transport;

    if (request->getTotalLength() > t->getMaxRpcSize()) {
        throw TransportException(HERE,
                                 "client request exceeds maximum rpc size");
    }

    // send out the request
    BufferDescriptor* bd = &t->txBuffers[t->currentTxBuffer];
    t->currentTxBuffer = (t->currentTxBuffer + 1) % MAX_TX_QUEUE_DEPTH;
    request->copy(0, request->getTotalLength(), bd->buffer);
    t->ibPostSendAndWait(qp, bd, request->getTotalLength());

    // Construct our ClientRpc in the response Buffer.
    //
    // We do this because we're loaning one of our registered receive buffers
    // to the caller of getReply() and need to issue it back to the HCA when
    // they're done with it.
    ClientRpc *rpc = new(response, MISC) ClientRpc(transport, qp, response);

    return rpc;
}

/**
 * Attempt to set up a QueuePair with the given server. The client
 * allocates a QueuePair and sends the necessary tuple to the
 * server to begin the handshake. The server then replies with its
 * QueuePair tuple information. This is all done over IP/UDP.
 */
InfRcTransport::QueuePair*
InfRcTransport::clientTrySetupQueuePair(const char* ip, int port)
{
    // XXX for slightly more security/robustness, we might want to have
    //     the client include a nonce with their request and have the
    //     server include it in the reply

    sockaddr_in sin;
    sin.sin_family = PF_INET;
    sin.sin_addr.s_addr = inet_addr(ip);
    sin.sin_port = htons(port);

    // Create a per-client-QP receive completion queue. This lets us avoid
    // having to manually demultiplex between the server QP and the various
    // client QPs. 
    ibv_cq *cq = ibv_create_cq(ctxt, MAX_SHARED_RX_QUEUE_DEPTH,
        NULL, NULL, 0);
    check_error_null(cq, "failed to create receive completion queue");

    // Create a new QueuePair and send its parameters to the server so it
    // can create its qp and reply with its parameters.
    QueuePair *qp = new QueuePair(ibPhysicalPort, pd, srq, commonTxCq, cq);
    QueuePairTuple outgoingQpt(ibGetLid(), qp->getLocalQpNumber(),
        qp->getInitialPsn());

    ssize_t len = sendto(clientSetupSocket, &outgoingQpt, sizeof(outgoingQpt),
        0, reinterpret_cast<sockaddr *>(&sin), sizeof(sin));
    if (len != sizeof(outgoingQpt)) {
        LOG(ERROR, "%s: sendto was short: %Zd", __func__, len);
        delete qp;
        throw TransportException(HERE, len);
    }

    QueuePairTuple incomingQpt;
    socklen_t sinlen = sizeof(sin);
    len = recvfrom(clientSetupSocket, &incomingQpt, sizeof(incomingQpt), 0,
        reinterpret_cast<sockaddr *>(&sin), &sinlen);
    if (len != sizeof(incomingQpt)) {
        LOG(ERROR, "%s: recvfrom was short: %Zd (errno %d: %s)", __func__, len,
            errno, strerror(errno));
        delete qp;
        throw TransportException(HERE, len);
    }

    // XXX- probably good to have that nonce...
    // XXX- also, need to add timeout/retry here.

    // plumb up our queue pair with the server's parameters.
    qp->plumb(&incomingQpt);

    return qp;
}

/**
 * Attempt to set up QueuePair with a connecting remote client. This
 * function does a non-blocking receive of an incoming client handshake,
 * creates the appropriate QueuePair on the server, and replies with its
 * parameters so that the client can complete its handshake and plumb
 * their QueuePair.
 */
void
InfRcTransport::serverTrySetupQueuePair()
{
    sockaddr_in sin;
    socklen_t sinlen = sizeof(sin);
    QueuePairTuple incomingQpt;

    ssize_t len = recvfrom(serverSetupSocket, &incomingQpt,
        sizeof(incomingQpt), 0, reinterpret_cast<sockaddr *>(&sin), &sinlen);
    if (len <= -1) {
        if (errno == EAGAIN)
            return;

        LOG(ERROR, "%s: recvfrom failed", __func__);
        throw TransportException(HERE, "recvfrom failed");
    } else if (len != sizeof(incomingQpt)) {
        LOG(WARNING, "%s: recvfrom got a strange incoming size: %Zd",
            __func__, len);
        return;
    }

    // create a new queue pair, set it up according to our client's parameters,
    // and feed back our lid, qpn, and psn information so they can complete
    // the out-of-band handshake.

    // XXX- we should look up the QueuePair first using incomingQpt, just to
    //      be sure, esp. if we use an unreliable means of handshaking, in
    //      which case the response to the client request could have been lost.

    QueuePair *qp = new QueuePair(ibPhysicalPort, pd, srq,
        commonTxCq, serverRxCq);
    qp->plumb(&incomingQpt);

    // now send the client back our queue pair information so they can
    // complete the initialisation.
    QueuePairTuple outgoingQpt(ibGetLid(), qp->getLocalQpNumber(),
        qp->getInitialPsn());
    len = sendto(serverSetupSocket, &outgoingQpt, sizeof(outgoingQpt), 0,
        reinterpret_cast<sockaddr *>(&sin), sinlen);
    if (len != sizeof(outgoingQpt)) {
        LOG(WARNING, "%s: sendto failed, len = %Zd\n", __func__, len);
        delete qp;
    }

    // maintain the qpn -> qp mapping
    queuePairMap[qp->getLocalQpNumber()] = qp;
}

/**
 * Find an installed infiniband device by name.
 *
 * \param name
 *      The string name of the interface to look for. If NULL,
 *      return the first one returned by the Verbs library.
 */
ibv_device*
InfRcTransport::ibFindDevice(const char *name)
{
    ibv_device **devices;

    devices = ibv_get_device_list(NULL);
    if (devices == NULL)
        return NULL;

    if (name == NULL)
        return devices[0];

    for (int i = 0; devices[i] != NULL; i++) {
        if (strcmp(devices[i]->name, name) == 0)
            return devices[i];
    }

    return NULL;
}

/**
 * Obtain the infiniband "local ID" of the currently used device and
 * port.
 */
int
InfRcTransport::ibGetLid()
{
    ibv_port_attr ipa;
    int ret = ibv_query_port(ctxt, ibPhysicalPort, &ipa);
    if (ret) {
        LOG(ERROR, "ibv_query_port failed on port %u\n", ibPhysicalPort);
        throw TransportException(HERE, ret);
    }
    return ipa.lid;
}

/**
 * Add the given BufferDescriptor to the shared receive queue.
 * 
 * \param bd
 *      The BufferDescriptor to add to the queue.
 */
void
InfRcTransport::ibPostSrqReceive(BufferDescriptor *bd)
{
    ibv_sge isge = {
        reinterpret_cast<uint64_t>(bd->buffer),
        getMaxRpcSize(),
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
 * \param qp
 *      The QueuePair on which to transmit the packet.
 * \param bd
 *      The BufferDescriptor that contains the data to be transmitted.
 * \param length
 *      The number of bytes used by the packet in the given BufferDescriptor.
 */
void
InfRcTransport::ibPostSend(QueuePair* qp, BufferDescriptor *bd, uint32_t length)
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
 * \param qp
 *      The QueuePair on which to transmit the packet.
 * \param bd
 *      The BufferDescriptor that contains the data to be transmitted.
 * \param length
 *      The number of bytes used by the packet in the given BufferDescriptor.
 */
void
InfRcTransport::ibPostSendAndWait(QueuePair* qp, BufferDescriptor *bd,
    uint32_t length)
{
    ibPostSend(qp, bd, length);

    ibv_wc wc;
    while (ibv_poll_cq(commonTxCq, 1, &wc) < 1) {}
    if (wc.status != IBV_WC_SUCCESS) {
        LOG(ERROR, "%s: wc.status(%d:%s) != IBV_WC_SUCCESS", __func__,
            wc.status, wcStatusToString(wc.status));
        throw TransportException(HERE, "ibPostSend failed");
    }
}

/**
 * Allocate a BufferDescriptor and register the backing memory with
 * the HCA.
 */
InfRcTransport::BufferDescriptor
InfRcTransport::allocateBufferDescriptorAndRegister()
{
    void *p = xmemalign(4096, getMaxRpcSize());

    ibv_mr *mr = ibv_reg_mr(pd, p, getMaxRpcSize(),
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
    check_error_null(mr, "failed to register ring buffer");

    return BufferDescriptor(reinterpret_cast<char *>(p), mr);
}

/**
 * Obtain the maximum rpc size. This is limited by the infiniband
 * specification to 2GB(!), though we artificially limit it to a
 * little more than a segment size to avoid allocating too much 
 * space in RX buffers.
 */
uint32_t
InfRcTransport::getMaxRpcSize() const
{
    return MAX_RPC_SIZE;
}

//-------------------------------------
// InfRcTransport::ServerRpc class
//-------------------------------------

/**
 * Construct a ServerRpc.
 * The input message is taken from transport->inputMessage, if
 * it contains data.
 *
 * \param transport
 *      The InfRcTransport object that this RPC is associated with.
 * \param qp
 *      The QueuePair associated with this RPC request.
 */
InfRcTransport::ServerRpc::ServerRpc(InfRcTransport* transport, QueuePair* qp)
    : transport(transport),
      qp(qp)
{
}

/**
 * Send a reply for an RPC.
 *
 * Transmits are done using a copy into a pre-registered HCA buffer.
 * The function blocks until the HCA returns success or failure.
 */
void
InfRcTransport::ServerRpc::sendReply()
{
    // "delete this;" on our way out of the method
    std::auto_ptr<InfRcTransport::ServerRpc> suicide(this);

    InfRcTransport *t = transport;

    if (replyPayload.getTotalLength() > t->getMaxRpcSize()) {
        throw TransportException(HERE,
                                 "server response exceeds maximum rpc size");
    }

    BufferDescriptor* bd = &t->txBuffers[t->currentTxBuffer];
    t->currentTxBuffer = (t->currentTxBuffer + 1) % MAX_TX_QUEUE_DEPTH;
    replyPayload.copy(0, replyPayload.getTotalLength(), bd->buffer);
    t->ibPostSendAndWait(qp, bd, replyPayload.getTotalLength());
}

//-------------------------------------
// InfRcTransport::ClientRpc class
//-------------------------------------

/**
 * Construct a ClientRpc.
 *
 * \param transport
 *      The InfRcTransport object that this RPC is associated with.
 * \param qp
 *      The QueuePair that is being used for thi RPC.
 * \param[out] response
 *      Buffer in which the response message should be placed.
 */
InfRcTransport::ClientRpc::ClientRpc(InfRcTransport* transport,
                                     QueuePair* qp, Buffer* response)
    : transport(transport),
      qp(qp),
      response(response)
{

}

/**
 * Blocks until the response buffer associated with this RPC is valid and
 * populated.
 *
 * This method must be called for each RPC before its result can be used.
 *
 * \throws TransportException
 *      If the RPC aborted.
 */
void
InfRcTransport::ClientRpc::getReply()
{
    InfRcTransport *t = transport;

    ibv_wc wc;
    while (ibv_poll_cq(qp->rxcq, 1, &wc) < 1) {}

    BufferDescriptor *bd = reinterpret_cast<BufferDescriptor *>(wc.wr_id);
    if (wc.status != IBV_WC_SUCCESS) {
        LOG(ERROR, "%s: wc.status(%d:%s) != IBV_WC_SUCCESS", __func__,
            wc.status, wcStatusToString(wc.status));
        transport->ibPostSrqReceive(bd);
        throw TransportException(HERE, wc.status);
    }

    PayloadChunk::appendToBuffer(response, bd->buffer, wc.byte_len, t, bd);
}

//-------------------------------------
// InfRcTransport::QueuePair class
//-------------------------------------

/**
 * Construct a QueuePair. This object hides some of the ugly
 * initialisation of Infiniband "queue pairs", which are single-side
 * transmit and receive queues. Somewhat confusingly, each communicating
 * end has a QueuePair, which are bound (one might say "paired", but that's
 * even more confusing). This object is somewhat analogous to a TCB in TCP. 
 *
 * After this method completes, the QueuePair will be in the INIT state.
 * A later call to #plumb() will transition it into the RTS state for
 * regular use.
 *
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
 *      shared queue.
 * \param txcq
 *      The Verbs completion queue to be used for transmissions on
 *      this QueuePair.
 * \param rxcq
 *      The Verbs completion queue to be used for receives on this
 *      QueuePair.
 */
InfRcTransport::QueuePair::QueuePair(int ibPhysicalPort, ibv_pd *pd,
    ibv_srq *srq, ibv_cq *txcq, ibv_cq *rxcq)
    : ibPhysicalPort(ibPhysicalPort),
      pd(pd),
      srq(srq),
      qp(NULL),
      txcq(txcq),
      rxcq(rxcq),
      initialPsn(0)
{
    const uint32_t maxSnd = MAX_TX_QUEUE_DEPTH;
    const uint32_t maxRcv = MAX_SHARED_RX_QUEUE_DEPTH;

    ibv_qp_init_attr qpia;
    memset(&qpia, 0, sizeof(qpia));
    qpia.send_cq = txcq;
    qpia.recv_cq = rxcq;
    qpia.srq = srq;                 // use the same shared receive queue
    qpia.cap.max_send_wr  = maxSnd; // max outstanding send requests
    qpia.cap.max_recv_wr  = maxRcv; // max outstanding recv requests
    qpia.cap.max_send_sge = 1;      // max send scatter-gather elements
    qpia.cap.max_recv_sge = 1;      // max recv scatter-gather elements
    qpia.cap.max_inline_data = 0;   // max bytes of immediate data on send q
    qpia.qp_type = IBV_QPT_RC;      // RC, UC, UD, or XRC
    qpia.sq_sig_all = 0;            // only generate CQEs on requested WQEs

    qp = ibv_create_qp(pd, &qpia);
    check_error_null(qp, "failed to create queue pair");

    // move from RESET to INIT state
    ibv_qp_attr qpa;
    memset(&qpa, 0, sizeof(qpa));
    qpa.qp_state   = IBV_QPS_INIT;
    qpa.pkey_index = 0;
    qpa.port_num   = ibPhysicalPort;
    qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
    int ret = ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                                      IBV_QP_PKEY_INDEX |
                                      IBV_QP_PORT |
                                      IBV_QP_ACCESS_FLAGS);
    if (ret) {
        ibv_destroy_qp(qp);
        LOG(ERROR, "%s: failed to transition to INIT state", __func__);
        throw TransportException(HERE, ret);
    }

    initialPsn = generateRandom() & 0xffffff;
}

/**
 * Destroy the QueuePair by freeing the Verbs resources allocated.
 */
InfRcTransport::QueuePair::~QueuePair()
{
    ibv_destroy_qp(qp);
}

/**
 * Bring an newly created QueuePair into the RTS state, enabling
 * regular bidirectional communication. This is necessary before
 * the QueuePair may be used.
 *
 * \param qpt
 *      QueuePairTuple representing the remote QueuePair. The Verbs
 *      interface requires us to exchange handshaking information
 *      manually. This includes initial sequence numbers, queue pair
 *      numbers, and the HCA infiniband addresses.
 */
void
InfRcTransport::QueuePair::plumb(QueuePairTuple *qpt)
{
    ibv_qp_attr qpa;
    int r;

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
}

/**
 * Get the initial packet sequence number for this QueuePair.
 * This is randomly generated on creation. It should not be confused
 * with the remote side's PSN, which is set in #plumb(). 
 */
uint32_t
InfRcTransport::QueuePair::getInitialPsn() const
{
    return initialPsn;
}

/**
 * Get the local queue pair number for this QueuePair.
 * QPNs are analogous to UDP/TCP port numbers.
 */
uint32_t
InfRcTransport::QueuePair::getLocalQpNumber() const
{
    return qp->qp_num;
}

/**
 * Get the remote queue pair number for this QueuePair, as set in #plumb().
 * QPNs are analogous to UDP/TCP port numbers.
 */
uint32_t
InfRcTransport::QueuePair::getRemoteQpNumber() const
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
InfRcTransport::QueuePair::getRemoteLid() const
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

//-------------------------------------
// InfRcTransport::PayloadChunk class
//-------------------------------------

/**
 * Append a subregion of payload data which releases the memory to the
 * HCA when its containing Buffer is destroyed.
 *
 * \param buffer
 *      The Buffer to append the data to.
 * \param data
 *      The address of the data to appear in the Buffer.
 * \param dataLength
 *      The length in bytes of the region starting at data that is a
 *      subregion of the payload.
 * \param transport
 *      The transport that owns the provided BufferDescriptor 'bd'.
 * \param bd 
 *      The BufferDescriptor to return to the HCA on Buffer destruction.
 */
InfRcTransport::PayloadChunk*
InfRcTransport::PayloadChunk::prependToBuffer(Buffer* buffer,
                                             char* data,
                                             uint32_t dataLength,
                                             InfRcTransport* transport,
                                             BufferDescriptor* bd)
{
    PayloadChunk* chunk =
        new(buffer, CHUNK) PayloadChunk(data, dataLength, transport, bd);
    Buffer::Chunk::prependChunkToBuffer(buffer, chunk);
    return chunk;
}

/**
 * Prepend a subregion of payload data which releases the memory to the
 * HCA when its containing Buffer is destroyed.
 *
 * \param buffer
 *      The Buffer to prepend the data to.
 * \param data
 *      The address of the data to appear in the Buffer.
 * \param dataLength
 *      The length in bytes of the region starting at data that is a
 *      subregion of the payload.
 * \param transport
 *      The transport that owns the provided BufferDescriptor 'bd'.
 * \param bd
 *      The BufferDescriptor to return to the HCA on Buffer destruction.
 */
InfRcTransport::PayloadChunk*
InfRcTransport::PayloadChunk::appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            InfRcTransport* transport,
                                            BufferDescriptor* bd)
{
    PayloadChunk* chunk =
        new(buffer, CHUNK) PayloadChunk(data, dataLength, transport, bd);
    Buffer::Chunk::appendChunkToBuffer(buffer, chunk);
    return chunk;
}

/// Returns memory to the HCA once the Chunk is discarded.
InfRcTransport::PayloadChunk::~PayloadChunk()
{
    transport->ibPostSrqReceive(bd);
}

/**
 * Construct a PayloadChunk which will release it's resources to the
 * HCA its containing Buffer is destroyed.
 *
 * \param data
 *      The address of the data to appear in the Buffer.
 * \param dataLength
 *      The length in bytes of the region starting at data that is a
 *      subregion of the payload.
 * \param transport
 *      The transport that owns the provided BufferDescriptor 'bd'.
 * \param bd 
 *      The BufferDescriptor to return to the HCA on Buffer destruction.
 */
InfRcTransport::PayloadChunk::PayloadChunk(void* data,
                                          uint32_t dataLength,
                                          InfRcTransport *transport,
                                          BufferDescriptor* bd)
    : Buffer::Chunk(data, dataLength),
      transport(transport),
      bd(bd)
{
}

}  // namespace RAMCloud
