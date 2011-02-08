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
 * The transport uses common pools of receive and transmit buffers that
 * are pre-registered with the HCA for direct access. All receive buffers
 * are placed on two shared receive queues (one for issuing RPCs and one for
 * servicing RPCs), which avoids having to allocate buffers to individual
 * receive queues for each client queue pair (this would be costly for many
 * queue pairs, and wasteful if they're idle). The shared receive queues can be
 * associated with many queue pairs, and each shared receive queue has its own
 * completion queue.
 *
 * In short, the receive path looks like the following:
 *  - As a server, we have just one completion queue for all incoming client
 *    queue pairs.
 *  - As a client, we have just one completion queue for all outgoing client
 *    queue pairs.
 *
 * For the transmit path, we have one completion queue for all cases, since
 * we currently do synchronous sends.
 *
 * Each receive and transmit buffer is sized large enough for the maximum
 * possible RPC size for simplicity. Note that if a node sends to another node
 * that does not have a sufficiently large receive buffer at the head of its
 * receive queue, _both_ ends will get an error (IBV_WC_REM_INV_REQ_ERR on the
 * sender, and IBV_WC_LOC_LEN_ERR on the receiver)! The HCA will _not_ search
 * the receive queue to find a larger posted buffer, nor will it scatter the
 * incoming data over multiple posted buffers. You have been warned.
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

/*
 * Random Notes:
 *  1) ibv_reg_mr() takes about 30usec to register one 4k page on the E5620.
 *     8MB takes about 1.25msec.  This implies that we can't afford to register
 *     on the fly.
 */

#include <errno.h>
#include <fcntl.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <boost/scoped_ptr.hpp>

#include "Common.h"
#include "TimeCounter.h"
#include "Transport.h"
#include "InfRcTransport.h"
#include "IpAddress.h"
#include "ServiceLocator.h"

#define check_error_null(x, s)                              \
    do {                                                    \
        if ((x) == NULL) {                                  \
            LOG(ERROR, "%s: %s", __func__, s);              \
            throw TransportException(HERE, errno);          \
        }                                                   \
    } while (0)

namespace RAMCloud {

//------------------------------
// InfRcTransport class
//------------------------------

template<typename Infiniband>
uint64_t InfRcTransport<Infiniband>::totalClientSendCopyTime;
template<typename Infiniband>
uint64_t InfRcTransport<Infiniband>::totalClientSendCopyBytes;
template<typename Infiniband>
uint64_t InfRcTransport<Infiniband>::totalSendReplyCopyTime;
template<typename Infiniband>
uint64_t InfRcTransport<Infiniband>::totalSendReplyCopyBytes;

/**
 * Construct a InfRcTransport.
 *
 * \param sl
 *      The ServiceLocator describing which HCA to use and the IP/UDP
 *      address and port numbers to use for handshaking. If NULL,
 *      the transport will be configured for client use only.
 */
template<typename Infiniband>
InfRcTransport<Infiniband>::InfRcTransport(const ServiceLocator *sl)
    : realInfiniband(),
      infiniband(),
      currentTxBuffer(0),
      serverSrq(NULL),
      clientSrq(NULL),
      serverRxCq(NULL),
      clientRxCq(NULL),
      commonTxCq(NULL),
      ibPhysicalPort(1),
      lid(0),
      serverSetupSocket(-1),
      clientSetupSocket(-1),
      queuePairMap(),
      clientSendQueue(),
      numUsedClientSrqBuffers(MAX_SHARED_RX_QUEUE_DEPTH),
      outstandingRpcs(),
      locatorString()
{
    const char *ibDeviceName = NULL;

    if (sl != NULL) {
        locatorString = sl->getOriginalString();

        try {
            ibDeviceName   = sl->getOption<const char *>("dev");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

        try {
            ibPhysicalPort = sl->getOption<int>("devport");
        } catch (ServiceLocator::NoSuchKeyException& e) {}
    }

    infiniband = realInfiniband.construct(ibDeviceName);

    // Step 1:
    //  Set up the udp sockets we use for out-of-band infiniband handshaking.

    // For clients, the kernel will automatically assign a dynamic port on
    // first use.
    clientSetupSocket = socket(PF_INET, SOCK_DGRAM, 0);
    if (clientSetupSocket == -1) {
        LOG(ERROR, "%s: failed to create client socket", __func__);
        throw TransportException(HERE, "client socket failed");
    }
    try {
        setNonBlocking(clientSetupSocket);
    } catch (...) {
        close(clientSetupSocket);
        throw;
    }

    // If this is a server, create a server setup socket and bind it.
    if (sl != NULL) {
        IpAddress address(*sl);

        serverSetupSocket = socket(PF_INET, SOCK_DGRAM, 0);
        if (serverSetupSocket == -1) {
            LOG(ERROR, "%s: failed ot create server socket", __func__);
            throw TransportException(HERE, "server socket failed");
        }

        if (bind(serverSetupSocket, &address.address,
          sizeof(address.address))) {
            close(serverSetupSocket);
            LOG(ERROR, "%s: failed to bind socket", __func__);
            throw TransportException(HERE, "socket failed");
        }

        try {
            setNonBlocking(serverSetupSocket);
        } catch (...) {
            close(serverSetupSocket);
            throw;
        }

        LOG(NOTICE, "InfRc listening on UDP: %s", address.toString().c_str());
    }

    // Step 2:
    //  Set up the initial verbs necessities: open the device, allocate
    //  protection domain, create shared receive queue, register buffers.

    lid = infiniband->getLid(ibPhysicalPort);

    // create two shared receive queues. all client queue pairs use one and all
    // server queue pairs use the other. we post receive buffer work requests
    // to these queues only. the motiviation is to avoid having to post at
    // least one buffer to every single queue pair (we may have thousands of
    // them with megabyte buffers).
    serverSrq = infiniband->createSharedReceiveQueue(MAX_SHARED_RX_QUEUE_DEPTH,
                                                     MAX_SHARED_RX_SGE_COUNT);
    check_error_null(serverSrq,
                     "failed to create server shared receive queue");
    clientSrq = infiniband->createSharedReceiveQueue(MAX_SHARED_RX_QUEUE_DEPTH,
                                                     MAX_SHARED_RX_SGE_COUNT);
    check_error_null(clientSrq,
                     "failed to create client shared receive queue");

    // XXX- for now we allocate TX and RX buffers and use them as a ring.
    for (uint32_t i = 0; i < MAX_SHARED_RX_QUEUE_DEPTH; i++) {
        serverRxBuffers[i] = infiniband->allocateBufferDescriptorAndRegister(
            getMaxRpcSize());
        postSrqReceiveAndKickTransmit(serverSrq, &serverRxBuffers[i]);
    }
    for (uint32_t i = 0; i < MAX_SHARED_RX_QUEUE_DEPTH; i++) {
        clientRxBuffers[i] = infiniband->allocateBufferDescriptorAndRegister(
            getMaxRpcSize());
        postSrqReceiveAndKickTransmit(clientSrq, &clientRxBuffers[i]);
    }
    for (uint32_t i = 0; i < MAX_TX_QUEUE_DEPTH; i++) {
        txBuffers[i] = infiniband->allocateBufferDescriptorAndRegister(
            getMaxRpcSize());
    }
    assert(numUsedClientSrqBuffers == 0);

    // create completion queues for server receive, client receive, and
    // server/client transmit
    serverRxCq = infiniband->createCompletionQueue(MAX_SHARED_RX_QUEUE_DEPTH);
    check_error_null(serverRxCq,
                     "failed to create server receive completion queue");

    clientRxCq = infiniband->createCompletionQueue(MAX_SHARED_RX_QUEUE_DEPTH);
    check_error_null(clientRxCq,
                     "failed to create client receive completion queue");

    commonTxCq = infiniband->createCompletionQueue(MAX_TX_QUEUE_DEPTH);
    check_error_null(commonTxCq,
                     "failed to create transmit completion queue");
}

template<typename Infiniband>
void
InfRcTransport<Infiniband>::setNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    if (flags == -1) {
        LOG(ERROR, "%s: fcntl F_GETFL failed", __func__);
        throw TransportException(HERE, "fnctl failed");
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
        LOG(ERROR, "%s: fcntl F_SETFL failed", __func__);
        throw TransportException(HERE, "fnctl failed");
    }
}

/**
 * Wait for an incoming request.
 *
 * The server polls the infiniband shared receive queue, as well as
 * the UDP setup socket. The former contains incoming RPCs, whereas
 * the latter is used to set up QueuePairs between clients and the
 * server, as an out-of-band handshake is needed.
 */
template<typename Infiniband>
Transport::ServerRpc*
InfRcTransport<Infiniband>::serverRecv()
{
    // query the infiniband adapter first. if there's nothing to process,
    // try to read a datagram from a connecting client.
    // in the future, this should occur in separate threads.
    ibv_wc wc;
    if (infiniband->pollCompletionQueue(serverRxCq, 1, &wc) >= 1) {
        if (queuePairMap.find(wc.qp_num) == queuePairMap.end()) {
            LOG(ERROR, "%s: failed to find qp_num in map", __func__);
            return NULL;
        }

        QueuePair *qp = queuePairMap[wc.qp_num];

        BufferDescriptor* bd =
            reinterpret_cast<BufferDescriptor*>(wc.wr_id);

        if (wc.status == IBV_WC_SUCCESS) {
            Header& header(*reinterpret_cast<Header*>(bd->buffer));
            ServerRpc *r = new ServerRpc(this, qp, header.nonce);
            PayloadChunk::appendToBuffer(&r->recvPayload,
                bd->buffer + sizeof(header),
                wc.byte_len - sizeof(header), this, serverSrq, bd);
            LOG(DEBUG, "Received request with nonce %016lx", header.nonce);
            return r;
        }

        LOG(ERROR, "%s: failed to receive rpc!", __func__);
        postSrqReceiveAndKickTransmit(serverSrq, bd);
    } else {
        serverTrySetupQueuePair();
    }

    return NULL;
}

/**
 * Construct a Session object for the public #getSession() interface.
 *
 * \param transport
 *      The transport this Session will be associated with.
 * \param sl
 *      The ServiceLocator describing the server to communicate with.
 */
template<typename Infiniband>
InfRcTransport<Infiniband>::InfRCSession::InfRCSession(
    InfRcTransport *transport, const ServiceLocator& sl)
    : transport(transport),
      qp(NULL)
{
    IpAddress address(sl);

    // create and set up a new queue pair for this client
    // TODO(ongaro): This probably doesn't need to allocate memory
    qp = transport->clientTrySetupQueuePair(address);
}

/**
 * Destroy the Session.
 */
template<typename Infiniband>
void
InfRcTransport<Infiniband>::InfRCSession::release()
{
    delete this;
}

/**
 * Issue an RPC request using infiniband->
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
template<typename Infiniband>
Transport::ClientRpc*
InfRcTransport<Infiniband>::InfRCSession::clientSend(Buffer* request,
                                                     Buffer* response)
{
    InfRcTransport *t = transport;

    if (request->getTotalLength() > t->getMaxRpcSize()) {
        throw TransportException(HERE,
                                 "client request exceeds maximum rpc size");
    }

    // Construct our ClientRpc in the response Buffer.
    //
    // We do this because we're loaning one of our registered receive buffers
    // to the caller of wait() and need to issue it back to the HCA when
    // they're done with it.
    ClientRpc *rpc = new(response, MISC) ClientRpc(transport, this,
                                                   request, response,
                                                   generateRandom());
    rpc->sendOrQueue();
    return rpc;
}

/**
 * Attempt to exchange QueuePair set up information by sending to the server
 * and waiting for an appropriate response. Only one request is sent for each
 * invocation, but the method may receive multiple reponses (e.g. delayed
 * responses to a previous invocation). It only returns on a matched response,
 * or if time runs out.
 *
 * \param sin
 *      The server to send to.
 * \param outgoingQpt
 *      Pointer to the QueuePairTuple to send to the server.
 * \param incomingQpt
 *      Pointer to space in which to store the QueuePairTuple returned by
 *      the server.
 * \param usTimeout
 *      Timeout to wait for a server response in microseconds.
 * \return
 *      true if a valid response was received within the specified amount of
 *      time, else false if either nothing comes back in time, or the responses
 *      received did not match the request (this can happen if responses are
 *      delayed, rather than lost).
 * \throw TransportException
 *      An exception is thrown if any of the socket system calls fail for
 *      some strange reason.
 */
template<typename Infiniband>
bool
InfRcTransport<Infiniband>::clientTryExchangeQueuePairs(struct sockaddr_in *sin,
    QueuePairTuple *outgoingQpt, QueuePairTuple *incomingQpt,
    uint32_t usTimeout)
{
    bool haveSent = false;

    while (1) {
        TimeCounter startTime;

        if (!haveSent) {
            ssize_t len = sendto(clientSetupSocket, outgoingQpt,
                sizeof(*outgoingQpt), 0, reinterpret_cast<sockaddr *>(sin),
                sizeof(*sin));
            if (len == -1) {
                if (errno != EINTR && errno != EAGAIN) {
                    LOG(ERROR, "%s: sendto returned error %d: %s",
                        __func__, errno, strerror(errno));
                    throw TransportException(HERE, len);
                }
            } else if (len != sizeof(*outgoingQpt)) {
                LOG(ERROR, "%s: sendto returned bad length (%Zd) while "
                    "sending to ip: [%s] port: [%d]", __func__, len,
                    inet_ntoa(sin->sin_addr), htons(sin->sin_port));
                throw TransportException(HERE, len);
            } else {
                haveSent = true;
            }
        }

        socklen_t sinlen = sizeof(sin);
        ssize_t len = recvfrom(clientSetupSocket, incomingQpt,
            sizeof(*incomingQpt), 0,
            reinterpret_cast<sockaddr *>(&sin), &sinlen);
        if (len == -1) {
            if (errno != EINTR && errno != EAGAIN) {
                LOG(ERROR, "%s: recvfrom returned error %d: %s",
                    __func__, errno, strerror(errno));
                throw TransportException(HERE, len);
            }
        } else if (len != sizeof(*incomingQpt)) {
            LOG(ERROR, "%s: recvfrom returned bad length (%Zd) while "
                "receiving from ip: [%s] port: [%d]", __func__, len,
                inet_ntoa(sin->sin_addr), htons(sin->sin_port));
            throw TransportException(HERE, len);
        } else {
            if (outgoingQpt->getNonce() == incomingQpt->getNonce())
                return true;

            LOG(WARNING,
                "%s: received nonce doesn't match (0x%016lx != 0x%016lx)",
                __func__, outgoingQpt->getNonce(), incomingQpt->getNonce());
        }

        uint64_t elapsedUs = startTime.stop() / 1000;
        if (elapsedUs >= (uint64_t)usTimeout)
            return false;
        usTimeout -= elapsedUs;
    }
}

/**
 * Attempt to set up a QueuePair with the given server. The client
 * allocates a QueuePair and sends the necessary tuple to the
 * server to begin the handshake. The server then replies with its
 * QueuePair tuple information. This is all done over IP/UDP.
 */
template<typename Infiniband>
typename Infiniband::QueuePair*
InfRcTransport<Infiniband>::clientTrySetupQueuePair(IpAddress& address)
{
    sockaddr_in* sin = reinterpret_cast<sockaddr_in*>(&address.address);

    // Create a new QueuePair and send its parameters to the server so it
    // can create its qp and reply with its parameters.
    QueuePair *qp = infiniband->createQueuePair(IBV_QPT_RC,
                                                ibPhysicalPort, clientSrq,
                                                commonTxCq, clientRxCq,
                                                MAX_TX_QUEUE_DEPTH,
                                                MAX_SHARED_RX_QUEUE_DEPTH);

    for (uint32_t i = 0; i < QP_EXCHANGE_MAX_TIMEOUTS; i++) {
        QueuePairTuple outgoingQpt(lid, qp->getLocalQpNumber(),
            qp->getInitialPsn(), generateRandom());
        QueuePairTuple incomingQpt;
        bool gotResponse;

        try {
            gotResponse = clientTryExchangeQueuePairs(sin, &outgoingQpt,
                                                      &incomingQpt,
                                                      QP_EXCHANGE_USEC_TIMEOUT);
        } catch (...) {
            delete qp;
            throw;
        }

        if (!gotResponse) {
            LOG(WARNING, "%s: timed out waiting for response; retrying",
                __func__);
            continue;
        }

        // plumb up our queue pair with the server's parameters.
        qp->plumb(&incomingQpt);
        return qp;
    }

    LOG(WARNING, "%s: failed to exchange with server (%s) within allotted "
        "%u microseconds (sent request %u times)\n", __func__,
        address.toString().c_str(),
        QP_EXCHANGE_USEC_TIMEOUT * QP_EXCHANGE_MAX_TIMEOUTS,
        QP_EXCHANGE_MAX_TIMEOUTS);
    delete qp;
    throw TransportException(HERE, "failed to connect to host");
}

/**
 * Attempt to set up QueuePair with a connecting remote client. This
 * function does a non-blocking receive of an incoming client handshake,
 * creates the appropriate QueuePair on the server, and replies with its
 * parameters so that the client can complete its handshake and plumb
 * their QueuePair.
 */
template<typename Infiniband>
void
InfRcTransport<Infiniband>::serverTrySetupQueuePair()
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

    QueuePair *qp = infiniband->createQueuePair(IBV_QPT_RC,
                                                ibPhysicalPort, serverSrq,
                                                commonTxCq, serverRxCq,
                                                MAX_TX_QUEUE_DEPTH,
                                                MAX_SHARED_RX_QUEUE_DEPTH);
    qp->plumb(&incomingQpt);

    // now send the client back our queue pair information so they can
    // complete the initialisation.
    QueuePairTuple outgoingQpt(lid, qp->getLocalQpNumber(),
        qp->getInitialPsn(), incomingQpt.getNonce());
    len = sendto(serverSetupSocket, &outgoingQpt, sizeof(outgoingQpt), 0,
        reinterpret_cast<sockaddr *>(&sin), sinlen);
    if (len != sizeof(outgoingQpt)) {
        LOG(WARNING, "%s: sendto failed, len = %Zd\n", __func__, len);
        delete qp;
        return;
    }

    // maintain the qpn -> qp mapping
    queuePairMap[qp->getLocalQpNumber()] = qp;
}

/**
 * Add the given BufferDescriptor to the given shared receive queue.
 * If a previous transmit was buffered due to lack of receive buffers,
 * this method will kick off a transmission.
 *
 * \param[in] srq
 *      The shared receive queue on which to enqueue this BufferDescriptor.
 * \param[in] bd
 *      The BufferDescriptor to enqueue.
 * \throw
 *      TransportException if posting to the queue fails.
 */
template<typename Infiniband>
void
InfRcTransport<Infiniband>::postSrqReceiveAndKickTransmit(ibv_srq* srq,
    BufferDescriptor *bd)
{
    infiniband->postSrqReceive(srq, bd);

    // TODO(ongaro): This condition is hacky. One idea is to wrap ibv_srq in an
    // object and make this a virtual method instead.
    if (srq == clientSrq) {
        --numUsedClientSrqBuffers;
        if (!clientSendQueue.empty()) {
            ClientRpc& rpc = clientSendQueue.front();
            clientSendQueue.pop_front();
            LOG(DEBUG, "Dequeued request with nonce %016lx", rpc.nonce);
            rpc.sendOrQueue();
        }
    }
}


/**
 * Obtain the maximum rpc size. This is limited by the infiniband
 * specification to 2GB(!), though we artificially limit it to a
 * little more than a segment size to avoid allocating too much 
 * space in RX buffers.
 */
template<typename Infiniband>
uint32_t
InfRcTransport<Infiniband>::getMaxRpcSize() const
{
    return MAX_RPC_SIZE;
}


template<typename Infiniband>
ServiceLocator
InfRcTransport<Infiniband>::getServiceLocator()
{
    return ServiceLocator(locatorString);
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
 * \param nonce
 *      Uniquely identifies this RPC.
 */
template<typename Infiniband>
InfRcTransport<Infiniband>::ServerRpc::ServerRpc(InfRcTransport* transport,
                                                 QueuePair* qp,
                                                 uint64_t nonce)
    : transport(transport),
      qp(qp),
      nonce(nonce)
{
}

/**
 * Send a reply for an RPC.
 *
 * Transmits are done using a copy into a pre-registered HCA buffer.
 * The function blocks until the HCA returns success or failure.
 */
template<typename Infiniband>
void
InfRcTransport<Infiniband>::ServerRpc::sendReply()
{
    LOG(DEBUG, "Sending response with nonce %016lx", nonce);
    // "delete this;" on our way out of the method
    boost::scoped_ptr<InfRcTransport::ServerRpc> suicide(this);

    InfRcTransport *t = transport;

    if (replyPayload.getTotalLength() > t->getMaxRpcSize()) {
        throw TransportException(HERE,
                                 "server response exceeds maximum rpc size");
    }

    BufferDescriptor* bd = &t->txBuffers[t->currentTxBuffer];
    t->currentTxBuffer = (t->currentTxBuffer + 1) % MAX_TX_QUEUE_DEPTH;
    new(&replyPayload, PREPEND) Header(nonce);
    uint64_t start = rdtsc();
    replyPayload.copy(0, replyPayload.getTotalLength(), bd->buffer);
    totalSendReplyCopyTime += rdtsc() - start;
    totalSendReplyCopyBytes += replyPayload.getTotalLength();
    t->infiniband->postSendAndWait(qp, bd, replyPayload.getTotalLength());
    replyPayload.truncateFront(sizeof(Header)); // for politeness
    LOG(DEBUG, "Sent response with nonce %016lx", nonce);
}

//-------------------------------------
// InfRcTransport::ClientRpc class
//-------------------------------------

/**
 * Construct a ClientRpc.
 *
 * \param transport
 *      The InfRcTransport object that this RPC is associated with.
 * \param session
 *      The session this RPC is associated with.
 * \param request
 *      Request payload.
 * \param[out] response
 *      Buffer in which the response message should be placed.
 * \param nonce
 *      Uniquely identifies this RPC.
 */
template<typename Infiniband>
InfRcTransport<Infiniband>::ClientRpc::ClientRpc(InfRcTransport* transport,
                                     InfRCSession* session,
                                     Buffer* request,
                                     Buffer* response,
                                     uint64_t nonce)
    : transport(transport),
      session(session),
      request(request),
      response(response),
      nonce(nonce),
      state(PENDING),
      queueEntries()
{

}

/**
 * Send the RPC request out onto the network if there is a receive buffer
 * available for its response, or queue it for transmission otherwise.
 */
template<typename Infiniband>
void
InfRcTransport<Infiniband>::ClientRpc::sendOrQueue()
{
    assert(state == PENDING);
    InfRcTransport* const t = transport;
    if (t->numUsedClientSrqBuffers < MAX_SHARED_RX_QUEUE_DEPTH) {
        // send out the request
        new(request, PREPEND) Header(nonce);
        BufferDescriptor* bd = &t->txBuffers[t->currentTxBuffer];
        t->currentTxBuffer = (t->currentTxBuffer + 1) % MAX_TX_QUEUE_DEPTH;
        uint64_t start = rdtsc();
        request->copy(0, request->getTotalLength(), bd->buffer);
        totalClientSendCopyTime += rdtsc() - start;
        totalClientSendCopyBytes += request->getTotalLength();
        LOG(DEBUG, "Sending request with nonce %016lx", nonce);
        t->infiniband->postSendAndWait(session->qp, bd,
                                       request->getTotalLength());
        request->truncateFront(sizeof(Header)); // for politeness

        t->outstandingRpcs.push_back(*this);
        ++t->numUsedClientSrqBuffers;
        state = REQUEST_SENT;
        LOG(DEBUG, "Sent request with nonce %016lx", nonce);
    } else {
        // no available receive buffers
        t->clientSendQueue.push_back(*this);
        LOG(DEBUG, "Queued send request with nonce %016lx", nonce);
    }
}

/**
 * Pull RPC responses off the client shared receive queue without blocking.
 */
template<typename Infiniband>
void
InfRcTransport<Infiniband>::poll()
{
    InfRcTransport *t = this;
    ibv_wc wc;
    while (infiniband->pollCompletionQueue(clientRxCq, 1, &wc) > 0) {
        BufferDescriptor *bd = reinterpret_cast<BufferDescriptor *>(wc.wr_id);
        if (wc.status != IBV_WC_SUCCESS) {
            LOG(ERROR, "%s: wc.status(%d:%s) != IBV_WC_SUCCESS", __func__,
                wc.status, infiniband->wcStatusToString(wc.status));
            t->postSrqReceiveAndKickTransmit(t->clientSrq, bd);
            throw TransportException(HERE, wc.status);
        }

        Header& header(*reinterpret_cast<Header*>(bd->buffer));
        LOG(DEBUG, "Received response with nonce %016lx", header.nonce);
        foreach (ClientRpc& rpc, t->outstandingRpcs) {
            if (rpc.nonce != header.nonce)
                continue;
            t->outstandingRpcs.erase(t->outstandingRpcs.iterator_to(rpc));
            uint32_t len = wc.byte_len - sizeof(header);
            if (t->numUsedClientSrqBuffers >= MAX_SHARED_RX_QUEUE_DEPTH / 2) {
                // clientSrq is low on buffers, better return this one
                LOG(DEBUG, "Copy and immediately return clientSrq buffer");
                memcpy(new(rpc.response, APPEND) char[len],
                       bd->buffer + sizeof(header),
                       len);
                t->postSrqReceiveAndKickTransmit(t->clientSrq, bd);
            } else {
                // rpc will hold one of clientSrq's buffers until
                // rpc.response is destroyed
                LOG(DEBUG, "Hang onto clientSrq buffer");
                PayloadChunk::appendToBuffer(rpc.response,
                                             bd->buffer + sizeof(header),
                                             len, t, t->clientSrq, bd);
            }
            rpc.state = ClientRpc::RESPONSE_RECEIVED;
            goto next;
        }
        LOG(WARNING, "dropped packet because no nonce matched %016lx",
                     header.nonce);
  next: { /* pass */ }
    }
}

// See Transport::ClientRpc::isReady for documentation.
template<typename Infiniband>
bool
InfRcTransport<Infiniband>::ClientRpc::isReady() {
    if (state == RESPONSE_RECEIVED)
        return true;
    transport->poll();
    return (state == RESPONSE_RECEIVED);
}

// See Transport::ClientRpc::wait for documentation.
template<typename Infiniband>
void
InfRcTransport<Infiniband>::ClientRpc::wait()
{
    while (state != RESPONSE_RECEIVED)
        transport->poll();
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
 * \param srq
 *      The shared receive queue which bd will be returned to.
 * \param bd 
 *      The BufferDescriptor to return to the HCA on Buffer destruction.
 */
template<typename Infiniband>
typename InfRcTransport<Infiniband>::PayloadChunk*
InfRcTransport<Infiniband>::PayloadChunk::prependToBuffer(Buffer* buffer,
                                             char* data,
                                             uint32_t dataLength,
                                             InfRcTransport* transport,
                                             ibv_srq* srq,
                                             BufferDescriptor* bd)
{
    PayloadChunk* chunk =
        new(buffer, CHUNK) PayloadChunk(data, dataLength, transport, srq, bd);
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
 * \param srq
 *      The shared receive queue which bd will be returned to.
 * \param bd
 *      The BufferDescriptor to return to the HCA on Buffer destruction.
 */
template<typename Infiniband>
typename InfRcTransport<Infiniband>::PayloadChunk*
InfRcTransport<Infiniband>::PayloadChunk::appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            InfRcTransport* transport,
                                            ibv_srq* srq,
                                            BufferDescriptor* bd)
{
    PayloadChunk* chunk =
        new(buffer, CHUNK) PayloadChunk(data, dataLength, transport, srq, bd);
    Buffer::Chunk::appendChunkToBuffer(buffer, chunk);
    return chunk;
}

/// Returns memory to the HCA once the Chunk is discarded.
template<typename Infiniband>
InfRcTransport<Infiniband>::PayloadChunk::~PayloadChunk()
{
    transport->postSrqReceiveAndKickTransmit(srq, bd);
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
 * \param srq
 *      The shared receive queue which bd will be returned to.
 * \param bd 
 *      The BufferDescriptor to return to the HCA on Buffer destruction.
 */
template<typename Infiniband>
InfRcTransport<Infiniband>::PayloadChunk::PayloadChunk(void* data,
                                          uint32_t dataLength,
                                          InfRcTransport *transport,
                                          ibv_srq* srq,
                                          BufferDescriptor* bd)
    : Buffer::Chunk(data, dataLength),
      transport(transport),
      srq(srq),
      bd(bd)
{
}

template class InfRcTransport<RealInfiniband>;

}  // namespace RAMCloud
