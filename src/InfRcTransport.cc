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

#include "Common.h"
#include "CycleCounter.h"
#include "RawMetrics.h"
#include "TimeCounter.h"
#include "Transport.h"
#include "InfRcTransport.h"
#include "IpAddress.h"
#include "PerfCounter.h"
#include "PerfStats.h"
#include "ServiceLocator.h"
#include "ShortMacros.h"
#include "TimeTrace.h"
#include "Util.h"
#include "WorkerManager.h"

#define check_error_null(x, s)                              \
    do {                                                    \
        if ((x) == NULL) {                                  \
            LOG(ERROR, "%s", s);                            \
            throw TransportException(HERE, errno);          \
        }                                                   \
    } while (0)

namespace RAMCloud {

//------------------------------
// InfRcTransport class
//------------------------------

/**
 * Construct a InfRcTransport.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param sl
 *      The ServiceLocator describing which HCA to use and the IP/UDP
 *      address and port numbers to use for handshaking. If NULL,
 *      the transport will be configured for client use only.
 */
InfRcTransport::InfRcTransport(Context* context,
                                           const ServiceLocator *sl)
    : context(context)
    , realInfiniband()
    , infiniband()
    , rxBuffers()
    , txBuffers()
    , freeTxBuffers()
    , serverSrq(NULL)
    , clientSrq(NULL)
    , serverRxCq(NULL)
    , clientRxCq(NULL)
    , commonTxCq(NULL)
    , ibPhysicalPort(1)
    , lid(0)
    , serverSetupSocket(-1)
    , clientSetupSocket(-1)
    , clientPort(0)
    , serverPortMap()
    , clientSendQueue()
    , numUsedClientSrqBuffers(MAX_SHARED_RX_QUEUE_DEPTH)
    , numFreeServerSrqBuffers(0)
    , outstandingRpcs()
    , pendingOutputBytes(0)
    , clientRpcsActiveTime()
    , locatorString()
    , poller(this)
    , serverConnectHandler()
    , logMemoryBase(0)
    , logMemoryBytes(0)
    , logMemoryRegion(0)
    , serverRpcPool()
    , clientRpcPool()
    , deadQueuePairs()
    , testingDontReallySend(false)
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

    // First, a UDP socket we can use to establish connections to other
    // servers.
    clientSetupSocket = socket(PF_INET, SOCK_DGRAM, 0);
    if (clientSetupSocket == -1) {
        LOG(ERROR, "failed to create client socket: %s", strerror(errno));
        throw TransportException(HERE, format(
                "failed to create client socket: %s", strerror(errno)));
    }
    struct sockaddr_in socketAddress;
    socketAddress.sin_family = AF_INET;
    socketAddress.sin_addr.s_addr = htonl(INADDR_ANY);
    socketAddress.sin_port = 0;
    if (bind(clientSetupSocket,
            reinterpret_cast<struct sockaddr*>(&socketAddress),
            sizeof(socketAddress)) == -1) {
        close(clientSetupSocket);
        LOG(WARNING, "couldn't bind port for clientSetupSocket: %s",
                strerror(errno));
        throw TransportException(HERE,
                "InfRcTransport couldn't bind port for clientSetupSocket",
                errno);
    }
    try {
        setNonBlocking(clientSetupSocket);
    } catch (...) {
        close(clientSetupSocket);
        throw;
    }
    socklen_t socketAddressLength = sizeof(socketAddress);
    if (getsockname(clientSetupSocket,
            reinterpret_cast<struct sockaddr*>(&socketAddress),
            &socketAddressLength) != 0) {
        close(clientSetupSocket);
        LOG(ERROR, "couldn't get port for clientSetupSocket: %s",
                strerror(errno));
        throw TransportException(HERE,
                "InfRcTransport couldn't set up clientSetupSocket",
                errno);
    }
    clientPort = NTOHS(socketAddress.sin_port);

    // If this is a server, create a server setup socket and bind it.
    if (sl != NULL) {
        IpAddress address(sl);

        serverSetupSocket = socket(PF_INET, SOCK_DGRAM, 0);
        if (serverSetupSocket == -1) {
            LOG(ERROR, "failed to create server socket: %s", strerror(errno));
            throw TransportException(HERE, format(
                    "failed to create server socket: %s", strerror(errno)));
        }

        if (bind(serverSetupSocket, &address.address,
          sizeof(address.address))) {
            close(serverSetupSocket);
            serverSetupSocket = -1;
            LOG(ERROR, "failed to bind socket for port %s: %s",
                    sl->getOption("port").c_str(), strerror(errno));
            throw TransportException(HERE, format(
                    "failed to bind socket for port %s: %s",
                    sl->getOption("port").c_str(), strerror(errno)));
        }

        try {
            setNonBlocking(serverSetupSocket);
        } catch (...) {
            close(serverSetupSocket);
            serverSetupSocket = -1;
            throw;
        }

        LOG(NOTICE, "InfRc listening on UDP: %s", address.toString().c_str());
        serverConnectHandler.construct(serverSetupSocket, this);
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

    // Note: RPC performance is highly sensitive to the buffer size. For
    // example, as of 11/2012, using buffers of (1<<23 + 200) bytes is
    // 1.3 microseconds slower than using buffers of (1<<23 + 4096)
    // bytes.  For now, make buffers large enough for the largest RPC,
    // and round up to the next multiple of 4096.  This approach isn't
    // perfect (for example buffers of 1<<23 bytes also seem to be slow)
    // but it will work for now.
    uint32_t bufferSize = (getMaxRpcSize() + 4095) & ~0xfff;

    rxBuffers.construct(infiniband->pd,
                        bufferSize,
                        uint32_t(MAX_SHARED_RX_QUEUE_DEPTH * 2));
    uint32_t i = 0;
    foreach (auto& bd, *rxBuffers) {
        if (i < MAX_SHARED_RX_QUEUE_DEPTH)
            postSrqReceiveAndKickTransmit(serverSrq, &bd);
        else
            postSrqReceiveAndKickTransmit(clientSrq, &bd);
        ++i;
    }
    assert(numUsedClientSrqBuffers == 0);

    txBuffers.construct(infiniband->pd,
                        bufferSize,
                        uint32_t(MAX_TX_QUEUE_DEPTH));
    foreach (auto& bd, *txBuffers)
        freeTxBuffers.push_back(&bd);

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

/**
 * Destructor for InfRcTransport.
 */
InfRcTransport::~InfRcTransport()
{
    // Note: this destructor isn't yet complete; it contains just enough cleanup
    // for the unit tests to run.
    if (serverSetupSocket != -1)
        close(serverSetupSocket);
    if (clientSetupSocket != -1)
        close(clientSetupSocket);
}

void
InfRcTransport::setNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    if (flags == -1) {
        LOG(ERROR, "fcntl F_GETFL failed: %s", strerror(errno));
        throw TransportException(HERE, format("fnctl failed: %s",
                strerror(errno)));
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
        LOG(ERROR, "fcntl F_SETFL failed: %s", strerror(errno));
        throw TransportException(HERE, format("fnctl failed: %s",
                strerror(errno)));
    }
}

/**
 * Construct a Session object for the public #getSession() interface.
 *
 * \param transport
 *      The transport this Session will be associated with.
 * \param sl
 *      The ServiceLocator describing the server to communicate with.
 * \param timeoutMs
 *      If there is an active RPC and we can't get any signs of life out
 *      of the server within this many milliseconds then the session will
 *      be aborted.  0 means we get to pick a reasonable default.
 *
 * \throw TransportException
 *      There was a problem that prevented us from creating the session.
 */
InfRcTransport::InfRcSession::InfRcSession(
    InfRcTransport *transport, const ServiceLocator* sl, uint32_t timeoutMs)
    : transport(transport)
    , serverAddress()
    , qp(NULL)
    , sessionAlarm(transport->context->sessionAlarmTimer, this,
            (timeoutMs != 0) ? timeoutMs : DEFAULT_TIMEOUT_MS)
{
    setServiceLocator(sl->getOriginalString());
    IpAddress address(sl);
    serverAddress = reinterpret_cast<struct sockaddr_in*>(
            &address.address)->sin_addr;

    // create and set up a new queue pair for this client
    // This probably doesn't need to allocate memory
    qp = transport->clientTrySetupQueuePair(address);
}

/**
 * Destructor for InfRcSessions.
 */
InfRcTransport::InfRcSession::~InfRcSession()
{
    abort();
    LOG(DEBUG, "Closing session with %s (client port %d)",
            inet_ntoa(serverAddress), transport->clientPort);
}

// See documentation for Transport::Session::abort.
void
InfRcTransport::InfRcSession::abort()
{
    for (ClientRpcList::iterator
            it(transport->clientSendQueue.begin());
            it != transport->clientSendQueue.end(); ) {
        ClientRpc& rpc = *it;
        it++;
        if (rpc.session == this) {
            LOG(NOTICE, "InfRcTransport aborting %s request to %s",
                    WireFormat::opcodeSymbol(rpc.request),
                    getServiceLocator().c_str());
            rpc.notifier->failed();
            erase(transport->clientSendQueue, rpc);
            transport->clientRpcPool.destroy(&rpc);
        }
    }
    for (ClientRpcList::iterator
            it(transport->outstandingRpcs.begin());
            it != transport->outstandingRpcs.end(); ) {
        ClientRpc& rpc = *it;
        it++;
        if (rpc.session == this) {
            LOG(NOTICE, "InfRcTransport aborting %s request to %s",
                    WireFormat::opcodeSymbol(rpc.request),
                    getServiceLocator().c_str());
            rpc.notifier->failed();
            erase(transport->outstandingRpcs, rpc);
            --transport->numUsedClientSrqBuffers;
            transport->clientRpcPool.destroy(&rpc);
            sessionAlarm.rpcFinished();
        }
    }
    if (qp)
        transport->deadQueuePairs.push_back(qp);
    qp = NULL;
}

// See Transport::Session::cancelRequest for documentation.
void
InfRcTransport::InfRcSession::cancelRequest(
    RpcNotifier* notifier)
{
    // Search for an RPC that refers to this notifier; if one is
    // found then remove all state relating to it.
    foreach (ClientRpc& rpc, transport->clientSendQueue) {
        if (rpc.notifier == notifier) {
            erase(transport->clientSendQueue, rpc);
            transport->clientRpcPool.destroy(&rpc);
            return;
        }
    }
    foreach (ClientRpc& rpc, transport->outstandingRpcs) {
        if (rpc.notifier == notifier) {

            // Wait until NIC completes all ongoing transmits. This will
            // guarantee that we don't send garbaged rpc if NIC has already
            // started DMA transmit of data from the rpc that we want to cancel.
            while (transport->freeTxBuffers.size() != MAX_TX_QUEUE_DEPTH) {
                transport->reapTxBuffers();
            }
            erase(transport->outstandingRpcs, rpc);
            transport->clientRpcPool.destroy(&rpc);
            --transport->numUsedClientSrqBuffers;
            sessionAlarm.rpcFinished();
            if (transport->outstandingRpcs.empty()) {
                transport->clientRpcsActiveTime.destroy();
            }
            return;
        }
    }
}

// See Transport::Session::getRpcInfo for documentation.
string
InfRcTransport::InfRcSession::getRpcInfo()
{
    const char* separator = "";
    string result;
    foreach (ClientRpc& rpc, transport->outstandingRpcs) {
        if (rpc.session == this) {
            result += separator;
            result += WireFormat::opcodeSymbol(rpc.request);
            separator = ", ";
        }
    }
    foreach (ClientRpc& rpc, transport->clientSendQueue) {
        if (rpc.session == this) {
            result += separator;
            result += WireFormat::opcodeSymbol(rpc.request);
            separator = ", ";
        }
    }
    if (result.empty())
        result = "no active RPCs";
    result += " to server at ";
    result += getServiceLocator();
    return result;
}

// See Transport::Session::sendRequest for documentation.
void
InfRcTransport::InfRcSession::sendRequest(Buffer* request,
        Buffer* response, RpcNotifier* notifier)
{
    response->reset();
    InfRcTransport *t = transport;
    if (qp == NULL) {
        notifier->failed();
        return;
    }

    LOG(DEBUG, "Sending %s request to %s with %u bytes",
            WireFormat::opcodeSymbol(request), getServiceLocator().c_str(),
            request->size());
    if (request->size() > t->getMaxRpcSize()) {
        throw TransportException(HERE,
             format("client request exceeds maximum rpc size "
                    "(attempted %u bytes, maximum %u bytes)",
                    request->size(),
                    t->getMaxRpcSize()));
    }
    ClientRpc *rpc = transport->clientRpcPool.construct(transport, this,
                                                        request, response,
                                                        notifier,
                                                        generateRandom());
    rpc->sendOrQueue();
}

/**
 * Constrctor for ServerPort object
 **/
InfRcTransport::InfRcServerPort::InfRcServerPort(
    InfRcTransport *transport,  QueuePair* qp)
        : transport(transport)
        , qp(qp)
          // Use longer timeout for initial session acquisition in
          // coordinator, etc.
        , portAlarm(transport->context->portAlarmTimer, this)
{
    // Inserts this alarm entry to portAlarmTimer.activeAlarms
    // and starts timer for this port
    portAlarm.startPortTimer();
}

InfRcTransport::InfRcServerPort::~InfRcServerPort()
{
    if (qp)
        transport->deadQueuePairs.push_back(qp); // to delete qp
    qp = NULL;
}

/**
 * Close and shutdown the server listening port
 * QP is deleted by the destructor of InfRcServerPort
 * Close is called, eg. at port watchdog timer at the timeout.
 **/
void
InfRcTransport::InfRcServerPort::close()
{
    // Remove unused hash entry from unorderd_map
    transport->serverPortMap.erase(qp->getLocalQpNumber());

    // Deleting self.
    // InfRcServerPort is must be dynamically allocated,
    // no duplicated deletion happens.
    delete this;
}

/**
 * Returns port identifier consists of client ip address
 **/
const string
InfRcTransport::InfRcServerPort::getPortName() const
{
    return qp->getSinName();
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
bool
InfRcTransport::clientTryExchangeQueuePairs(struct sockaddr_in *sin,
    QueuePairTuple *outgoingQpt, QueuePairTuple *incomingQpt,
    uint32_t usTimeout)
{
    bool haveSent = false;
    uint64_t startTime = Cycles::rdtsc();

    while (1) {
        if (!haveSent) {
            ssize_t len = sendto(clientSetupSocket, outgoingQpt,
                sizeof(*outgoingQpt), 0, reinterpret_cast<sockaddr *>(sin),
                sizeof(*sin));
            if (len == -1) {
                if (errno != EINTR && errno != EAGAIN) {
                    LOG(ERROR, "sendto returned error %d: %s",
                        errno, strerror(errno));
                    throw TransportException(HERE, errno);
                }
            } else if (len != sizeof(*outgoingQpt)) {
                LOG(ERROR, "sendto returned bad length (%Zd) while "
                    "sending to ip: [%s] port: [%d]", len,
                    inet_ntoa(sin->sin_addr), NTOHS(sin->sin_port));
                throw TransportException(HERE, errno);
            } else {
                haveSent = true;
            }
        }

        struct sockaddr_in recvSin;
        socklen_t sinlen = sizeof(recvSin);
        ssize_t len = recvfrom(clientSetupSocket, incomingQpt,
            sizeof(*incomingQpt), 0,
            reinterpret_cast<sockaddr *>(&recvSin), &sinlen);
        if (len == -1) {
            if (errno != EINTR && errno != EAGAIN) {
                LOG(ERROR, "recvfrom returned error %d: %s",
                    errno, strerror(errno));
                throw TransportException(HERE, errno);
            }
        } else if (len != sizeof(*incomingQpt)) {
            LOG(ERROR, "recvfrom returned bad length (%Zd) while "
                "receiving from ip: [%s] port: [%d]", len,
                inet_ntoa(recvSin.sin_addr), NTOHS(recvSin.sin_port));
            throw TransportException(HERE, errno);
        } else {
            if (outgoingQpt->getNonce() == incomingQpt->getNonce())
                return true;

            LOG(WARNING, "bad nonce from %s (expected 0x%016lx, "
                "got 0x%016lx, port %d); ignoring",
                inet_ntoa(sin->sin_addr), outgoingQpt->getNonce(),
                incomingQpt->getNonce(), clientPort);
        }

        double timeLeft = usTimeout - Cycles::toSeconds(Cycles::rdtsc() -
                startTime)*1e06;
        if (timeLeft < 0)
            return false;

        // JIRA Issue: RAM-668:
        // The following isn't safe, at a minimum because some
        // other stack frame can start using clientSetupSocket.
        //
        // We need to call the dispatcher in order to let other event handlers
        // run (this is particularly important if the server we are trying to
        // connect to is us).
        Dispatch& dispatch = *context->dispatch;
        if (dispatch.isDispatchThread()) {
            dispatch.poll();
        }
    }
}

/**
 * Attempt to set up a QueuePair with the given server. The client
 * allocates a QueuePair and sends the necessary tuple to the
 * server to begin the handshake. The server then replies with its
 * QueuePair tuple information. This is all done over IP/UDP.
 *
 * \throw TransportException
 *      There was a problem that prevented us from creating the session.
 */
Infiniband::QueuePair*
InfRcTransport::clientTrySetupQueuePair(IpAddress& address)
{
    sockaddr_in* sin = reinterpret_cast<sockaddr_in*>(&address.address);

    // Create a new QueuePair and send its parameters to the server so it
    // can create its qp and reply with its parameters.
    QueuePair *qp = infiniband->createQueuePair(IBV_QPT_RC,
                                                ibPhysicalPort, clientSrq,
                                                commonTxCq, clientRxCq,
                                                MAX_TX_QUEUE_DEPTH,
                                                MAX_SHARED_RX_QUEUE_DEPTH);
    uint64_t nonce = generateRandom();
    LOG(DEBUG, "starting to connect to %s via local port %d, nonce 0x%lx",
            inet_ntoa(sin->sin_addr), clientPort, nonce);

    for (uint32_t i = 0; i < QP_EXCHANGE_MAX_TIMEOUTS; i++) {
        QueuePairTuple outgoingQpt(downCast<uint16_t>(lid),
                                   qp->getLocalQpNumber(),
                                   qp->getInitialPsn(), nonce,
                                   name);
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
            // To avoid log clutter, only print a log message for the
            // first retry.
            if (i == 0) {
                LOG(WARNING, "timed out waiting for response from %s; retrying",
                    address.toString().c_str());
            }
            ++metrics->transport.retrySessionOpenCount;
            continue;
        }
        LOG(DEBUG, "connected to %s via local port %d",
                inet_ntoa(sin->sin_addr), clientPort);

        // plumb up our queue pair with the server's parameters.
        qp->plumb(&incomingQpt);
        return qp;
    }

    LOG(WARNING, "failed to exchange with server (%s) within allotted "
        "%u microseconds (sent request %u times, local port %d)",
        address.toString().c_str(),
        QP_EXCHANGE_USEC_TIMEOUT * QP_EXCHANGE_MAX_TIMEOUTS,
        QP_EXCHANGE_MAX_TIMEOUTS,
        clientPort);
    delete qp;
    throw TransportException(HERE, "failed to connect to host");
}

/**
 * This method is invoked by the dispatcher when #serverSetupSocket becomes
 * readable. It attempts to set up QueuePair with a connecting remote
 * client.
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 *      (OR-ed combination of Dispatch::FileEvent bits).
 */
void
InfRcTransport::ServerConnectHandler::handleFileEvent(int events)
{
    sockaddr_in sin;
    socklen_t sinlen = sizeof(sin);
    QueuePairTuple incomingQpt;

    ssize_t len = recvfrom(transport->serverSetupSocket, &incomingQpt,
        sizeof(incomingQpt), 0, reinterpret_cast<sockaddr *>(&sin), &sinlen);
    if (len <= -1) {
        if (errno == EAGAIN)
            return;
        LOG(ERROR, "recvfrom failed: %s", strerror(errno));
        return;
    } else if (len != sizeof(incomingQpt)) {
        LOG(WARNING, "recvfrom got a strange incoming size: %Zd", len);
        return;
    }

    // create a new queue pair, set it up according to our client's parameters,
    // and feed back our lid, qpn, and psn information so they can complete
    // the out-of-band handshake.

    // Note: It is possible that we already created a queue pair, but the
    // response to the client was lost and so we allocated another.
    // We should probably look up the QueuePair first using incomingQpt,
    // just to be sure, esp. if we use an unreliable means of handshaking, in
    // which case the response to the client request could have been lost.

    QueuePair *qp = transport->infiniband->createQueuePair(
            IBV_QPT_RC,
            transport->ibPhysicalPort,
            transport->serverSrq,
            transport->commonTxCq,
            transport->serverRxCq,
            MAX_TX_QUEUE_DEPTH,
            MAX_SHARED_RX_QUEUE_DEPTH);
    qp->plumb(&incomingQpt);
    qp->setPeerName(incomingQpt.getPeerName());
    LOG(DEBUG, "New queue pair for %s:%u, nonce 0x%lx (total creates "
            "%d, deletes %d)",
            inet_ntoa(sin.sin_addr), HTONS(sin.sin_port),
            incomingQpt.getNonce(),
            transport->infiniband->totalQpCreates,
            transport->infiniband->totalQpDeletes);

    // now send the client back our queue pair information so they can
    // complete the initialisation.
    QueuePairTuple outgoingQpt(downCast<uint16_t>(transport->lid),
                               qp->getLocalQpNumber(),
                               qp->getInitialPsn(), incomingQpt.getNonce());
    len = sendto(transport->serverSetupSocket, &outgoingQpt,
            sizeof(outgoingQpt), 0, reinterpret_cast<sockaddr *>(&sin),
            sinlen);
    if (len != sizeof(outgoingQpt)) {
        LOG(WARNING, "sendto failed, len = %Zd", len);
        delete qp;
        return;
    }

    // store some identifying client information
    qp->handshakeSin = sin;

    // Dynamically instanciates a new InfRcServerPort associating
    // the newly created queue pair.
    // It is saved in serverPortMap with QpNumber a key.
    transport->serverPortMap[qp->getLocalQpNumber()] =
            new InfRcServerPort(transport, qp);
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
void
InfRcTransport::postSrqReceiveAndKickTransmit(ibv_srq* srq,
    BufferDescriptor *bd)
{
    infiniband->postSrqReceive(srq, bd);

    // This condition is hacky. One idea is to wrap ibv_srq in an
    // object and make this a virtual method instead.
    if (srq == clientSrq) {
        --numUsedClientSrqBuffers;
        if (!clientSendQueue.empty()) {
            ClientRpc& rpc = clientSendQueue.front();
            clientSendQueue.pop_front();
            rpc.sendOrQueue();
            double waitTime = Cycles::toSeconds(Cycles::rdtsc()
                    - rpc.waitStart);
            if (waitTime > 1e-03) {
                LOG(WARNING, "Outgoing %s RPC delayed for %.2f ms because "
                        "of insufficient receive buffers",
                        WireFormat::opcodeSymbol(rpc.request),
                        waitTime*1e03);
            }
        }
    } else {
        ++numFreeServerSrqBuffers;
    }
}

/**
 * Return a free transmit buffer, wrapped by its corresponding
 * BufferDescriptor. If there are none, block until one is available.
 *
 * Any errors from previous transmissions are basically
 *               thrown on the floor, though we do log them. We need
 *               to think a bit more about how this 'fire-and-forget'
 *               behaviour impacts our Transport API.
 */
Infiniband::BufferDescriptor*
InfRcTransport::getTransmitBuffer()
{
    // if we've drained our free tx buffer pool, we must wait.
    while (freeTxBuffers.empty()) {
        reapTxBuffers();

        if (freeTxBuffers.empty()) {
            // We are temporarily out of buffers. Time how long it takes
            // before a transmit buffer becomes available again (a long
            // time could indicate deadlock); in the normal case this code
            // is not invoked.
            uint64_t start = Cycles::rdtsc();
            while (freeTxBuffers.empty())
                reapTxBuffers();
            double waitMs = 1e03 * Cycles::toSeconds(Cycles::rdtsc() - start);
            if (waitMs > 5.0)  {
                LOG(WARNING, "Long delay waiting for transmit buffers "
                        "(%.1f ms); deadlock or target crashed?", waitMs);
            }
        }
    }

    BufferDescriptor* bd = freeTxBuffers.back();
    freeTxBuffers.pop_back();
    return bd;
}

/**
 * Check for completed transmissions and, if any are done,
 * reclaim their buffers for future transmissions.
 *
 * \return
 *      The number of buffers reaped.
 */
int
InfRcTransport::reapTxBuffers()
{
    ibv_wc retArray[MAX_TX_QUEUE_DEPTH];
    int n = infiniband->pollCompletionQueue(commonTxCq,
                                            MAX_TX_QUEUE_DEPTH,
                                            retArray);

    for (int i = 0; i < n; i++) {
        BufferDescriptor* bd =
            reinterpret_cast<BufferDescriptor*>(retArray[i].wr_id);
        pendingOutputBytes -= bd->messageBytes;
        freeTxBuffers.push_back(bd);

        if (retArray[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR, "Transmit failed for buffer %lu: %s",
                reinterpret_cast<uint64_t>(bd),
                infiniband->wcStatusToString(retArray[i].status));
        }
    }

    // Has TX just transitioned to idle?
    if (n > 0 && freeTxBuffers.size() == MAX_TX_QUEUE_DEPTH) {
        // It's now safe to delete queue pairs (see comment by declaration
        // for deadQueuePairs).
        while (!deadQueuePairs.empty()) {
            delete deadQueuePairs.back();
            deadQueuePairs.pop_back();
        }
    }

    return n;
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
    return MAX_RPC_LEN;
}

string
InfRcTransport::getServiceLocator()
{
    return locatorString;
}

/**
 * Post a message for transmit by the HCA, attempting to zero-copy any
 * data from registered buffers (currently only seglets that are part of
 * the log). Transparently handles buffers with more chunks than the
 * scatter-gather entry limit and buffers that mix registered and
 * non-registered chunks.
 *
 * \param nonce
 *      Unique RPC identifier to transmit before message.
 * \param message
 *      Buffer containing the message that should be transmitted
 *      to the endpoint listening on #session.
 * \param qp
 *      Queue pair on which to transmit the message.
 */
void
InfRcTransport::sendZeroCopy(uint64_t nonce, Buffer* message, QueuePair* qp)
{
    const bool allowZeroCopy = true;
    uint32_t lastChunkIndex = message->getNumberChunks() - 1;
    ibv_sge isge[MAX_TX_SGE_COUNT];

    uint32_t chunksUsed = 0;
    uint32_t sgesUsed = 0;
    BufferDescriptor* bd = getTransmitBuffer();
    bd->messageBytes = message->size();

    // The variables below allow us to collect several chunks from the
    // Buffer into a single sge in some situations. They describe a
    // range of bytes in bd that have not yet been put in an sge, but
    // must go into the next sge.
    char* unaddedStart = bd->buffer;
    char* unaddedEnd = bd->buffer;

    *(reinterpret_cast<uint64_t*>(unaddedStart)) = nonce;
    unaddedEnd += sizeof(nonce);

    Buffer::Iterator it(message);
    while (!it.isDone()) {
        const uintptr_t addr = reinterpret_cast<const uintptr_t>(it.getData());
        // See if we can transmit this chunk from its current location
        // (zero copy) vs. copying it into a transmit buffer:
        // * The chunk must lie in the range of registered memory that
        //   the NIC knows about.
        // * If we run out of sges, then everything has to be copied
        //   (but save the last sge for the last chunk, since it's the
        //   one most likely to benefit from zero copying.
        // * For small chunks, it's cheaper to copy than to send a
        //   separate descriptor to the NIC.
        if (allowZeroCopy &&
            // The "4" below means this: can't do zero-copy for this chunk
            // unless there are at least 4 sges left (1 for unadded data, one
            // for this zero-copy chunk, 1 for more unadded data up to the
            // last chunk, and one for a final zero-copy chunk), or this is
            // the last chunk (in which there better be at least 2 sge's left).
            (sgesUsed <= MAX_TX_SGE_COUNT - 4 ||
             chunksUsed == lastChunkIndex) &&
            addr >= logMemoryBase &&
            (addr + it.getLength()) <= (logMemoryBase + logMemoryBytes) &&
            it.getLength() > 500)
        {
            if (unaddedStart != unaddedEnd) {
                isge[sgesUsed] = {
                    reinterpret_cast<uint64_t>(unaddedStart),
                    downCast<uint32_t>(unaddedEnd - unaddedStart),
                    bd->mr->lkey
                };
                ++sgesUsed;
                unaddedStart = unaddedEnd;
            }

            isge[sgesUsed] = {
                addr,
                it.getLength(),
                logMemoryRegion->lkey
            };
            ++sgesUsed;
        } else {
            CycleCounter<RawMetric>
                copyTicks(&metrics->transport.transmit.copyTicks);
            memcpy(unaddedEnd, it.getData(), it.getLength());
            unaddedEnd += it.getLength();
        }
        it.next();
        ++chunksUsed;
    }
    if (unaddedStart != unaddedEnd) {
        isge[sgesUsed] = {
            reinterpret_cast<uint64_t>(unaddedStart),
            downCast<uint32_t>(unaddedEnd - unaddedStart),
            bd->mr->lkey
        };
        ++sgesUsed;
        unaddedStart = unaddedEnd;
    }

    ibv_send_wr txWorkRequest;

    memset(&txWorkRequest, 0, sizeof(txWorkRequest));
    txWorkRequest.wr_id = reinterpret_cast<uint64_t>(bd);// stash descriptor ptr
    txWorkRequest.next = NULL;
    txWorkRequest.sg_list = isge;
    txWorkRequest.num_sge = sgesUsed;
    txWorkRequest.opcode = IBV_WR_SEND;
    txWorkRequest.send_flags = IBV_SEND_SIGNALED;

    // We can get a substantial latency improvement (nearly 2usec less per RTT)
    // by inlining data with the WQE for small messages. The Verbs library
    // automatically takes care of copying from the SGEs to the WQE.
    if ((message->size()) <= Infiniband::MAX_INLINE_DATA)
        txWorkRequest.send_flags |= IBV_SEND_INLINE;

    metrics->transport.transmit.iovecCount += sgesUsed;
    metrics->transport.transmit.byteCount += message->size();
    PerfStats::threadStats.networkOutputBytes += message->size();
    CycleCounter<RawMetric> _(&metrics->transport.transmit.ticks);
    ibv_send_wr* badTxWorkRequest;
    if (expect_true(!testingDontReallySend)) {
        if (ibv_post_send(qp->qp, &txWorkRequest, &badTxWorkRequest)) {
            throw TransportException(HERE, "ibv_post_send failed");
        }
        pendingOutputBytes += bd->messageBytes;
    } else {
        for (int i = 0; i < txWorkRequest.num_sge; ++i) {
            const ibv_sge& sge = txWorkRequest.sg_list[i];
            TEST_LOG("isge[%d]: %u bytes %s", i, sge.length,
                     (logMemoryRegion && sge.lkey ==
                        logMemoryRegion->lkey) ?
                     "ZERO-COPY" : "COPIED");
        }
    }
}

/**
 * Record a name that we can use to identify this application/machine
 * to peers.
 *
 * \param debugName
 *      Name suitable for use in log messages on servers that we
 *      interact with.
 */
void
InfRcTransport::setName(const char* debugName)
{
    snprintf(name, sizeof(name), "%s", debugName);
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
InfRcTransport::ServerRpc::ServerRpc(InfRcTransport* transport,
                                     QueuePair* qp,
                                     uint64_t nonce)
    : rpcServiceTime(&ReadRequestHandle_MetricSet::rpcServiceTime, false),
      transport(transport),
      qp(qp),
      nonce(nonce)
{ }

/**
 * Send a reply for an RPC.
 *
 * Transmits are done using a copy into a pre-registered HCA buffer.
 * The function blocks until the HCA returns success or failure.
 */
void
InfRcTransport::ServerRpc::sendReply()
{
    this->rpcServiceTime.stop();
    ReadRequestHandle_MetricSet::Interval interval(
            &ReadRequestHandle_MetricSet::serviceReturnToPostSend);

    CycleCounter<RawMetric> _(&metrics->transport.transmit.ticks);
    ++metrics->transport.transmit.messageCount;
    ++metrics->transport.transmit.packetCount;

    InfRcTransport *t = transport;

    // "t->serverRpcPool.destroy(this);" on our way out of the method
    ServerRpcPoolGuard<ServerRpc> suicide(t->serverRpcPool, this);

    if (replyPayload.size() > t->getMaxRpcSize()) {
        throw TransportException(HERE,
             format("server response exceeds maximum rpc size "
                    "(attempted %u bytes, maximum %u bytes)",
                    replyPayload.size(),
                    t->getMaxRpcSize()));
    }

    t->sendZeroCopy(nonce, &replyPayload, qp);
    interval.stop();

    // Restart port watchdog for this server port

    uint32_t qpNum = qp->getLocalQpNumber(); // get QP number from qp instance
    if (t->serverPortMap.find(qpNum)
        == t->serverPortMap.end()) {
        // Error because serverPortMap[qpNum] is not exist,
        // which should be created at the qp instanciation.
        LOG(ERROR, "failed to find qp_num %ud in serverPortMap", qpNum);
    } else {
        InfRcServerPort *port = t->serverPortMap[qpNum];
        port->portAlarm.requestArrived();
    }
}

/**
 * Return the RPC source's (i.e. client's) address in string form.
 */
string
InfRcTransport::ServerRpc::getClientServiceLocator()
{
    return format("infrc:host=%s,port=%hu",
        inet_ntoa(qp->handshakeSin.sin_addr), NTOHS(qp->handshakeSin.sin_port));
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
 * \param notifier
 *      Used to notify wrappers when the RPC is finished.
 */
InfRcTransport::ClientRpc::ClientRpc(InfRcTransport* transport,
                                     InfRcSession* session,
                                     Buffer* request,
                                     Buffer* response,
                                     RpcNotifier* notifier,
                                     uint64_t nonce)
    : transport(transport)
    , session(session)
    , request(request)
    , response(response)
    , notifier(notifier)
    , nonce(nonce)
    , waitStart(0)
    , state(PENDING)
    , queueEntries()
{

}

/**
 * Send the RPC request out onto the network if there is a receive buffer
 * available for its response, or queue it for transmission otherwise.
 */
void
InfRcTransport::ClientRpc::sendOrQueue()
{
    assert(state == PENDING);
    InfRcTransport* const t = transport;
    if (t->numUsedClientSrqBuffers < MAX_SHARED_RX_QUEUE_DEPTH) {
        // send out the request
        if (t->outstandingRpcs.empty()) {
            t->clientRpcsActiveTime.construct(
                &metrics->transport.clientRpcsActiveTicks);
        }
        ++metrics->transport.transmit.messageCount;
        ++metrics->transport.transmit.packetCount;

        t->sendZeroCopy(nonce, request, session->qp);

        t->outstandingRpcs.push_back(*this);
        session->sessionAlarm.rpcStarted();
        ++t->numUsedClientSrqBuffers;
        state = REQUEST_SENT;
    } else {
        // no available receive buffers
        waitStart = Cycles::rdtsc();
        t->clientSendQueue.push_back(*this);
    }
}

/**
 * This method is invoked by the dispatcher's inner polling loop; it
 * checks for incoming RPC requests and responses and processes them.
 *
 * \return
 *      Nonzero if we were able to do anything useful, zero if there was
 *      no work to be done.
 */
int
InfRcTransport::Poller::poll()
{
    InfRcTransport* t = transport;
    static const int MAX_COMPLETIONS = 10;
    ibv_wc wc[MAX_COMPLETIONS];
    int foundWork = 0;

    // First check for responses to requests that we have made.
    if (!t->outstandingRpcs.empty()) {
        int numResponses = t->infiniband->pollCompletionQueue(t->clientRxCq,
                MAX_COMPLETIONS, wc);
        for (int i = 0; i < numResponses; i++) {
            foundWork = 1;
            ibv_wc* response = &wc[i];
            CycleCounter<RawMetric> receiveTicks;
            BufferDescriptor *bd =
                        reinterpret_cast<BufferDescriptor *>(response->wr_id);
            if (response->byte_len < 1000)
                prefetch(bd->buffer, response->byte_len);
            PerfStats::threadStats.networkInputBytes += response->byte_len;
            if (response->status != IBV_WC_SUCCESS) {
                LOG(ERROR, "wc.status(%d:%s) != IBV_WC_SUCCESS",
                    response->status,
                    t->infiniband->wcStatusToString(response->status));
                t->postSrqReceiveAndKickTransmit(t->clientSrq, bd);
                throw TransportException(HERE, response->status);
            }

            Header& header(*reinterpret_cast<Header*>(bd->buffer));
            foreach (ClientRpc& rpc, t->outstandingRpcs) {
                if (rpc.nonce != header.nonce)
                    continue;
                t->outstandingRpcs.erase(t->outstandingRpcs.iterator_to(rpc));
                rpc.session->sessionAlarm.rpcFinished();
                uint32_t len = response->byte_len - sizeof32(header);
                if (t->numUsedClientSrqBuffers >=
                        MAX_SHARED_RX_QUEUE_DEPTH / 2) {
                    // clientSrq is low on buffers, better return this one
                    rpc.response->appendCopy(bd->buffer + sizeof(header), len);
                    t->postSrqReceiveAndKickTransmit(t->clientSrq, bd);
                } else {
                    // rpc will hold one of clientSrq's buffers until
                    // rpc.response is destroyed
                    PayloadChunk::appendToBuffer(rpc.response,
                                                 bd->buffer + sizeof(header),
                                                 len, t, t->clientSrq, bd);
                }
                LOG(DEBUG, "Received %s response from %s with %u bytes",
                        WireFormat::opcodeSymbol(rpc.request),
                        rpc.session->getServiceLocator().c_str(),
                        rpc.response->size());
                rpc.state = ClientRpc::RESPONSE_RECEIVED;
                ++metrics->transport.receive.messageCount;
                ++metrics->transport.receive.packetCount;
                metrics->transport.receive.iovecCount +=
                    rpc.response->getNumberChunks();
                metrics->transport.receive.byteCount +=
                    rpc.response->size();
                metrics->transport.receive.ticks += receiveTicks.stop();
                rpc.notifier->completed();
                t->clientRpcPool.destroy(&rpc);
                if (t->outstandingRpcs.empty())
                    t->clientRpcsActiveTime.destroy();
                goto next;
            }

            // nonce doesn't match any outgoingRpcs, which means that
            // numUsedClientsrqBuffers was not previously incremented by
            // the start of an rpc. Thus, it is incremented here (since
            // we're "using" it right now) right before posting it back.
            t->numUsedClientSrqBuffers++;
            t->postSrqReceiveAndKickTransmit(t->clientSrq, bd);
            LOG(NOTICE, "incoming data doesn't match active RPC "
                "(nonce 0x%016lx); perhaps RPC was cancelled?",
                header.nonce);

      next: { /* pass */ }
        }
    }

    // Next, check for incoming RPC requests (assuming that we are a server).
    if (t->serverSetupSocket >= 0) {
        CycleCounter<RawMetric> receiveTicks;
        int numRequests = t->infiniband->pollCompletionQueue(t->serverRxCq,
                MAX_COMPLETIONS, wc);
        if ((t->numFreeServerSrqBuffers - numRequests) == 0) {
            // The receive buffer queue has run completely dry. This is bad
            // for performance: if any requests arrive while the queue is empty,
            // Infiniband imposes a long wait period (milliseconds?) before
            // the caller retries.
            RAMCLOUD_CLOG(WARNING, "Infiniband receive buffers ran out "
                    "(%d new requests arrived); could cause significant "
                    "delays", numRequests);
        }
        for (int i = 0; i < numRequests; i++) {
            foundWork = 1;
            ibv_wc* request = &wc[i];
            ReadRequestHandle_MetricSet::Interval interval
                (&ReadRequestHandle_MetricSet::requestToHandleRpc);

            BufferDescriptor* bd =
                reinterpret_cast<BufferDescriptor*>(request->wr_id);
            if (request->byte_len < 1000)
                prefetch(bd->buffer, request->byte_len);
            PerfStats::threadStats.networkInputBytes += request->byte_len;

            if (t->serverPortMap.find(request->qp_num)
                    == t->serverPortMap.end()) {
                LOG(ERROR, "failed to find qp_num in map");
                goto done;
            }

            InfRcServerPort *port = t->serverPortMap[request->qp_num];
            QueuePair *qp = port->qp;

            --t->numFreeServerSrqBuffers;

            if (request->status != IBV_WC_SUCCESS) {
                LOG(ERROR, "failed to receive rpc!");
                t->postSrqReceiveAndKickTransmit(t->serverSrq, bd);
                goto done;
            }
            Header& header(*reinterpret_cast<Header*>(bd->buffer));
            ServerRpc *r = t->serverRpcPool.construct(t, qp, header.nonce);

            uint32_t len = request->byte_len - sizeof32(header);
            // It's very important that we don't let the receive buffer
            // queue get completely empty (if this happens, Infiniband
            // won't retry until after a long delay), so when the queue
            // starts running low we copy incoming packets in order to
            // return the buffers immediately. The constant below was
            // originally 2, but that turned out not to be sufficient.
            // Measurements of the YCSB benchmarks in 7/2015 suggest that
            // a value of 4 is (barely) okay, but we now use 8 to provide a
            // larger margin of safety, even if a burst of packets arrives.
            if (t->numFreeServerSrqBuffers < 8) {
                r->requestPayload.appendCopy(bd->buffer + sizeof(header), len);
                t->postSrqReceiveAndKickTransmit(t->serverSrq, bd);
            } else {
                // Let the request use the NIC's buffer directly in order
                // to avoid copying; it will be returned when the request
                // buffer is destroyed.
                PayloadChunk::appendToBuffer(&r->requestPayload,
                    bd->buffer + sizeof32(header),
                    len, t, t->serverSrq, bd);
            }

            port->portAlarm.requestArrived(); // Restarts the port watchdog
            interval.stop();
            r->rpcServiceTime.start();
            t->context->workerManager->handleRpc(r);
            ++metrics->transport.receive.messageCount;
            ++metrics->transport.receive.packetCount;
            metrics->transport.receive.iovecCount +=
                r->requestPayload.getNumberChunks();
            metrics->transport.receive.byteCount +=
                r->requestPayload.size();
            metrics->transport.receive.ticks += receiveTicks.stop();
        }
    }

  done:
    // Retrieve transmission buffers from the NIC once they have been
    // sent. It's done here in the hopes that it will happen when we
    // have nothing else to do, so it's effectively free.  It's much
    // more efficient if we can reclaim several buffers at once, so wait
    // until buffers are running low before trying to reclaim.  This
    // optimization improves the throughput of "clusterperf readThroughput"
    // by about 5% (as of 7/2015).
    if (t->freeTxBuffers.size() < 3) {
        t->reapTxBuffers();
        if (t->freeTxBuffers.size() >= 3) {
            foundWork = 1;
        }
    }
    return foundWork;
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
InfRcTransport::PayloadChunk*
InfRcTransport::PayloadChunk::prependToBuffer(Buffer* buffer,
                                             char* data,
                                             uint32_t dataLength,
                                             InfRcTransport* transport,
                                             ibv_srq* srq,
                                             BufferDescriptor* bd)
{
    PayloadChunk* chunk = buffer->allocAux<PayloadChunk>(data, dataLength,
            transport, srq, bd);
    buffer->prependChunk(chunk);
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
InfRcTransport::PayloadChunk*
InfRcTransport::PayloadChunk::appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            InfRcTransport* transport,
                                            ibv_srq* srq,
                                            BufferDescriptor* bd)
{
    PayloadChunk* chunk = buffer->allocAux<PayloadChunk>(data, dataLength,
            transport, srq, bd);
    buffer->appendChunk(chunk);
    return chunk;
}

/// Returns memory to the HCA once the Chunk is discarded.
InfRcTransport::PayloadChunk::~PayloadChunk()
{
    // It's crucial that we either make sure we are running in the Dispatch
    // thread or hand the work off to the dispatch thread before invoking any
    // transport methods. In all other cases we go through layers (such as
    // WorkerSession) that ensure serialized access to the transport. This
    // fancy Buffer destructor trick is an exception: it may be directly
    // invoked by a worker.
    if (transport->context->dispatch->isDispatchThread())
        transport->postSrqReceiveAndKickTransmit(srq, bd);
    else
        transport->context->dispatchExec->addRequest<ReturnNICBuffer>(
                transport, srq, bd);
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
InfRcTransport::PayloadChunk::PayloadChunk(void* data,
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

char InfRcTransport::name[50] = "?unknown?";

}  // namespace RAMCloud
