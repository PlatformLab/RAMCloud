/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <errno.h>
#include <fcntl.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "Common.h"
#include "BenchUtil.h"
#include "FailureDetector.h"
#include "IpAddress.h"
#include "ProtoBuf.h"
#include "Rpc.h"
#include "Syscall.h"

namespace RAMCloud {

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* FailureDetector::sys = &defaultSyscall;

/**
 * Create a new FailureDetector object.
 *
 * \param[in] coordinatorLocatorString
 *      The ServiceLocator string of the coordinator. 
 * \param[in] listeningLocatorsString
 *      String of ServiceLocators we're listening on. Can be obtained
 *      from TransportMananger via getListeningLocatorsString().
 * \param[in] type
 *      The type of server to probe: MASTER or BACKUP.
 */
FailureDetector::FailureDetector(string coordinatorLocatorString,
    string listeningLocatorsString, ServerType type)
    : clientFd(-1),
      serverFd(-1),
      coordFd(-1),
      type(type),
      coordinator(coordinatorLocatorString),
      localLocator(listeningLocatorsString),
      serverList(),
      terminate(false),
      queue(TIMEOUT_USECS),
      haveLoggedNoServers(false)
{
    clientFd = sys->socket(PF_INET, SOCK_DGRAM, 0);
    serverFd = sys->socket(PF_INET, SOCK_DGRAM, 0);
    coordFd  = sys->socket(PF_INET, SOCK_DGRAM, 0);

    sockaddr_in sin = serviceLocatorStringToSockaddrIn(listeningLocatorsString);
    int r = bind(serverFd, reinterpret_cast<sockaddr*>(&sin), sizeof(sin));

    if (clientFd == -1 || serverFd == -1 || coordFd == -1 || r == -1) {
        sys->close(clientFd);
        sys->close(serverFd);
        sys->close(coordFd);
        throw Exception(HERE);
    }

    LOG(NOTICE, "listening on UDP socket %s:%d for incoming pings",
        inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));
}

FailureDetector::~FailureDetector()
{
    sys->close(clientFd);
    sys->close(serverFd);
    sys->close(coordFd);
}

/**
 * Given a ServiceLocator for a server (master or backup), generate the IP and
 * UDP port they should be listening on for incoming pings, and return the
 * appropriate sockaddr_in struct.
 *
 * Since there may be multiple protocols used with different ports (and, perhaps,
 * IPs), we need to establish an order of precedence. It's currently:
 *      - infrc
 *      - fast+udp
 *      - tcp
 *
 * Once we have the values, we simply add 2111 to the port number.
 */
sockaddr_in
FailureDetector::serviceLocatorStringToSockaddrIn(string sl)
{
    auto locators = ServiceLocator::parseServiceLocators(sl);
    ServiceLocator* useSl = NULL;
    string order[3] = { "infrc", "fast+udp", "tcp" };
    foreach (auto& s, order) {
        foreach (auto& l, locators) {
            if (l.getProtocol() == s) {
                useSl = &l;
                break;
            }
        }
        if (useSl != NULL)
            break;
    }

    if (useSl == NULL)
        throw Exception(HERE, "could not determine IP/port for sl string");

    IpAddress addr(*useSl);
    sockaddr_in sin;
    sin.sin_family = PF_INET;
    memcpy(&sin, &addr.address, sizeof(sin));
    sin.sin_port = htons(ntohs(sin.sin_port) + 2111);
    return sin;
}

/**
 * Handle an incoming ping request. This simply requires sending the
 * payload back to the sender.
 */
void
FailureDetector::handleIncomingRequest(char* buf, ssize_t bytes,
    sockaddr_in* sourceAddress)
{
    LOG(DEBUG, "incoming request from %s:%d", inet_ntoa(sourceAddress->sin_addr),
        ntohs(sourceAddress->sin_port));

    // there are just two types of requests: ping, and proxy ping.
    // the former requires us to send a reply to the sender.
    // the latter requires us to ping a host and keep track, so that we
    // can notify the coordinator if its unreachable.

    RpcRequestCommon* req = reinterpret_cast<RpcRequestCommon*>(buf);
    if (req->type == PING) {
        // just turn the ping around
        ssize_t r = sys->sendto(serverFd, buf, bytes, 0,
            reinterpret_cast<sockaddr*>(sourceAddress), sizeof(*sourceAddress));
        if (r != bytes)
            LOG(WARNING, "sendto returned wrong number of bytes (%Zd)", r);
    } else if (req->type == PROXY_PING) {
        /// XXXXX
    } else {
        LOG(WARNING, "unknown request encountered (%u); ignoring",
            (uint32_t)req->type);
    }
}

/**
 * Handle an incoming ping response, i.e. a reply to one of our requests.
 * If the nonce matches our last ping sent, set a flag so we know it didn't
 * time out in the mainLoop. If it doesn't match, drop and log.
 */
void
FailureDetector::handleIncomingResponse(char* buf, ssize_t bytes,
    sockaddr_in* sourceAddress)
{
    LOG(DEBUG, "incoming ping response from %s:%d",
        inet_ntoa(sourceAddress->sin_addr), ntohs(sourceAddress->sin_port));

    if (bytes != 8) {
        LOG(WARNING, "payload isn't 8 bytes!");
        return;
    }

    uint64_t* nonce = reinterpret_cast<uint64_t*>(buf);
    if (queue.dequeue(*nonce)) {
        LOG(DEBUG, "received response from %s:%d",
            inet_ntoa(sourceAddress->sin_addr), ntohs(sourceAddress->sin_port));
    } else {
        LOG(WARNING, "received invalid nonce -- too late?");
    }
}

/**
 * Update our server list from the coordinator's reply to a list request.
 * Yes, this assumes that we can fit the whole thing in a single UDP frame.
 * This should be fine, as the average size appears to be about 45 bytes per
 * host, and we'll have 9000-byte frames on Ethernet and even larger ones
 * with infiniband.
 */
void
FailureDetector::handleCoordinatorResponse(char* buf, ssize_t bytes,
    sockaddr_in* sourceAddress)
{
    LOG(DEBUG, "incoming coordinator response from %s:%d",
        inet_ntoa(sourceAddress->sin_addr), ntohs(sourceAddress->sin_port));

    GetServerListRpc::Response* resp =
        reinterpret_cast<GetServerListRpc::Response*>(buf);

    if (bytes < (int)sizeof(*resp)) {
        LOG(WARNING, "impossibly small coordinator response: %Zd bytes", bytes);
        return;
    }

    serverList.Clear();
    Buffer b;
    Buffer::Chunk::appendToBuffer(&b, buf, bytes);
    ProtoBuf::parseFromResponse(b, sizeof(*resp),
        resp->serverListLength, serverList);
}

/**
 * Choose a random server from our list and ping it. Only one oustanding
 * ping is permitted at any time. Update our state so we can track if a
 * response arrives before the timeout.
 */
void
FailureDetector::pingRandomServer()
{
    if (serverList.server_size() == 0 || (serverList.server_size() == 1 &&
      serverList.server(0).service_locator() == localLocator)) {
        // if we have no servers to ping, or we're the only one on the list,
        // then just log that fact the first time and do nothing.
        if (!haveLoggedNoServers) {
            LOG(NOTICE, "No servers besides myself to probe! "
                "List has %d entries.", serverList.server_size());
            haveLoggedNoServers = true;
        }
        return;
    }

    const string* locator = &localLocator;
    while (*locator == localLocator) {
        int index = generateRandom() % serverList.server_size();
        locator = &serverList.server(index).service_locator();
    }

    uint64_t nonce = generateRandom();
    sockaddr_in sin = serviceLocatorStringToSockaddrIn(*locator);
    ssize_t r = sys->sendto(clientFd, &nonce,
        sizeof(nonce), 0, reinterpret_cast<sockaddr*>(&sin), sizeof(sin));
    if (r != sizeof(nonce))
        LOG(WARNING, "sendto failed; couldn't ping server! (r = %Zd)", r);
    else
        queue.enqueue(*locator, nonce);
}

/**
 * Send a HintServerDown rpc request to the Coordinator. No reply will be
 * issued.
 */
void
FailureDetector::alertCoordinator(TimeoutQueue::TimeoutEntry* te)
{
    const string& loc(te->locator);

    int bytesNeeded = loc.length() + 1 + sizeof(HintServerDownRpc::Request);
    char buf[bytesNeeded];
    memset(buf, 0, bytesNeeded);

    HintServerDownRpc::Request* rpc =
        reinterpret_cast<HintServerDownRpc::Request*>(buf);
    rpc->common.type = HINT_SERVER_DOWN;
    rpc->serviceLocatorLength = loc.length() + 1;
    memcpy(&buf[sizeof(*rpc)], loc.c_str(), loc.length());

    sockaddr_in sin = serviceLocatorStringToSockaddrIn(coordinator);
    ssize_t r = sys->sendto(coordFd, buf, bytesNeeded, 0,
        reinterpret_cast<sockaddr*>(&sin), sizeof(sin));
    if (r != sizeof(rpc))
        LOG(WARNING, "failed to send hint server down rpc to coordinator");
}

/**
 * Receive a message on the given file descriptor and call the appropriate
 * handler for it. This function can block, so be sure to check if data's
 * available first by using, e.g. select(), if you want it to return quickly.
 */
void
FailureDetector::processPacket(int fd)
{
    char buf[MAXIMUM_MTU_BYTES];
    sockaddr_in address;
    socklen_t addressLength = sizeof(address);

    ssize_t r = sys->recvfrom(fd, buf, sizeof(buf), 0,
        reinterpret_cast<sockaddr*>(&address), &addressLength);
    if (r >= 0) {
        if (addressLength != sizeof(address)) {
            LOG(ERROR, "weird address length: %d (expected %d)",
                (int)addressLength, (int)sizeof(address));
        } else {
            if (fd == serverFd)
                handleIncomingRequest(buf, r, &address);
            else if (fd == clientFd)
                handleIncomingResponse(buf, r, &address);
            else if (fd == coordFd)
                handleCoordinatorResponse(buf, r, &address);
            else
                LOG(ERROR, "bad fd: %d; what the heck?", fd);
        }
    } else if (errno != EAGAIN) {
            LOG(ERROR, "r == %Zd, errno == %d (%s)", r, errno, strerror(errno));
    }
}

/**
 * Request the list of servers from the Coordinator. This simply sends out a
 * datagram and expects one to eventually come back. We should do this
 * periodically to get an updated view of the system.
 */
void
FailureDetector::requestServerList()
{
    GetServerListRpc::Request rpc;
    rpc.common.type = GET_SERVER_LIST;
    rpc.serverType = type;
    sockaddr_in sin = serviceLocatorStringToSockaddrIn(coordinator);
    ssize_t r = sys->sendto(coordFd, &rpc, sizeof(rpc), 0,
        reinterpret_cast<sockaddr*>(&sin), sizeof(sin));
    if (r != sizeof(rpc))
        LOG(WARNING, "failed to send host list request to coordinator");
}

/**
 * Spin forever, probing hosts, checkings for responses, and alerting the
 * coordinator of any timeouts. 
 */
void
FailureDetector::mainLoop()
{
    uint64_t lastPingUsec = 0;

    while (!terminate) {
        // check if time to ping again 
        uint64_t nowUsec = cyclesToNanoseconds(rdtsc()) / 1000;
        if (nowUsec >= (lastPingUsec + PROBE_INTERVAL_USECS)) {
            pingRandomServer();
            lastPingUsec = nowUsec;
        }

        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(clientFd, &fds);
        FD_SET(serverFd, &fds);
        FD_SET(coordFd, &fds);

        uint64_t nextPingMicros =
            PROBE_INTERVAL_USECS - (nowUsec - lastPingUsec);
        uint64_t sleepMicros = MIN(queue.microsUntilNextTimeout(),
                                   nextPingMicros);
        timeval tv;
        tv.tv_sec  = sleepMicros / 1000000;
        tv.tv_usec = sleepMicros % 1000000;

        int nfds = MAX(MAX(clientFd, serverFd), coordFd) + 1;
        int r = sys->select(nfds, &fds, NULL, NULL, &tv);
        if (r == -1) {
            LOG(ERROR, "select returned %d (errno %d: %s)",
                r, errno, strerror(errno));
            throw Exception(HERE);
        }

        if (FD_ISSET(clientFd, &fds))
            processPacket(clientFd);
        if (FD_ISSET(serverFd, &fds))
            processPacket(serverFd);
        if (FD_ISSET(coordFd, &fds))
            processPacket(coordFd);

        // check for ping timeout(s)
        while (Tub<TimeoutQueue::TimeoutEntry> te = queue.dequeue())
            alertCoordinator(te.get());
    }
}

/////////////////////////////////
// FailureDetector::TimeoutQueue
/////////////////////////////////

/**
 * Create a new TimeoutQueue. A TimeoutQueue contains a list of probes
 * (consistenting of a 64-bit nonce and a ServiceLocator string) in
 * non-descending order of expiration. There is one common timeout for
 * all operations. The queue may be queried for probes that have expired,
 * as well as for the number of microseconds until the oldest probe will
 * expire (i.e. how long the caller can sleep for).
 *
 * \param[in] timeoutUsecs
 *      The number of microseconds to ait until an enqueued probe may
 *      be dequeued due to a timeout.
 */
FailureDetector::TimeoutQueue::TimeoutQueue(uint64_t timeoutUsecs)
    : entries(), timeoutUsecs(timeoutUsecs)
{
}

/**
 * Enqueue a probe that was just made. The time as of this function's
 * invocation is taken as the transmission time of the probe.
 *
 * \param[in] locator
 *      The ServiceLocator string of the service that was probed.
 * \param[in] nonce
 *      The random 64-bit nonce associated with this specific probe.
 *      Nonces should be unique, as they are used for dequeuing a
 *      previous probe. Random collisions are not catastrophic, but
 *      may result in missing timeouts or false timeouts.
 */
void
FailureDetector::TimeoutQueue::enqueue(string locator, uint64_t nonce)
{
    uint64_t now = cyclesToNanoseconds(rdtsc()) / 1000;
    entries.push_back(TimeoutEntry(now, locator, nonce));
}

/**
 * Dequeue the oldest timed out probe, if there is one. The Tub returned
 * will be empty if there is nothing to remove. This function should be
 * called until an empty Tub is encountered.
 */
Tub<FailureDetector::TimeoutQueue::TimeoutEntry>
FailureDetector::TimeoutQueue::dequeue()
{
    uint64_t now = cyclesToNanoseconds(rdtsc()) / 1000;
    auto it = entries.begin();
    while (it != entries.end()) {
        if (now >= (it->startUsec + timeoutUsecs))
            return {*it};
        else
            break;          // non-descending order means we can bail early
        it++;
    }
    return {};
}

/**
 * Dequeue a specific probe that was previously enqueued. This is
 * typically used to remove a probe for which a ping response was
 * received in time.
 *
 * \param[in] nonce
 *      The 64-bit nonce of the probe to remove. This is the same value
 *      that was passed to enqueue.
 *
 * \return
 *      A Tub containing the dequeue probe. If no match was found, an
 *      empty Tub is returned.
 */
Tub<FailureDetector::TimeoutQueue::TimeoutEntry>
FailureDetector::TimeoutQueue::dequeue(uint64_t nonce)
{
    auto it = entries.begin();
    while (it != entries.end()) {
        if (it->nonce == nonce) {
            entries.erase(it);
            return {*it};
        }
        it++;
    }
    return {};
}

/**
 * Obtain the number of microseconds until the next timeout. This
 * should be used to calculate the minimum amount of time to wait
 * before attempting another dequeue() invocation for timed-out
 * probes.
 */
uint64_t
FailureDetector::TimeoutQueue::microsUntilNextTimeout()
{
    uint64_t now = cyclesToNanoseconds(rdtsc()) / 1000;
    auto it = entries.begin();
    if (it == entries.end())
        return ~(uint64_t)0; 
    uint64_t next = it->startUsec + timeoutUsecs;
    if (next >= now)
        return next - now;
    return 0;
}

} // namespace
