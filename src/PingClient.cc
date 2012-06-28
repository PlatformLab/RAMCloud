/* Copyright (c) 2011-2012 Stanford University
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

#include "Common.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "PingClient.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Tells the host to print an error message and exit with a status of zero.
 * The host will not respond to this RPC, so the caller needs to cancel the RPC
 * to reclaim its resources. However, the caller should not cancel the RPC
 * until it is sure that the host has exited, by querying the coordinator.
 * Otherwise, there could be a race where the RPC is still in the client's
 * queue when it is canceled.
 *
 * \warning This should only be used for testing!
 *
 * \param client 
 *      The PingClient instance over which the RPC should be issued.
 * \param serviceLocator
 *      The service locator of the host you wish to kill.
 *
 * \exception InternalError
 */
PingClient::Kill::Kill(PingClient& client, const char *serviceLocator)
    : client(client)
    , requestBuffer()
    , responseBuffer()
    , state()
{
    responseBuffer.reset();
    client.allocHeader<KillRpc>(requestBuffer);
    Transport::SessionRef session =
            client.context.transportManager->getSession(serviceLocator);
    state = client.send<KillRpc>(session,
                                 requestBuffer,
                                 responseBuffer);
}

/**
 * Retrieve performance counters from a given server.
 *
 * \param serviceLocator
 *      Identifies the server whose metrics should be retrieved.
 *
 * \return
 *       The performance metrics retrieved from \c serviceLocator.
 */
ServerMetrics
PingClient::getMetrics(const char* serviceLocator)
{
    // Fill in the request.
    Buffer req, resp;
    allocHeader<GetMetricsRpc>(req);
    Transport::SessionRef session =
            context.transportManager->getSession(serviceLocator);
    const GetMetricsRpc::Response& respHdr(
            sendRecv<GetMetricsRpc>(session, req, resp));
    checkStatus(HERE);
    resp.truncateFront(sizeof(respHdr));
    assert(respHdr.messageLength == resp.getTotalLength());
    ServerMetrics metrics;
    metrics.load(resp);
    return metrics;
}

/**
 * Issues a trivial RPC to test that a server exists and is responsive.
 *
 * \param serviceLocator
 *      Identifies the server to ping.
 * \param callerId
 *      Used to inform the pinged server of which server is sending the ping.
 *      Used on the pingee side for debug logging.
 *      Clients and coordinators should use an invalid ServerId (ServerID()).
 * \param nonce
 *      Arbitrary 64-bit value to pass to the server; the server will return
 *      this value in its response.
 * \param timeoutNanoseconds
 *      The maximum amount of time to wait for a response (in nanoseconds).
 * \param[out] serverListVersion
 *      If a ServerList was associated with the pinged server's PingService,
 *      then its version will be returned here. Otherwise, the remote
 *      PingService will return 0.
 * \return
 *      The value returned by the server, which should be the same as \a nonce
 *      (this method does not verify that the value does in fact match).
 *
 * \throw TimeoutException
 *      The server did not respond within \a timeoutNanoseconds.
 */
uint64_t
PingClient::ping(const char* serviceLocator,
                 ServerId callerId,
                 uint64_t nonce,
                 uint64_t timeoutNanoseconds,
                 uint64_t* serverListVersion)
{
    // Fill in the request.
    Buffer req, resp;
    PingRpc::Request& reqHdr(allocHeader<PingRpc>(req));
    reqHdr.callerId = callerId.getId();
    reqHdr.nonce = nonce;

    // Send the request and wait for a response.
    Transport::SessionRef session =
            context.transportManager->getSession(serviceLocator);
    AsyncState state = send<PingRpc>(session, req, resp);
    uint64_t abortTime = Cycles::rdtsc() + Cycles::fromNanoseconds(
            timeoutNanoseconds);
    while (true) {
        if (context.dispatch->isDispatchThread())
            context.dispatch->poll();
        if (state.isReady())
            break;
        if (Cycles::rdtsc() >= abortTime) {
            state.cancel();
            throw TimeoutException(HERE);
        }
    }
    const PingRpc::Response& respHdr(recv<PingRpc>(state));

    // Process the response.
    checkStatus(HERE);

    if (serverListVersion != NULL)
        *serverListVersion = respHdr.serverListVersion;
    return respHdr.nonce;
}

/**
 * Ask one service to ping another service (useful for checking possible
 * connectivity issues).
 *
 * \param serviceLocator1
 *      The proxy ping request will be sent to this server.
 * \param serviceLocator2
 *      When \a serviceLocator1 receives the proxy ping request, it will
 *      attempt to ping this server.
 * \param timeoutNanoseconds1
 *      The maximum amount of time (in nanoseconds) that we will wait
 *      for \a serviceLocator1 to respond (should be greater than
 *      \a timeoutNanoseconds2)
 * \param timeoutNanoseconds2
 *      The maximum amount of time (in nanoseconds) that \a serviceLocator1
 *      will wait for \a serviceLocator2 to respond.
 * \result
 *      The amount of time it took \a serviceLocator2 to respond to the ping
 *      request from \a serviceLocator1.  If no response was received within
 *      \a timeoutNanoseconds2, or if the ping response doesn't contain the
 *      expected nonce, then -1 is returned.
 *
 * \throw TimeoutException
 *      \a serviceLocator1 did not respond within \a timeoutNanoseconds1.
 */
uint64_t
PingClient::proxyPing(const char* serviceLocator1,
                      const char* serviceLocator2,
                      uint64_t timeoutNanoseconds1,
                      uint64_t timeoutNanoseconds2)
{
    // Fill in the request.
    Buffer req, resp;
    ProxyPingRpc::Request& reqHdr(allocHeader<ProxyPingRpc>(req));
    reqHdr.timeoutNanoseconds = timeoutNanoseconds2;
    uint32_t length = downCast<uint32_t>(strlen(serviceLocator2) + 1);
    reqHdr.serviceLocatorLength = length;
    memcpy(new(&req, APPEND) char[length], serviceLocator2, length);

    // Send the request and wait for a response.
    Transport::SessionRef session =
            context.transportManager->getSession(serviceLocator1);
    AsyncState state = send<ProxyPingRpc>(session, req, resp);
    uint64_t abortTime = Cycles::rdtsc() + Cycles::fromNanoseconds(
            timeoutNanoseconds1);
    while (true) {
        if (context.dispatch->isDispatchThread())
            context.dispatch->poll();
        if (state.isReady())
            break;
        if (Cycles::rdtsc() >= abortTime) {
            state.cancel();
            throw TimeoutException(HERE);
        }
    }
    const ProxyPingRpc::Response& respHdr(recv<ProxyPingRpc>(state));

    // Process the response.
    checkStatus(HERE);
    return respHdr.replyNanoseconds;
}

}  // namespace RAMCloud
