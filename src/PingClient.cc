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

#include "Common.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "PingClient.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Retrieve performance counters from a given server.
 *
 * \param serviceLocator
 *      Identifies the server whose metrics should be retrieved.
 * \param metrics
 *      Store the metrics here, replacing any existing contents.
 */
void
PingClient::getMetrics(const char* serviceLocator, MetricsHash& metrics)
{
    // Fill in the request.
    Buffer req, resp;
    allocHeader<GetMetricsRpc>(req);
    Transport::SessionRef session =
            transportManager.getSession(serviceLocator);
    const GetMetricsRpc::Response& respHdr(
            sendRecv<GetMetricsRpc>(session, req, resp));
    checkStatus(HERE);
    resp.truncateFront(sizeof(respHdr));
    assert(respHdr.messageLength == resp.getTotalLength());
    metrics.clear();
    metrics.load(resp);
}

/**
 * Issues a trivial RPC to test that a server exists and is responsive.
 *
 * \param serviceLocator
 *      Identifies the server to ping.
 * \param nonce
 *      Arbitrary 64-bit value to pass to the server; the server will return
 *      this value in its response.
 * \param timeoutNanoseconds
 *      The maximum amount of time to wait for a response (in nanoseconds).
 * \result
 *      The value returned by the server, which should be the same as \a nonce
 *      (this method does not verify that the value does in fact match).
 *
 * \throw TimeoutException
 *      The server did not respond within \a timeoutNanoseconds.
 */
uint64_t
PingClient::ping(const char* serviceLocator, uint64_t nonce,
                 uint64_t timeoutNanoseconds)
{
    // Fill in the request.
    Buffer req, resp;
    PingRpc::Request& reqHdr(allocHeader<PingRpc>(req));
    reqHdr.nonce = nonce;

    // Send the request and wait for a response.
    Transport::SessionRef session =
            transportManager.getSession(serviceLocator);
    AsyncState state = send<PingRpc>(session, req, resp);
    uint64_t abortTime = Cycles::rdtsc() + Cycles::fromNanoseconds(
            timeoutNanoseconds);
    while (true) {
        if (dispatch->isDispatchThread())
            dispatch->poll();
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
            transportManager.getSession(serviceLocator1);
    AsyncState state = send<ProxyPingRpc>(session, req, resp);
    uint64_t abortTime = Cycles::rdtsc() + Cycles::fromNanoseconds(
            timeoutNanoseconds1);
    while (true) {
        if (dispatch->isDispatchThread())
            dispatch->poll();
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
