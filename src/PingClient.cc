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
#include "ClientException.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "PingClient.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Issue a trivial RPC to test that a server exists and is responsive.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param targetId
 *      Identifies the server to which the RPC should be sent.
 * \param callerId
 *      Used to inform the pinged server of which server is sending the ping.
 *      Used on the pingee side for debug logging.
 *      Clients and coordinators should use an invalid ServerId (ServerID()).
 *
 * \return
 *      If \a serverId had a server list, then its version number is returned;
 *      otherwise zero is returned.
 *
 * \throw ServerDoesntExistException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */ 
uint64_t
PingClient::ping(Context& context, ServerId targetId, ServerId callerId)
{
    PingRpc2 rpc(context, targetId, callerId);
    return rpc.wait();
}

/**
 * Constructor for PingRpc2: initiates an RPC in the same way as
 * #PingClient::ping, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param targetId
 *      Identifies the server to which the RPC should be sent.
 * \param callerId
 *      Used to inform the pinged server of which server is sending the ping.
 *      Used on the pingee side for debug logging.
 *      Clients and coordinators should use an invalid ServerId (ServerID()).
 */
PingRpc2::PingRpc2(Context& context, ServerId targetId, ServerId callerId)
    : ServerIdRpcWrapper(context, targetId,
            sizeof(WireFormat::Ping::Response))
{
    WireFormat::Ping::Request& reqHdr(
            allocHeader<WireFormat::Ping>());
    reqHdr.callerId = callerId.getId();
    send();
}

/**
 * Wait for a ping RPC to complete.
 *
 * \return
 *      If the target server had a server list, then its version number
 *      is returned; otherwise zero is returned.
 *
 * \throw ServerDoesntExistException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
uint64_t
PingRpc2::wait()
{
    waitAndCheckErrors();
    const WireFormat::Ping::Response& respHdr(
            getResponseHeader<WireFormat::Ping>());
    return respHdr.serverListVersion;
}

/**
 * Wait for a ping RPC to complete, but only wait for a given amount of
 * time, and return if no response is received by then.
 *
 * \param timeoutNanoseconds
 *      If no response is received within this many nanoseconds, then
 *      give up.
 *
 * \return
 *      If no response was received within \c timeoutNanoseconds, or if
 *      we reach a point where the target server is no longer part of the
 *      cluster, then all ones is returned (note: this method will not
 *      throw ServerDoesntExistsException). If the target server responds
 *      and it has a server list, then the version number for its server
 *      list is returned. If the server responded but has no server list,
 *      then zero is returned.
 */
uint64_t
PingRpc2::wait(uint64_t timeoutNanoseconds)
{
    uint64_t abortTime = Cycles::rdtsc() +
            Cycles::fromNanoseconds(timeoutNanoseconds);
    if (!waitInternal(*context.dispatch, abortTime)) {
        TEST_LOG("timeout");
        return ~0UL;
    }
    if (serverDown) {
        TEST_LOG("server doesn't exist");
        return ~0UL;
    }
    if (responseHeader->status != STATUS_OK)
        ClientException::throwException(HERE, responseHeader->status);
    const WireFormat::Ping::Response& respHdr(
            getResponseHeader<WireFormat::Ping>());
    return respHdr.serverListVersion;
}

/**
 * Ask one service to ping another service (useful for checking possible
 * connectivity issues).
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param proxyId
 *      Identifies the server to which the RPC should be sent; this server
 *      will ping \a targetId.
 * \param targetId
 *      Identifies the server that \a proxyId will ping.
 * \param timeoutNanoseconds
 *      The maximum amount of time (in nanoseconds) that \a proxyId
 *      will wait for \a targetId to respond.
 *
 * \result
 *      The amount of time it took \a targetId to respond to the ping
 *      request from \a proxyId.  If no response was received within
 *      \a timeoutNanoseconds, then all ones is returned.
 *
 * \throw ServerDoesntExistException
 *      Generated if \a proxyId is not part of the cluster; if it ever
 *      existed, it has since crashed.
 */ 
uint64_t
PingClient::proxyPing(Context& context, ServerId proxyId, ServerId targetId,
        uint64_t timeoutNanoseconds)
{
    ProxyPingRpc2 rpc(context, proxyId, targetId, timeoutNanoseconds);
    return rpc.wait();
}

/**
 * Constructor for ProxyPingRpc2: initiates an RPC in the same way as
 * #PingClient::proxyPing, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param proxyId
 *      Identifies the server to which the RPC should be sent; this server
 *      will ping \a targetId.
 * \param targetId
 *      Identifies the server that \a proxyId will ping.
 * \param timeoutNanoseconds
 *      The maximum amount of time (in nanoseconds) that \a proxyId
 *      will wait for \a targetId to respond.
 */
ProxyPingRpc2::ProxyPingRpc2(Context& context, ServerId proxyId,
        ServerId targetId, uint64_t timeoutNanoseconds)
    : ServerIdRpcWrapper(context, proxyId,
            sizeof(WireFormat::ProxyPing::Response))
{
    WireFormat::ProxyPing::Request& reqHdr(
            allocHeader<WireFormat::ProxyPing>());
    reqHdr.serverId = targetId.getId();
    reqHdr.timeoutNanoseconds = timeoutNanoseconds;
    send();
}

/**
 * Wait for a proxyPing RPC to complete.
 *
 * \return
 *      The amount of time it took the target server to respond to the ping
 *      request.  If the proxy didn't receive a response within the timeout
 *      period, then all ones is returned.
 *
 * \throw ServerDoesntExistException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
uint64_t
ProxyPingRpc2::wait()
{
    waitAndCheckErrors();
    const WireFormat::ProxyPing::Response& respHdr(
            getResponseHeader<WireFormat::ProxyPing>());
    return respHdr.replyNanoseconds;
}

}  // namespace RAMCloud
