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
#include "RawMetrics.h"
#include "ShortMacros.h"
#include "PingClient.h"
#include "PingService.h"
#include "ServerList.h"

namespace RAMCloud {

/**
 * Construct a PingService.
 *
 * \param context
 *      Overall information about the RAMCloud server. The caller is assumed
 *      to have associated a serverList with this context; if not, this service
 *      will not return a valid ServerList version in response to pings.
 */
PingService::PingService(Context* context)
    : context(context)
    , ignoreKill(false)
{
}

/**
 * Top-level service method to handle the GET_METRICS request.
 *
 * \copydetails Service::ping
 */
void
PingService::getMetrics(const WireFormat::GetMetrics::Request& reqHdr,
             WireFormat::GetMetrics::Response& respHdr,
             Rpc& rpc)
{
    string serialized;
    metrics->serialize(serialized);
    respHdr.messageLength = downCast<uint32_t>(serialized.length());
    memcpy(new(&rpc.replyPayload, APPEND) uint8_t[respHdr.messageLength],
           serialized.c_str(), respHdr.messageLength);
}

/**
 * Top-level service method to handle the PING request.
 *
 * \copydetails Service::ping
 */
void
PingService::ping(const WireFormat::Ping::Request& reqHdr,
             WireFormat::Ping::Response& respHdr,
             Rpc& rpc)
{
    if (ServerId(reqHdr.callerId) == ServerId()) {
        LOG(DEBUG, "Received ping request from unknown endpoint "
            "(perhaps the coordinator or a client)");
    } else {
        LOG(DEBUG, "Received ping request from server %s",
            ServerId(reqHdr.callerId).toString().c_str());
    }
    respHdr.serverListVersion = 0;
    if (context->serverList != NULL)
        respHdr.serverListVersion = context->serverList->getVersion();
}

/**
 * Top-level service method to handle the PROXY_PING request.
 *
 * \copydetails Service::ping
 */
void
PingService::proxyPing(const WireFormat::ProxyPing::Request& reqHdr,
             WireFormat::ProxyPing::Response& respHdr,
             Rpc& rpc)
{
    uint64_t start = Cycles::rdtsc();
    PingRpc pingRpc(context, ServerId(reqHdr.serverId), ServerId());
    respHdr.replyNanoseconds = ~0UL;
    if (pingRpc.wait(reqHdr.timeoutNanoseconds) != ~0UL) {
        respHdr.replyNanoseconds = Cycles::toNanoseconds(
                Cycles::rdtsc() - start);
    }
}

/**
 * For debugging and testing this function tells the server to kill itself.
 * There will be no response to the RPC for this message, and the process
 * will exit with status code 0.
 *
 * TODO(Rumble): Should be only for debugging and performance testing.
 */
void
PingService::kill(const WireFormat::Kill::Request& reqHdr,
                  WireFormat::Kill::Response& respHdr,
                  Rpc& rpc)
{
    LOG(ERROR, "Server remotely told to kill itself.");
    if (!ignoreKill)
        exit(0);
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
PingService::dispatch(WireFormat::Opcode opcode, Rpc& rpc)
{
    switch (opcode) {
        case WireFormat::GetMetrics::opcode:
            callHandler<WireFormat::GetMetrics, PingService,
                        &PingService::getMetrics>(rpc);
            break;
        case WireFormat::Ping::opcode:
            callHandler<WireFormat::Ping, PingService, &PingService::ping>(rpc);
            break;
        case WireFormat::ProxyPing::opcode:
            callHandler<WireFormat::ProxyPing, PingService,
                        &PingService::proxyPing>(rpc);
            break;
        case WireFormat::Kill::opcode:
            callHandler<WireFormat::Kill, PingService,
                        &PingService::kill>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

} // namespace RAMCloud
