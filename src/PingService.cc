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
#include "BenchUtil.h"
#include "PingClient.h"
#include "PingService.h"

namespace RAMCloud {

/**
 * Top-level service method to handle the PING request.
 *
 * \copydetails Service::ping
 */
void
PingService::ping(const PingRpc::Request& reqHdr,
             PingRpc::Response& respHdr,
             Rpc& rpc)
{
    TEST_LOG("nonce: %ld", reqHdr.nonce);
    respHdr.nonce = reqHdr.nonce;
}

/**
 * Top-level service method to handle the PROXY_PING request.
 *
 * \copydetails Service::ping
 */
void
PingService::proxyPing(const ProxyPingRpc::Request& reqHdr,
             ProxyPingRpc::Response& respHdr,
             Rpc& rpc)
{
    PingClient client;
    const char* serviceLocator = getString(rpc.requestPayload,
                                           sizeof(ProxyPingRpc::Request),
                                           reqHdr.serviceLocatorLength);
    uint64_t start = rdtsc();
    try {
        uint64_t result = client.ping(serviceLocator, 99999,
                                      reqHdr.timeoutNanoseconds);
        if (result == 99999U) {
            respHdr.replyNanoseconds = cyclesToNanoseconds(rdtsc() - start);
        } else {
            respHdr.replyNanoseconds = -1;
        }
    }
    catch (TimeoutException& e) {
        respHdr.replyNanoseconds = -1;
    }

}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
PingService::dispatch(RpcOpcode opcode, Rpc& rpc)
{
    switch (opcode) {
        case PingRpc::opcode:
            callHandler<PingRpc, PingService, &PingService::ping>(rpc);
            break;
        case ProxyPingRpc::opcode:
            callHandler<ProxyPingRpc, PingService,
                        &PingService::proxyPing>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

} // namespace RAMCloud
