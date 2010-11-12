/* Copyright (c) 2010 Stanford University
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

#include "Server.h"

namespace RAMCloud {

/**
 * Find and validate a string in a buffer.  This method is invoked
 * by RPC handlers expecting a null-terminated string to be present
 * in an incoming request. It makes sure that the buffer contains
 * adequate space for a string of a given length at a given location
 * in the buffer, and it verifies that the string is null-terminated
 * and non-empty.
 *
 * \param buffer
 *      Buffer containing the desired string; typically an RPC
 *      request payload.
 * \param offset
 *      Location of the first byte of the string within the buffer.
 * \param length
 *      Total length of the string, including terminating null
 *      character.
 *
 * \return
 *      Pointer that can be used to access the string.  The string is
 *      guaranteed to exist in its entirety and to be null-terminated.
 *      (One condition we don't check for: premature termination via a
 *      null character in the middle of the string).
 *
 * \exception MessageTooShort
 *      The buffer isn't large enough for the expected size of the
 *      string.
 * \exception RequestFormatError
 *      The string was not null-terminated or had zero length.
 */
const char*
Server::getString(Buffer& buffer, uint32_t offset, uint32_t length) const {
    const char* result;
    if (length == 0) {
        throw RequestFormatError(HERE);
    }
    // TODO(ongaro): update to check result against NULL instead
    if (buffer.getTotalLength() < (offset + length)) {
        throw MessageTooShortError(HERE);
    }
    result = static_cast<const char*>(buffer.getRange(offset, length));
    if (result[length - 1] != '\0') {
        throw RequestFormatError(HERE);
    }
    return result;
}

/**
 * Top-level server method to handle the PING request.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters
 *      for this operation.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call; can be
 *      used to read additional information beyond the request header
 *      and/or append additional information to the response buffer.
 */
void
Server::ping(const PingRpc::Request& reqHdr,
             PingRpc::Response& respHdr,
             Transport::ServerRpc& rpc)
{
    // Nothing to do here.
    TEST_LOG("ping");
    LOG(DEBUG, "RPCs serviced");
    uint64_t totalCount = 0;
    uint64_t totalTime = 0;
    for (uint32_t i = 0; i < ILLEGAL_RPC_TYPE; i++) {
        uint64_t h = rpcsHandled[i];
        uint64_t t = rpcsTime[i];
        if (!h)
            continue;
        totalCount += h;
        totalTime += t;
        LOG(DEBUG, "%5u: %10lu %10lu", i, h, t);
    }
    LOG(DEBUG, "Total: %10lu %10lu", totalCount, totalTime);
}

/**
 * Dispatch an RPC to the right handler based on its type.
 */
void
Server::dispatch(RpcType type, Transport::ServerRpc& rpc, Responder& responder)
{
    switch (type) {
        case PingRpc::type:
            callHandler<PingRpc, Server, &Server::ping>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * Serve RPCs forever.
 */
void __attribute__ ((noreturn))
Server::run()
{
    while (true)
        handleRpc<Server>();
}

} // namespace RAMCloud
