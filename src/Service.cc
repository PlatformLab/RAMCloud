/* Copyright (c) 2010-2011 Stanford University
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

#include "Service.h"
#include "ServiceManager.h"
#include "TransportManager.h"

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
Service::getString(Buffer& buffer, uint32_t offset, uint32_t length) {
    const char* result;
    if (length == 0) {
        throw RequestFormatError(HERE);
    }
    result = static_cast<const char*>(buffer.getRange(offset, length));
    if (result == NULL) {
        throw MessageTooShortError(HERE);
    }
    if (result[length - 1] != '\0') {
        throw RequestFormatError(HERE);
    }
    return result;
}

/**
 * Top-level service method to handle the PING request.
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
Service::ping(const PingRpc::Request& reqHdr,
             PingRpc::Response& respHdr,
             Rpc& rpc)
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
        rpcsHandled[i] = 0;
        rpcsTime[i] = 0;
        LOG(DEBUG, "%5u: %10lu %10lu", i, h, t);
    }
    LOG(DEBUG, "Total: %10lu %10lu", totalCount, totalTime);
    transportManager.dumpStats();
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
Service::dispatch(RpcOpcode opcode, Rpc& rpc)
{
    switch (opcode) {
        case PingRpc::opcode:
            callHandler<PingRpc, Service, &Service::ping>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * This method is invoked by ServiceManager to process an incoming
 * RPC.  Under normal conditions, when this method returns the RPC has
 * been serviced and a response has been prepared, but the response
 * has not yet been sent back to the client (this will happen later on
 * in the dispatch thread).
 *
 * \param rpc
 *      An incoming RPC that is ready to be serviced.
 */
void
Service::handleRpc(Rpc& rpc) {
    // This method takes care of things that are the same in all services,
    // such as keeping statistics. It then calls a service-specific dispatch
    // method to handle the details of performing the RPC.
    const RpcRequestCommon* header;
    header = rpc.requestPayload.getStart<RpcRequestCommon>();
    if (header == NULL) {
        prepareErrorResponse(rpc.replyPayload, STATUS_MESSAGE_TOO_SHORT);
        return;
    }

    // The check below is needed to avoid out-of-range accesses to
    // rpcsHandled etc.
    uint32_t opcode = header->opcode;
    if (opcode >= ILLEGAL_RPC_TYPE)
        opcode = ILLEGAL_RPC_TYPE;
    rpcsHandled[opcode]++;
    uint64_t start = rdtsc();
    try {
        dispatch(RpcOpcode(header->opcode), rpc);
    } catch (ClientException& e) {
        prepareErrorResponse(rpc.replyPayload, e.status);
    }
    rpcsTime[opcode] += rdtsc() - start;
}

/**
 * Fill in an RPC response buffer to indicate that the RPC failed with
 * a particular status.
 *
 * \param replyPayload
 *      Buffer that should contain the response. If there is already a
 *      header in the buffer, it is retained and this method simply overlays
 *      a new status in it (the RPC may have placed additional error
 *      information there, such as the actual version of an object when
 *      a version match fails).
 * \param status
 *      The problem that caused the RPC to fail.
 */
void
Service::prepareErrorResponse(Buffer& replyPayload, Status status)
{
    RpcResponseCommon* responseCommon = const_cast<RpcResponseCommon*>(
        replyPayload.getStart<RpcResponseCommon>());
    if (responseCommon == NULL) {
        // Response is currently empty; add a header to it.
        responseCommon =
            new(&replyPayload, APPEND) RpcResponseCommon;
    }
    responseCommon->status = status;
}

/**
 * A worker thread can invoke this method to start sending a reply to
 * an RPC.  Normally a worker thread does not call this method; when it
 * returns from handling the request the reply will be sent automatically.
 * However, in some cases the worker may want to send the reply before
 * returning, so that it can do additional work without delaying the
 * reply.  In those cases it invokes this method.
 */
void
Service::Rpc::sendReply()
{
    // The "if" statement below is only needed to simplify tests; it should
    // never be needed in a real system.
    if (worker != NULL) {
        worker->sendReply();
    }
}

} // namespace RAMCloud
