/* Copyright (c) 2010-2015 Stanford University
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

#include "Cycles.h"
#include "RawMetrics.h"
#include "RpcLevel.h"
#include "Service.h"
#include "ShortMacros.h"
#include "TransportManager.h"
#include "WorkerManager.h"

namespace RAMCloud {

/**
 * Constructor for Service objects.
 */
Service::Service()
    : serverId()
{
}

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
Service::getString(Buffer* buffer, uint32_t offset, uint32_t length) {
    const char* result;
    if (length == 0) {
        throw RequestFormatError(HERE);
    }
    result = static_cast<const char*>(buffer->getRange(offset, length));
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
Service::ping(const WireFormat::Ping::Request* reqHdr,
             WireFormat::Ping::Response* respHdr,
             Rpc* rpc)
{
    // This method no longer serves any useful purpose (as of 6/2011) and
    // shouldn't get invoked except during tests.  It stays around mostly
    // so that other methods can \copydoc its documentation.
    TEST_LOG("Service::ping invoked");
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
Service::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    switch (opcode) {
        case WireFormat::Ping::opcode:
            callHandler<WireFormat::Ping, Service, &Service::ping>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * This method is invoked by WorkerManager to process an incoming
 * RPC.  Under normal conditions, when this method returns the RPC has
 * been serviced and a response has been prepared, but the response
 * has not yet been sent back to the client (the caller will take
 * care of this).
 *
 * \param context
 *      Overall information about the server (e.g. holds array of
 *      defined services).
 * \param rpc
 *      An incoming RPC that is ready to be serviced. We assume that
 *      the caller has already verified that the request has a valid
 *      opcode, so we don't recheck it here.
 */
void
Service::handleRpc(Context* context, Rpc* rpc) {
    // This method takes care of things that are the same in all services,
    // such as keeping statistics. It then calls a service-specific dispatch
    // method to handle the details of performing the RPC.
    const WireFormat::RequestCommon* header;
    header = rpc->requestPayload->getStart<WireFormat::RequestCommon>();
    if (header == NULL) {
        prepareErrorResponse(rpc->replyPayload, STATUS_MESSAGE_TOO_SHORT);
        return;
    }
    if ((header->service >= WireFormat::INVALID_SERVICE)
            || (context->services[header->service] == NULL)) {
        prepareErrorResponse(rpc->replyPayload, STATUS_SERVICE_NOT_AVAILABLE);
        return;
    }
    Service* service = context->services[header->service];

    WireFormat::Opcode opcode = WireFormat::Opcode(header->opcode);
    (&metrics->rpc.rpc0Count)[opcode]++;
    RpcLevel::setCurrentOpcode(opcode);
    uint64_t start = Cycles::rdtsc();
    try {
        service->dispatch(opcode, rpc);
    } catch (RetryException& e) {
        prepareRetryResponse(rpc->replyPayload, e.minDelayMicros,
                e.maxDelayMicros, e.message);
    } catch (ClientException& e) {
        prepareErrorResponse(rpc->replyPayload, e.status);
    }
#ifdef TESTING
    // This code is only needed when running unit tests (without it,
    // the weird structure of the unit test results in lots of errors in
    // RpcLevel::checkCall). The code is harmless outside of unit tests,
    // but it just wastes time.
    RpcLevel::setCurrentOpcode(RpcLevel::NO_RPC);
#endif
    (&metrics->rpc.rpc0Ticks)[opcode] += Cycles::rdtsc() - start;
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
Service::prepareErrorResponse(Buffer* replyPayload, Status status)
{
    WireFormat::ResponseCommon* responseCommon =
        const_cast<WireFormat::ResponseCommon*>(
        replyPayload->getStart<WireFormat::ResponseCommon>());
    if (responseCommon == NULL) {
        // Response is currently empty; add a header to it.
        responseCommon =
            replyPayload->emplaceAppend<WireFormat::ResponseCommon>();
    }
    responseCommon->status = status;
}

/**
 * Fill in an RPC response buffer to indicate that the client should
 * retry the request later.
 *
 * \param replyPayload
 *      Buffer that should contain the response. Any existing information
 *      in the buffer is deleted, and the buffers filled in with a
 *      ReplyResponse containing the argument information.
 * \param minDelayMicros
 *     Lower-bound on how long the client should wait before retrying
 *     the RPC, in microseconds.
 * \param maxDelayMicros
 *     Upper bound on the client delay, in microseconds: the client
 *     should pick a random number between minDelayMicros and
 *     maxDelayMicros and wait that many microseconds before retrying
 *     the RPC.
 * \param message
 *     Human-readable message describing the reason for the retry; this
 *     is likely to get logged on the client side. NULL means there is
 *     no message.
 */
void
Service::prepareRetryResponse(Buffer* replyPayload, uint32_t minDelayMicros,
        uint32_t maxDelayMicros, const char* message)
{
    replyPayload->reset();
    WireFormat::RetryResponse* response =
            replyPayload->emplaceAppend<WireFormat::RetryResponse>();
    response->common.status = STATUS_RETRY;
    response->minDelayMicros = minDelayMicros;
    response->maxDelayMicros = maxDelayMicros;
    if (message != NULL) {
        response->messageLength = downCast<uint32_t>(strlen(message) + 1);
        replyPayload->appendCopy(message, response->messageLength);
    } else {
        response->messageLength = 0;
    }
}

/**
 * This method is invoked once by Server.cc to notify the service that the
 * server has enlisted with the coordinator and to provide the ServerId it
 * was assigned. We simply record the serverId and call into the subclass so
 * that it can do any work it had been deferring until now.
 *
 * \param assignedServerId
 *      The ServerId assigned to this server by the coordinator.
 */
void
Service::setServerId(ServerId assignedServerId)
{
    assert(!serverId.isValid());
    serverId = assignedServerId;

    // Call into the subclass (if implemented) to notify that enlistment has
    // completed and that 'serverId' ready for use.
    initOnceEnlisted();
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
