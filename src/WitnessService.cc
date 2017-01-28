/* Copyright (c) 2009-2016 Stanford University
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

#include "WitnessService.h"
#include "Memory.h"
#include "ServerConfig.h"
#include "ShortMacros.h"
#include "Status.h"

namespace RAMCloud {

/**
 * Default constructor.
 *
 * \param context
 *      Overall information about the RAMCloud server. The new service
 *      will be registered in this context.
 * \param config
 *      Settings for this instance. The caller guarantees that config will
 *      exist for the duration of this WitnessService's lifetime.
 */
WitnessService::WitnessService(Context* context,
                               const ServerConfig* config)
    : context(context)
    , mutex()
    , config(config)
    , buffers()
{
    context->services[WireFormat::WITNESS_SERVICE] = this;
}

/**
 * Default destructor.
 */
WitnessService::~WitnessService()
{
    context->services[WireFormat::WITNESS_SERVICE] = NULL;
}

// See Server::dispatch.
void
WitnessService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
//
//    if (!initCalled) {
//        LOG(WARNING, "%s invoked before initialization complete; "
//                "returning STATUS_RETRY", WireFormat::opcodeSymbol(opcode));
//        throw RetryException(HERE, 100, 100,
//                "backup service not yet initialized");
//    }
//    CycleCounter<RawMetric> serviceTicks(&metrics->backup.serviceTicks);

    switch (opcode) {
        case WireFormat::WitnessStart::opcode:
            callHandler<WireFormat::WitnessStart, WitnessService,
                        &WitnessService::start>(rpc);
            break;
        case WireFormat::WitnessRecord::opcode:
            // Only used by unit tests using MockCluster. The real version of
            // this invoker is in WorkerManager::handleRpc().
            WitnessService::record(
                    rpc->requestPayload->getStart<
                            WireFormat::WitnessRecord::Request>(),
                    rpc->replyPayload->emplaceAppend<
                            WireFormat::WitnessRecord::Response>(),
                    rpc->requestPayload);
            rpc->sendReply();
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * Prepare the witness.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to free.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
WitnessService::start(const WireFormat::WitnessStart::Request* reqHdr,
                      WireFormat::WitnessStart::Response* respHdr,
                      Rpc* rpc)
{
    Lock _(mutex);
    Master* buffer = reinterpret_cast<Master*>(
                            Memory::xmalloc(HERE, sizeof(Master)));
    memset(buffer, 0, sizeof(Master));
    buffer->id = reqHdr->targetMasterId;
    buffer->writable = true;
    buffers.insert(std::make_pair(buffer->id, buffer));

    LOG(NOTICE, "Witness buffer is reserved for master <%" PRIu64 "> at %p",
                reqHdr->targetMasterId, buffer);

    // Prepare response
    respHdr->bufferBasePtr = reinterpret_cast<uint64_t>(buffer);
}

} // namespace RAMCloud
