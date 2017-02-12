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
    , gcTracker(context, this)
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
    switch (opcode) {
        case WireFormat::WitnessStart::opcode:
            callHandler<WireFormat::WitnessStart, WitnessService,
                        &WitnessService::start>(rpc);
            break;
        case WireFormat::WitnessGetRecoveryData::opcode:
            callHandler<WireFormat::WitnessGetRecoveryData, WitnessService,
                        &WitnessService::getRecoveryData>(rpc);
            break;
        case WireFormat::WitnessGc::opcode:
            // Only used by unit tests using MockCluster. The real version of
            // this invoker is in WorkerManager::handleRpc().
            WitnessService::gc(
                    rpc->requestPayload->getStart<
                            WireFormat::WitnessGc::Request>(),
                    rpc->replyPayload->emplaceAppend<
                            WireFormat::WitnessGc::Response>(),
                    rpc->requestPayload);
            rpc->sendReply();
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

void
WitnessService::getRecoveryData(
        const WireFormat::WitnessGetRecoveryData::Request* reqHdr,
        WireFormat::WitnessGetRecoveryData::Response* respHdr,
        Rpc* rpc)
{
    Lock _(mutex);
    Master* master = buffers[reqHdr->crashedMasterId];
    assert(master->id == reqHdr->crashedMasterId);
    assert(!master->writable);

    int numEntry = 0;
    for (int i = reqHdr->continuation; i < NUM_ENTRIES_PER_TABLE; ++i) {
        Entry& entry = master->table[i];
        if (entry.occupied &&
                entry.header.tableId == reqHdr->tableId &&
                entry.header.keyHash >= reqHdr->firstKeyHash &&
                entry.header.keyHash <= reqHdr->lastKeyHash) {
            numEntry++;
            rpc->replyPayload->emplaceAppend<int16_t>(
                    downCast<int16_t>(entry.header.requestSize));
            rpc->replyPayload->append(entry.request,
                    downCast<uint32_t>(entry.header.requestSize));
            //TODO: need a mechanism to check we are over RPC reply limit.
            // Now, 2KB * 512 = 1MB, should be okay.
        }
    }
    LOG(NOTICE, "found %d requests for tablet %lu %lu %lu (scanned indices from"
            " %d to %d)", numEntry, reqHdr->tableId, reqHdr->firstKeyHash,
            reqHdr->lastKeyHash, reqHdr->continuation, NUM_ENTRIES_PER_TABLE);

    respHdr->numOps = numEntry;
    respHdr->continuation = 0;
    respHdr->common.status = STATUS_OK;
}

/**
 * Garbage collect witness buffers for any server that gets removed
 * from the cluster. Called when changes to the server wide server list
 * are enqueued.
 */
void
WitnessService::trackerChangesEnqueued()
{
    ServerDetails server;
    ServerChangeEvent event;
    while (gcTracker.getChange(server, event)) {
        if (event == SERVER_REMOVED) {
            Lock _(mutex);
            auto it = buffers.find(server.serverId.getId());
            if (it != buffers.end()) {
                Master* buffer = it->second;
                assert(buffer->id == server.serverId.getId());
                buffers.erase(it);
//                free(buffer);
                // TODO: use pool instead of alloc & free everytime to avoid
                //      random segfault.
                // TODO: Schedule this in dispatch thread...
                LOG(NOTICE, "Deleted witness buffer for master <%" PRIu64 ">",
                        server.serverId.getId());
            }
        }
    }
}

} // namespace RAMCloud
