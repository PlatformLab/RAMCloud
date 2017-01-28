/* Copyright (c) 2017 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "ShortMacros.h"
#include "LinearizableObjectRpcWrapper.h"
#include "Logger.h"
#include "ObjectFinder.h"
#include "UnsyncedObjectRpcWrapper.h"
#include "UnsyncedRpcTracker.h"
#include "RamCloud.h"
#include "WitnessService.h"
#include "WitnessTracker.h"

namespace RAMCloud {

/**
 * Constructor for UnsyncedObjectRpcWrapper objects.
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param async
 *      Backup replication happens async. This wrapper should properly
 *      use Witness (if available) to guarantee durability.
 * \param tableId
 *      The table containing the desired object.
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It need not be null terminated.  The caller is responsible for ensuring
 *      that this key remains valid until the call completes.
 * \param keyLength
 *      Size in bytes of key.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked here to ensure that they
 *      contain at least this much data, and a pointer to the header
 *      will be stored in the responseHeader for the use of wrapper
 *      subclasses.
 * \param response
 *      Optional client-supplied buffer to use for the RPC's response;
 *      if NULL then we use a built-in buffer. Any existing contents
 *      of this buffer will be cleared automatically by the transport.
 */
UnsyncedObjectRpcWrapper::UnsyncedObjectRpcWrapper(RamCloud* ramcloud,
        bool async, uint64_t tableId, const void* key, uint16_t keyLength,
        uint32_t responseHeaderLength, Buffer* response)
    : LinearizableObjectRpcWrapper(ramcloud, true, tableId, key, keyLength,
            responseHeaderLength, response)
    , ramcloud(ramcloud)
    , async(async ? (WITNESS_PER_MASTER ? ASYNC_DURABLE : ASYNC) : SYNC)
{
}

/**
 * Alternate constructor for UnsyncedObjectRpcWrapper objects, in which the
 * desired server is specified with a key hash, rather than a key value.
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param async
 *      Backup replication happens async. This wrapper should properly
 *      use Witness (if available) to guarantee durability.
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      Key hash that identifies a particular tablet (and, hence, the
 *      server storing that tablet).
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked here to ensure that they
 *      contain at least this much data, and a pointer to the header
 *      will be stored in the responseHeader for the use of wrapper
 *      subclasses.
 * \param response
 *      Optional client-supplied buffer to use for the RPC's response;
 *      if NULL then we use a built-in buffer.
 */
UnsyncedObjectRpcWrapper::UnsyncedObjectRpcWrapper(RamCloud* ramcloud,
        bool async, uint64_t tableId, uint64_t keyHash,
        uint32_t responseHeaderLength, Buffer* response)
    : LinearizableObjectRpcWrapper(ramcloud, true, tableId, keyHash,
            responseHeaderLength, response)
    , ramcloud(ramcloud)
    , async(async ? (WITNESS_PER_MASTER ? ASYNC_DURABLE : ASYNC) : SYNC)
{
}

void
UnsyncedObjectRpcWrapper::cancel()
{
    LinearizableObjectRpcWrapper::cancel();
    for (int i = 0; i < WITNESS_PER_MASTER; ++i) {
        if (witnessRecordRpcs[i]) {
            witnessRecordRpcs[i]->cancel();
        }
    }
}

bool
UnsyncedObjectRpcWrapper::isReady()
{
    for (int i = 0; i < WITNESS_PER_MASTER; ++i) {
        if (witnessRecordRpcs[i] && !witnessRecordRpcs[i]->isReady()) {
            return false;
        }
    }
    return LinearizableObjectRpcWrapper::isReady();
}

// See RpcWrapper for documentation.
void
UnsyncedObjectRpcWrapper::send()
{
    try {
        auto sessions = context->objectFinder->tryLookupWithWitness(tableId,
                                                                    keyHash);
        session = sessions.toMaster;
        if (session) {
            state = IN_PROGRESS;
            session->sendRequest(&request, response, this);
            session->lastUseTime = Cycles::rdtsc();
        } else {
            retry(0, 0);
        }
        // Send Witness record request if we have full set of witness ready.
        if (async == ASYNC_DURABLE &&
                sessions.toWitness[WITNESS_PER_MASTER - 1]) {
            assert(WITNESS_PER_MASTER - 1 >= 0);

            for (int i = 0; i < WITNESS_PER_MASTER; ++i) {
                int16_t clearHashIndices[3];
                ramcloud->witnessTracker->getDeletable(
                        sessions.witnessServerIds[i],
                        sessions.masterId.getId(),
                        clearHashIndices);
                int16_t hashIndex = static_cast<int16_t>(
                        keyHash & WitnessService::HASH_BITMASK);
                witnessRecordRpcs[i].construct(context,
                    sessions.toWitness[i], sessions.witnessServerIds[i],
                    sessions.masterId.getId(), sessions.witnessBufferBasePtr[i],
                    clearHashIndices, hashIndex, tableId, keyHash, rawRequest);
            }
        }
    } catch (TableDoesntExistException& e) {
        response->reset();
        response->emplaceAppend<WireFormat::ResponseCommon>()->status =
                STATUS_TABLE_DOESNT_EXIST;
        state = FINISHED;
    }
}

bool
UnsyncedObjectRpcWrapper::waitInternal(Dispatch* dispatch,
                                       uint64_t abortTime)
{
    if (!LinearizableObjectRpcWrapper::waitInternal(dispatch, abortTime)) {
        return false; // Aborted by timeout. Shouldn't process RPC's response.
    }

    auto respCommon = reinterpret_cast<const WireFormat::MasterResponseCommon*>(
            responseHeader);
    if (respCommon->status != STATUS_OK) {
        ClientException::throwException(HERE, respCommon->status);
    }

    if (async == ASYNC_DURABLE) {
        assert(WITNESS_PER_MASTER);
        if (!witnessRecordRpcs[WITNESS_PER_MASTER - 1]) {
            async = SYNC;
        } else {
            for (int i = 0; i < WITNESS_PER_MASTER; ++i) {
                if (!witnessRecordRpcs[i]->wait()) {
                    // Witness rejected recording request. Must sync..
                    async = SYNC;
                    break;
                }
            }
        }
    }

    if (async == SYNC) {
        UnsyncedRpcTracker::SyncRpc rpc(context, session, respCommon->logState);
        LogState newLogState;
        rpc.wait(&newLogState);
        ramcloud->unsyncedRpcTracker->updateLogState(session.get(),
                                                     newLogState);
    }
    return true;
}

/**
 * Fabricate a UnsyncedRpcTracker callback that frees witness entries written
 * by this RPC.
 *
 * \return
 *      Callback to be registered in UnsyncedRpcTracker.
 */
std::function<void()>
UnsyncedObjectRpcWrapper::getWitnessFreeFunc()
{
    uint64_t witnessServerId[WITNESS_PER_MASTER];
    uint64_t targetMasterId[WITNESS_PER_MASTER];
    int16_t hashIndex[WITNESS_PER_MASTER];
    for (int i = 0; i < WITNESS_PER_MASTER; ++i) {
        if (witnessRecordRpcs[i]) {
            witnessServerId[i] = witnessRecordRpcs[i]->witnessServerId;
            targetMasterId[i] = witnessRecordRpcs[i]->targetMasterId;
            hashIndex[i] = witnessRecordRpcs[i]->hashIndex;
        }
    }
    WitnessTracker* witnessTracker = ramcloud->witnessTracker;
    assert(witnessTracker);
    return [witnessServerId, targetMasterId, hashIndex, witnessTracker](){
        assert(witnessTracker);
        for (int i = 0; i < WITNESS_PER_MASTER; ++i) {
            if (hashIndex[i] != -1) {
                witnessTracker->free(witnessServerId[i],
                                     targetMasterId[i],
                                     hashIndex[i]);
            }
        }
    };
}

///////////////////////////////////
// WitnessRecordRpc
///////////////////////////////////
UnsyncedObjectRpcWrapper::WitnessRecordRpc::WitnessRecordRpc(Context* context,
        Transport::SessionRef& sessionToWitness, uint64_t witnessId,
        uint64_t targetMasterId, uint64_t bufferBasePtr,
        int16_t clearHashIndices[], int16_t hashIndex,
        uint64_t tableId, uint64_t keyHash, ClientRequest clientRequest)
    : RpcWrapper(sizeof(WireFormat::WitnessRecord::Response), NULL)
    , context(context)
    , tableId(tableId)
    , witnessServerId(witnessId)
    , targetMasterId(targetMasterId)
    , hashIndex(hashIndex)
{
    session = sessionToWitness;
    WireFormat::WitnessRecord::Request* reqHdr(
            allocHeader<WireFormat::WitnessRecord>());
    reqHdr->targetMasterId = targetMasterId;
    reqHdr->bufferBasePtr = bufferBasePtr;
    memcpy(reqHdr->clearHashIndices, clearHashIndices,
           sizeof(reqHdr->clearHashIndices));
    reqHdr->hashIndex = hashIndex; //Think... do we need this arg? or calc here?
    reqHdr->entryHeader = {clientRequest.size, tableId, keyHash};
    // Now append request..
    request.append(clientRequest.data, clientRequest.size);
    send();
}

bool
UnsyncedObjectRpcWrapper::WitnessRecordRpc::wait()
{
    simpleWait(context);
    const WireFormat::WitnessRecord::Response* respHdr(
            getResponseHeader<WireFormat::WitnessRecord>());
    // TODO(seojin): check failed RPC state is correctly fire off exception??
    if (!respHdr->accepted) {
        witnessServerId = 0;
        targetMasterId = 0;
        hashIndex = -1;
    }
    return respHdr->accepted;
}

// See RpcWrapper for documentation.
bool
UnsyncedObjectRpcWrapper::WitnessRecordRpc::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mapping for our object.
    // Then retry.
    context->transportManager->flushSession(session->serviceLocator);
    context->objectFinder->flush(tableId);
    if (context->unsyncedRpcTracker) {
        context->unsyncedRpcTracker->flushSession(session.get());
    }
    session = NULL;
    return true;
}

} // namespace RAMCloud
