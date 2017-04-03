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
#include "TimeTrace.h"

namespace RAMCloud {

uint64_t UnsyncedObjectRpcWrapper::rejectCount = 0;
uint64_t UnsyncedObjectRpcWrapper::totalCount = 0;

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
    // Temporary hack for benchmark CGAR-C without witness...
#ifdef CGARC_ONLY
    if (this->async == ASYNC_DURABLE)
        this->async = ASYNC;
#endif
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
    // Temporary hack for benchmark CGAR-C without witness...
#ifdef CGARC_ONLY
    if (this->async == ASYNC_DURABLE)
        this->async = ASYNC;
#endif
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
            auto header = request.getStart<WireFormat::AsyncRequestCommon>();
            header->witnessListVersion = sessions.witnessListVersion;
            session->sendRequest(&request, response, this);
            session->lastUseTime = Cycles::rdtsc();
        } else {
            retry(0, 0);
        }
        TimeTrace::record("WriteRpc send (main)");
        // Send Witness record request if we have full set of witness ready.
        if (async == ASYNC_DURABLE &&
                sessions.toWitness[WITNESS_PER_MASTER - 1]) {
            assert(WITNESS_PER_MASTER - 1 >= 0);

            for (int i = 0; i < WITNESS_PER_MASTER; ++i) {
                if (witnessRecordRpcs[i]) {
                    // Main RPC is retrying, do nothing since
                    // WitnessRecordRpc was already constructed before.
                    continue;
                }
                int16_t hashIndex = static_cast<int16_t>(
                        keyHash & WitnessService::HASH_BITMASK);
                witnessRecordRpcs[i].construct(context,
                    sessions.toWitness[i], sessions.witnessServerIds[i],
                    sessions.masterId.getId(), sessions.witnessBufferBasePtr[i],
                    hashIndex, tableId, keyHash, rawRequest);
                TimeTrace::record("WriteRpc send (witness)");
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
    ++totalCount;
    bool shouldSync = false;
    if (async == ASYNC_DURABLE) {
        assert(WITNESS_PER_MASTER);
        if (!witnessRecordRpcs[WITNESS_PER_MASTER - 1]) {
            shouldSync = true;
        } else {
            for (int i = 0; i < WITNESS_PER_MASTER; ++i) {
                bool accepted = witnessRecordRpcs[i]->wait();
                TimeTrace::record("Witness wait.");
                if (!accepted) {
                    // Witness rejected recording request. Must sync..
                    shouldSync = true;
                    break;
                }
            }
        }
    }

    if (!LinearizableObjectRpcWrapper::waitInternal(dispatch, abortTime)) {
        return false; // Aborted by timeout. Shouldn't process RPC's response.
    }
    TimeTrace::record("Main linearizable RPC waitInternal.");

    auto respCommon = reinterpret_cast<const WireFormat::MasterResponseCommon*>(
            responseHeader);
    if (respCommon->status != STATUS_OK) {
        ClientException::throwException(HERE, respCommon->status);
    }
    // This is also called in register unsynced... WTH.. remove one in
    // registerUnsynced. I am keeping this here to benefit for synchronous RPC's
    // result as well.
    ramcloud->unsyncedRpcTracker->updateLogState(session.get(),
                                                 respCommon->logState);

    // Skip sync if it was already synced by master.
    if (shouldSync &&
            respCommon->logState.appended > respCommon->logState.synced) {
        ++rejectCount;
        UnsyncedRpcTracker::SyncRpc rpc(context, session, respCommon->logState);
//        TimeTrace::record("syncRpc send.");
        LogState newLogState;
        if (!rpc.wait(&newLogState)) {
            clearAndRetry(0, 0);
            return waitInternal(dispatch, abortTime);
        }
//        TimeTrace::record("syncRpc wait.");
        ramcloud->unsyncedRpcTracker->updateLogState(session.get(),
                                                     newLogState);
//        TimeTrace::record("syncRpc updateLogState.");
    }
    TimeTrace::record("syncRpc done.");
    return true;
}

/**
 * This method is invoked in situations where the RPC should be retried although
 * it was finished before. (eg. syncRpc failed.)
 * This method resets every internal states set by previous send.
 * \param minDelayMicros
 *      Minimum time to wait, in microseconds.
 * \param maxDelayMicros
 *      Maximum time to wait, in microseconds. The actual delay time
 *      will be chosen randomly between minDelayMicros and maxDelayMicros.
 */
void
UnsyncedObjectRpcWrapper::clearAndRetry(uint32_t minDelayMicros,
                                        uint32_t maxDelayMicros)
{
    response->reset();
    responseHeader = NULL;
    retry(minDelayMicros, maxDelayMicros);
}

///////////////////////////////////
// WitnessRecordRpc
///////////////////////////////////
UnsyncedObjectRpcWrapper::WitnessRecordRpc::WitnessRecordRpc(Context* context,
        Transport::SessionRef& sessionToWitness, uint64_t witnessId,
        uint64_t targetMasterId, uint64_t bufferBasePtr, int16_t hashIndex,
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
    if (session) {
        context->transportManager->flushSession(session->serviceLocator);
    }
    context->objectFinder->flush(tableId);
    if (session && context->unsyncedRpcTracker) {
        context->unsyncedRpcTracker->flushSession(session.get());
    }
    session = NULL;
    return true;
}

} // namespace RAMCloud
