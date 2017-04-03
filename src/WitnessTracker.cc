/* Copyright (c) 2017 Stanford University
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

#include "WitnessTracker.h"
#include "ObjectManager.h"
#include "ShortMacros.h"
#include "Service.h"
#include "WitnessClient.h"
#include "WitnessService.h"
#include "TimeTrace.h"

namespace RAMCloud {

uint WitnessTracker::syncBatchSize = 20;

/**
 * Default constructor
 */
WitnessTracker::WitnessTracker(Context* context, ObjectManager* objectManager)
    : context(context)
    , objectManager(objectManager)
    , listVersion(0)
    , witnesses()
    , unsyncedRpcs()
    , mutex("WitnessTracker::mutex")
{
}

/**
 * Default destructor
 */
WitnessTracker::~WitnessTracker()
{
}

void
WitnessTracker::listChanged(uint32_t newListVersion, int numWitnesses,
        WireFormat::NotifyWitnessChange::WitnessInfo witnessList[])
{
    Lock _(mutex);
    listVersion = newListVersion;
    witnesses.clear();

    char msg[200] = "Witness list is changed. New witnessIds:";
    for (int i = 0; i < numWitnesses; ++i) {
        Witness witness { ServerId(witnessList[i].witnessServerId),
                          witnessList[i].bufferBasePtr };
        witnesses.push_back(witness);
        snprintf(msg + strlen(msg), 200, " %lu",
                 witnessList[i].witnessServerId);
    }
    LOG(NOTICE, "%s", msg);
}

void
WitnessTracker::registerRpcAndSyncBatch(bool syncEarly,
                                        uint64_t keyHash,
                                        uint64_t clientLeaseId,
                                        uint64_t rpcId,
                                        LogPosition logPos)
{
    Tub<Lock> lock;
    lock.construct(mutex);
    int16_t hashIndex = static_cast<int16_t>(
            keyHash & WitnessService::HASH_BITMASK);
    GcInfo info;
    info.gcEntry = { hashIndex, clientLeaseId, rpcId };
    info.logPos = logPos;
    unsyncedRpcs.push_back(info);

    // If batch count is met, sync and GC synced entries in Witnesses.
    if (syncEarly || unsyncedRpcs.size() >= syncBatchSize) {
        TimeTrace::record("WitnessTracker::registerRpcAndSyncBatch sync start");
#ifndef CGARC_ONLY
        std::vector<GcInfo> syncedEntries;
        syncedEntries.swap(unsyncedRpcs);
#else
        unsyncedRpcs.clear();
#endif
        lock.destroy();

        // 1. Sync
        objectManager->syncChanges();

        CycleCounter<uint64_t> __(&PerfStats::threadStats.temp3);

#ifndef CGARC_ONLY
        // 2. Reset synced entries. [Mutex required]
        lock.construct(mutex);
        size_t numWitness = witnesses.size();
        Tub<WitnessGcRpc>* gcRpcs = new Tub<WitnessGcRpc>[numWitness];
        Buffer* respBuffers = new Buffer[numWitness];
        ServerId serverId =
                context->services[WireFormat::MASTER_SERVICE]->serverId;
        for (uint i = 0; i < numWitness; ++i) {
            Witness witness = witnesses[i];
            gcRpcs[i].construct(context, witness.witnessId,
                    &respBuffers[i], serverId, witness.bufferBasePtr,
                    syncedEntries);
        }
        lock.destroy();

        // 3. wait for responses & process..
        std::vector<ClientRequest> blockingRequests;
        for (uint i = 0; i < numWitness; ++i) {
            try {
                gcRpcs[i]->wait(&blockingRequests);
            } catch (ServerNotUpException&) {
            }
        }
        // Retry the blocking requests & push_back RPC info to unsyncedRpcs
        // again.
        Buffer reqPayload;
        Buffer replyPayload;
        for (ClientRequest clientReq : blockingRequests) {
            LOG(WARNING, "BUG. this is not yet implemented.");
            reqPayload.reset();
            replyPayload.reset();
            reqPayload.appendExternal(clientReq.data, clientReq.size);

            WireFormat::AsyncRequestCommon* header =
                    reqPayload.getStart<WireFormat::AsyncRequestCommon>();
            assert(header);
            assert(header->service == WireFormat::MASTER_SERVICE);
            Service::Rpc retryRpc(NULL, &reqPayload, &replyPayload);
            WireFormat::Opcode opcode = WireFormat::Opcode(header->opcode);

            try {
                context->services[WireFormat::MASTER_SERVICE]->
                        dispatch(opcode, &retryRpc);
            } catch (StaleRpcException& e) {
            } catch (RetryException& e) {
                LOG(WARNING, "Retry exception thrown while unblocking witness"
                        " entry witness for %s RPC",
                        WireFormat::opcodeSymbol(opcode));
            } catch (ClientException& e) {
            }
        }
        // TODO: add leasId and rpcIds into the queue again.

        delete[] respBuffers;
        delete[] gcRpcs;
#endif
        TimeTrace::record("WitnessTracker::registerRpcAndSyncBatch sync end");
    }
}

void
WitnessTracker::freeWitnessEntries(LogPosition& synced) {
    Tub<Lock> lock;
    lock.construct(mutex);

    std::vector<GcInfo> syncedEntries;
    std::vector<GcInfo> unsyncedEntries;
    for (GcInfo& gcInfo : unsyncedRpcs) {
        if (gcInfo.logPos <= synced) {
            syncedEntries.push_back(gcInfo);
        } else {
            unsyncedEntries.push_back(gcInfo);
        }
    }
    if (!syncedEntries.empty()) {
        unsyncedEntries.swap(unsyncedRpcs);

        // 2. Send witness reset. [Mutex required]
        size_t numWitness = witnesses.size();
        Tub<WitnessGcRpc>* gcRpcs = new Tub<WitnessGcRpc>[numWitness];
        Buffer* respBuffers = new Buffer[numWitness];
        ServerId serverId =
                context->services[WireFormat::MASTER_SERVICE]->serverId;
        for (uint i = 0; i < numWitness; ++i) {
            Witness witness = witnesses[i];
            gcRpcs[i].construct(context, witness.witnessId,
                    &respBuffers[i], serverId, witness.bufferBasePtr,
                    syncedEntries);
        }

        lock.destroy();

        // Wait for Witness reset RPCs.
        std::vector<ClientRequest> blockingRequests;
        for (uint i = 0; i < numWitness; ++i) {
            try {
                gcRpcs[i]->wait(&blockingRequests);
            } catch (ServerNotUpException&) {
            }
        }
        delete[] respBuffers;
        delete[] gcRpcs;
    }
}

} // namespace RAMCloud
