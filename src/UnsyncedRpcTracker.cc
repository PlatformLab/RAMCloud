/* Copyright (c) 2016 Stanford University
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

#include "UnsyncedRpcTracker.h"
#include "ClientException.h"
#include "ObjectRpcWrapper.h"
#include "RamCloud.h"
#include "RpcRequestPool.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Default constructor
 *
 * \param ramcloud
 *      Overall information about the calling client.
 */
UnsyncedRpcTracker::UnsyncedRpcTracker(RamCloud* ramcloud)
    : masters()
    , mutex()
    , ramcloud(ramcloud)
{
    ramcloud->clientContext->unsyncedRpcTracker = this;
}

/**
 * Default destructor
 */
UnsyncedRpcTracker::~UnsyncedRpcTracker()
{
    for (MasterMap::iterator it = masters.begin(); it != masters.end(); ++it) {
        Master* master = it->second;
        delete master;
    }
}

/**
 * Saves the information of an non-durable RPC whose response is received.
 * Any non-durable RPC should register itself before returning wait() call.
 *
 * \param session
 *      Transport session where this RPC is sent to.
 * \param rpcRequest
 *      Pointer to RPC request which is previously sent to target master.
 *      This memory must be either from Allocator or malloc.
 *      Onwership of this memory is now in UnsyncedRpcTracker. No one should
 *      modify or free this memory.
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      Key hash that identifies a particular tablet.
 * \param objVer
 *      Version of object after applying modifications by this RPC.
 * \param logPos
 *      Location of new value of object in master after applying this RPC.
 * \param callback
 *      Callback desired to be invoked when the modifications on the object
 *      by this RPC becomes durable (by replicating to backups).
 */
void
UnsyncedRpcTracker::registerUnsynced(Transport::SessionRef session,
                                    ClientRequest rpcRequest,
                                    uint64_t tableId,
                                    uint64_t keyHash,
                                    uint64_t objVer,
                                    WireFormat::LogState logPos,
                                    std::function<void()> callback)
{
    Lock lock(mutex);
    Master* master = getOrInitMasterRecord(session);
    master->rpcs.emplace(rpcRequest, tableId, keyHash, objVer, logPos,
                         callback);
    master->updateLogState(ramcloud, logPos);
}

/**
 * Invoked if there is a problem with a session to master, which hints
 * a possible crash of the master. It will recover all possibly lost updates
 * by retrying requests that are not known to be replicated to backups.
 *
 * \param sessionPtr
 *      A raw pointer to session which is being destroyed.
 */
void
UnsyncedRpcTracker::flushSession(Transport::Session* sessionPtr)
{
    Lock lock(mutex);
    Master* master;
    MasterMap::iterator it = masters.find(sessionPtr);
    if (it != masters.end()) {
        master = it->second;
    } else {
        return;
    }

    LOG(NOTICE, "Flushing session in UnsyncedRpcTracker. Total retries: %d",
                static_cast<int>(master->rpcs.size()));
    while (!master->rpcs.empty()) {
        uint64_t tableId = master->rpcs.front().tableId;
        uint64_t keyHash = master->rpcs.front().keyHash;
        ClientRequest request = master->rpcs.front().request;
        // TODO(seojin): fire in parallel?
        RetryUnsyncedRpc retryRpc(ramcloud->clientContext,
                                  tableId, keyHash, request);
        retryRpc.wait();

        // TODO(seojin): package them in a single function or destructor?
        // Problem: not sure the destructor will be called on std::stack::pop().
        UnsyncedRpc& rpc = master->rpcs.front();
        rpc.callback();
        ramcloud->rpcRequestPool->free(rpc.request.data);
        master->rpcs.pop();
    }
    LOG(NOTICE, "Done with sending retries for unsynced RPCs.");
}

/**
 * Garbage collect RPC information for requests whose updates are made durable
 * and invoke callbacks for those requests.
 *
 * \param sessionPtr
 *      Session which represents a target master.
 * \param masterLogState
 *      Master's log state including the master's log position up to which
 *      all log is replicated to backups.
 */
void
UnsyncedRpcTracker::updateLogState(Transport::Session* sessionPtr,
                                   WireFormat::LogState masterLogState)
{
    Lock lock(mutex);
    Master* master;
    MasterMap::iterator it = masters.find(sessionPtr);
    if (it != masters.end()) {
        master = it->second;
    } else {
        return;
    }

    master->updateLogState(ramcloud, masterLogState);
}

/**
 * Check the liveness of the masters that has not been contacted recently.
 */
void
UnsyncedRpcTracker::pingMasterByTimeout()
{
    /*
    vector<ClientLease> victims;
    {
        Lock lock(mutex);

        // Sweep table and pick candidates.
        victims.reserve(Cleaner::maxIterPerPeriod / 10);

        ClientMap::iterator it;
        if (cleaner.nextClientToCheck) {
            it = clients.find(cleaner.nextClientToCheck);
        } else {
            it = clients.begin();
        }
        for (int i = 0; i < Cleaner::maxIterPerPeriod && it != clients.end();
                        //&& victims.size() < Cleaner::maxCheckPerPeriod;
             ++i, ++it) {
            Client* client = it->second;

            ClientLease lease = {it->first,
                                 client->leaseExpiration.getEncoded(),
                                 0};
            if (leaseValidator->needsValidation(lease)) {
                victims.push_back(lease);
            }
        }
        if (it == clients.end()) {
            cleaner.nextClientToCheck = 0;
        } else {
            cleaner.nextClientToCheck = it->first;
        }
    }

    // Check with coordinator whether the lease is expired.
    // And erase entry if the lease is expired.
    for (uint32_t i = 0; i < victims.size(); ++i) {
        Lock lock(mutex);
        // Do not clean if this client record is protected
        if (clients[victims[i].leaseId]->doNotRemove)
            continue;
        // Do not clean if there are RPCs still in progress for this client.
        if (clients[victims[i].leaseId]->numRpcsInProgress)
            continue;

        ClientLease lease = victims[i];
        if (leaseValidator->validate(lease, &lease)) {
            clients[victims[i].leaseId]->leaseExpiration =
                                            ClusterTime(lease.leaseExpiration);
        } else {
            TabletManager::Protector tp(tabletManager);
            if (tp.notReadyTabletExists()) {
                // Since there is a NOT_READY tablet (eg. recovery/migration),
                // we cannot garbage collect expired clients safely.
                // Both RpcResult entries and participant list entry need to be
                // recovered to make a correct GC decision, but with a tablet
                // currently NOT_READY, it is possible to have only RpcResult
                // recovered, not Transaction ParticipantList entry yet.
                return;
            }
            // After preventing the start of tablet migration or recovery,
            // check SingleClientProtector once more before deletion.
            if (clients[victims[i].leaseId]->doNotRemove)
                continue;

            clients.erase(victims[i].leaseId);
        }
    }
     */
}

/**
 * Wait for backup replication of all changes made by this client up to now.
 */
void
UnsyncedRpcTracker::sync()
{
    Lock lock(mutex);
    for (MasterMap::iterator it = masters.begin(); it != masters.end(); ++it) {
        Master* master = it->second;
        if (!master->rpcs.empty()) {
            master->syncRpcHolder.construct(ramcloud->clientContext,
                                            master->session,
                                            master->lastestLogState);
        }
    }

    for (MasterMap::iterator it = masters.begin(); it != masters.end(); ++it) {
        Master* master = it->second;
        if (master->syncRpcHolder) {
            WireFormat::LogState newLogState;
            master->syncRpcHolder->wait(&newLogState);
            master->syncRpcHolder.destroy();
            master->updateLogState(ramcloud, newLogState);
        }
    }
}

/**
 * Return a pointer to the requested client record; create a new record if
 * one does not already exist.
 *
 * \param session
 *      The boost_intrusive pointer to transport session
 * \return
 *      Pointer to the existing or newly inserted master record.
 */
UnsyncedRpcTracker::Master*
UnsyncedRpcTracker::getOrInitMasterRecord(Transport::SessionRef& session)
{
    Master* master = NULL;
    Transport::Session* sessionPtr = session.get();
    MasterMap::iterator it = masters.find(sessionPtr);
    if (it != masters.end()) {
        master = it->second;
    } else {
        master = new Master(session);
        masters[sessionPtr] = master;
    }
    assert(master != NULL);
    return master;
}

/////////////////////////////////////////
// Sync RPC
/////////////////////////////////////////

/**
 * Construct and sends SyncRpc.
 * \param context
 *      Client context. Mainly for dispatch during wait.
 * \param sessionToMaster
 *      Session reference to which ask for backup replication.
 * \param objPos
 *      Log position to which we wait for replications.
 */
UnsyncedRpcTracker::SyncRpc::SyncRpc(Context* context,
        Transport::SessionRef& sessionToMaster, LogState objPos)
    : RpcWrapper(sizeof(WireFormat::SyncLog::Response), NULL)
    , context(context)
{
    session = sessionToMaster;
    WireFormat::SyncLog::Request* reqHdr(
            allocHeader<WireFormat::SyncLog>());
    reqHdr->syncGoal = objPos;
    send();
}

/**
 * Wait for a Sync RPC to complete, and fetches new state of master's log.
 *
 * \param[out] newLogState
 *      If non-NULL, the up-to-date value of master's log state is returned.
 */
void
UnsyncedRpcTracker::SyncRpc::wait(LogState* newLogState)
{
    waitInternal(context->dispatch);
    const WireFormat::SyncLog::Response* respHdr(
            getResponseHeader<WireFormat::SyncLog>());

    if (newLogState != NULL)
        *newLogState = respHdr->logState;

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
}

/////////////////////////////////////////
// RetryUnsyncedRPC
/////////////////////////////////////////

/**
 * Constructs and sends retry of an RPC.
 *
 * \param context
 *      RAMCloud client context.
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      Key hash that identifies a particular tablet.
 * \param requestToRetry
 */
UnsyncedRpcTracker::RetryUnsyncedRpc::RetryUnsyncedRpc(Context* context,
        uint64_t tableId, uint64_t keyHash, ClientRequest requestToRetry)
    : ObjectRpcWrapper(context, tableId, keyHash,
                       sizeof(WireFormat::ResponseCommon), NULL)
{
    rawRequest = requestToRetry;
    WireFormat::AsyncRequestCommon* common =
            reinterpret_cast<WireFormat::AsyncRequestCommon*>(rawRequest.data);

    // Set flag of retry, so that master will process it during recovery.
    common->asyncType = WireFormat::Asynchrony::RETRY;

    request.appendExternal(rawRequest.data, rawRequest.size);
    send();
}

/////////////////////////////////////////
// Master
/////////////////////////////////////////

/**
 * Update the saved log state if the given one is newer, and
 * garbage collect RPC information for requests whose updates are made durable
 * and invoke callbacks for those requests.
 *
 * \param ramcloud
 *      Pointer to RamCloud instance. Used to find rpcRequestPool.
 * \param newLogState
 *      Master's log state including the master's log position up to which
 *      all log is replicated to backups.
 */
void
UnsyncedRpcTracker::Master::updateLogState(RamCloud* ramcloud,
                                           LogState newLogState)
{
    if (lastestLogState < newLogState) {
        lastestLogState = newLogState;
    }

    while (!rpcs.empty()) {
        UnsyncedRpc& rpc = rpcs.front();
        if (!rpc.logPosition.isSynced(newLogState)) {
            break;
        }
        rpc.callback();
        ramcloud->rpcRequestPool->free(rpc.request.data);
        rpcs.pop();
    }
}

} // namespace RAMCloud
