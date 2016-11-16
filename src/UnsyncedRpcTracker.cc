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
#include "ObjectRpcWrapper.h"

namespace RAMCloud {

/**
 * Default constructor
 *
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      the dispatcher and masterService.
 */
UnsyncedRpcTracker::UnsyncedRpcTracker(Context* context)
    : masters()
    , mutex()
    , context(context)
{
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
                                    void* rpcRequest,
                                    uint64_t tableId,
                                    uint64_t keyHash,
                                    uint64_t objVer,
                                    WireFormat::LogPosition logPos,
                                    std::function<void()> callback)
{
    Lock lock(mutex);
    Master* master = getOrInitMasterRecord(session);
    master->rpcs.emplace(rpcRequest, tableId, keyHash, objVer, logPos,
                         callback);
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

    while (!master->rpcs.empty()) {
        uint64_t tableId = master->rpcs.front().tableId;
        uint64_t keyHash = master->rpcs.front().keyHash;
        void* request = master->rpcs.front().request;
        // TODO(seojin): fire in parallel?
        RetryUnsyncedRpc retryRpc(context, tableId, keyHash, request);
        retryRpc.wait();
    }
}

/**
 * Garbage collect RPC information for requests whose updates are made durable
 * and invoke callbacks for those requests.
 *
 * \param sessionPtr
 *      Session which represents a target master.
 * \param syncPoint
 *      Master's log location up to which all log is replicated to backups.
 */
void
UnsyncedRpcTracker::UpdateSyncPoint(Transport::Session* sessionPtr,
                                    WireFormat::LogPosition syncPoint)
{
    Lock lock(mutex);
    Master* master;
    MasterMap::iterator it = masters.find(sessionPtr);
    if (it != masters.end()) {
        master = it->second;
    } else {
        return;
    }

    while (!master->rpcs.empty()) {
        if (master->rpcs.front().logPosition > syncPoint) {
            break;
        }
        master->rpcs.front().callback();
        master->rpcs.pop();
    }
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

} // namespace RAMCloud
