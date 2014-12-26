/* Copyright (c) 2014 Stanford University
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

#include "UnackedRpcResults.h"
#include "MasterService.h"

//TODO(seojin): Make a single variable for this.
// copied from LeaseManager.cc
const uint64_t LEASE_TERM_US = 300*1e6;      // 5 min = 300,000,000 us

namespace RAMCloud {

/**
 * Default constructor
 */
UnackedRpcResults::UnackedRpcResults(Context* context)
    : clients(20)
    , mutex()
    , cleaner(context, this)
    , clientsSweepingInProgress(false)
    , cv_clients()
    , default_rpclist_size(5)
{}

/**
 * Default destructor
 */
UnackedRpcResults::~UnackedRpcResults()
{
    for (ClientMap::iterator it = clients.begin(); it != clients.end(); ++it) {
        Client* client = it->second;
        delete client;
    }
}

/**
 * Start background timer to perform cleaning clients entry with expired lease.
 */
void
UnackedRpcResults::startCleaner()
{
    cleaner.start(0);
}

/**
 * Check whether a particular rpc id has been received in this master
 * previously. If so, returns information about that RPC. If not, creates
 * a new record indicating that the RPC is now being processed.
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC whose id number is rpcId.
 * \param leaseTerm
 *      Client's lease expiration time.
 * \param[out] result
 *      If the RPC already has been completed, the pointer to its result (the \a
 *      result value previously passed to #UnackedRpcResults::recordCompletion)
 *      will be stored here.
 *      If the RPC is still in progress or never started, NULL will be stored.
 *
 * \return
 *      True if the RPC with \a rpcId has been started previously.
 *      False if the \a rpcId has never been passed to this method
 *      for this client before.
 *
 * \throw StaleRpcException
 *      The rpc with \a rpcId is already acknowledged by client, so we should
 *      not have received this request in the first place.
 */
bool
UnackedRpcResults::checkDuplicate(uint64_t clientId,
                                  uint64_t rpcId,
                                  uint64_t ackId,
                                  uint64_t leaseTerm,
                                  void** result)
{
    std::unique_lock<std::mutex> lock(mutex);
    *result = NULL;
    Client* client;

    ClientMap::iterator it = clients.find(clientId);
    if (it == clients.end()) {
        bool rehashingOccurs = static_cast<double>(clients.size()) + 1.5 >=
                clients.max_load_factor() *
                    static_cast<double>(clients.bucket_count());
        if (rehashingOccurs && clientsSweepingInProgress) {
            while (clientsSweepingInProgress) {
                cv_clients.wait(lock);
            }
        }

        client = new Client(default_rpclist_size);
        clients[clientId] = client;
    } else {
        client = it->second;
    }

    //1. Update leaseTerm and ack.
    if (client->leaseTerm < leaseTerm)
        client->leaseTerm = leaseTerm;
    if (client->maxAckId < ackId)
        client->maxAckId = ackId;

    //2. Check if the same RPC has been started.
    //   There are four cases to handle.
    if (rpcId <= client->maxAckId) {
        //StaleRpc: rpc is already acknowledged.
        throw StaleRpcException(HERE);
    } else if (client->maxRpcId < rpcId) {
        //Beyond the window of maxAckId and maxRpcId.
        client->maxRpcId = rpcId;
        client->recordNewRpc(rpcId);
        return false;
    } else if (client->hasRecord(rpcId)) {
        //Inside the window duplicate rpc.
        *result = client->result(rpcId);
        return true;
    } else {
        //Inside the window new rpc.
        client->recordNewRpc(rpcId);
        return false;
    }
}

/**
 * Used during recovery to figure out whether to retain a log record containing
 * the results of an RPC. This function indicates whether we can tell from the
 * information seen so far that a given RPC's result need not be retained.
 * (the client has acknowledged receiving the result.)
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC with \a rpcId.
 *
 * \return
 *      True if the log record may be useful, or
 *      false if the log record is not needed anymore.
 */
bool
UnackedRpcResults::shouldRecover(uint64_t clientId,
                                 uint64_t rpcId,
                                 uint64_t ackId)
{
    Lock lock(mutex);
    ClientMap::iterator it;
    it = clients.find(clientId);
    if (it == clients.end()) {
        clients[clientId] = new Client(default_rpclist_size);
        it = clients.find(clientId);
    }
    Client* client = it->second;
    if (client->maxAckId < ackId)
        client->maxAckId = ackId;

    return rpcId > client->maxAckId;
}

/**
 * Records the completed RPC's result.
 * After a linearizable RPC completes, this function should be invoked.
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      Id of the RPC that just completed.
 * \param result
 *      The pointer to the result of rpc in log.
 */
void
UnackedRpcResults::recordCompletion(uint64_t clientId,
                                    uint64_t rpcId,
                                    void* result)
{
    Lock lock(mutex);
    ClientMap::iterator it = clients.find(clientId);
    assert(it != clients.end());
    Client* client = it->second;

    assert(client->maxAckId < rpcId);
    if (client->maxRpcId < rpcId) {
        client->maxRpcId = rpcId;
    }

    client->updateResult(rpcId, result);
}

/**
 * Recover a record of an RPC from RpcRecord log entry.
 * It may insert a new clientId to #clients. (Protected with concurrent GC.)
 * The leaseTerm is not provided and fetched from coordinator lazily while
 * servicing an RPC from same client or during GC of cleanByTimeout().
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC whose id number is rpcId.
 * \param result
 *      The reference to log entry of RpcRecord.
 */
void
UnackedRpcResults::recoverRecord(uint64_t clientId,
                                 uint64_t rpcId,
                                 uint64_t ackId,
                                 void* result)
{
    std::unique_lock<std::mutex> lock(mutex);
    Client* client;

    ClientMap::iterator it = clients.find(clientId);
    if (it == clients.end()) {
        bool rehashingOccurs = static_cast<double>(clients.size()) + 1.5 >=
                clients.max_load_factor() *
                    static_cast<double>(clients.bucket_count());
        if (rehashingOccurs && clientsSweepingInProgress) {
            while (clientsSweepingInProgress) {
                cv_clients.wait(lock);
            }
        }

        client = new Client(default_rpclist_size);
        clients[clientId] = client;
    } else {
        client = it->second;
    }

    //1. Handle Ack.
    if (client->maxAckId < ackId)
        client->maxAckId = ackId;

    //2. Check if the same RPC has been started.
    //   There are four cases to handle.
    if (rpcId <= client->maxAckId) {
        //rpc is already acknowledged. No need to retain record.
    } else if (client->maxRpcId < rpcId) {
        //Beyond the window of maxAckId and maxRpcId.
        client->maxRpcId = rpcId;
        client->recordNewRpc(rpcId);
        client->updateResult(rpcId, result);
    } else if (client->hasRecord(rpcId)) {
        LOG(WARNING, "Duplicate RpcRecord found during recovery.");
    } else {
        //Inside the window new rpc.
        client->recordNewRpc(rpcId);
        client->updateResult(rpcId, result);
    }
}

/**
 * This method indicates whether the client has acknowledged receiving
 * the result from a given RPC. This should be used by LogCleaner to check
 * whether it should remove a log element or copy to new location.
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 *
 * \return
 *      True if the RPC is already acknowledged and safe to clean its record.
 *      False if the RPC record is still valid and need to be kept.
 */
bool
UnackedRpcResults::isRpcAcked(uint64_t clientId, uint64_t rpcId)
{
    Lock lock(mutex);
    ClientMap::iterator it;
    it = clients.find(clientId);
    if (it == clients.end()) {
        return true;
    } else {
        Client* client = it->second;
        return client->maxAckId >= rpcId;
    }
}

/**
 * Clean up stale clients who haven't communicated long.
 * Should not concurrently run this function in several threads.
 * Serialized by Cleaner inherited from WorkerTimer.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 */
void
UnackedRpcResults::cleanByTimeout(Context* context)
{
    {
        Lock lock(mutex);
        clientsSweepingInProgress = true;
    }

    // Sweep table and pick candidates.
    vector<uint64_t> victims;
    victims.reserve(Cleaner::maxCheckPerPeriod);

    uint64_t clusterTime = context->masterService->clusterTime;

    ClientMap::iterator it;
    for (it = clients.begin();
         it != clients.end() && victims.size() < Cleaner::maxCheckPerPeriod;
         ++it) {
        Client* client = it->second;
        Lock lock(mutex); //Not needed if we use atomic for leaseTerm.
                          //Or fine grained locking per client.
        if (client->leaseTerm <= clusterTime) {
            victims.push_back(it->first);
        }
    }

    {
        Lock lock(mutex);
        clientsSweepingInProgress = false;
    }

    cv_clients.notify_all();

    // Check with coordinator whether the lease is expired.
    // And erase entry if the lease is expired.
    uint64_t maxClusterTime = 0;
    for (uint32_t i = 0; i < victims.size(); ++i) {
        WireFormat::ClientLease lease =
            CoordinatorClient::getLeaseInfo(context, victims[i]);
        if (lease.leaseId == 0) {
            Lock lock(mutex);
            clients.erase(victims[i]);
        } else {
            Lock lock(mutex);
            clients[victims[i]]->leaseTerm = lease.leaseTerm;
        }

        if (maxClusterTime < lease.timestamp)
            maxClusterTime = lease.timestamp;
    }

    if (maxClusterTime)
        context->masterService->updateClusterTime(maxClusterTime);
}

/**
 * Constructor for the UnackedRpcResults' Cleaner.
 *
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      the dispatcher.
 * \param unackedRpcResults
 *      Provides access to the unackedRpcResults to perform cleaning.
 */
UnackedRpcResults::Cleaner::Cleaner(Context* context,
                                    UnackedRpcResults* unackedRpcResults)
    : WorkerTimer(context->dispatch)
    , context(context)
    , unackedRpcResults(unackedRpcResults)
{
}

/**
 * This handler performs a cleaning pass on UnackedRpcResults.
 */
void
UnackedRpcResults::Cleaner::handleTimerEvent()
{

    unackedRpcResults->cleanByTimeout(context);

    // Run once per 1/10 of lease term to keep expected garbage ~ 5%.
    this->start(Cycles::rdtsc()+Cycles::fromNanoseconds(LEASE_TERM_US * 100));
}

/**
 * Check Client's internal data structure holds any record of an outstanding
 * RPC with \a rpcId.
 *
 * \param rpcId
 *      The id of Rpc.
 *
 * \return
 *      True if there is a record for rpc with rpcId. False otherwise.
 */
bool
UnackedRpcResults::Client::hasRecord(uint64_t rpcId) {
    return rpcs[rpcId % len].id == rpcId;
}

/**
 * Gets the previously recorded result of an rpc.
 *
 * \param rpcId
 *      The id of an Rpc.
 *
 * \return
 *      The pointer to the result of the RPC.
 */
void*
UnackedRpcResults::Client::result(uint64_t rpcId) {
    assert(rpcs[rpcId % len].id == rpcId);
    return rpcs[rpcId % len].result;
}

/**
 * Mark the start of processing a new RPC on Client's internal data strucutre.
 *
 * \param rpcId
 *      The id of new Rpc.
 */
void
UnackedRpcResults::Client::recordNewRpc(uint64_t rpcId) {
    if (rpcs[rpcId % len].id > maxAckId)
        resizeRpcs(10);
    rpcs[rpcId % len] = UnackedRpc(rpcId, NULL);
}

/**
 * Record result of rpc on Client's internal data structure.
 * Caller of this function must make sure #Client::recordNewRpc was called
 * with \a rpcId.
 *
 * \param rpcId
 *      The id of Rpc.
 * \param result
 *      New pointer to result of the Rpc.
 */
void
UnackedRpcResults::Client::updateResult(uint64_t rpcId, void* result) {
    assert(rpcs[rpcId % len].id == rpcId);
    rpcs[rpcId % len].result = result;
}

/**
 * Resizes Client's rpcs size. This function is called when the size of
 * Client's rpcs is not enough to keep all valid RPCs' info.
 *
 * \param increment
 *      The increment of the size of the dynamic array, #Client::rpcs
 */
void
UnackedRpcResults::Client::resizeRpcs(int increment) {
    int newLen = len + increment;
    UnackedRpc* to = new UnackedRpc[newLen](); //initialize with <0, NULL>

    for (int i = 0; i < len; ++i) {
        if (rpcs[i].id <= maxAckId)
            continue;
        to[rpcs[i].id % newLen] = rpcs[i];
    }
    delete rpcs;
    rpcs = to;
    len = newLen;
}

} // namespace RAMCloud
