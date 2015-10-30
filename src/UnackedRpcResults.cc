/* Copyright (c) 2014-2015 Stanford University
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
#include "LeaseCommon.h"
#include "MasterService.h"

namespace RAMCloud {

/**
 * Default constructor. Performs a #UnackedRpcResults::checkDuplicate()
 * and the outcome of checkDuplicate is saved to be retrieved later.
 *
 * \param unackedRpcResults
 *      Pointer to UnackedRpcResults instance to operate on.
 * \param clientLease
 *      The client lease associated with this RPC.  Contain the client lease id,
 *      the lease expiration time.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC whose id number is rpcId.
 *
 * \throw ExpiredLeaseException
 *      The lease for \a clientId is already expired in coordinator.
 *      Master rejects rpcs with expired lease, and client must obtain
 *      brand-new lease from coordinator.
 * \throw StaleRpcException
 *      The rpc with \a rpcId is already acknowledged by client, so we should
 *      not have received this request in the first place.
 */
UnackedRpcHandle::UnackedRpcHandle(
        UnackedRpcResults* unackedRpcResults,
        WireFormat::ClientLease clientLease,
        uint64_t rpcId,
        uint64_t ackId)
    : clientId(clientLease.leaseId)
    , rpcId(rpcId)
    , duplicate(false)
    , resultPtr(0)
    , rpcResults(unackedRpcResults)
{
    void* result;
    duplicate = rpcResults->checkDuplicate(clientLease, rpcId, ackId, &result);
    resultPtr = reinterpret_cast<uint64_t>(result);
}

/**
 * Copy constructor.
 */
UnackedRpcHandle::UnackedRpcHandle(const UnackedRpcHandle& origin)
    : clientId(origin.clientId)
    , rpcId(origin.rpcId)
    , duplicate(origin.duplicate)
    , resultPtr(origin.resultPtr)
    , rpcResults(origin.rpcResults)
{
}

/**
 * Copy assignment.
 */
UnackedRpcHandle&
UnackedRpcHandle::operator=(const UnackedRpcHandle& origin)
{
    clientId = origin.clientId;
    rpcId = origin.rpcId;
    duplicate = origin.duplicate;
    resultPtr = origin.resultPtr;
    rpcResults = origin.rpcResults;
    return *this;
}

/**
 * Default destructor. If the RPC request was not duplicate, it either reset
 * the entry in UnackedRpcResults (if some client exception is thrown before
 * recordCompletion call) or change RPC status from in progress to completed.
 */
UnackedRpcHandle::~UnackedRpcHandle()
{
    if (!isDuplicate()) {
        if (isInProgress()) {
            // Remove the record of this RPC in UnackedRpcResults
            rpcResults->resetRecord(clientId, rpcId);
        } else {
            // Record the saved RpcResult pointer.
            rpcResults->recordCompletion(clientId, rpcId,
                                         reinterpret_cast<void*>(resultPtr));
        }
    }
}

/**
 * Queries the outcome of checkDuplicate in constructor.
 *
 * \return true if RPC request is duplicate.
 */
bool
UnackedRpcHandle::isDuplicate()
{
    return duplicate;
}

/**
 * Queries whether RPC is in progress state. It may indicate information
 * obtained from checkDuplicate call or from recordCompletion of current handle.
 *
 * \return true if RPC request is in progress.
 */
bool
UnackedRpcHandle::isInProgress()
{
    return resultPtr == 0;
}

/**
 * Getter method for log location of RPC result.
 *
 * \return  log location of RPC result.
 */
uint64_t
UnackedRpcHandle::resultLoc()
{
    return resultPtr;
}

/**
 * Save a log location of RPC result to update UnackedRpcResults later in
 * destructor.
 *
 * \param   result log location of RPC result.
 */
void
UnackedRpcHandle::recordCompletion(uint64_t result)
{
    resultPtr = result;
}

/**
 * Default constructor
 *
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      the dispatcher and masterService.
 * \param freer
 *      Pointer to reference freer. During garbage collection by ackId,
 *      this freer should be used to designate specific log entry as cleanable.
 * \param leaseValidator
 *      Allows this module to determine if a given lease is still valid.
 */
UnackedRpcResults::UnackedRpcResults(Context* context,
                                     AbstractLog::ReferenceFreer* freer,
                                     ClientLeaseValidator* leaseValidator)
    : clients(20)
    , mutex()
    , default_rpclist_size(50)
    , context(context)
    , leaseValidator(leaseValidator)
    , cleaner(this)
    , freer(freer)
{
}

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
 * Resets log entry freer.
 */
void
UnackedRpcResults::resetFreer(AbstractLog::ReferenceFreer* freer)
{
    freer = freer;
}

/**
 * Check whether a particular rpc id has been received in this master
 * previously. If so, returns information about that RPC. If not, creates
 * a new record indicating that the RPC is now being processed.
 *
 * \param clientLease
 *      The client lease associated with this RPC.  Contain the client lease id,
 *      the lease expiration time.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC whose id number is rpcId.
 * \param[out] resultPtrOut
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
 * \throw ExpiredLeaseException
 *      The lease for \a clientId is already expired in coordinator.
 *      Master rejects rpcs with expired lease, and client must obtain
 *      brand-new lease from coordinator.
 * \throw StaleRpcException
 *      The rpc with \a rpcId is already acknowledged by client, so we should
 *      not have received this request in the first place.
 */
bool
UnackedRpcResults::checkDuplicate(WireFormat::ClientLease clientLease,
                                  uint64_t rpcId,
                                  uint64_t ackId,
                                  void** resultPtrOut)
{
    Lock lock(mutex);
    *resultPtrOut = NULL;
    Client* client;

    uint64_t clientId = clientLease.leaseId;

    ClientMap::iterator it = clients.find(clientId);
    if (it == clients.end()) {
        client = new Client(default_rpclist_size);
        clients[clientId] = client;
    } else {
        client = it->second;
    }

    // Update lease with more up-to-date information if available to avoid
    // unnecessary lease validation.
    if (ClusterTime(clientLease.leaseExpiration) < client->leaseExpiration) {
        clientLease.leaseExpiration = client->leaseExpiration.getEncoded();
    }
    // Make sure lease is valid.
    if (!leaseValidator->validate(clientLease, &clientLease)) {
        throw ExpiredLeaseException(HERE);
    }

    ClusterTime leaseExpiration(clientLease.leaseExpiration);

    //1. Update leaseExpiration and ack.
    if (client->leaseExpiration < leaseExpiration)
        client->leaseExpiration = leaseExpiration;
    if (client->maxAckId < ackId) {
        client->processAck(ackId, freer);
    }

    //2. Check if the same RPC has been started.
    //   There are four cases to handle.
    if (rpcId <= client->maxAckId) {
        //StaleRpc: rpc is already acknowledged.
        throw StaleRpcException(HERE);
    } else if (client->maxRpcId < rpcId) {
        // This is a new RPC that we are seeing for the first time.
        client->maxRpcId = rpcId;
        client->recordNewRpc(rpcId);
        client->numRpcsInProgress++;
        return false;
    } else if (client->hasRecord(rpcId)) {
        // We have already seen this RPC before (but it may or may not have
        // finished processing). Return the result from the prior invocation,
        // if there is one.
        *resultPtrOut = client->result(rpcId);
        return true;
    } else {
        // This is a new RPC that we are seeing for the first time, but RPC was
        // arrived out-of-order.
        client->recordNewRpc(rpcId);
        client->numRpcsInProgress++;
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
        client->processAck(ackId, freer);

    if (client->hasRecord(rpcId)) {
        LOG(WARNING,
                "Duplicate RpcResult or ParticipantList found during recovery. "
                "<clientID, rpcID, ackId> = <"
                "%" PRIu64 ", " "%" PRIu64 ", " "%" PRIu64 ">",
                clientId, rpcId, ackId);
        return false;
    }

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
 * \param ignoreIfAcked
 *      If true, no-op if RPC is already acked instead of assertion failure.
 *      This flag is used to prevent error during relocation by log cleaner.
 */
void
UnackedRpcResults::recordCompletion(uint64_t clientId,
                                      uint64_t rpcId,
                                      void* result,
                                      bool ignoreIfAcked)
{
    Lock lock(mutex);
    ClientMap::iterator it = clients.find(clientId);
    if (ignoreIfAcked && it == clients.end()) {
        return;
    }
    assert(it != clients.end());
    Client* client = it->second;

    if (ignoreIfAcked && client->maxAckId >= rpcId) {
        return;
    }
    assert(client->maxAckId < rpcId);
    assert(client->maxRpcId >= rpcId);

    client->updateResult(rpcId, result);
    client->numRpcsInProgress--;
}

/**
 * Recover a record of an RPC from RpcResult log entry.
 * It may insert a new clientId to #clients. (Protected with concurrent GC.)
 * The leaseExpiration is not provided and fetched from coordinator lazily while
 * servicing an RPC from same client or during GC of cleanByTimeout().
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC whose id number is rpcId.
 * \param result
 *      The reference to log entry of RpcResult.
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
        client = new Client(default_rpclist_size);
        clients[clientId] = client;
    } else {
        client = it->second;
    }

    //1. Handle Ack.
    if (client->maxAckId < ackId)
        client->processAck(ackId, freer);

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
        LOG(WARNING,
                "Duplicate RpcResult or ParticipantList found during recovery. "
                "<clientID, rpcID, ackId> = <"
                "%" PRIu64 ", " "%" PRIu64 ", " "%" PRIu64 ">",
                clientId, rpcId, ackId);
    } else {
        //Inside the window new rpc.
        client->recordNewRpc(rpcId);
        client->updateResult(rpcId, result);
    }
}

void
UnackedRpcResults::resetRecord(uint64_t clientId, uint64_t rpcId)
{
    std::unique_lock<std::mutex> lock(mutex);
    Client* client;

    ClientMap::iterator it = clients.find(clientId);
    if (it == clients.end()) {
        return;
    } else {
        client = it->second;
    }
    client->rpcs[rpcId % client->len].id = 0;
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
 */
void
UnackedRpcResults::cleanByTimeout()
{
    vector<ClientLease> victims;
    {
        Lock lock(mutex);

        // Sweep table and pick candidates.
        victims.reserve(Cleaner::maxCheckPerPeriod);

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
    }

    // Check with coordinator whether the lease is expired.
    // And erase entry if the lease is expired.
    ClusterTime maxClusterTime;
    for (uint32_t i = 0; i < victims.size(); ++i) {
        Lock lock(mutex);
        if (clients[victims[i].leaseId]->numRpcsInProgress)
            continue;

        ClientLease lease = victims[i];
        if (leaseValidator->validate(lease, &lease)) {
            clients[victims[i].leaseId]->leaseExpiration =
                                            ClusterTime(lease.leaseExpiration);
        } else {
            clients.erase(victims[i].leaseId);
        }
    }
}

/**
 * Returns true if a record exists for the given client id and RPC id; false,
 * otherwise.  This method is used only for unit testing.
 */
bool
UnackedRpcResults::hasRecord(uint64_t clientId, uint64_t rpcId) {
    Client* client;
    ClientMap::iterator it = clients.find(clientId);
    if (it == clients.end()) {
        return false;
    }

    client = it->second;

    if (rpcId <= client->maxAckId) {
        return false;
    }

    return client->hasRecord(rpcId);
}

/**
 * Constructor for the UnackedRpcResults' Cleaner.
 *
 * \param unackedRpcResults
 *      Provides access to the unackedRpcResults to perform cleaning.
 */
UnackedRpcResults::Cleaner::Cleaner(UnackedRpcResults* unackedRpcResults)
    : WorkerTimer(unackedRpcResults->context->dispatch)
    , unackedRpcResults(unackedRpcResults)
    , nextClientToCheck(0)
{
}

/**
 * This handler performs a cleaning pass on UnackedRpcResults.
 */
void
UnackedRpcResults::Cleaner::handleTimerEvent()
{

    unackedRpcResults->cleanByTimeout();

    // Run once per 1/10 of lease term to keep expected garbage ~ 5%.
    this->start(Cycles::rdtsc()+Cycles::fromNanoseconds(
            LeaseCommon::LEASE_TERM.toNanoseconds() / 10));
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
 * Mark the start of processing a new RPC on Client's internal data structure.
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
 * Processes ack provided by the client. Frees log entries for RpcResult up to
 * ackId, and bumps maxAckId.
 * \param ackId
 *      The ack number transmitted with the RPC whose id number is rpcId.
 * \param freer
 *      Pointer to reference freer. This freer should be used to set
 *      log entries used for RpcResult as free.
 */
void
UnackedRpcResults::Client::processAck(uint64_t ackId,
                                      AbstractLog::ReferenceFreer* freer) {
    for (uint64_t id = maxAckId + 1; id <= ackId; id++) {
        if (rpcs[id % len].id == id) {
            uint64_t resPtr = reinterpret_cast<uint64_t>(rpcs[id % len].result);
            if (resPtr) {
                Log::Reference reference(resPtr);
                freer->freeLogEntry(reference);
            } else {
                LOG(WARNING, "client acked unfinished RPC with "
                             "rpcId <%" PRIu64 ">", id);
            }
        }
    }
    maxAckId = ackId;
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
    //int newLen = len + increment;
    int newLen = len * 2;
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
