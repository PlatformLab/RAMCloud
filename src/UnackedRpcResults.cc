/* Copyright (c) 2014-2016 Stanford University
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
 * Default destructor. If the RPC request was not duplicate and some client
 * exception is thrown before recordCompletion call, it resets the entry in
 * UnackedRpcResults.
 */
UnackedRpcHandle::~UnackedRpcHandle()
{
    if (!isDuplicate() && isInProgress()) {
        // Remove the record of this RPC in UnackedRpcResults
        rpcResults->resetRecord(clientId, rpcId);
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
 * Save a log location of RPC result and update UnackedRpcResults.
 *
 * \param   result log location of RPC result.
 */
void
UnackedRpcHandle::recordCompletion(uint64_t result)
{
    resultPtr = result;
    // Record the saved RpcResult pointer.
    rpcResults->recordCompletion(clientId, rpcId,
                                 reinterpret_cast<void*>(resultPtr));
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
 * \param tabletManager
 *      Pointer to the TabletManager which will tell if any tablet is NOT_READY.
 */
UnackedRpcResults::UnackedRpcResults(Context* context,
                                     AbstractLog::ReferenceFreer* freer,
                                     ClientLeaseValidator* leaseValidator,
                                     TabletManager* tabletManager)
    : clients(20)
    , mutex()
    , default_rpclist_size(50)
    , context(context)
    , leaseValidator(leaseValidator)
    , cleaner(this)
    , freer(freer)
    , tabletManager(tabletManager)
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
    bool isDuplicate = false;

    uint64_t clientId = clientLease.leaseId;

    Client* client = getOrInitClientRecord(clientId, lock);

    // Update lease with more up-to-date information if available to avoid
    // unnecessary lease validation.
    if (ClusterTime(clientLease.leaseExpiration) < client->leaseExpiration) {
        clientLease.leaseExpiration = client->leaseExpiration.getEncoded();
    }

    // Check if the same RPC has been started.
    // There are four cases to handle.
    if (rpcId <= client->maxAckId) {
        //StaleRpc: rpc is already acknowledged.
        throw StaleRpcException(HERE);
    } else if (client->maxRpcId < rpcId) {
        // This is a new RPC that we are seeing for the first time.
        // Make sure the lease is valid before accepting the new request.
        if (!leaseValidator->validate(clientLease, &clientLease)) {
            throw ExpiredLeaseException(HERE);
        }
        client->maxRpcId = rpcId;
        client->recordNewRpc(rpcId);
        client->numRpcsInProgress++;
        isDuplicate = false;
    } else if (client->hasRecord(rpcId)) {
        // We have already seen this RPC before (but it may or may not have
        // finished processing). Return the result from the prior invocation,
        // if there is one.
        *resultPtrOut = client->result(rpcId);
        isDuplicate = true;
    } else {
        // This is a new RPC that we are seeing for the first time, but RPC was
        // arrived out-of-order.
        // Make sure the lease is valid before accepting the new request.
        if (!leaseValidator->validate(clientLease, &clientLease)) {
            throw ExpiredLeaseException(HERE);
        }
        client->recordNewRpc(rpcId);
        client->numRpcsInProgress++;
        isDuplicate = false;
    }

    // Update leaseExpiration and ack for future reference.
    ClusterTime leaseExpiration(clientLease.leaseExpiration);
    if (client->leaseExpiration < leaseExpiration) {
        client->leaseExpiration = leaseExpiration;
    }
    if (client->maxAckId < ackId) {
         client->processAck(ackId, freer);
    }

    return isDuplicate;
}

/**
 * Used during recovery to determine whether to retain a log record containing
 * either the results of an RPC or a transaction participant list.
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC with \a rpcId.
 * \param entryType
 *      The log entry type of the record in question. Used for better logging.
 *
 * \return
 *      True if the log record may be useful, or
 *      false if the log record is not needed anymore.
 */
bool
UnackedRpcResults::shouldRecover(uint64_t clientId,
                                 uint64_t rpcId,
                                 uint64_t ackId,
                                 LogEntryType entryType)
{
    Lock lock(mutex);
    Client* client = getOrInitClientRecord(clientId, lock);
    if (client->maxAckId < ackId)
        client->processAck(ackId, freer);

    if (client->hasRecord(rpcId)) {
        RAMCLOUD_CLOG(WARNING,
                "Duplicate %s found during recovery. <clientID, rpcID, ackId> "
                "= <%" PRIu64 ", " "%" PRIu64 ", " "%" PRIu64 ">",
                LogEntryTypeHelpers::toString(entryType),
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
    Client* client = getClientRecord(clientId, lock);
    if (ignoreIfAcked && client == NULL) {
        return;
    }

    assert(client != NULL);

    if (ignoreIfAcked && client->maxAckId >= rpcId) {
        return;
    }
    assert(client->maxRpcId >= rpcId);

    // If a client cancels an RPC, the current RPC may have been acked while
    // being processed. In this case, we can just delete the completion record.
    if (client->maxAckId < rpcId) {
        client->updateResult(rpcId, result);
    }
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
    Lock lock(mutex);
    Client* client = getOrInitClientRecord(clientId, lock);

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
    Lock lock(mutex);
    Client* client = getClientRecord(clientId, lock);

    if (client == NULL) {
        return;
    }

    // If a client cancels an RPC, the current RPC may have been acked while
    // being processed. In this case, we should not overwrite.
    if (client->maxAckId < rpcId) {
        client->rpcs[rpcId % client->len].id = 0;
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
    Client* client = getClientRecord(clientId, lock);
    if (client == NULL) {
        return true;
    } else {
        return client->maxAckId >= rpcId;
    }
}

/**
 * Construct to prevent a specific client's record from being removed.
 *
 * \param unackedRpcResults
 *      The unackedRpcResults that owns the client's record.
 * \param clientId
 *      The id of the client whose record is to be saved.
 */
UnackedRpcResults::SingleClientProtector::SingleClientProtector(
        UnackedRpcResults* unackedRpcResults,
        uint64_t clientId)
    : unackedRpcResults(unackedRpcResults)
    , clientId(clientId)
{
    Lock lock(unackedRpcResults->mutex);
    // Make a new client record if it doesn't exist.
    Client* client = unackedRpcResults->getOrInitClientRecord(clientId, lock);
    ++client->doNotRemove;
}

/**
 * Destruct to allow cleaning to resume on the client records previously
 * protected by this objects the construction.
 */
UnackedRpcResults::SingleClientProtector::~SingleClientProtector()
{
    Lock lock(unackedRpcResults->mutex);
    Client* client = unackedRpcResults->getClientRecord(clientId, lock);
    assert(client != NULL);
    --client->doNotRemove;
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
    if (rpcs[rpcId % len].id > maxAckId) {
        uint64_t difference = (rpcId > rpcs[rpcId % len].id) ?
                                rpcId - rpcs[rpcId % len].id :
                                rpcs[rpcId % len].id - rpcId;
        resizeRpcs(static_cast<int>(difference * 2));
    }
    assert(rpcs[rpcId % len].id <= maxAckId);
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
 * \param newLen
 *      The new size of the dynamic array, #Client::rpcs
 */
void
UnackedRpcResults::Client::resizeRpcs(int newLen) {
    UnackedRpc* to = new UnackedRpc[newLen](); //initialize with <0, NULL>

    for (int i = 0; i < len; ++i) {
        if (rpcs[i].id <= maxAckId)
            continue;
        assert(to[rpcs[i].id % newLen].id == 0);
        to[rpcs[i].id % newLen] = rpcs[i];
    }
    delete[] rpcs;
    rpcs = to;
    len = newLen;
}

/**
 * Return a pointer to the requested client record if it exists.
 *
 * \param clientId
 *      The id of the client whose record should be returned.
 * \param lock
 *      Used to ensure that caller has acquired UnackedRpcResults::mutex.
 *      Not actually used by the method.
 * \return
 *      Pointer to the client record if one exists; NULL otherwise.
 */
UnackedRpcResults::Client*
UnackedRpcResults::getClientRecord(uint64_t clientId, Lock& lock)
{
    Client* client = NULL;
    ClientMap::iterator it = clients.find(clientId);
    if (it != clients.end()) {
        client = it->second;
    }
    return client;
}

/**
 * Return a pointer to the requested client record; create a new record if
 * one does not already exist.
 *
 * \param clientId
 *      The id of the client whose record should be returned.
 * \param lock
 *      Used to ensure that caller has acquired UnackedRpcResults::mutex.
 *      Not actually used by the method.
 * \return
 *      Pointer to the existing or newly inserted client record.
 */
UnackedRpcResults::Client*
UnackedRpcResults::getOrInitClientRecord(uint64_t clientId, Lock& lock)
{
    Client* client = NULL;
    ClientMap::iterator it = clients.find(clientId);
    if (it != clients.end()) {
        client = it->second;
    } else {
        client = new Client(default_rpclist_size);
        clients[clientId] = client;
    }
    assert(client != NULL);
    return client;
}

} // namespace RAMCloud
