/* Copyright (c) 2015 Stanford University
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

#include "ClientTransactionTask.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Constructor for a transaction task.
 *
 * \param ramcloud
 *      Overall information about the calling client.
 */
ClientTransactionTask::ClientTransactionTask(RamCloud* ramcloud)
    : ramcloud(ramcloud)
    , participantCount(0)
    , participantList()
    , state(INIT)
    , decision(WireFormat::TxDecision::UNDECIDED)
    , lease()
    , txId(0)
    , prepareRpcs()
    , decisionRpcs()
    , commitCache()
    , nextCacheEntry()
    , poller()
{
}

/**
 * Find and return the cache entry identified by the given key.
 *
 * \param key
 *      Key of the object contained in the cache entry that should be returned.
 * \return
 *      Returns a pointer to the cache entry if found.  Returns NULL otherwise.
 *      Pointer is invalid once the commitCache is modified.
 */
ClientTransactionTask::CacheEntry*
ClientTransactionTask::findCacheEntry(Key& key)
{
    CacheKey cacheKey = {key.getTableId(), key.getHash()};
    CommitCacheMap::iterator it = commitCache.lower_bound(cacheKey);
    CacheEntry* entry = NULL;

    while (it != commitCache.end()) {
        if (cacheKey < it->first) {
            break;
        } else if (it->second.objectBuf) {
            Key otherKey(it->first.tableId,
                         it->second.objectBuf->getKey(),
                         it->second.objectBuf->getKeyLength());
            if (key == otherKey) {
                entry = &it->second;
                break;
            }
        }
        it++;
    }
    return entry;
}

/**
 * Inserts a new cache entry with the provided key and value.  Other members
 * of the cache entry are left to their default values.  This method must not
 * be called once the transaction has started committing.
 *
 * \param key
 *      Key of the object to inserted into the cache.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 * \return
 *      Returns a pointer to the inserted cache entry.  Pointer is invalid
 *      once the commitCache is modified.
 */
ClientTransactionTask::CacheEntry*
ClientTransactionTask::insertCacheEntry(Key& key, const void* buf,
        uint32_t length)
{
    CacheKey cacheKey = {key.getTableId(), key.getHash()};
    CommitCacheMap::iterator it = commitCache.insert(
            std::make_pair(cacheKey, CacheEntry()));
    it->second.objectBuf = new ObjectBuffer();
    Object::appendKeysAndValueToBuffer(
            key, buf, length, it->second.objectBuf, true);
    return &it->second;
}

/**
 * Make incremental progress toward committing the transaction.  This method
 * is called during the poll loop when this task needs to make progress (i.e.
 * if the transaction is in the process of committing).
 * \return
 *      0 means no progress was made in this call; 1 means we did something
 *      useful.
 */
int
ClientTransactionTask::performTask()
{
    int foundWork = 0;
    try {
        if (state == INIT) {
            // Build participant list
            initTask();
            nextCacheEntry = commitCache.begin();
            state = PREPARE;
        }
        if (state == PREPARE) {
            foundWork |= sendPrepareRpc();
            foundWork |= processPrepareRpcResults();
            if (prepareRpcs.empty() && nextCacheEntry == commitCache.end()) {
                nextCacheEntry = commitCache.begin();
                if (decision != WireFormat::TxDecision::ABORT) {
                    decision = WireFormat::TxDecision::COMMIT;
                }
                state = DECISION;
            }
        }
        if (state == DECISION) {
            foundWork |= sendDecisionRpc();
            foundWork |= processDecisionRpcResults();
            if (decisionRpcs.empty() && nextCacheEntry == commitCache.end()) {
                ramcloud->rpcTracker.rpcFinished(txId);
                state = DONE;
            }
        }
    } catch (ClientException& e) {
        // If there are any unexpected problems with the commit protocol, STOP.
        // This shouldn't happen unless there is a bug.
        foundWork = 1;
        prepareRpcs.clear();
        decisionRpcs.clear();
        switch (state) {
            case INIT:
            case PREPARE:
                RAMCLOUD_LOG(ERROR,
                        "Unexpected exception '%s' while preparing "
                        "transaction commit; will result in internal error.",
                        statusToString(e.status));
                break;
            case DECISION:
                RAMCLOUD_LOG(WARNING,
                        "Unexpected exception '%s' while issuing transaction "
                        "decisions; likely recoverable.",
                        statusToString(e.status));
                break;
            default:
                RAMCLOUD_LOG(NOTICE,
                        "Unexpected exception '%s' after committing "
                        "transaction.",
                        statusToString(e.status));
        }
        ramcloud->rpcTracker.rpcFinished(txId);
        state = DONE;
    }
    return foundWork;
}

/**
 * Schedule a ClientTransactionTask to start executing the commit protocol.
 *
 * \param taskPtr
 *      Shared pointer to the ClientTransactionTask to be run.
 */
void
ClientTransactionTask::start(std::shared_ptr<ClientTransactionTask>& taskPtr)
{
    assert(taskPtr);
    ClientTransactionTask* task = taskPtr.get();
    if (!task->poller) {
        task->poller.construct(task->ramcloud->clientContext->dispatch,
                               taskPtr);
    }
}

/**
 * Construct a Poller causing a specified ClientTransactionTask to be run.
 *
 * \param dispatch
 *      Dispatch object through which the poller will be invoked.
 * \param taskPtr
 *      ClientTransactionTask to be run.
 */
ClientTransactionTask::Poller::Poller(Dispatch* dispatch,
        std::shared_ptr<ClientTransactionTask>& taskPtr)
    : Dispatch::Poller(dispatch, "ClientTransactionTask::Poller")
    , running(false)
    , taskPtr(taskPtr)
{}

/**
 * Drives the execution of the ClientTransactionTask's rules engine.
 */
int
ClientTransactionTask::Poller::poll()
{
    int foundWork = 0;

    // Make sure the recursive calls don't execute.
    if (!running) {
        running = true;
        ClientTransactionTask* task = taskPtr.get();
        foundWork |= task->performTask();
        running = false;

        // Destroy poller (self) if task is complete; must be last action.
        if (task->isReady()) {
            task->poller.destroy();
        }
    }
    return foundWork;
}

/**
 * Initialize all necessary values of the commit task in preparation for the
 * commit protocol.  This includes building the send-ready buffer of
 * participants to be included in every prepare rpc and also the allocation of
 * rpcIds.  Used in the commit method.  Factored out mostly for ease of testing.
 */
void
ClientTransactionTask::initTask()
{
    lease = ramcloud->clientLease.getLease();
    txId = ramcloud->rpcTracker.newRpcIdBlock(this, commitCache.size());

    nextCacheEntry = commitCache.begin();
    uint64_t i = 0;
    while (nextCacheEntry != commitCache.end()) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        entry->rpcId = txId + i++;
        participantList.emplaceAppend<WireFormat::TxParticipant>(
                key->tableId,
                static_cast<uint64_t>(key->keyHash),
                entry->rpcId);
        participantCount++;
        nextCacheEntry++;
    }
    assert(i == commitCache.size());
}

/**
 * Process any decision rpcs that have completed.  Used in performTask.
 * Factored out mostly for clarity and ease of testing.
 * \return
 *      0 means the method did some useful work; 0 means there was no
 *      work to do.
 */
int
ClientTransactionTask::processDecisionRpcResults()
{
    int foundWork = 0;

    // Process outstanding RPCs.
    std::list<DecisionRpc>::iterator it = decisionRpcs.begin();
    for (; it != decisionRpcs.end(); it++) {
        DecisionRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }
        foundWork = 1;

        try {
            rpc->wait();
            // At this point the decision must have been received successfully.
            // Nothing left to do.
            TEST_LOG("STATUS_OK");
        } catch (UnknownTabletException& e) {
            // Target server did not contain the requested tablet; the
            // operations should have been already marked for retry. Nothing
            // left to do.
            TEST_LOG("STATUS_UNKNOWN_TABLET");
        } catch (ServerNotUpException& e) {
            // If the target server is not up; the operations should have been
            // already marked for retry.  Nothing left to do.
            TEST_LOG("STATUS_SERVER_NOT_UP");
        }

        // Destroy object.
        it = decisionRpcs.erase(it);
    }
    return foundWork;
}

/**
 * Process any prepare rpcs that have completed.  Used in performTask.  Factored
 * out mostly for clarity and ease of testing.
 * \return
 *      0 means the method did some useful work; 0 means there was no
 *      work to do.
 */
int
ClientTransactionTask::processPrepareRpcResults()
{
    int foundWork = 0;

    // Process outstanding RPCs.
    std::list<PrepareRpc>::iterator it = prepareRpcs.begin();
    for (; it != prepareRpcs.end(); it++) {
        PrepareRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }
        foundWork = 1;

        try {
            if (rpc->wait() == WireFormat::TxPrepare::ABORT) {
                decision = WireFormat::TxDecision::ABORT;
            }
        } catch (UnknownTabletException& e) {
            // Target server did not contain the requested tablet; the
            // operations should have been already marked for retry. Nothing
            // left to do.
            TEST_LOG("STATUS_UNKNOWN_TABLET");
        } catch (ServerNotUpException& e) {
            // If the target server is not up; the operations should have been
            // already marked for retry.  Nothing left to do.
            TEST_LOG("STATUS_SERVER_NOT_UP");
        }

        // Destroy object.
        it = prepareRpcs.erase(it);
    }
    return foundWork;
}

/**
 * Send out a batch of un-sent decision notifications as a single DecisionRpc
 * if not all masters have been notified.  Used in performTask.  Factored out
 * mostly for clarity and ease of testing.
 * \return
 *      0 means the method did some useful work; 0 means there was no
 *      work to do.
 */
int
ClientTransactionTask::sendDecisionRpc()
{
    int foundWork = 0;
    DecisionRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextCacheEntry != commitCache.end(); nextCacheEntry++) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        // Skip the entry if the decision was already sent.  This might happen
        // when an RPC receives STATUS_RETRY and we need to look through all
        // the entries again looking for entries that have been marked PENDING
        // indicating the decisions need to be resent; entries not marked don't
        // need to be resent.
        if (entry->state == CacheEntry::DECIDE) {
            continue;
        }
        foundWork = 1;

        // Batch is done naively assuming that tables are partitioned across
        // servers into contiguous key-hash ranges (tablets).  The commit cache
        // is iterated in key-hash order batching together decisions
        // notifications that share a destination server.
        //
        // This naive approach behaves poorly if the table is highly sharded
        // resulting in poor batching.
        if (nextRpc == NULL) {
            rpcSession =
                    ramcloud->objectFinder.lookup(key->tableId,
                                                  key->keyHash);
            decisionRpcs.emplace_back(ramcloud, rpcSession, this);
            nextRpc = &decisionRpcs.back();
        }

        Transport::SessionRef session =
                ramcloud->objectFinder.lookup(key->tableId, key->keyHash);
        if (session->getServiceLocator() != rpcSession->getServiceLocator()
                || !nextRpc->appendOp(nextCacheEntry)) {
            break;
        }
    }
    if (nextRpc) {
        nextRpc->send();
    }
    return foundWork;
}

/**
 * Send out a batch of un-sent prepare requests in a single PrepareRpc if there
 * are remaining un-prepared transaction ops.  Used in performTask.  Factored
 * out mostly for clarity and ease of testing.
 * \return
 *      0 means the method did some useful work; 0 means there was no
 *      work to do.
 */
int
ClientTransactionTask::sendPrepareRpc()
{
    int foundWork = 0;
    PrepareRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextCacheEntry != commitCache.end(); nextCacheEntry++) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        // Skip the entry if the prepare was already sent.  This might happen
        // when an RPC receives STATUS_RETRY and we need to look through all
        // the entries again looking for entries that have been marked PENDING
        // indicating the prepares need to be resent; entries not marked don't
        // need to be resent.
        if (entry->state == CacheEntry::PREPARE) {
            continue;
        }
        foundWork = 1;

        // Batch is done naively assuming that tables are partitioned across
        // servers into contiguous key-hash ranges (tablets).  The commit cache
        // is iterated in key-hash order batching together prepare requests
        // that share a destination server.
        //
        // This naive approach behaves poorly if the table is highly sharded
        // resulting in poor batching.
        if (nextRpc == NULL) {
            rpcSession =
                    ramcloud->objectFinder.lookup(key->tableId,
                                                  key->keyHash);
            prepareRpcs.emplace_back(ramcloud, rpcSession, this);
            nextRpc = &prepareRpcs.back();
        }

        Transport::SessionRef session =
                ramcloud->objectFinder.lookup(key->tableId, key->keyHash);
        if (session->getServiceLocator() != rpcSession->getServiceLocator()
                || !nextRpc->appendOp(nextCacheEntry)) {
            break;
        }
    }
    if (nextRpc) {
        nextRpc->send();
    }
    return foundWork;
}

// See RpcTracker::TrackedRpc for documentation.
void ClientTransactionTask::tryFinish()
{
    // Making forward progress requires the follow:
    //  (1) Calling performTask
    //  (2) Allowing the transport to run by calling poll
    // This method would only be called if this task is active.  Since active
    // tasks are driven by their ClientTransactionTask::Poller (i.e. the poller
    // calls performTask if the tasks is active) and the poller runs in the poll
    // loop, it is sufficient to simply call poll.
    ramcloud->poll();
}

/**
 * Constructor for ClientTransactionRpcWrapper.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the transaction task that issued this request.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked by this class to ensure that
 *      they contain at least this much data, wrapper subclasses can
 *      use the getResponseHeader method to access the response header
 *      once isReady has returned true.
 */
ClientTransactionTask::ClientTransactionRpcWrapper::ClientTransactionRpcWrapper(
        RamCloud* ramcloud,
        Transport::SessionRef session,
        ClientTransactionTask* task,
        uint32_t responseHeaderLength)
    : RpcWrapper(responseHeaderLength)
    , ramcloud(ramcloud)
    , task(task)
    , ops()
{
    this->session = session;
}

// See RpcWrapper for documentation.
bool
ClientTransactionTask::ClientTransactionRpcWrapper::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        markOpsForRetry();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
ClientTransactionTask::ClientTransactionRpcWrapper::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        ramcloud->clientContext->transportManager->flushSession(
                session->getServiceLocator());
        session = NULL;
    }
    markOpsForRetry();
    return true;
}

// See RpcWrapper for documentation.
void
ClientTransactionTask::ClientTransactionRpcWrapper::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Constructor for a DecisionRpc.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the transaction task that issued this request.
 */
ClientTransactionTask::DecisionRpc::DecisionRpc(RamCloud* ramcloud,
        Transport::SessionRef session,
        ClientTransactionTask* task)
    : ClientTransactionRpcWrapper(ramcloud,
                                  session,
                                  task,
                                  sizeof(WireFormat::TxDecision::Response))
    , reqHdr(allocHeader<WireFormat::TxDecision>())
{
    reqHdr->decision = task->decision;
    reqHdr->leaseId = task->lease.leaseId;
    reqHdr->participantCount = 0;
}

/**
 * Append an operation to the end of this decision rpc.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 * \return
 *      True if the op was successfully appended; false otherwise.
 */
bool
ClientTransactionTask::DecisionRpc::appendOp(CommitCacheMap::iterator opEntry)
{
    if (reqHdr->participantCount >= DecisionRpc::MAX_OBJECTS_PER_RPC) {
        return false;
    }

    const CacheKey* key = &opEntry->first;
    CacheEntry* entry = &opEntry->second;

    request.emplaceAppend<WireFormat::TxParticipant>(
            key->tableId,
            static_cast<uint64_t>(key->keyHash),
            entry->rpcId);

    entry->state = CacheEntry::DECIDE;
    ops[reqHdr->participantCount] = opEntry;
    reqHdr->participantCount++;
    return true;
}

/**
 * Wait for the Decision RPC to be acknowledged.
 *
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster; if it ever
 *      existed, it has since crashed.  Operations have been marked for retry;
 *      caller can and should discard this RPC.
 * \throw UnknownTabletException
 *      The target server is not the owner of one or more of the included
 *      operations.  This could have occurred due to an out of date tablet map.
 *      Operations have been marked for retry; caller can and should discard
 *      this RPC.
 */
void
ClientTransactionTask::DecisionRpc::wait()
{
    waitInternal(ramcloud->clientContext->dispatch);

    if (getState() == FAILED) {
        // Target server was not reachable. Retry has already been arranged.
        throw ServerNotUpException(HERE);
    } else if (responseHeader->status != STATUS_OK) {
        ClientException::throwException(HERE, responseHeader->status);
    }
}

/**
 * This method is invoked when a decision RPC couldn't complete successfully. It
 * arranges for prepares to be tried again for all of the participant objects in
 * that request.
 */
void
ClientTransactionTask::DecisionRpc::markOpsForRetry()
{
    for (uint32_t i = 0; i < reqHdr->participantCount; i++) {
        const CacheKey* key = &ops[i]->first;
        CacheEntry* entry = &ops[i]->second;
        ramcloud->objectFinder.flush(key->tableId);
        entry->state = CacheEntry::PENDING;
    }
    task->nextCacheEntry = task->commitCache.begin();
}

/**
 * Constructor for PrepareRpc.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the transaction task that issued this request.
 */
ClientTransactionTask::PrepareRpc::PrepareRpc(RamCloud* ramcloud,
        Transport::SessionRef session, ClientTransactionTask* task)
    : ClientTransactionRpcWrapper(ramcloud,
                                  session,
                                  task,
                                  sizeof(WireFormat::TxDecision::Response))
    , reqHdr(allocHeader<WireFormat::TxPrepare>())
{
    reqHdr->lease = task->lease;
    reqHdr->ackId = ramcloud->rpcTracker.ackId();
    reqHdr->participantCount = task->participantCount;
    reqHdr->opCount = 0;
    request.appendExternal(&task->participantList);
}

/**
 * Append an operation to the end of this prepare rpc.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 * \return
 *      True if the op was successfully appended; false otherwise.
 */
bool
ClientTransactionTask::PrepareRpc::appendOp(CommitCacheMap::iterator opEntry)
{
    if (reqHdr->opCount >= PrepareRpc::MAX_OBJECTS_PER_RPC) {
        return false;
    }

    const CacheKey* key = &opEntry->first;
    CacheEntry* entry = &opEntry->second;

    switch (entry->type) {
        case CacheEntry::READ:
            request.emplaceAppend<WireFormat::TxPrepare::Request::ReadOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf->getKeyLength(), entry->rejectRules);
            request.appendExternal(entry->objectBuf->getKey(),
                    entry->objectBuf->getKeyLength());
            break;
        case CacheEntry::REMOVE:
            request.emplaceAppend<WireFormat::TxPrepare::Request::RemoveOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf->getKeyLength(), entry->rejectRules);
            request.appendExternal(entry->objectBuf->getKey(),
                    entry->objectBuf->getKeyLength());
            break;
        case CacheEntry::WRITE:
            request.emplaceAppend<WireFormat::TxPrepare::Request::WriteOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf->size(), entry->rejectRules);
            request.appendExternal(entry->objectBuf);
            break;
        default:
            RAMCLOUD_LOG(ERROR, "Unknown transaction op type.");
            return false;
    }

    entry->state = CacheEntry::PREPARE;
    ops[reqHdr->opCount] = opEntry;
    reqHdr->opCount++;
    return true;
}

/**
 * Wait for the Prepare request to complete, and return participant servers
 * vote to either proceed or abort.
 *
 * \return
 *      The participant server's response to the request to prepare the included
 *      transaction operations for commit.  See WireFormat::TxPrepare::Vote for
 *      documentation of possible responses.
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster; if it ever
 *      existed, it has since crashed.  Operations have been marked for retry;
 *      caller can and should discard this RPC.
 * \throw UnknownTabletException
 *      The target server is not the owner of one or more of the included
 *      operations.  This could have occurred due to an out of date tablet map.
 *      Operations have been marked for retry; caller can and should discard
 *      this RPC.
 */
WireFormat::TxPrepare::Vote
ClientTransactionTask::PrepareRpc::wait()
{
    waitInternal(ramcloud->clientContext->dispatch);

    if (getState() == FAILED) {
        // Target server was not reachable. Retry has already been arranged.
        throw ServerNotUpException(HERE);
    } else if (responseHeader->status != STATUS_OK) {
        ClientException::throwException(HERE, responseHeader->status);
    }

    WireFormat::TxPrepare::Response* respHdr =
            response->getStart<WireFormat::TxPrepare::Response>();
    return respHdr->vote;
}

/**
 * This method is invoked when a prepare RPC couldn't complete successfully. It
 * arranges for prepares to be tried again for all of the participant objects in
 * that request.
 */
void
ClientTransactionTask::PrepareRpc::markOpsForRetry()
{
    for (uint32_t i = 0; i < reqHdr->opCount; i++) {
        const CacheKey* key = &ops[i]->first;
        CacheEntry* entry = &ops[i]->second;
        ramcloud->objectFinder.flush(key->tableId);
        entry->state = CacheEntry::PENDING;
    }
    task->nextCacheEntry = task->commitCache.begin();
}

} // namespace RAMCloud
