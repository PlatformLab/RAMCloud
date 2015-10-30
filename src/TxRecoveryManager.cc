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

#include "TxRecoveryManager.h"
#include "MasterService.h"

namespace RAMCloud {

/**
 * Constructor for the transaction recovery manager.
 *
 * \param context
 *      Overall information about the server that runs this manager.
 */
TxRecoveryManager::TxRecoveryManager(Context* context)
    : WorkerTimer(context->dispatch)
    , lock("TxRecoveryManager::lock")
    , context(context)
    , recoveringIds()
    , recoveries()
{
}

/**
 * Called when the manager has been scheduled to do some work.  This method
 * will perform some incremental work and reschedule itself if more work needs
 * to be done.
 */
void
TxRecoveryManager::handleTimerEvent()
{
    Lock _(lock);

    // See if any of the recoveries need to do more work.
    RecoveryList::iterator it = recoveries.begin();
    while (it != recoveries.end()) {
        RecoveryTask* task = &(*it);
        if (task->isReady()) {
            // Mark recovery complete.
            recoveringIds.erase(task->getId());
            it = recoveries.erase(it);
        } else {
            task->performTask();
            it++;
        }
    }

    // Reschedule if there are still recoveries in progress.
    if (recoveries.size() > 0) {
        this->start(0);
    }
}

/**
 * Request that the specified transaction be recovered if it is not already in
 * the process of being recovered.
 *
 * \param rpcReq
 *      Request buffer of the TxHintFailed rpc containing the list participants
 *      that are involved in the transaction to be recovered.
 */
void
TxRecoveryManager::handleTxHintFailed(Buffer* rpcReq)
{
    /*** Make sure this is the correct target recovery manager. ***/
    WireFormat::TxHintFailed::Request* reqHdr =
            rpcReq->getStart<WireFormat::TxHintFailed::Request>();
    if (reqHdr->participantCount < 1) {
        throw RequestFormatError(HERE);
    }
    uint32_t offset = sizeof32(*reqHdr);
    WireFormat::TxParticipant* participant =
            rpcReq->getOffset<WireFormat::TxParticipant>(offset);
    if (!context->getMasterService()->tabletManager.getTablet(
            participant->tableId, participant->keyHash)) {
        throw UnknownTabletException(HERE);
    }
    uint64_t leaseId = reqHdr->leaseId;
    uint64_t txId = reqHdr->clientTxId;
    uint32_t participantCount = reqHdr->participantCount;
    RecoveryId recoveryId = {leaseId, txId};

    /*** Make sure we did not already receive this request. ***/
    Lock _(lock);
    if (recoveringIds.find(recoveryId) != recoveringIds.end()) {
        // This transaction is already recovering.
        return;
    }

    /*** Schedule a new recovery. ***/
    recoveringIds.insert(recoveryId);
    recoveries.emplace_back(
            context, leaseId, txId, *rpcReq, participantCount, offset);
    this->start(0);
}

/**
 * Check to see if the recovery manager still needs the referenced decision
 * record to be kept around.
 *
 * \param record
 *      Reference to the record of interest.
 * \return
 *      True if the record is still needed.  False if it is ok for the record
 *      to be cleaned.
 */
bool
TxRecoveryManager::isTxDecisionRecordNeeded(TxDecisionRecord& record)
{
    Lock _(lock);

    uint64_t leaseId = record.getLeaseId();
    uint64_t transactionId = record.getTransactionId();
    RecoveryId recoveryId = {leaseId, transactionId};
    if (recoveringIds.find(recoveryId) == recoveringIds.end()) {
        // This transaction is still recovering.
        return false;
    }
    return true;
}

/**
 * Recover and restart a possibly failed transaction recovery.  Used during
 * master crash recovery when taking ownership of TxDecisionRecords.
 *
 * \param record
 *      The TxDecisionRecord that represents an ongoing transaction recovery
 *      that this module should now take ownership over.
 * \return
 *      True if the record is needed and should be kept.  False, if the record
 *      is no longer necessary.
 */
bool
TxRecoveryManager::recoverRecovery(TxDecisionRecord& record)
{
    RecoveryId recoveryId = {record.getLeaseId(),
                             record.getTransactionId()};

    /*** Make sure we did not already receive this request. ***/
    Lock _(lock);
    if (expect_false(recoveringIds.find(recoveryId) != recoveringIds.end())) {
        // This transaction is already recovering.
        return false;
    }

    /*** Schedule a recovery. ***/
    recoveringIds.insert(recoveryId);
    recoveries.emplace_back(context, record);
    this->start(0);
    return true;
}

/**
 * Constructor for the transaction recovery task.  Used when processing
 * initial transaction recovery requests.
 *
 * \param context
 *      Overall information about this server.
 * \param leaseId
 *      Id of the lease associated with the transaction to be recovered.
 * \param transactionId
 *      Id of the recovering transaction.
 * \param participantBuffer
 *      Buffer containing the WireFormat list of the participants that arrived
 *      as a part of a TxHintFailed request that initiated this recovery.  This
 *      constructor assumes the buffer is well formed. If the request is
 *      malformed, the behavior is undefined.
 * \param participantCount
 *      Number of participants contained with the participantBuffer.  Must be
 *      at lease one.
 * \param offset
 *      The offset into the participantBuffer where the list of participants
 *      start.
 */
TxRecoveryManager::RecoveryTask::RecoveryTask(
        Context* context, uint64_t leaseId, uint64_t transactionId,
        Buffer& participantBuffer, uint32_t participantCount,
        uint32_t offset)
    : context(context)
    , leaseId(leaseId)
    , transactionId(transactionId)
    , state(State::REQUEST_ABORT)
    , decision(WireFormat::TxDecision::UNDECIDED)
    , participants()
    , nextParticipantEntry()
    , decisionRpcs()
    , requestAbortRpcs()
{
    /*** Build a participant list ***/
    WireFormat::TxParticipant* participant;
    for (uint32_t i = 0; i < participantCount; i++) {
        participant =
                participantBuffer.getOffset<WireFormat::TxParticipant>(offset);
        uint64_t tableId = participant->tableId;
        uint64_t keyHash = participant->keyHash;
        uint64_t rpcId = participant->rpcId;
        participants.emplace_back(tableId,
                                  keyHash,
                                  rpcId);
        offset += sizeof32(*participant);
    }
    nextParticipantEntry = participants.begin();
}

/**
 * Constructor for the transaction recovery task.  Used when continuing
 * recovering after a recovery failure due to crashed master.
 *
 * \param context
 *      Overall information about this server.
 * \param record
 *      Decision record containing information about a recovery that was in the
 *      process of recovering but may have failed before completing.  This
 *      record must be will formed (e.g. have at lease one participant),
 *      otherwise the behavior is undefined.
 */
TxRecoveryManager::RecoveryTask::RecoveryTask(
        Context* context, TxDecisionRecord& record)
    : context(context)
    , leaseId(record.getLeaseId())
    , transactionId(record.getTransactionId())
    , state(State::DECIDE)
    , decision(record.getDecision())
    , participants()
    , nextParticipantEntry()
    , decisionRpcs()
    , requestAbortRpcs()
{
    /*** Build a participant list ***/
    WireFormat::TxParticipant participant;
    for (uint32_t i = 0; i < record.getParticipantCount(); i++) {
        participant = record.getParticipant(i);
        uint64_t tableId = participant.tableId;
        uint64_t keyHash = participant.keyHash;
        uint64_t rpcId = participant.rpcId;
        participants.emplace_back(tableId,
                                  keyHash,
                                  rpcId);
    }
    nextParticipantEntry = participants.begin();
}

/**
 * Make incremental progress towards recovering this transaction.
 */
void
TxRecoveryManager::RecoveryTask::performTask()
{
    if (state == State::REQUEST_ABORT) {
        sendRequestAbortRpc();
        try {
            processRequestAbortRpcResults();
        } catch (StaleRpcException& e) {
            // Finish early since the recovery process is not actually needed.
            requestAbortRpcs.clear();   // cleanup early to avoid extra work.
            state = State::DONE;
            return;
        }
        if (nextParticipantEntry == participants.end()
            && requestAbortRpcs.empty())
        {
            // Done with the request abort phase.
            // Build decision record to be logged.
            if (decision != WireFormat::TxDecision::ABORT) {
                decision = WireFormat::TxDecision::COMMIT;
            }

            ParticipantList::iterator it = participants.begin();
            Participant* participant = &(*it);
            TxDecisionRecord record(
                    participant->tableId,
                    participant->keyHash,
                    leaseId,
                    transactionId,
                    decision,
                    WallTime::secondsTimestamp());
            for (; it != participants.end(); it++) {
                participant = &(*it);
                record.addParticipant(
                        participant->tableId,
                        participant->keyHash,
                        participant->rpcId);
            }
            context->getMasterService()->objectManager.writeTxDecisionRecord(
                    record);
            context->getMasterService()->objectManager.syncChanges();

            // Change state to cause next phase to execute.
            state = State::DECIDE;
            nextParticipantEntry = participants.begin();
        }
    }
    if (state == State::DECIDE) {
        sendDecisionRpc();
        processDecisionRpcResults();
        if (nextParticipantEntry == participants.end()
            && requestAbortRpcs.empty())
        {
            // Done with the request abort phase.
            // Change state to cause next phase to execute.
            state = State::DONE;
        }
    }
}

/**
 * Constructor for TxRecoveryRpcWrapper.
 *
 * \param context
 *      Overall information about this server.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the task that issued this request.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked by this class to ensure that
 *      they contain at least this much data, wrapper subclasses can
 *      use the getResponseHeader method to access the response header
 *      once isReady has returned true.
 */
TxRecoveryManager::RecoveryTask::TxRecoveryRpcWrapper::TxRecoveryRpcWrapper(
        Context* context,
        Transport::SessionRef session,
        RecoveryTask* task,
        uint32_t responseHeaderLength)
    : RpcWrapper(responseHeaderLength)
    , context(context)
    , task(task)
    , ops()
    , participantCount(NULL)
{
    this->session = session;
}

// See RpcWrapper for documentation.
void
TxRecoveryManager::RecoveryTask::TxRecoveryRpcWrapper::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Append an operation to the end of this Transaction Recovery RPC.  This method
 * provides a common implementation for both Decision and RequestAbort RPCs.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 * \param state
 *      Participant::State the provided opEntry should entry signaling marking
 *      the entry as having finished this stage.
 * \return
 *      True if the op was successfully appended; false otherwise.
 */
bool
TxRecoveryManager::RecoveryTask::TxRecoveryRpcWrapper::appendOp(
        ParticipantList::iterator opEntry,
        Participant::State state)
{
    if (*participantCount >= RequestAbortRpc::MAX_OBJECTS_PER_RPC) {
        return false;
    }

    Participant* entry = &(*opEntry);
    request.emplaceAppend<WireFormat::TxParticipant>(
            entry->tableId, entry->keyHash, entry->rpcId);

    entry->state = state;
    ops[*participantCount] = opEntry;
    (*participantCount)++;
    return true;
}

// See RpcWrapper for documentation.
bool
TxRecoveryManager::RecoveryTask::TxRecoveryRpcWrapper::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        markOpsForRetry();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
TxRecoveryManager::RecoveryTask::TxRecoveryRpcWrapper::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        context->transportManager->flushSession(session->getServiceLocator());
        session = NULL;
    }
    markOpsForRetry();
    return true;
}

/**
 * Handle the case where the RPC may have been sent to the wrong server; resets
 * the state of the included ops scheduling them to be resent.
 */
void
TxRecoveryManager::RecoveryTask::TxRecoveryRpcWrapper::markOpsForRetry()
{
    for (uint32_t i = 0; i < *participantCount; i++) {
        Participant* entry = &(*ops[i]);
        context->objectFinder->flush(entry->tableId);
        entry->state = Participant::PENDING;
    }
    task->nextParticipantEntry = task->participants.begin();
    TEST_LOG("Retry marked.");
}

/**
 * Constructor for a decision rpc.
 *
 * \param context
 *      Overall information about this server.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the task that issued this request.
 */
TxRecoveryManager::RecoveryTask::DecisionRpc::DecisionRpc(Context* context,
        Transport::SessionRef session,
        RecoveryTask* task)
    : TxRecoveryRpcWrapper(context,
                           session,
                           task,
                           sizeof(WireFormat::TxDecision::Response))
    , reqHdr(allocHeader<WireFormat::TxDecision>())
{
    reqHdr->decision = task->decision;
    reqHdr->leaseId = task->leaseId;
    reqHdr->participantCount = 0;
    participantCount = &reqHdr->participantCount;
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
TxRecoveryManager::RecoveryTask::DecisionRpc::appendOp(
        ParticipantList::iterator opEntry)
{
    return TxRecoveryRpcWrapper::appendOp(opEntry, Participant::DECIDE);
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
TxRecoveryManager::RecoveryTask::DecisionRpc::wait()
{
    waitInternal(context->dispatch);

    if (getState() == FAILED) {
        // Target server was not reachable. Retry has already been arranged.
        throw ServerNotUpException(HERE);
    } else if (responseHeader->status != STATUS_OK) {
        ClientException::throwException(HERE, responseHeader->status);
    }
}

/**
 * Process any decision rpcs that have completed.  Factored out mostly for ease
 * of testing.
 */
void
TxRecoveryManager::RecoveryTask::processDecisionRpcResults()
{
    // Process outstanding RPCs.
    std::list<DecisionRpc>::iterator it = decisionRpcs.begin();
    for (; it != decisionRpcs.end(); it++) {
        DecisionRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }

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
}

/**
 * Send out a decision rpc if not all master have been notified.  Factored out
 * mostly for ease of testing.
 */
void
TxRecoveryManager::RecoveryTask::sendDecisionRpc()
{
    // Issue an additional rpc.
    DecisionRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextParticipantEntry != participants.end(); nextParticipantEntry++) {
        Participant* entry = &(*nextParticipantEntry);

        if (entry->state == Participant::DECIDE) {
            continue;
        }

        if (nextRpc == NULL) {
            rpcSession = context->objectFinder->lookup(entry->tableId,
                                                       entry->keyHash);
            decisionRpcs.emplace_back(context, rpcSession, this);
            nextRpc = &decisionRpcs.back();
        }

        Transport::SessionRef session =
                context->objectFinder->lookup(entry->tableId,
                                              entry->keyHash);
        if (session->getServiceLocator() != rpcSession->getServiceLocator()
            || !nextRpc->appendOp(nextParticipantEntry)) {
            break;
        }
    }
    if (nextRpc) {
        nextRpc->send();
    }
}

/**
 * Constructor for a request abort rpc.
 *
 * \param context
 *      Overall information about this server.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the task that issued this request.
 */
TxRecoveryManager::RecoveryTask::RequestAbortRpc::RequestAbortRpc(
            Context* context,
            Transport::SessionRef session,
            RecoveryTask* task)
    : TxRecoveryRpcWrapper(context,
                           session,
                           task,
                           sizeof(WireFormat::TxRequestAbort::Response))
    , reqHdr(allocHeader<WireFormat::TxRequestAbort>())
{
    reqHdr->leaseId = task->leaseId;
    reqHdr->participantCount = 0;
    participantCount = &reqHdr->participantCount;
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
TxRecoveryManager::RecoveryTask::RequestAbortRpc::appendOp(
        ParticipantList::iterator opEntry)
{
        return TxRecoveryRpcWrapper::appendOp(opEntry, Participant::ABORT);
}

/**
 * Wait for the RequestAbort request to complete, and return participant servers
 * vote to either abort or commit.
 *
 * \return
 *      The participant server's response to the request to abort the
 *      transaction.  See WireFormat::TxRequestAbort::Vote for documentation of
 *      possible responses.
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster; if it ever
 *      existed, it has since crashed.  Operation has been marked for retry;
 *      caller can and should discard this RPC.
 * \throw UnknownTabletException
 *      The target server is not the owner of one or more of the included
 *      operations.  This could have occurred due to an out of date tablet map.
 *      Operation has been marked for retry; caller can and should discard
 *      this RPC.
 */
WireFormat::TxPrepare::Vote
TxRecoveryManager::RecoveryTask::RequestAbortRpc::wait()
{
    waitInternal(context->dispatch);

    if (getState() == FAILED) {
        // Target server was not reachable. Retry has already been arranged.
        throw ServerNotUpException(HERE);
    } else if (responseHeader->status != STATUS_OK) {
        ClientException::throwException(HERE, responseHeader->status);
    }

    WireFormat::TxRequestAbort::Response* respHdr =
            response->getStart<WireFormat::TxRequestAbort::Response>();
    return respHdr->vote;
}

/**
 * Process any request abort rpcs that have completed.  Factored out mostly for
 * ease of testing.
 *
 * \throw StaleRpcException
 *      At least one of the requested aborts could not complete because the
 *      client has already completed the transaction protocol.
 */
void
TxRecoveryManager::RecoveryTask::processRequestAbortRpcResults()
{
    // Process outstanding RPCs.
    std::list<RequestAbortRpc>::iterator it = requestAbortRpcs.begin();
    for (; it != requestAbortRpcs.end(); it++) {
        RequestAbortRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }

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
        it = requestAbortRpcs.erase(it);
    }
}

/**
 * Send out a request abort rpc if not all master have been asked.  Factored out
 * mostly for ease of testing.
 */
void
TxRecoveryManager::RecoveryTask::sendRequestAbortRpc()
{
    // Issue an additional rpc.
    RequestAbortRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextParticipantEntry != participants.end(); nextParticipantEntry++) {
        Participant* entry = &(*nextParticipantEntry);

        if (entry->state == Participant::ABORT) {
            continue;
        }

        if (nextRpc == NULL) {
            rpcSession = context->objectFinder->lookup(entry->tableId,
                                                       entry->keyHash);
            requestAbortRpcs.emplace_back(context, rpcSession, this);
            nextRpc = &requestAbortRpcs.back();
        }

        Transport::SessionRef session =
                context->objectFinder->lookup(entry->tableId,
                                              entry->keyHash);
        if (session->getServiceLocator() != rpcSession->getServiceLocator()
            || !nextRpc->appendOp(nextParticipantEntry)) {
            break;
        }
    }

    if (nextRpc) {
        nextRpc->send();
    }
}

/**
 * Returns a human readable string representing the contents of the task.
 * Used during testing.
 */
string
TxRecoveryManager::RecoveryTask::toString()
{
    string s;
    s.append(format("RecoveryTask :: lease{%lu} transaction{%lu}",
                    leaseId, transactionId));
    switch (state) {
        case State::REQUEST_ABORT:
            s.append(" state{REQUEST_ABORT}");
            break;
        case State::DECIDE:
            s.append(" state{DECIDE}");
            break;
        case State::DONE:
            s.append(" state{DONE}");
            break;
    }
    switch (decision) {
        case WireFormat::TxDecision::COMMIT:
            s.append(" decision{COMMIT}");
            break;
        case WireFormat::TxDecision::ABORT:
            s.append(" decision{ABORT}");
            break;
        case WireFormat::TxDecision::UNDECIDED:
            s.append(" decision{INVALID}");
            break;
    }

    s.append(format(" participants["));
    ParticipantList::iterator it = participants.begin();
    for (; it != participants.end(); it++) {
        s.append(format(" {%lu, %lu, %lu}",
                        it->tableId, it->keyHash, it->rpcId));
    }
    s.append(format(" ]"));
    return s;
}


} // end RAMCloud
