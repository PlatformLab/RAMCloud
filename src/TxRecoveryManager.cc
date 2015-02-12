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
    , mutex("TxRecoveryManager::lock")
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
    Lock lock(mutex);

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

    // Reschedule if there is still recoveries in progress.
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
    if (!context->masterService->tabletManager.getTablet(
            participant->tableId, participant->keyHash)) {
        throw UnknownTabletException(HERE);
    }
    uint64_t leaseId = reqHdr->leaseId;
    uint64_t rpcId = participant->rpcId;
    uint32_t participantCount = reqHdr->participantCount;
    RecoveryId recoveryId = {leaseId, rpcId};

    /*** Make sure we did not already receive this request. ***/
    Lock lock(mutex);
    if (recoveringIds.find(recoveryId) != recoveringIds.end()) {
        // This transaction is already recovering.
        return;
    }

    /*** Schedule a new recovery. ***/
    recoveringIds.insert(recoveryId);
    recoveries.emplace_back(
            context, leaseId, *rpcReq, participantCount, offset);
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
    Lock lock(mutex);
    if (record.getParticipantCount() < 1) {
        RAMCLOUD_LOG(ERROR, "TxDecisionRecord missing participant information");
        return false;
    }

    WireFormat::TxParticipant participant = record.getParticipant(0);
    uint64_t leaseId = record.getLeaseId();
    uint64_t rpcId = participant.rpcId;
    RecoveryId recoveryId = {leaseId, rpcId};
    if (recoveringIds.find(recoveryId) == recoveringIds.end()) {
        // This transaction is still recovering.
        return false;
    }
    return true;
}

/**
 * Constructor for the transaction recovery task.  Used when processing
 * initial transaction recovery requests.
 *
 * \param context
 *      Overall information about this server.
 * \param leaseId
 *      If of the lease associated with the transaction to be recovered.
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
        Context* context, uint64_t leaseId,
        Buffer& participantBuffer, uint32_t participantCount,
        uint32_t offset)
    : context(context)
    , leaseId(leaseId)
    , state(State::REQEST_ABORT)
    , decision(WireFormat::TxDecision::INVALID)
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
    if (state == State::REQEST_ABORT) {
        processRequestAbortRpcs();
        sendRequestAbortRpc();
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
                    decision,
                    WallTime::secondsTimestamp());
            while (it != participants.end()) {
                participant = &(*it);
                record.addParticipant(
                        participant->tableId,
                        participant->keyHash,
                        participant->rpcId);

            }
            context->masterService->objectManager.writeTxDecisionRecord(record);
            context->masterService->objectManager.syncChanges();

            // Change state to cause next phase to execute.
            state = State::DECIDE;
            nextParticipantEntry = participants.begin();
        }
    } else if (state == State::DECIDE) {
        processDecisionRpcs();
        sendDecisionRpc();
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
 * Polls waiting for the recovery task to complete.
 */
void
TxRecoveryManager::RecoveryTask::wait()
{
    while (!isReady()) {
        performTask();
    }
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
    : RpcWrapper(sizeof(WireFormat::TxDecision::Request))
    , context(context)
    , session(session)
    , task(task)
    , ops()
    , reqHdr(allocHeader<WireFormat::TxDecision>())
{
    reqHdr->decision = task->decision;
    reqHdr->leaseId = task->leaseId;
    reqHdr->participantCount = 0;
}

// See RpcWrapper for documentation.
bool
TxRecoveryManager::RecoveryTask::DecisionRpc::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        retryRequest();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
TxRecoveryManager::RecoveryTask::DecisionRpc::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        context->transportManager->flushSession(session->getServiceLocator());
        session = NULL;
    }
    retryRequest();
    return true;
}

// See RpcWrapper for documentation.
void
TxRecoveryManager::RecoveryTask::DecisionRpc::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Append an operation to the end of this decision rpc.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 */
void
TxRecoveryManager::RecoveryTask::DecisionRpc::appendOp(
        ParticipantList::iterator opEntry)
{
    Participant* entry = &(*opEntry);
    request.emplaceAppend<WireFormat::TxParticipant>(
            entry->tableId, entry->keyHash, entry->rpcId);

    entry->state = Participant::DECIDE;
    ops[reqHdr->participantCount] = opEntry;
    reqHdr->participantCount++;
}

/**
 * Handle the case where the RPC may have been sent to the wrong server.
 */
void
TxRecoveryManager::RecoveryTask::DecisionRpc::retryRequest()
{
    for (uint32_t i = 0; i < reqHdr->participantCount; i++) {
        Participant* entry = &(*ops[i]);
        context->masterService->objectFinder.flush(entry->tableId);
        entry->state = Participant::PENDING;
    }
    task->nextParticipantEntry = task->participants.begin();
}

/**
 * Process any decision rpcs that have completed.  Factored out mostly for ease
 * of testing.
 */
void
TxRecoveryManager::RecoveryTask::processDecisionRpcs()
{
    // Process outstanding RPCs.
    std::list<DecisionRpc>::iterator it = decisionRpcs.begin();
    for (; it != decisionRpcs.end(); it++) {
        DecisionRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }

        WireFormat::TxDecision::Response* respHdr =
                rpc->response->getStart<WireFormat::TxDecision::Response>();
        if (respHdr != NULL) {
            if (respHdr->common.status == STATUS_OK ||
                respHdr->common.status == STATUS_UNKNOWN_TABLET) {
                // Nothing to do.
            } else {
                RAMCLOUD_CLOG(WARNING, "unexpected return status %s while"
                              "trying to recover transaction for leaseId %lu",
                              statusToSymbol(respHdr->common.status), leaseId);
            }
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

        if (entry->state == Participant::DECIDE ||
            entry->state == Participant::FAILED) {
            continue;
        }

        try {
            if (nextRpc == NULL) {
                rpcSession =
                        context->masterService->objectFinder.lookup(
                                entry->tableId,
                                entry->keyHash);
                decisionRpcs.emplace_back(context, rpcSession, this);
                nextRpc = &decisionRpcs.back();
            }

            Transport::SessionRef session =
                    context->masterService->objectFinder.lookup(
                                entry->tableId,
                                entry->keyHash);
            if (session->getServiceLocator() == rpcSession->getServiceLocator()
                && nextRpc->reqHdr->participantCount <
                        DecisionRpc::MAX_OBJECTS_PER_RPC) {
                nextRpc->appendOp(nextParticipantEntry);
            } else {
                break;
            }
        } catch (TableDoesntExistException&) {
            entry->state = Participant::FAILED;
            RAMCLOUD_CLOG(WARNING, "trying to recover transaction for leaseId"
                    " %lu but table with id %lu does not exist.",
                    leaseId, entry->tableId);
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
    : RpcWrapper(sizeof(WireFormat::TxRequestAbort::Request))
    , context(context)
    , session(session)
    , task(task)
    , ops()
    , reqHdr(allocHeader<WireFormat::TxRequestAbort>())
{
    reqHdr->leaseId = task->leaseId;
    reqHdr->participantCount = 0;
}

// See RpcWrapper for documentation.
bool
TxRecoveryManager::RecoveryTask::RequestAbortRpc::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        retryRequest();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
TxRecoveryManager::RecoveryTask::RequestAbortRpc::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        context->transportManager->flushSession(session->getServiceLocator());
        session = NULL;
    }
    retryRequest();
    return true;
}

// See RpcWrapper for documentation.
void
TxRecoveryManager::RecoveryTask::RequestAbortRpc::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Append an operation to the end of this decision rpc.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 */
void
TxRecoveryManager::RecoveryTask::RequestAbortRpc::appendOp(
        ParticipantList::iterator opEntry)
{
    Participant* entry = &(*opEntry);
    request.emplaceAppend<WireFormat::TxParticipant>(
            entry->tableId, entry->keyHash, entry->rpcId);

    entry->state = Participant::ABORT;
    ops[reqHdr->participantCount] = opEntry;
    reqHdr->participantCount++;
}

/**
 * Handle the case where the RPC may have been sent to the wrong server.
 */
void
TxRecoveryManager::RecoveryTask::RequestAbortRpc::retryRequest()
{
    for (uint32_t i = 0; i < reqHdr->participantCount; i++) {
        Participant* entry = &(*ops[i]);
        context->masterService->objectFinder.flush(entry->tableId);
        entry->state = Participant::PENDING;
    }
    task->nextParticipantEntry = task->participants.begin();
}

/**
 * Process any request abort rpcs that have completed.  Factored out mostly for
 * ease of testing.
 */
void
TxRecoveryManager::RecoveryTask::processRequestAbortRpcs()
{
    // Process outstanding RPCs.
    std::list<RequestAbortRpc>::iterator it = requestAbortRpcs.begin();
    for (; it != requestAbortRpcs.end(); it++) {
        RequestAbortRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }

        WireFormat::TxRequestAbort::Response* respHdr =
                rpc->response->getStart<WireFormat::TxRequestAbort::Response>();
        if (respHdr != NULL) {
            if (respHdr->common.status == STATUS_OK) {
                if (respHdr->vote != WireFormat::TxRequestAbort::COMMIT) {
                    decision = WireFormat::TxDecision::ABORT;
                }
            } else if (respHdr->common.status == STATUS_UNKNOWN_TABLET) {
                // Nothing to do.
            } else {
                RAMCLOUD_CLOG(WARNING, "unexpected return status %s while"
                              "trying to recover transaction for leaseId %lu",
                              statusToSymbol(respHdr->common.status), leaseId);
            }
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

        if (entry->state == Participant::ABORT ||
            entry->state == Participant::FAILED) {
            continue;
        }

        try {
            if (nextRpc == NULL) {
                rpcSession =
                        context->masterService->objectFinder.lookup(
                                entry->tableId,
                                entry->keyHash);
                requestAbortRpcs.emplace_back(context, rpcSession, this);
                nextRpc = &requestAbortRpcs.back();
            }

            Transport::SessionRef session =
                    context->masterService->objectFinder.lookup(
                                entry->tableId,
                                entry->keyHash);
            if (session->getServiceLocator() == rpcSession->getServiceLocator()
                && nextRpc->reqHdr->participantCount <
                        RequestAbortRpc::MAX_OBJECTS_PER_RPC) {
                nextRpc->appendOp(nextParticipantEntry);
            } else {
                break;
            }
        } catch (TableDoesntExistException&) {
            entry->state = Participant::FAILED;
            RAMCLOUD_CLOG(WARNING, "trying to recover transaction for leaseId"
                    " %lu but table with id %lu does not exist.",
                    leaseId, entry->tableId);
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
    s.append(format("RecoveryTask :: lease{%lu}", leaseId));
    switch (state) {
        case State::REQEST_ABORT:
            s.append(" state{REQEST_ABORT}");
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
        case WireFormat::TxDecision::INVALID:
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
