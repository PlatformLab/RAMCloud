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

TxRecoveryManager::TxRecoveryManager(Context* context)
    : mutex("TxRecoveryManager::lock")
    , context(context)
    , recoveringIds()
{

}

TxRecoveryManager::~TxRecoveryManager() {
}

/**
 * Recover the specified transaction if it is not already in the process of
 * being recovered.
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

    /*** Make sure we did not already receive this request. ***/
    uint64_t leaseId = reqHdr->leaseId;
    uint64_t rpcId = participant->rpcId;
    RecoveryId recoveryId = {leaseId, rpcId};

    {
        Lock lock(mutex);
        if (recoveringIds.find(recoveryId) != recoveringIds.end()) {
            // This transaction is already recovering.
            return;
        }
        recoveringIds.insert(recoveryId);
    }

    /*** Build a local participant list ***/
    ParticipantList participantList;
    for (uint32_t i = 0; i < reqHdr->participantCount; i++) {
        participant = rpcReq->getOffset<WireFormat::TxParticipant>(offset);
        uint64_t tableId = participant->tableId;
        uint64_t keyHash = participant->keyHash;
        uint64_t rpcId = participant->rpcId;
        participantList.emplace_back(tableId,
                                     keyHash,
                                     rpcId);
        offset += sizeof32(*participant);
    }

    /*** Collect Request_Abort VOTES ***/
    RequestAbortTask requestAbortTask(context, leaseId, &participantList);
    WireFormat::TxDecision::Decision decision = requestAbortTask.wait();

    /*** Log Decision ***/
    {
        Lock lock(mutex);
    }

    /*** Send Decisions ***/
    DecisionTask sendDecisionTask(
            context, decision, leaseId, &participantList);
    sendDecisionTask.wait();

    /*** Mark the recovery complete ***/
    {
        Lock lock(mutex);
        recoveringIds.erase(recoveryId);
    }
}

/**
 * Constructor for the SendDecisionTask.
 *
 * \param context
 *      Overall information about this server.
 * \param decision
 *      The decision that should be sent to all transaction participants.
 * \param leaseId
 *      Id of the lease with which the recovering transaction was started.
 * \param pList
 *      Pointer to the list of participants that should be consulted regarding
 *      this abort request.  No items should be added or removed from this list
 *      during the lifetime of this task.  Must not be NULL.
 */
TxRecoveryManager::DecisionTask::DecisionTask(
        Context* context, WireFormat::TxDecision::Decision decision,
        uint64_t leaseId, ParticipantList* pList)
    : context(context)
    , decision(decision)
    , leaseId(leaseId)
    , goalReached(false)
    , pList(pList)
    , defaultPList()
    , nextParticipantEntry(pList->begin())
    , decisionRpcs()
{}

/**
 * Make incremental progress towards notifying all participant masters of the
 * decision for this recovering transaction.
 */
void
TxRecoveryManager::DecisionTask::performTask()
{
    if (!goalReached) {
        processDecisionRpcs();
        sendDecisionRpc();
    }
    if (decisionRpcs.empty() && nextParticipantEntry == pList->end())
        goalReached = true;
}

/**
 * Polls waiting for the decision task to complete.
 */
void
TxRecoveryManager::DecisionTask::wait()
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
TxRecoveryManager::DecisionTask::DecisionRpc::DecisionRpc(Context* context,
        Transport::SessionRef session,
        DecisionTask* task)
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
TxRecoveryManager::DecisionTask::DecisionRpc::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        retryRequest();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
TxRecoveryManager::DecisionTask::DecisionRpc::handleTransportError()
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
TxRecoveryManager::DecisionTask::DecisionRpc::send()
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
TxRecoveryManager::DecisionTask::DecisionRpc::appendOp(
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
TxRecoveryManager::DecisionTask::DecisionRpc::retryRequest()
{
    for (uint32_t i = 0; i < reqHdr->participantCount; i++) {
        Participant* entry = &(*ops[i]);
        context->masterService->objectFinder.flush(entry->tableId);
        entry->state = Participant::PENDING;
    }
    task->nextParticipantEntry = task->pList->begin();
}

/**
 * Process any decision rpcs that have completed.  Factored out mostly for ease
 * of testing.
 */
void
TxRecoveryManager::DecisionTask::processDecisionRpcs()
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
TxRecoveryManager::DecisionTask::sendDecisionRpc()
{
    // Issue an additional rpc.
    DecisionRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextParticipantEntry != pList->end(); nextParticipantEntry++) {
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
 * Constructor for the RequestAbortTask.
 *
 * \param context
 *      Overall information about this server.
 * \param leaseId
 *      Id of the lease with which the recovering transaction was started.
 * \param pList
 *      Pointer to the list of participants that should be consulted regarding
 *      this abort request.  No items should be added or removed from this list
 *      during the lifetime of this task.  Must not be NULL.
 */
TxRecoveryManager::RequestAbortTask::RequestAbortTask(
        Context* context, uint64_t leaseId, ParticipantList* pList)
    : context(context)
    , leaseId(leaseId)
    , goalReached(false)
    , decision(WireFormat::TxDecision::COMMIT)
    , pList(pList)
    , nextParticipantEntry(pList->begin())
    , requestAbortRpcs()
{}

/**
 * Make incremental progress towards collecting all the votes to find out
 * whether this recovering transaction should be aborted or not.
 */
void
TxRecoveryManager::RequestAbortTask::performTask()
{
    if (!goalReached) {
        processRequestAbortRpcs();
        sendRequestAbortRpc();
    }
    if (requestAbortRpcs.empty() && nextParticipantEntry == pList->end())
        goalReached = true;
}

/**
 * Polls waiting for the RequestAbortTask to complete.
 *
 * \return
 *      The decision of whether the recovering transaction should be committed
 *      or aborted.
 */
WireFormat::TxDecision::Decision
TxRecoveryManager::RequestAbortTask::wait()
{
    while (!isReady()) {
        performTask();
    }
    return decision;
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
TxRecoveryManager::RequestAbortTask::RequestAbortRpc::RequestAbortRpc(
            Context* context,
            Transport::SessionRef session,
            RequestAbortTask* task)
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
TxRecoveryManager::RequestAbortTask::RequestAbortRpc::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        retryRequest();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
TxRecoveryManager::RequestAbortTask::RequestAbortRpc::handleTransportError()
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
TxRecoveryManager::RequestAbortTask::RequestAbortRpc::send()
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
TxRecoveryManager::RequestAbortTask::RequestAbortRpc::appendOp(
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
TxRecoveryManager::RequestAbortTask::RequestAbortRpc::retryRequest()
{
    for (uint32_t i = 0; i < reqHdr->participantCount; i++) {
        Participant* entry = &(*ops[i]);
        context->masterService->objectFinder.flush(entry->tableId);
        entry->state = Participant::PENDING;
    }
    task->nextParticipantEntry = task->pList->begin();
}

/**
 * Process any request abort rpcs that have completed.  Factored out mostly for
 * ease of testing.
 */
void
TxRecoveryManager::RequestAbortTask::processRequestAbortRpcs()
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
TxRecoveryManager::RequestAbortTask::sendRequestAbortRpc()
{
    // Issue an additional rpc.
    RequestAbortRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextParticipantEntry != pList->end(); nextParticipantEntry++) {
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


} // end RAMCloud
