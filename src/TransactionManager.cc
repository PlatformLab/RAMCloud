/* Copyright (c) 2015-2016 Stanford University
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

#include "TransactionManager.h"

#include "LeaseCommon.h"
#include "MasterClient.h"
#include "MasterService.h"
#include "ObjectManager.h"

namespace RAMCloud {

/**
 * Construct TransactionManager.
 *
 * \param context
 *      Overall information about the RAMCloud server and provides access to
 *      the dispatcher.
 * \param log
 *      Pointer to the log in which the tracked ParticipantList log entries will
 *      be stored; used to lookup and free stored ParticipantList log entries.
 *      Should point to the same underlying log that is passed as a parameter to
 *      the registerTransaction method.
 * \param unackedRpcResults
 *      Pointer to the UnackedRpcResults which holds the transaction prepare
 *      votes that need to be kept around to ensure recovery works correctly.
 */
TransactionManager::TransactionManager(Context* context,
                                       AbstractLog* log,
                                       UnackedRpcResults* unackedRpcResults)
    : mutex()
    , context(context)
    , log(log)
    , unackedRpcResults(unackedRpcResults)
    , items()
    , transactions()
{
}

/**
 * Default destructor.
 */
TransactionManager::~TransactionManager()
{
    Lock lock(mutex);
    for (auto it = items.begin(); it != items.end(); ++it) {
        PreparedItem* item = it->second;
        delete item;
    }

    for (auto it = transactions.begin(); it != transactions.end(); ++it) {
        InProgressTransaction* tx = it->second;
        delete tx;
    }
}

/**
 * Register a transaction to indicate that it is in-progress.  Registration
 * ensures that a transaction will be recovered in the event of a crashed or
 * unexpectedly slow client.
 *
 * This method call is idempotent and can be safely called multiple times on the
 * same transaction.  This method should be called whenever a transaction is
 * being prepared or when a participant list object is recovered/migrated.
 *
 * \param participantList
 *      The ParticipantList of the transaction to be registered.  Used to get
 *      the TransactionId and size of the transaction.  Also ensures the caller
 *      has the full list.
 * \param assembledParticipantList
 *      Contains a fully serialized copy of the participantList which will be
 *      appended to the log.  This requires the caller perform the serialization
 *      but in some cases it is already done.
 * \param log
 *      The log in which the transaction participant list will be persisted.
 * \returns
 *      STATUS_OK if the transaction could be registered.
 *      STATUS_RETRY otherwise.
 */
Status
TransactionManager::registerTransaction(ParticipantList& participantList,
                                        Buffer& assembledParticipantList,
                                        AbstractLog* log)
{
    Lock lock(mutex);

    TransactionId txId = participantList.getTransactionId();
    InProgressTransaction* transaction = getOrAddTransaction(txId, lock);

    if (transaction->participantListLogRef == AbstractLog::Reference()) {
        // Write the ParticipantList into the Log, update the table.
        if (!log->append(LOG_ENTRY_TYPE_TXPLIST,
                         assembledParticipantList,
                         &transaction->participantListLogRef))
        {
            // The log is out of space. Tell the client to retry and hope
            // that the cleaner makes space soon.
            return STATUS_RETRY;
        }

        // Participant List records are not accounted for in the table stats.
        // The assumption is that the Participant List records should occupy a
        // relatively small fraction of the server's log and thus should not
        // significantly affect table stats estimate.
    } else {
        TEST_LOG("Skipping duplicate call to register transaction <%lu, %lu>",
                 txId.clientLeaseId, txId.clientTransactionId);
    }

    // Set the timeout for this transaction.  Should be longer than the amount
    // of time we expect to this transaction takes to complete.
    if (transaction->timeoutCycles == 0) {
        transaction->timeoutCycles = participantList.getParticipantCount() *
                                     Cycles::fromMicroseconds(50000);
    }

    // Start the timer to give the client some time before the timeout triggers.
    // If this is a duplicate call, restart the timer to give the client more
    // time since the client is making progress.  However, the timer should not
    // be reset when there is an outstanding txHintFailedRpc since the timer
    // should be scheduled to poll for the result (see the handleTimerEvent
    // method in InProgressTransaction).
    if (!transaction->txHintFailedRpc) {
        transaction->start(Cycles::rdtsc() + transaction->timeoutCycles);
    }

    return STATUS_OK;
}

/**
 * Signal that transaction recovery has reached a decision and that this
 * transaction can be considered complete as soon as all prepared operations
 * have been processed.  Needed to ensure that transaction meta-data is kept
 * available while recoveries may still be required.
 *
 * \param txId
 *      Id of the transaction that can be marked recovered.
 */
void
TransactionManager::markTransactionRecovered(TransactionId txId)
{
    Lock lock(mutex);
    InProgressTransaction* transaction = getTransaction(txId, lock);
    if (transaction != NULL) {
        transaction->recovered = true;
    }
}

/**
 * Relocate a ParticipantList entry that is being cleaned and update the
 * TrasactionManager accordingly. The cleaner invokes this method for every
 * ParticipantList entry it comes across when processing a segment. If the entry
 * is no longer needed, nothing will be done. If it is needed, the provided
 * relocator will be used to copy the entry to a new location and the entry's
 * reference in the TrasactionManager will be updated before returning.
 *
 * It is possible that relocation may fail (because more memory needs to be
 * allocated). In this case, the method will just return. The cleaner will
 * note the failure, allocate more memory, and try again.
 *
 * \param oldBuffer
 *      Buffer pointing to the ParticipantList entry's current location, which
 *      will be invalid after this call returns.
 * \param oldReference
 *      Reference to the old ParticipantList entry in the log.  Used to detect
 *      duplicate entries that may not be needed.
 * \param relocator
 *      The relocator is used to copy a live entry to a new location in the
 *      log and get a reference to that new location. If the entry is not
 *      needed, the relocator will not be used.
 */
void
TransactionManager::relocateParticipantList(Buffer& oldBuffer,
                                            Log::Reference oldReference,
                                            LogEntryRelocator& relocator)
{
    Lock lock(mutex);

    ParticipantList participantList(oldBuffer);
    TransactionId txId = participantList.getTransactionId();
    InProgressTransaction* transaction = getTransaction(txId, lock);

    // See if this transaction is still going on and if the participant list
    // is not a duplicate.
    if (transaction != NULL
            && transaction->participantListLogRef == oldReference) {
        // Try to relocate it. If it fails, just return. The cleaner will
        // allocate more memory and retry.
        if (!relocator.append(LOG_ENTRY_TYPE_TXPLIST, oldBuffer))
            return;

        transaction->participantListLogRef = relocator.getNewReference();
    } else {
        // Participant List will be dropped/"cleaned"

        // Participant List records are not accounted for in the table stats.
        // The assumption is that the Participant List records should occupy a
        // relatively small fraction of the server's log and thus should not
        // significantly affect table stats estimate.
    }
}

/**
 * Add a pointer to the referenced preparedOp into lookup table.
 *
 * \param txId
 *      Identifier for the transaction that includes the perparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 * \param newOpPtr
 *      Log::Reference to preparedOp in main log.
 * \param isRecovery
 *      Caller should set this flag true if it is adding entries while
 *      replaying recovery segments; With true value, it will not start
 *      WorkerTimer. The timers will start during final stage of recovery
 *      by #regrabLocksAfterRecovery().
 */
void
TransactionManager::bufferOp(TransactionId txId,
                             uint64_t rpcId,
                             uint64_t newOpPtr,
                             bool isRecovery)
{
    Lock lock(mutex);

    assert(items.find(std::make_pair(txId.clientLeaseId, rpcId))
            == items.end());
    InProgressTransaction* transaction = getOrAddTransaction(txId, lock);
    PreparedItem* item = new PreparedItem(context, transaction, newOpPtr);
    items[std::make_pair(txId.clientLeaseId, rpcId)] = item;
    if (!isRecovery) {
        item->start(Cycles::rdtsc() +
                    Cycles::fromMicroseconds(PreparedItem::TX_TIMEOUT_US));
    }
}

/**
 * Remove a pointer to preparedOp from lookup table.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 */
void
TransactionManager::removeOp(uint64_t leaseId,
                             uint64_t rpcId)
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it != items.end()) {
        delete it->second;
        items.erase(it);
    }
}

/**
 * Return a log reference to the preparedOp saved by a previous call to
 * TransactionManager::bufferOp().
 *
 * During recovery, ObjectManager::replaySegment() must check
 * TransactionManager::isDeleted() is false before invoking this method for
 * checking whether the PreparedOp log entry is already in log.
 * (isDeleted() == true already means no need to replay the PreparedOp.)
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 *
 * \return
 *      Log::Reference::toInteger() value for the preparedOp in log.
 *      0 is returned if we cannot find a reference to PreparedOp previously
 *      buffered.
 */
uint64_t
TransactionManager::getOp(uint64_t leaseId, uint64_t rpcId)
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it == items.end()) {
        return 0;
    } else {
        // During recovery, must check isDeleted before using this method since
        // we set it->second = NULL (instead of pointer to PreparedItem) to mark
        // the PreparedOpTombstone for this PreparedOp is seen.
        // It is intentionally left as assertion error. (instead of return 0.)
        // After the end of recovery all NULL entries should be removed, and
        // this assertion check will fail if there's a bug in code.
        assert(it->second);

        uint64_t newOpPtr = it->second->newOpPtr;
        return newOpPtr;
    }
}

/**
 * Update a pointer to preparedOp into lookup table.
 * This method is used by LogCleaner to relocate Segment.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 * \param newOpPtr
 *      Log::Reference to preparedOp in main log.
 */
void
TransactionManager::updateOpPtr(uint64_t leaseId,
                                uint64_t rpcId,
                                uint64_t newOpPtr)
{
    Lock lock(mutex);
    PreparedItem* item = items[std::make_pair(leaseId, rpcId)];
    item->newOpPtr = newOpPtr;
}

/**
 * Handles timeout of the staged preparedOp. Requests to initiate
 * recovery of whole transaction.
 */
void
TransactionManager::PreparedItem::handleTimerEvent()
{
    // This timer handler asynchronously notifies the recovery manager that this
    // transaction is taking a long time and may have failed.  This handler may
    // be called multiple times to ensure the notification is delivered.
    //
    // Note: This notification was previously done in synchronously, but holding
    // the thread and worker timer resources while waiting for the notification
    // to be acknowledge could caused deadlock when the notification is sent to
    // the same server.

    // Construct and send the RPC if it has not been done.
    if (!txHintFailedRpc) {
        Buffer opBuffer;
        Log::Reference opRef(newOpPtr);
        context->getMasterService()->objectManager.getLog()->getEntry(
                opRef, opBuffer);
        PreparedOp op(opBuffer, 0, opBuffer.size());

        //TODO(seojin): RAM-767. op.participants can be stale while log
        //              cleaning. It is possible to cause invalid memory access.

        TransactionId txId = op.getTransactionId();

        UnackedRpcHandle participantListLocator(
                &context->getMasterService()->unackedRpcResults,
                {txId.clientLeaseId, 0, 0},
                txId.clientTransactionId,
                0);

        if (participantListLocator.isDuplicate()) {
            Buffer pListBuf;
            Log::Reference pListRef(participantListLocator.resultLoc());
            context->getMasterService()->objectManager.getLog()->getEntry(
                    pListRef, pListBuf);
            ParticipantList participantList(pListBuf);
            txId = participantList.getTransactionId();
            TEST_LOG("TxHintFailed RPC is sent to owner of tableId %lu and "
                    "keyHash %lu.",
                    participantList.getTableId(), participantList.getKeyHash());

            txHintFailedRpc.construct(context,
                    participantList.getTableId(),
                    participantList.getKeyHash(),
                    txId.clientLeaseId,
                    txId.clientTransactionId,
                    participantList.getParticipantCount(),
                    participantList.participants);
        } else {
            // Abort; there is no way to send the RPC if we can't find the
            // participant list.  Hopefully, some other server still has the
            // list.  Log this situation as it is a bug if it occurs.
            RAMCLOUD_LOG(WARNING, "Unable to find participant list record for "
                    "TxId (%lu, %lu); client transaction recovery could not be "
                    "requested.", txId.clientLeaseId, txId.clientTransactionId);
            return;
        }
    }

    // RPC should have been sent.
    if (!txHintFailedRpc->isReady()) {
        // If the RPC is not yet ready, reschedule the worker timer to poll for
        // the RPC's completion.
        this->start(0);
    } else {
        // The RPC is ready; "wait" on it as is convention.
        txHintFailedRpc->wait();
        txHintFailedRpc.destroy();

        // Wait for another TX_TIMEOUT_US before getting worried again and
        // resending the hint-failed notification.
        this->start(Cycles::rdtsc() + Cycles::fromMicroseconds(TX_TIMEOUT_US));
    }
}

/**
 * Acquire transaction locks in objectManager for each entries
 * in items. This should be called after replaying all segments
 * and before changing tablet status from RECOVERING to NORMAL.
 *
 * \param objectManager
 *      The pointer to objectManager which holds transaction LockTable.
 */
void
TransactionManager::regrabLocksAfterRecovery(ObjectManager* objectManager)
{
    Lock lock(mutex);
    ItemsMap::iterator it = items.begin();
    while (it != items.end()) {
        PreparedItem *item = it->second;

        if (item == NULL) { //Cleanup marks for deleted.
            items.erase(it++);
        } else {
            Buffer buffer;
            Log::Reference ref(item->newOpPtr);
            objectManager->getLog()->getEntry(ref, buffer);
            PreparedOp op(buffer, 0, buffer.size());
            objectManager->tryGrabTxLock(op.object, ref);

            if (!item->isRunning()) {
                item->start(Cycles::rdtsc() +
                        Cycles::fromMicroseconds(PreparedItem::TX_TIMEOUT_US));
            }

            ++it;
        }
    }
}

/**
 * Mark a specific preparedOp is deleted.
 * This method is used by replaySegment during recovery when it finds
 * a tombstone for PreparedOp.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 */
void
TransactionManager::markOpDeleted(uint64_t leaseId,
                                  uint64_t rpcId)
{
    Lock lock(mutex);
    assert(items.find(std::make_pair(leaseId, rpcId)) == items.end() ||
           items.find(std::make_pair(leaseId, rpcId))->second == NULL);
    items[std::make_pair(leaseId, rpcId)] = NULL;
}

/**
 * Check a specific preparedOp is marked as deleted.
 * This method is used by replaySegment during recovery of preparedOp.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 */
bool
TransactionManager::isOpDeleted(uint64_t leaseId,
                                uint64_t rpcId)
{
    Lock lock(mutex);

    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it == items.end()) {
        return false;
    } else {
        if (it->second == NULL) {
            return true;
        } else {
            return false;
        }
    }
}

/**
 * InProgressTransaction constructor; should also call registerAndStart to
 * complete the registration of the transaction.
 *
 * \param manager
 *      The TransactionManager that holds this InProgressTransaction record.
 * \param txId
 *      The id of this in progress transaction.
 * \param lock
 *      Used to ensure that caller has acquired TransactionManager::mutex.
 *      Not actually used by the method.
 */
TransactionManager::InProgressTransaction::InProgressTransaction(
        TransactionManager* manager,
        TransactionId txId,
        TransactionManager::Lock& lock)
    : WorkerTimer(manager->context->dispatch)
    , preparedOpCount(0)
    , manager(manager)
    , txId(txId)
    , participantListLogRef()
    , recovered(false)
    , txHintFailedRpc()
    , timeoutCycles(0)
    , holdOnClientRecord(manager->unackedRpcResults, txId.clientLeaseId)
{
}

/**
 * InProgressTransaction destructor.
 *
 * NOTE: Should always be called with the TransactionManager::mutex acquired.
 */
TransactionManager::InProgressTransaction::~InProgressTransaction()
{
    assert(!manager->mutex.try_lock());

    if (participantListLogRef != AbstractLog::Reference()) {
        manager->log->free(participantListLogRef);
    }
}

/**
 * This method is called when a transaction has timed out because it did not
 * complete in a timely manner.  This method will perform make some incremental
 * progress toward initiating transaction recovery and will reschedule itself
 * to run in the future if more work needs to be done.
 */
void
TransactionManager::InProgressTransaction::handleTimerEvent()
{
    // This timer handler asynchronously notifies the recovery manager that this
    // transaction is taking a long time and may have failed.  This handler may
    // be called multiple times to ensure the notification is delivered.
    //
    // Note: This notification was previously done in synchronously, but holding
    // the thread and worker timer resources while waiting for the notification
    // to be acknowledge could caused deadlock when the notification is sent to
    // the same server.  Furthermore, the transaction manager lock is held
    // during this call so delaying this method would prevent other transactions
    // from being processed.
    TransactionManager::Lock lock(manager->mutex);

    // Transaction is no longer in progress; delete this object.
    if (preparedOpCount <= 0) {
        if (manager->unackedRpcResults->isRpcAcked(txId.clientLeaseId,
                                                   txId.clientTransactionId)
                || recovered) {

            TEST_LOG("TxID <%lu,%lu> has completed; OK to clean.",
                txId.clientLeaseId, txId.clientTransactionId);

            // The transaction is complete
            manager->transactions.erase(txId);
            delete this;
            return;
        }
    }
    // Else, the transaction did not complete before it timed-out.

    // Construct and send the txHintFailedRpc if it has not been done.
    if (!txHintFailedRpc) {
        if (participantListLogRef == AbstractLog::Reference()) {
            // Abort; there is no way to send the RPC if we can't find the
            // participant list.  Hopefully, some other server still has the
            // list.  Log this situation as it is a bug if it occurs.
            RAMCLOUD_LOG(ERROR, "Unable to initiate transaction recovery for "
                    "TxId (%lu, %lu) because participant list record could not "
                    "be found; BUG; transaction timeout timer may have started "
                    "without first being registered.",
                    txId.clientLeaseId, txId.clientTransactionId);
            this->start(Cycles::rdtsc() + timeoutCycles);
            return;
        }

        Buffer pListBuf;
        manager->log->getEntry(participantListLogRef, pListBuf);
        ParticipantList participantList(pListBuf);
        assert(txId == participantList.getTransactionId());

        TEST_LOG("TxID <%lu,%lu> sending TxHintFailed RPC to owner of tableId "
                "%lu and keyHash %lu.",
                txId.clientLeaseId, txId.clientTransactionId,
                participantList.getTableId(), participantList.getKeyHash());

        txHintFailedRpc.construct(manager->context,
                participantList.getTableId(),
                participantList.getKeyHash(),
                txId.clientLeaseId,
                txId.clientTransactionId,
                participantList.getParticipantCount(),
                participantList.participants);
    }

    // RPC should have been sent.
    if (!txHintFailedRpc->isReady()) {
        // If the RPC is not yet ready, reschedule the worker timer to poll for
        // the RPC's completion.
        this->start(0);

        TEST_LOG("TxID <%lu,%lu> waiting for TxHintFailed RPC ack.",
                txId.clientLeaseId, txId.clientTransactionId);
    } else {
        // The RPC is ready; "wait" on it as is convention.
        txHintFailedRpc->wait();
        txHintFailedRpc.destroy();

        // Wait for another timeoutCycles before getting worried again and
        // resending the hint-failed notification.
        this->start(Cycles::rdtsc() + timeoutCycles);

        TEST_LOG("TxID <%lu,%lu> received ack for TxHintFailed RPC; will wait "
                "for next timeout.",
                txId.clientLeaseId, txId.clientTransactionId);
    }
}

/**
 * Returns a pointer to a InProgressTransaction object if it exists.
 *
 * \param txId
 *      Id of the transaction to be returned.
 * \param lock
 *      Used to ensure that caller has acquired TransactionManager::mutex.
 *      Not actually used by the method.
 * \return
 *      Pointer to a registered InProgressTransaction if it exists.
 *      NULL otherwise.
 */
TransactionManager::InProgressTransaction*
TransactionManager::getTransaction(TransactionId txId, Lock& lock)
{
    InProgressTransaction* transaction = NULL;
    TransactionRegistry::iterator it = transactions.find(txId);
    if (it != transactions.end()) {
        transaction = it->second;
    }
    return transaction;
}

/**
 * Returns a pointer to a InProgressTransaction object; constructs a new
 * InProgressTransaction if one doesn't already exist.
 *
 * \param txId
 *      Id of the transaction that is (or will be) in the transaction
 *      registry and returned.
 * \param lock
 *      Used to ensure that caller has acquired TransactionManager::mutex.
 *      Not actually used by the method.
 * \return
 *      Pointer to a registered InProgressTransaction.
 */
TransactionManager::InProgressTransaction*
TransactionManager::getOrAddTransaction(TransactionId txId, Lock& lock)
{
    InProgressTransaction* transaction = NULL;
    TransactionRegistry::iterator it = transactions.find(txId);
    if (it != transactions.end()) {
        transaction = it->second;
    } else {
        transaction = new InProgressTransaction(this, txId, lock);
        transactions[txId] = transaction;
    }
    assert(transaction != NULL);
    return transaction;
}



} // namespace RAMCloud
