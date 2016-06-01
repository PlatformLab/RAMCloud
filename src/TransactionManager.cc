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
 */
TransactionManager::TransactionManager(
        Context* context,
        UnackedRpcResults* unackedRpcResults)
    : mutex()
    , context(context)
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
 * being prepared and when a participant list object is recovered/migrated.
 *
 * The lifetime of the participant list over approximates the lifetime of an
 * in-progress transaction thus the participant list for this transaction must
 * be in the log and accessible via UnackedRpcResults when calling this method.
 *
 * \param participantList
 *      The ParticipantList of the transaction to be registered.  Used to get
 *      the TransactionId and size of the transaction.  Also ensures the caller
 *      has the full list.
 */
void
TransactionManager::registerTransaction(ParticipantList& participantList)
{
    Lock lock(mutex);
    TransactionId txId = participantList.getTransactionId();
    InProgressTransaction* transaction = getOrAddTransaction(txId, lock);

    // Set the timeout for this transaction.  Should be longer than the amount
    // of time we expect to this transaction takes to complete.
    if (transaction->timeoutCycles == 0) {
        transaction->timeoutCycles = participantList.getParticipantCount() *
                                     Cycles::fromMicroseconds(50000);
    }

    if (!transaction->isRunning()) {
        transaction->start(Cycles::rdtsc() + transaction->timeoutCycles);
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
 */
TransactionManager::InProgressTransaction::InProgressTransaction(
        TransactionManager* manager,
        TransactionId txId)
    : WorkerTimer(manager->context->dispatch)
    , preparedOpCount(0)
    , manager(manager)
    , txId(txId)
    , recoveryDecided(false)
    , txHintFailedRpc()
    , timeoutCycles(0)
    , holdOnClientRecord(manager->unackedRpcResults, txId.clientLeaseId)
{
}

/**
 * InProgressTransaction destructor.
 */
TransactionManager::InProgressTransaction::~InProgressTransaction()
{
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
        transaction = new InProgressTransaction(this, txId);
        transactions[txId] = transaction;
    }
    assert(transaction != NULL);
    return transaction;
}



} // namespace RAMCloud
