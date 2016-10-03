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

#ifndef RAMCLOUD_TRANSACTIONMANAGER_H
#define RAMCLOUD_TRANSACTIONMANAGER_H

#include <deque>
#include <map>
#include <queue>
#include <unordered_map>
#include <utility>

#include "Common.h"
#include "Log.h"
#include "MasterClient.h"
#include "ParticipantList.h"
#include "PreparedOp.h"
#include "SpinLock.h"
#include "TransactionId.h"
#include "UnackedRpcResults.h"
#include "WireFormat.h"
#include "WorkerTimer.h"
#include "Unlock.h"

namespace RAMCloud {

class ObjectManager;

/**
 * The TransactionManager provides access to and controls the lifetime of
 * server-side transaction data stored in the log which includes
 * PreparedOp and ParticipantList objects.  It also serves to trigger
 * transaction recovery if transactions aren't completed in a timely manner.
 */
class TransactionManager {
  PUBLIC:
    TransactionManager(Context* context,
                       AbstractLog* log,
                       UnackedRpcResults* unackedRpcResults);
    ~TransactionManager();

    // Transaction methods
    Status registerTransaction(ParticipantList& participantList,
                               Buffer& assembledParticipantList,
                               AbstractLog* log);
    void markTransactionRecovered(TransactionId txId);

    // Participant List methods
    void relocateParticipantList(Buffer& oldBuffer,
                                 Log::Reference oldReference,
                                 LogEntryRelocator& relocator);

    // Prepared Op methods
    void bufferOp(TransactionId txId, uint64_t rpcId, uint64_t newOpPtr);
    void removeOp(uint64_t leaseId, uint64_t rpcId);
    uint64_t getOp(uint64_t leaseId, uint64_t rpcId);
    void updateOpPtr(uint64_t leaseId, uint64_t rpcId, uint64_t newOpPtr);
    void markOpDeleted(uint64_t leaseId, uint64_t rpcId);
    bool isOpDeleted(uint64_t leaseId, uint64_t rpcId);
    void regrabLocksAfterRecovery(ObjectManager* objectManager);

    /**
     * This class is used to prevent the garbage collection of a (soon to be)
     * registered transaction.  Creating a Protector object prevents the removal
     * the specified transaction registration until the Protector is destroyed.
     * When multiple instances of a Protector are created to protect a single
     * transaction registration, removal of the transaction will not resume
     * until all relevant instances of the Protector have been deleted.
     *
     * This is used during transaction prepare phase processing to ensure that
     * registered transactions are not removed before related prepare operations
     * can be buffered.
     */
    class Protector {
      PUBLIC:
        Protector(TransactionManager* transactionManager, TransactionId txId);
        ~Protector();

      PRIVATE:
        // TransactionManager that owns the transaction's registration.
        TransactionManager* transactionManager;

        // Id of the transaction that should be protected.
        TransactionId txId;

        DISALLOW_COPY_AND_ASSIGN(Protector);
    };

  PRIVATE:
    /**
     * Monitor-style lock. Any operation on internal data structure should
     * hold this lock.
     */
    std::mutex mutex;
    typedef std::lock_guard<std::mutex> Lock;

    /// RAMCloud context needed to support the used of WorkerTimers and RPCs.
    Context* context;

    /// Pointer to the log in which the tracked ParticipantList log entries will
    /// be stored; used to lookup and free stored ParticipantList log entries.
    AbstractLog* log;

    /// UnackedRpcResults holds the transaction prepare votes that need to be
    /// kept around to ensure recovery works correctly.
    UnackedRpcResults* unackedRpcResults;

    /**
     * Represents a transaction that is in the process of being committed on
     * this master.  An instance of this object is used to ensure that:
     *  (1) the corresponding transaction will be recovered if the transaction
     *      isn't completed in a timely manner, and
     *  (2) the data needed to complete transaction recovery is available until
     *      all the participant masters have committed or recovered.
     * To accomplish this, the TransactionManager will maintain an instance of
     * this object as long as the transaction is not known to have completed.
     */
    class InProgressTransaction : public WorkerTimer {
        friend class TransactionManager;
      PUBLIC:
        InProgressTransaction(TransactionManager* manager,
                              TransactionId txId,
                              TransactionManager::Lock& lock);
        ~InProgressTransaction();
        virtual void handleTimerEvent();
        bool isComplete(TransactionManager::Lock& lock);

        /// Number of prepared but uncommitted ops for this transaction.
        int preparedOpCount;
      PRIVATE:
        /// The manager that owns this transaction progress record.
        TransactionManager* manager;

        /// Id of the transaction that is in progress.
        TransactionId txId;

        /// Log Reference to the ParticipantList of this transaction
        AbstractLog::Reference participantListLogRef;

        /// Flag indicating whether or not recovery for this transaction has
        /// reached a safe point where we can consider this transaction
        /// no longer in progress.
        bool recovered;

        /// Prevents this transaction from being garbage collected.  Used by the
        /// TransactionManager::Protector during the transaction prepare phase
        /// to ensure the transaction is not removed before the prepare request
        /// is processed.
        int cleaningDisabled;

        /// TxHintFailed RPC to be issued asynchronously if the transaction does
        /// not complete within timeoutCycles
        Tub<TxHintFailedRpc> txHintFailedRpc;

        /// How long (in cycles) this transaction should wait before it should
        /// request that recovery be initiated.
        uint64_t timeoutCycles;

        /// The SingleClientRpcResultsProtector object ensures that RPC results
        /// or this transaction are kept around until this transaction is no
        /// longer considered in progress.
        UnackedRpcResults::SingleClientProtector rpcResultsProtector;

        DISALLOW_COPY_AND_ASSIGN(InProgressTransaction);
    };

    /**
     * Helper class responsible from removing completed transactions from the
     * TransactionManager::TransactionRegistry and deleting the corresponding
     * InProgressTransaction object.
     *
     * Used to avoid having InProgressTransaction delete themselves in their
     * timer handler which results in deadlock (see WorkerTimer::stopInternal).
     */
    class TransactionRegistryCleaner : public WorkerTimer {
      PUBLIC:
        /// TransactionRegistryCleaner constructor.
        explicit TransactionRegistryCleaner(TransactionManager* manager)
            : WorkerTimer(manager->context->dispatch)
            , manager(manager)
            , cleaningQueue()
        {}

        virtual void handleTimerEvent();
        void queueForCleaning(TransactionId txId,
                              TransactionManager::Lock& lock);
      PRIVATE:
        /// TransactionManager who's registry is to be cleaned.
        TransactionManager* manager;

        /// Queue of InProgressTransactions that will be cleaned if they
        /// are considered "complete."
        std::queue<TransactionId, std::deque<TransactionId> > cleaningQueue;

        DISALLOW_COPY_AND_ASSIGN(TransactionRegistryCleaner);
    };

    /**
     * Wrapper for the pointer to PreparedOp in order to reference count the
     * transaction to which this op belongs.
     */
    class PreparedItem {
      public:
        /**
         * Default constructor.
         *
         * The TransactionManager monitor lock should be held while calling this
         * constructor.
         *
         * \param transaction
         *      The transaction to which this prepared op belongs.
         * \param newOpPtr
         *      Log reference to PreparedOp in the log.
         */
        PreparedItem(
                InProgressTransaction* transaction,
                uint64_t newOpPtr)
            :  transaction(transaction)
            , newOpPtr(newOpPtr)
        {
            transaction->preparedOpCount++;
        }

        /**
         * Default destructor.
         *
         * The TransactionManager monitor lock should be held while calling this
         * destructor.
         */
        ~PreparedItem() {
            transaction->preparedOpCount--;
        }

        /// The transaction to which this prepared op belongs.
        InProgressTransaction* transaction;

        /// Log reference to PreparedOp in the log.
        uint64_t newOpPtr;
      private:
        DISALLOW_COPY_AND_ASSIGN(PreparedItem);
    };

    /// mapping from <LeaseId, RpcId> to PreparedItem.
    std::map<std::pair<uint64_t, uint64_t>, PreparedItem*> items;
    typedef std::map<std::pair<uint64_t, uint64_t>, PreparedItem*> ItemsMap;

    /**
     * Keeps track of all currently in-progress transactions; this map should
     * contain a single entry for each transaction currently being processed by
     * this master.
     */
    typedef std::unordered_map<TransactionId,
                               InProgressTransaction*,
                               TransactionId::Hasher> TransactionRegistry;
    TransactionRegistry transactions;

    /**
     * Cleans complete transactions from the TransactionRegistry.
     */
    TransactionRegistryCleaner cleaner;

    InProgressTransaction* getTransaction(TransactionId txId,
                                                    Lock& lock);
    InProgressTransaction* getOrAddTransaction(TransactionId txId,
                                                    Lock& lock);

    DISALLOW_COPY_AND_ASSIGN(TransactionManager);
};

}

#endif // RAMCLOUD_TRANSACTIONMANAGER_H
