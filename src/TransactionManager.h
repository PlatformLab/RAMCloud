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

#include <map>
#include <unordered_map>
#include <utility>
#include "Common.h"
#include "ParticipantList.h"
#include "PreparedOp.h"
#include "SpinLock.h"
#include "WireFormat.h"
#include "WorkerTimer.h"
#include "MasterClient.h"
#include "TransactionId.h"

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
    explicit TransactionManager(Context* context);
    ~TransactionManager();

    // Transaction methods
    void registerTransaction(ParticipantList& participantList);
    void markTransactionRecovered(TransactionId txId);

    // Prepared Op methods
    void bufferOp(TransactionId txId, uint64_t rpcId, uint64_t newOpPtr,
                     bool inRecovery = false);
    void removeOp(uint64_t leaseId, uint64_t rpcId);
    uint64_t getOp(uint64_t leaseId, uint64_t rpcId);
    void updateOpPtr(uint64_t leaseId, uint64_t rpcId, uint64_t newOpPtr);
    void markOpDeleted(uint64_t leaseId, uint64_t rpcId);
    bool isOpDeleted(uint64_t leaseId, uint64_t rpcId);
    void regrabLocksAfterRecovery(ObjectManager* objectManager);

  PRIVATE:
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
      PUBLIC:
        explicit InProgressTransaction(Context* context)
            : WorkerTimer(context->dispatch)
            , preparedOpCount(0)
        {}

        ~InProgressTransaction() {}

        /// Number of prepared but uncommitted ops for this transaction.
        int preparedOpCount;
      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(InProgressTransaction);
    };

    /**
     * Wrapper for the pointer to PreparedOp with WorkerTimer.
     * This represents an active locking on an object, and its timer
     * make sure transaction recovery starts after timeout.
     */
    class PreparedItem : public WorkerTimer {
      public:
        /**
         * Default constructor.
         *
         * The TransactionManager monitor lock should be held while calling this
         * constructor.
         *
         * \param context
         *      RAMCloud context to work on.
         * \param transaction
         *      The transaction to which this prepared op belongs.
         * \param newOpPtr
         *      Log reference to PreparedOp in the log.
         */
        PreparedItem(
                Context* context,
                InProgressTransaction* transaction,
                uint64_t newOpPtr)
            : WorkerTimer(context->dispatch)
            , context(context)
            , transaction(transaction)
            , newOpPtr(newOpPtr)
            , txHintFailedRpc()
        {
            transaction->preparedOpCount++;
        }

        /**
         * Default destructor. Stops WorkerTimer and waits for running handler
         * for safe destruction.
         *
         * The TransactionManager monitor lock should be held while calling this
         * destructor.
         */
        ~PreparedItem() {
            transaction->preparedOpCount--;
            stop();
        }

        //TODO(seojin): handler may not protected from destruction.
        //              Resolve this later.
        virtual void handleTimerEvent();

        /// Shared RAMCloud information.
        Context* context;

        /// The transaction to which this prepared op belongs.
        InProgressTransaction* transaction;

        /// Log reference to PreparedOp in the log.
        uint64_t newOpPtr;

        /// TxHintFailed RPC to be issued asynchronously if the transaction does
        /// not complete within TX_TIMEOUT_US
        Tub<TxHintFailedRpc> txHintFailedRpc;

        /// Timeout value for the active PreparedOp (lock record).
        /// This timeout should be larger than 2*RTT to give enough time for
        /// a client to complete transaction.
        /// In case of client failure, smaller timeout makes the initiation of
        /// transaction recovery faster, shortening object lock time.
        /// However, using small timeout must be carefully chosen since large
        /// server span of a transaction increases the time gap between the
        /// first phase and the second phase of a transaction.
        static const uint64_t TX_TIMEOUT_US = 50000;
      private:
        DISALLOW_COPY_AND_ASSIGN(PreparedItem);
    };

    Context* context;

    /**
     * Monitor-style lock. Any operation on internal data structure should
     * hold this lock.
     */
    std::mutex mutex;
    typedef std::lock_guard<std::mutex> Lock;

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

    InProgressTransaction* getRegisteredTransaction(TransactionId txId,
                                                    Lock& lock);
    InProgressTransaction* getOrRegisterTransaction(TransactionId txId,
                                                    Lock& lock);

    DISALLOW_COPY_AND_ASSIGN(TransactionManager);
};

}

#endif // RAMCLOUD_TRANSACTIONMANAGER_H
