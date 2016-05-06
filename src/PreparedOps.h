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

#ifndef RAMCLOUD_PREPAREDOPS_H
#define RAMCLOUD_PREPAREDOPS_H

#include <map>
#include <unordered_map>
#include <utility>
#include "Common.h"
#include "PreparedOp.h"
#include "SpinLock.h"
#include "WireFormat.h"
#include "WorkerTimer.h"
#include "MasterClient.h"
#include "TransactionId.h"

namespace RAMCloud {

class ObjectManager;

/**
 * A table for all PreparedOps for all currently executing transactions
 * on a server. Decision RPC handler fetches preparedOp from this table.
 */
class PreparedOps {
  PUBLIC:
    explicit PreparedOps(Context* context);
    ~PreparedOps();

    void bufferOp(uint64_t leaseId, uint64_t rpcId, uint64_t newOpPtr,
                     bool inRecovery = false);
    void removeOp(uint64_t leaseId, uint64_t rpcId);
    uint64_t getOp(uint64_t leaseId, uint64_t rpcId);
    void updatePtr(uint64_t leaseId, uint64_t rpcId, uint64_t newOpPtr);

    void markDeleted(uint64_t leaseId, uint64_t rpcId);
    bool isDeleted(uint64_t leaseId, uint64_t rpcId);
    void regrabLocksAfterRecovery(ObjectManager* objectManager);

  PRIVATE:
    /**
     * Wrapper for the pointer to PreparedOp with WorkerTimer.
     * This represents an active locking on an object, and its timer
     * make sure transaction recovery starts after timeout.
     */
    class PreparedItem : public WorkerTimer {
      public:
        /**
         * Default constructor.
         * \param context
         *      RAMCloud context to work on.
         * \param newOpPtr
         *      Log reference to PreparedOp in the log.
         */
        PreparedItem(Context* context, uint64_t newOpPtr)
            : WorkerTimer(context->dispatch)
            , context(context)
            , newOpPtr(newOpPtr)
            , txHintFailedRpc()
        {}

        /**
         * Default destructor. Stops WorkerTimer and waits for running handler
         * for safe destruction.
         */
        ~PreparedItem() {
            stop();
        }

        //TODO(seojin): handler may not protected from destruction.
        //              Resolve this later.
        virtual void handleTimerEvent();

        /// Shared RAMCloud information.
        Context* context;

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

    DISALLOW_COPY_AND_ASSIGN(PreparedOps);
};

}

#endif // RAMCLOUD_PREPAREDWRITES_H
