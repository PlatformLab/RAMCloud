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

#ifndef RAMCLOUD_TXRECOVERYMANAGER_H
#define RAMCLOUD_TXRECOVERYMANAGER_H

#include <list>
#include <set>

#include "Common.h"
#include "RpcWrapper.h"
#include "TxDecisionRecord.h"
#include "WireFormat.h"
#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * A TxRecoveryManager runs on a manages ongoing transaction recoveries to which
 * it has been assigned.  When servers, get worried that a transaction may have
 * failed to run to completion, transaction recoveries are initiated.  The
 * running of those recoveries is handled by this module.
 *
 * Requesting additional recoveries is thread-safe with the processing of the
 * managed recoveries.  Recoveries are run eagerly but in the background with
 * respect to the operations that request recoveries.
 */
class TxRecoveryManager : public WorkerTimer {
  PUBLIC:
    explicit TxRecoveryManager(Context* context);

    virtual void handleTimerEvent();
    void handleTxHintFailed(Buffer* rpcReq);
    bool isTxDecisionRecordNeeded(TxDecisionRecord& record);
    bool recoverRecovery(TxDecisionRecord& record);

  PRIVATE:
    /// Forward declaration.
    class RecoveryTask;

    /// Used as a monitor style lock on this module.
    SpinLock lock;
    typedef std::lock_guard<SpinLock> Lock;

    /// Overall information about this server.
    Context* context;

    /// Uniquely identifies those transactions whose recoveries have started
    /// but not finished.
    struct RecoveryId {
        /// Id of the lease with which the recovering transaction was issued.
        uint64_t leaseId;
        /// Id of the recovering transaction.
        uint64_t transactionId;

        /**
         * The operator < is overridden to implement the
         * correct comparison for the recoveringIds.
         */
        bool operator<(const RecoveryId& recoveryId) const {
            return leaseId < recoveryId.leaseId ||
                    (leaseId == recoveryId.leaseId
                            && transactionId < recoveryId.transactionId);
        }
    };

    /// Keeps track of those transactions whose recoveries have started but not
    /// finished.  (Should be able to use a std::map once emplace is supported.)
    std::set<RecoveryId> recoveringIds;
    typedef std::list<RecoveryTask> RecoveryList;
    /// Contains all currently active transactions.
    RecoveryList recoveries;

    /**
     * Used to keep track of an individual participants of a transaction.
     */
    struct Participant {
        uint64_t tableId;
        uint64_t keyHash;
        uint64_t rpcId;
        enum State { PENDING, ABORT, DECIDE } state;

        Participant(uint64_t tableId, uint64_t keyHash, uint64_t rpcId)
            : tableId(tableId)
            , keyHash(keyHash)
            , rpcId(rpcId)
            , state(PENDING)
        {
        }
    };
    typedef std::list<Participant> ParticipantList;

    class RecoveryTask {
      PUBLIC:
        RecoveryTask(Context* context, uint64_t leaseId, uint64_t transactionId,
                Buffer& participantBuffer, uint32_t participantCount,
                uint32_t offset = 0);
        RecoveryTask(Context* context, TxDecisionRecord& record);

        RecoveryId getId() { return {leaseId, participants.begin()->rpcId}; }
        bool isReady() { return (state == State::DONE); }
        void performTask();

      PRIVATE:
        /// Encapsulates common state and methods of the Decision and
        /// RequestAbort RPCs.
        class TxRecoveryRpcWrapper : public RpcWrapper {
          PUBLIC:
            TxRecoveryRpcWrapper(Context* context,
                    Transport::SessionRef session,
                    RecoveryTask* task,
                    uint32_t responseHeaderLength);
            ~TxRecoveryRpcWrapper() {}
            void send();

          PROTECTED:
            bool appendOp(ParticipantList::iterator opEntry,
                          Participant::State state);
            bool checkStatus();
            bool handleTransportError();
            void markOpsForRetry();

            /// Overall server state information.
            Context* context;

            /// Task that issued this rpc.
            RecoveryTask* task;

            /// Information about all of the ops that are being requested
            /// in this RPC.
#ifdef TESTING
            static const uint32_t MAX_OBJECTS_PER_RPC = 3;
#else
            static const uint32_t MAX_OBJECTS_PER_RPC = 75;
#endif
            ParticipantList::iterator ops[MAX_OBJECTS_PER_RPC];

            // Pointer to participantCount field in request header.
            // Used to provide access to field of both RequestAbort and Decision
            // headers to common methods.
            uint32_t* participantCount;

            DISALLOW_COPY_AND_ASSIGN(TxRecoveryRpcWrapper);
        };

        /// Encapsulates the state of a single Decision RPC sent to a single
        /// server.
        class DecisionRpc : public TxRecoveryRpcWrapper {
          PUBLIC:
            DecisionRpc(Context* context, Transport::SessionRef session,
                        RecoveryTask* task);
            ~DecisionRpc() {}
            bool appendOp(ParticipantList::iterator opEntry);
            void wait();

          PROTECTED:
            /// Header for the RPC (used to update count as objects are added).
            WireFormat::TxDecision::Request* reqHdr;

            DISALLOW_COPY_AND_ASSIGN(DecisionRpc);
        };

        /// Encapsulates the state of a single RequestAbortRpc sent to a single
        /// server.
        class RequestAbortRpc : public TxRecoveryRpcWrapper {
          PUBLIC:
            RequestAbortRpc(Context* context, Transport::SessionRef session,
                    RecoveryTask* task);
            ~RequestAbortRpc() {}
            bool appendOp(ParticipantList::iterator opEntry);
            WireFormat::TxPrepare::Vote wait();

          PROTECTED:
            /// Header for the RPC (used to update count as objects are added).
            WireFormat::TxRequestAbort::Request* reqHdr;

            DISALLOW_COPY_AND_ASSIGN(RequestAbortRpc);
        };

        /// Overall server information about the server running this recovery.
        Context* context;
        /// Id of the lease associated with the transaction being recovered.
        uint64_t leaseId;
        /// Id of the recovering transaction.
        uint64_t transactionId;
        /// Current phase of the recovery.
        enum State {REQUEST_ABORT, DECIDE, DONE} state;
        /// The decision that the transaction will be driven to.
        WireFormat::TxDecision::Decision decision;
        /// List of the participants of the transaction being recovered.
        ParticipantList participants;
        /// Iterator into the participant list used to keep track of how much
        /// process has been made.
        ParticipantList::iterator nextParticipantEntry;
        /// List of outstanding decision rpcs.
        std::list<DecisionRpc> decisionRpcs;
        /// List of outstanding request abort rpcs.
        std::list<RequestAbortRpc> requestAbortRpcs;

        void processDecisionRpcResults();
        void sendDecisionRpc();
        void processRequestAbortRpcResults();
        void sendRequestAbortRpc();

        string toString();

        DISALLOW_COPY_AND_ASSIGN(RecoveryTask);
    };
    DISALLOW_COPY_AND_ASSIGN(TxRecoveryManager);
};

} // end RAMCloud

#endif  /* RAMCLOUD_TXRECOVERYMANAGER_H */

