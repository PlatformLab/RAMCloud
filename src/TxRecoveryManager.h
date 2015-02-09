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
#include "WireFormat.h"

namespace RAMCloud {

class TxRecoveryManager {
  PUBLIC:
    explicit TxRecoveryManager(Context* context);
    virtual ~TxRecoveryManager();
    void handleTxHintFailed(Buffer* rpcReq);

  PRIVATE:
    /// Used as a monitor style lock on this module.
    SpinLock mutex;
    typedef std::lock_guard<SpinLock> Lock;

    /// Overall information about this server.
    Context* context;

    /// Uniquely identifies those transactions whose recoveries have started
    /// but not finished.
    struct RecoveryId {
        /// Id of the lease with which the recovering transaction was issued.
        uint64_t leaseId;
        /// The identifying rpcId of the recovering transaction; namely the
        /// rpcId of the first participant in the transaction.
        uint64_t rpcId;

        /**
         * The operator < is overridden to implement the
         * correct comparison for the recoveringIds.
         */
        bool operator<(const RecoveryId& recoveryId) const {
            return leaseId < recoveryId.leaseId ||
                (leaseId == recoveryId.leaseId && rpcId < recoveryId.rpcId);
        }
    };

    /// Keeps track of those transactions whose recoveries have started but not
    /// finished.
    std::set<RecoveryId> recoveringIds;

    /**
     * Used to keep track of an individual participants of a transaction.
     */
    struct Participant {
        uint64_t tableId;
        uint64_t keyHash;
        uint64_t rpcId;
        enum { PENDING, ABORT, DECIDE, FAILED } state;

        Participant(uint64_t tableId, uint64_t keyHash, uint64_t rpcId)
            : tableId(tableId)
            , keyHash(keyHash)
            , rpcId(rpcId)
            , state(PENDING)
        {
        }
    };
    typedef std::list<Participant> ParticipantList;

    class DecisionTask {
      PUBLIC:
        DecisionTask(Context* context,
                WireFormat::TxDecision::Decision decision,
                uint64_t leaseId, ParticipantList* pList);

        bool isReady() { return goalReached; }
        void performTask();
        void wait();

      PRIVATE:
        /// Encapsulates the state of a single Decision RPC sent to a single
        /// server.
        class DecisionRpc : public RpcWrapper {
            friend class DecisionTask;
          public:
            DecisionRpc(Context* context, Transport::SessionRef session,
                        DecisionTask* task);
            ~DecisionRpc() {}

            bool checkStatus();
            bool handleTransportError();
            void send();

            void appendOp(ParticipantList::iterator opEntry);
            void retryRequest();

            /// Overall server state information.
            Context* context;

            /// Session that will be used to transmit the RPC.
            Transport::SessionRef session;

            /// Task that issued this rpc.
            DecisionTask* task;

            /// Information about all of the ops that are being requested
            /// in this RPC.
    #ifdef TESTING
            static const uint32_t MAX_OBJECTS_PER_RPC = 3;
    #else
            static const uint32_t MAX_OBJECTS_PER_RPC = 75;
    #endif
            ParticipantList::iterator ops[MAX_OBJECTS_PER_RPC];

            /// Header for the RPC (used to update count as objects are added).
            WireFormat::TxDecision::Request* reqHdr;

            DISALLOW_COPY_AND_ASSIGN(DecisionRpc);
        };

        Context* context;
        WireFormat::TxDecision::Decision decision;
        uint64_t leaseId;
        bool goalReached;
        ParticipantList *pList;
        Tub<ParticipantList> defaultPList;
        ParticipantList::iterator nextParticipantEntry;
        std::list<DecisionRpc> decisionRpcs;

        void processDecisionRpcs();
        void sendDecisionRpc();

        DISALLOW_COPY_AND_ASSIGN(DecisionTask);
    };

    class RequestAbortTask {
      PUBLIC:
        RequestAbortTask(Context* context, uint64_t leaseId,
                ParticipantList* pList);
        bool isReady() { return goalReached; }
        void performTask();
        WireFormat::TxDecision::Decision wait();

      PRIVATE:
        /// Encapsulates the state of a single RequestAbortRpc sent to a single
        /// server.
        class RequestAbortRpc : public RpcWrapper {
            friend class RequestAbortTask;
          public:
            RequestAbortRpc(Context* context, Transport::SessionRef session,
                    RequestAbortTask* task);
            ~RequestAbortRpc() {}

            bool checkStatus();
            bool handleTransportError();
            void send();

            void appendOp(ParticipantList::iterator opEntry);
            void retryRequest();

            /// Overall server state information.
            Context* context;

            /// Session that will be used to transmit the RPC.
            Transport::SessionRef session;

            /// Task that issued this rpc.
            RequestAbortTask* task;

            /// Information about all of the ops that are being requested
            /// in this RPC.
        #ifdef TESTING
            static const uint32_t MAX_OBJECTS_PER_RPC = 3;
        #else
            static const uint32_t MAX_OBJECTS_PER_RPC = 75;
        #endif
            ParticipantList::iterator ops[MAX_OBJECTS_PER_RPC];

            /// Header for the RPC (used to update count as objects are added).
            WireFormat::TxRequestAbort::Request* reqHdr;

            DISALLOW_COPY_AND_ASSIGN(RequestAbortRpc);
        };

        Context* context;
        uint64_t leaseId;
        bool goalReached;
        WireFormat::TxDecision::Decision decision;
        ParticipantList *pList;
        ParticipantList::iterator nextParticipantEntry;
        std::list<RequestAbortRpc> requestAbortRpcs;

        void processRequestAbortRpcs();
        void sendRequestAbortRpc();

        DISALLOW_COPY_AND_ASSIGN(RequestAbortTask);
    };
    DISALLOW_COPY_AND_ASSIGN(TxRecoveryManager);
};

} // end RAMCloud

#endif  /* RAMCLOUD_TXRECOVERYMANAGER_H */

