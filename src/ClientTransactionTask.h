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

#ifndef RAMCLOUD_CLIENTTRANSACTIONTASK_H
#define RAMCLOUD_CLIENTTRANSACTIONTASK_H

#include <list>
#include <map>
#include <memory>

#include "Dispatch.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * This class manages the state of a Transaction. It caches information about
 * all of the operations that are part of the transaction (reads, writes,
 * deletes), and it executes the client-driven protocol for committing the
 * transaction. Furthermore, it allows the commit protocol to be executed
 * asynchronously.
 */
class ClientTransactionTask : public RpcTracker::TrackedRpc {
  PUBLIC:
    /**
     * Structure to define the contents of the CommitCache.
     */
    struct CacheEntry {
        enum Type { READ, REMOVE, WRITE, INVALID };
        /// Type of the cached object entry.  Used to specify what kind of
        /// transaction operation needs to be performed during commit.
        Type type;
        /// Cached object value.  Used to service reads and defer writes and
        /// removes until commit-time.  Ideally this would be a unique pointer
        /// to manage the memory automatically but std::multimap is missing the
        /// emplace feature.  Instead, we must manually dynamically allocate
        /// the object and ensure there is only one pointer to it so that it
        /// will not be double freed.
        ObjectBuffer* objectBuf;
        /// Conditions upon which the transaction operation associated with
        /// this object should abort.
        RejectRules rejectRules;

        /// The rpcId to uniquely identify this operation.
        uint64_t rpcId;
        /// Used to keep track of what stage in the commit process this
        /// operation has reached.
        enum { PENDING, PREPARE, DECIDE } state;

        /// Default constructor for CacheEntry.
        CacheEntry()
            : type(CacheEntry::INVALID)
            , objectBuf(NULL)
            , rejectRules({0, 0, 0, 0, 0})
            , rpcId(0)
            , state(PENDING)
        {}

        /// Copy constructor for CacheEntry, used to get around the missing
        /// emplace feature in std::multimap.
        explicit CacheEntry(const CacheEntry& other)
            : type(other.type)
            , objectBuf(other.objectBuf)
            , rejectRules(other.rejectRules)
            , rpcId(other.rpcId)
            , state(other.state)
        {}

        /// Destructor for CacheEntry.
        ~CacheEntry()
        {
            // This delete would not be necessary if std::multimap supported
            // emplace operations.
            if (objectBuf)
                delete objectBuf;
        }

        /// Assignment operator for CacheEntry, used to get around the missing
        /// emplace feature in std::multimap.
        CacheEntry& operator=(const CacheEntry& other)
        {
            if (this != &other) {
                type = other.type;
                objectBuf = other.objectBuf;
                rejectRules = other.rejectRules;
                rpcId = other.rpcId;
                state = other.state;
            }
            return *this;
        }
    };

    explicit ClientTransactionTask(RamCloud* ramcloud);
    ~ClientTransactionTask() {
        RAMCLOUD_TEST_LOG("Destructor called.");
    }

    CacheEntry* findCacheEntry(Key& key);
    /// Return the transaction commit decision if a decision has been reached.
    /// Otherwise, INVALID will be returned.
    WireFormat::TxDecision::Decision getDecision() { return decision; }
    CacheEntry* insertCacheEntry(Key& key, const void* buf, uint32_t length);
    /// Check if the task has completed the commit protocol.
    bool isReady() { return (state == DONE); }
    /// Check if all decisions have been sent.
    bool allDecisionsSent() {
        return (state == DONE ||
                (state == DECISION && nextCacheEntry == commitCache.end()));
    }
    void performTask();

  PRIVATE:
    // Forward declaration of RPCs
    class PrepareRpc;
    class DecisionRpc;

    /// Overall client state information.
    RamCloud* ramcloud;

  PUBLIC:
    /// Flag that can be set indicating that the transaction is read-only and
    /// the read-only optimization can be used.
    bool readOnly;

  PRIVATE:
    /// Number of participant objects/operations.
    uint32_t participantCount;
    /// Expandable raw storage for the List of participant object identifiers.
    Buffer participantList;

    /// Keeps track of the task currently executing phase.
    enum State { INIT,      /// Acquire and assign LeaseIds and RpcIds.
                 PREPARE,   /// Send out PrepareRpcs and collect votes.
                 DECISION,  /// Send out DecisionRpcs based on votes.
                 DONE       /// Execution has terminated (w/ or w/o errors).
            } state;

    /// This transaction's decision to either COMMIT or ABORT.
    WireFormat::TxDecision::Decision decision;

    /// Lease information for to this transaction.
    WireFormat::ClientLease lease;

    /// RpcId used to identify this transaction.  Also is the rpcId that should
    /// be completed once the transaction is complete.
    uint64_t txId;

    /// List of "in flight" Prepare Rpcs.
    std::list<PrepareRpc> prepareRpcs;
    /// List of "in flight" Decision Rpcs.
    std::list<DecisionRpc> decisionRpcs;

    /**
     * Structure to define the key search value for the CommitCache map.
     * CacheKeys in the CommitCache map are not necessarily unique (e.g.
     * multiple keys may have the same KeyHash).
     */
    struct CacheKey {
        uint64_t tableId;       // tableId of the tablet
        KeyHash keyHash;        // start key hash value

        /**
         * The operator < is overridden to implement the
         * correct comparison for the CommitCache map.
         */
        bool operator<(const CacheKey& key) const {
            return tableId < key.tableId ||
                (tableId == key.tableId && keyHash < key.keyHash);
        }
    };

    /**
     * The Commit Cache is used to keep track of the transaction operations to
     * be performed during commit and well as cache read and write values to
     * services subsequent reads.
     */
    typedef std::multimap<CacheKey, CacheEntry> CommitCacheMap;
    CommitCacheMap commitCache;

    /// Used to keep track of which cache entry to process next as part of the
    /// commit protocol.
    CommitCacheMap::iterator nextCacheEntry;

    void initTask();
    void processDecisionRpcResults();
    void processPrepareRpcResults();
    void sendDecisionRpc();
    void sendPrepareRpc();
    virtual void tryFinish();

    /// Encapsulates common state and methods of Decision and Prepare RPCs.
    class ClientTransactionRpcWrapper : public RpcWrapper {
      PUBLIC:
        ClientTransactionRpcWrapper(RamCloud* ramcloud,
                Transport::SessionRef session,
                ClientTransactionTask* task,
                uint32_t responseHeaderLength);
        ~ClientTransactionRpcWrapper() {}
        virtual bool appendOp(CommitCacheMap::iterator opEntry) = 0;
        void send();

      PROTECTED:
        bool checkStatus();
        bool handleTransportError();
        virtual void markOpsForRetry() = 0;

        /// Overall client state information.
        RamCloud* ramcloud;

        /// ClientTransactionTask that issued this rpc.
        ClientTransactionTask* task;

        /// Information about all of the ops that are being requested
        /// in this RPC.
#ifdef TESTING
        static const uint32_t MAX_OBJECTS_PER_RPC = 3;
#else
        static const uint32_t MAX_OBJECTS_PER_RPC = 75;
#endif
        CommitCacheMap::iterator ops[MAX_OBJECTS_PER_RPC];

        DISALLOW_COPY_AND_ASSIGN(ClientTransactionRpcWrapper);
    };

    /// Encapsulates the state of a single Decision RPC sent to a single server.
    class DecisionRpc : public ClientTransactionRpcWrapper {
      PUBLIC:
        DecisionRpc(RamCloud* ramcloud, Transport::SessionRef session,
                    ClientTransactionTask* task);
        ~DecisionRpc() {}
        bool appendOp(CommitCacheMap::iterator opEntry);
        void wait();

      PROTECTED:
        void markOpsForRetry();

        /// Header for the RPC (used to update count as objects are added).
        WireFormat::TxDecision::Request* reqHdr;

        DISALLOW_COPY_AND_ASSIGN(DecisionRpc);
    };

    /// Encapsulates the state of a single Prepare RPC sent to a single server.
    class PrepareRpc : public ClientTransactionRpcWrapper {
      PUBLIC:
        PrepareRpc(RamCloud* ramcloud, Transport::SessionRef session,
                ClientTransactionTask* task);
        ~PrepareRpc() {}
        bool appendOp(CommitCacheMap::iterator opEntry);
        WireFormat::TxPrepare::Vote wait();

      PROTECTED:
        void markOpsForRetry();

        /// Header for the RPC (used to update count as objects are added).
        WireFormat::TxPrepare::Request* reqHdr;

        DISALLOW_COPY_AND_ASSIGN(PrepareRpc);
    };

    DISALLOW_COPY_AND_ASSIGN(ClientTransactionTask);
};

} // end RAMCloud

#endif  /* RAMCLOUD_CLIENTTRANSACTIONTASK_H */
