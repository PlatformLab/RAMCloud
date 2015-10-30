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

#ifndef RAMCLOUD_PREPAREDOPS_H
#define RAMCLOUD_PREPAREDOPS_H

#include <map>
#include <unordered_map>
#include <utility>
#include "Common.h"
#include "Object.h"
#include "SpinLock.h"
#include "WireFormat.h"
#include "WorkerTimer.h"

namespace RAMCloud {

class ObjectManager;

/**
 * Encapsulates the unique identifier for a specific transaction.
 */
struct TransactionId {
    /// Constructor for a TransactionId object.
    TransactionId(uint64_t clientLeaseId, uint64_t clientTransactionId)
        : clientLeaseId(clientLeaseId)
        , clientTransactionId(clientTransactionId)
    {}

    /// Equality operator; implemented to support use of TransactionId objects
    /// as a key in an std::unordered_map.
    bool operator==(const TransactionId &other) const {
        return (clientLeaseId == other.clientLeaseId
                && clientTransactionId == other.clientTransactionId);
    }

    /// Hash operator; implemented to support use of TransactionId objects
    /// as a key in an std::unordered_map.
    struct Hasher {
        std::size_t operator()(const TransactionId& txId) const {
            std::size_t h1 = std::hash<uint64_t>()(txId.clientLeaseId);
            std::size_t h2 = std::hash<uint64_t>()(txId.clientTransactionId);
            return h1 ^ (h2 << 1);
        }
    };

    /// Id of the client lease that issued this transaction.
    uint64_t clientLeaseId;
    /// Transaction Id given to this transaction by the client.  This value
    /// uniquely identifies this transaction among the transactions from the
    /// same client.  Combining this value with the clientLeaseId will provide
    /// a system wide unique transaction identifier.
    uint64_t clientTransactionId;
};

/**
 * This class defines the format of a prepared transaction operation stored in
 * the log and provides methods to easily construct new ones to be appended
 * and interpret ones that have already been written.
 *
 * An instance of this class serves as a lock record for the object as well.
 * During RC recovery of preparedOp log, a master should lock the corresponding
 * object.
 *
 * Each preparedOp contains a header and an instance of Object. When stored in
 * the log, a preparedOp has the following layout:
 *
 * +-------------------+--------+
 * | PreparedOp Header | Object |
 * +-------------------+--------+
 *
 * Everything except the header is of variable length.
 */
class PreparedOp {
  public:
    PreparedOp(WireFormat::TxPrepare::OpType type,
               uint64_t clientId, uint64_t clientTxId, uint64_t rpcId,
               uint64_t tableId, uint64_t version, uint32_t timestamp,
               Buffer& keysAndValueBuffer, uint32_t startDataOffset = 0,
               uint32_t length = 0);
    PreparedOp(WireFormat::TxPrepare::OpType type,
               uint64_t clientId, uint64_t clientTxId, uint64_t rpcId,
               Key& key, const void* value, uint32_t valueLength,
               uint64_t version, uint32_t timestamp,
               Buffer& buffer, uint32_t *length = NULL);
    PreparedOp(Buffer& buffer, uint32_t offset, uint32_t length);

    /**
     * This data structure defines the format of a preparedOp header stored in a
     * master server's log.
     */
    class Header {
      public:
        Header(WireFormat::TxPrepare::OpType type,
               uint64_t clientId,
               uint64_t clientTxId,
               uint64_t rpcId)
            : type(type)
            , clientId(clientId)
            , clientTxId(clientTxId)
            , rpcId(rpcId)
            , checksum(0)
        {
        }

        /// Type of the staged operation.
        WireFormat::TxPrepare::OpType type;

        /// leaseId given for this prepare.
        uint64_t clientId;

        /// Combined with the clientId, the clientTxId provides a system-wide
        /// unique identifier for the transaction.
        uint64_t clientTxId;

        /// rpcId given for this prepare.
        uint64_t rpcId;

        /// CRC32C checksum covering everything but this field, including the
        /// keys and the value.
        uint32_t checksum;

    } __attribute__((__packed__));

    /// Copy of the PreparedOp header.
    Header header;

    /// Object to be written during COMMIT phase.
    /// Its value is empty if OpType is READ or REMOVE; it only contains
    /// key information in that case.
    Object object;

    void assembleForLog(Buffer& buffer);
    bool checkIntegrity();
    uint32_t computeChecksum();
    TransactionId getTransactionId();

    DISALLOW_COPY_AND_ASSIGN(PreparedOp);
};

/**
 * This class defines the format of a tombstone for prepared transaction
 * operation record stored in the log and provides methods to easily construct
 * new ones to be appended and interpret ones that have already been written.
 *
 * This PreparedOpTombstone functions similar to ObjectTombstone for Object.
 *
 * PreparedOpTombstones serve as records indicating that specific operation of
 * a transaction is applied to the system (due to TX commit or abort).
 * Since PreparedOp functions as a durable lock record, these
 * PreparedOpTombstones are necessary to avoid re-locking objects of previously
 * completed transactions during failure recovery.
 *
 * The two different types of tombstones are used for initial tombstone creation
 * and reading tombstone from the log. This is same as PreparedOp, Object,
 * and ObjectTombstone.
 */
class PreparedOpTombstone {
  public:
    PreparedOpTombstone(PreparedOp& op, uint64_t segmentId);
    explicit PreparedOpTombstone(Buffer& buffer, uint32_t offset = 0);

    void assembleForLog(Buffer& buffer);
    bool checkIntegrity();
    uint32_t computeChecksum();

    /**
     * This data structure defines the format of a preparedOp's tombstone
     * in a master server's log.
     */
    class Header {
      public:
        Header(uint64_t tableId,
               KeyHash keyHash,
               uint64_t leaseId,
               uint64_t rpcId,
               uint64_t segmentId)
            : tableId(tableId),
              keyHash(keyHash),
              clientLeaseId(leaseId),
              rpcId(rpcId),
              segmentId(segmentId),
              checksum(0)
        {
        }

        /// TableId for log distribution during recovery.
        uint64_t tableId;

        /// KeyHash for log distribution during recovery.
        KeyHash keyHash;

        /// leaseId of the client that initiated the transaction.
        /// A (ClientLeaseId, rpcId) tuple uniquely identifies a preparedOp
        /// log entry.
        uint64_t clientLeaseId;

        /// rpcId given for the prepare that this tombstone is for.
        /// A (ClientLeaseId, rpcId) tuple uniquely identifies a preparedOp
        /// log entry.
        uint64_t rpcId;

        /// The log segment that the dead preparedOp this tombstone refers to
        /// was in. Once this segment is no longer in the system, this tombstone
        /// is no longer necessary and may be garbage collected.
        uint64_t segmentId;

        /// CRC32C checksum covering everything but this field, including the
        /// key.
        uint32_t checksum;
    } __attribute__((__packed__));

    /// Copy of the tombstone header that is in, or will be written to, the log.
    Header header;

    /// If a tombstone is being read from a serialized copy (for instance, from
    /// the log), this will point to the buffer that refers to the entire
    /// tombstone. This is NULL for a new tombstone that is being constructed.
    Buffer* tombstoneBuffer;

    DISALLOW_COPY_AND_ASSIGN(PreparedOpTombstone);
};

/**
 * This class defines the format of a ParticipantList record stored in the log
 * and provides methods to easily construct new ones to be appended and
 * interpret ones that have already been written.
 *
 * In other words, this code centralizes the format and parsing of
 * ParticipantList records (essentially serialization and deserialization).
 * Different constructors serve these two purposes.
 *
 * Each record contains one or more WireFormat::TxParticipant entries and a
 * couple other pieces of metadata.
 */
class ParticipantList {
  PUBLIC:
    ParticipantList(WireFormat::TxParticipant* participants,
                    uint32_t participantCount,
                    uint64_t clientLeaseId,
                    uint64_t clientTransactionId);
    ParticipantList(Buffer& buffer, uint32_t offset = 0);

    void assembleForLog(Buffer& buffer);

    bool checkIntegrity();
    uint32_t computeChecksum();
    TransactionId getTransactionId();

    /**
     * This data structure defines the format of a preparedOp header stored in a
     * master server's log.
     */
    class Header {
      public:
        Header(uint64_t clientLeaseId,
               uint64_t clientTransactionId,
               uint32_t participantCount)
            : clientLeaseId(clientLeaseId)
            , clientTransactionId(clientTransactionId)
            , participantCount(participantCount)
            , checksum(0)
        {
        }

        /// leaseId of the client that initiated the transaction.
        /// A (ClientLeaseId, ClientTransactionId) tuple uniquely identifies
        /// this transaction and this participant list.
        uint64_t clientLeaseId;

        /// Transaction Id given to this transaction by the client.  This value
        /// uniquely identifies this transaction among the transactions from the
        /// same client.  Combining this value with the clientLeaseId will
        /// provide a system wide unique transaction identifier.
        uint64_t clientTransactionId;

        /// Number of objects participating in the current transaction (and
        /// thus in this list).
        uint32_t participantCount;

        /// CRC32C checksum covering everything but this field, including the
        /// keys and the value.
        uint32_t checksum;

    } __attribute__((__packed__));

    /// Copy of the PreparedOp header.
    Header header;

    /// Pointer to the array of #WireFormat::TxParticipant containing the
    /// tableId, keyHash, and rpcId of every operations in this transactions.
    /// The actual data reside in RPC payload or in log. This pointer should not
    /// be passed around outside the lifetime of a single RPC handler or epoch.
    WireFormat::TxParticipant* participants;

    DISALLOW_COPY_AND_ASSIGN(ParticipantList);
};

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
    uint64_t popOp(uint64_t leaseId, uint64_t rpcId);
    uint64_t peekOp(uint64_t leaseId, uint64_t rpcId);
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
            , newOpPtr(newOpPtr) {}

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

        /// Timeout value for the active PreparedOp (lock record).
        /// This timeout should be larger than 2*RTT to give enough time for
        /// a client to complete transaction.
        /// In case of client failure, smaller timeout makes the initiation of
        /// transaction recovery faster, shortening object lock time.
        /// However, using small timeout must be carefully chosen since large
        /// server span of a transaction increases the time gap between the
        /// first phase and the second phase of a transaction.
        static const uint64_t TX_TIMEOUT_US = 500;
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
