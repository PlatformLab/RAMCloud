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

#ifndef RAMCLOUD_PREPAREDWRITES_H
#define RAMCLOUD_PREPAREDWRITES_H

#include <map>
#include <utility>
#include "Common.h"
#include "Object.h"
#include "SpinLock.h"
#include "WorkerTimer.h"

namespace RAMCloud {

class ObjectManager;

class PreparedOp {
  public:
    PreparedOp(WireFormat::TxPrepare::OpType type,
               uint64_t clientId, uint64_t rpcId,
               uint32_t participantCount,
               WireFormat::TxParticipant* participants,
               uint64_t tableId, uint64_t version, uint32_t timestamp,
               Buffer& keysAndValueBuffer, uint32_t startDataOffset = 0,
               uint32_t length = 0);
    PreparedOp(WireFormat::TxPrepare::OpType type,
               uint64_t clientId, uint64_t rpcId,
               uint32_t participantCount,
               WireFormat::TxParticipant* participants,
               Key& key, const void* value, uint32_t valueLength,
               uint64_t version, uint32_t timestamp,
               Buffer& buffer, uint32_t *length = NULL);
    PreparedOp(Buffer& buffer, uint32_t offset, uint32_t length);

    class Header {
      public:
        Header(WireFormat::TxPrepare::OpType type,
               uint64_t clientId,
               uint64_t rpcId,
               uint32_t participantCount)
            : type(type),
              clientId(clientId),
              rpcId(rpcId),
              participantCount(participantCount),
              checksum(0)
        {
        }

        /// Type of the staged operation.
        WireFormat::TxPrepare::OpType type;

        /// leaseId given for this prepare.
        uint64_t clientId;

        /// rpcId given for this prepare.
        uint64_t rpcId;

        /// Number of objects participating current transaction.
        uint32_t participantCount;

        /// CRC32C checksum covering everything but this field, including the
        /// keys and the value.
        uint32_t checksum;

    } __attribute__((__packed__));
    static_assert(sizeof(Header) == 28,
        "Unexpected serialized PreparedOp size");

    /// Copy of the PreparedOp header.
    Header header;

    /// Pointer to the array of #WireFormat::TxParticipant which contains
    /// all information of objects participating transaction.
    WireFormat::TxParticipant* participants;

    /// Object to be written during COMMIT phase.
    /// Its value is empty if OpType is READ or REMOVE; it only contains
    /// key information in that case.
    Object object;

    void assembleForLog(Buffer& buffer);
    bool checkIntegrity();
    uint32_t computeChecksum();

    DISALLOW_COPY_AND_ASSIGN(PreparedOp);
};

class PreparedOpTombstone {
  public:
    PreparedOpTombstone(PreparedOp& op, uint64_t segmentId);
    explicit PreparedOpTombstone(Buffer& buffer, uint32_t offset = 0);

    void assembleForLog(Buffer& buffer);
    bool checkIntegrity();
    uint32_t computeChecksum();

    /**
     * This data structure defines the format of an preparedOp's tombstone
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
              leaseId(leaseId),
              rpcId(rpcId),
              segmentId(segmentId),
              checksum(0)
        {
        }

        /// TableId for log distribution during recovery.
        uint64_t tableId;

        /// KeyHash for log distribution during recovery.
        KeyHash keyHash;

        /// leaseId given for the prepare that this tombstone is for.
        /// A (leaseId, rpcId) tuple uniquely identifies a preparedOp log entry.
        uint64_t leaseId;

        /// rpcId given for the prepare that this tombstone is for.
        /// A (leaseId, rpcId) tuple uniquely identifies a preparedOp log entry.
        uint64_t rpcId;

        /// The log segment that the dead preparedOp this tombstone refers to
        /// was in. Once this segment is no longer in the system, this tombstone
        /// is no longer necessary and may be garbage collected.
        uint64_t segmentId;

        /// CRC32C checksum covering everything but this field, including the
        /// key.
        uint32_t checksum;
    } __attribute__((__packed__));
    static_assert(sizeof(Header) == 44,
        "Unexpected serialized ObjectTombstone size");

    /// Copy of the tombstone header that is in, or will be written to, the log.
    Header header;

    /// If a tombstone is being read from a serialized copy (for instance, from
    /// the log), this will point to the buffer that refers to the entire
    /// tombstone. This is NULL for a new tombstone that is being constructed.
    Buffer* tombstoneBuffer;

    DISALLOW_COPY_AND_ASSIGN(PreparedOpTombstone);
};

/**
 * A table for all staged PreparedOp. Decision RPC handler fetches preparedOp
 * from this table.
 */
class PreparedWrites {
  PUBLIC:
    explicit PreparedWrites(Context* context);
    ~PreparedWrites();

    void bufferWrite(uint64_t leaseId, uint64_t rpcId, uint64_t newOpPtr,
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
     * This represents a active locking on a object, and its timer
     * make sure transaction recovery starts after timeout.
     */
    class PreparedItem : public WorkerTimer {
      public:
        PreparedItem(Context* context, uint64_t newOpPtr)
            : WorkerTimer(context->dispatch)
            , newOpPtr(newOpPtr) {}

        //TODO(seojin): handler may not protected from destruction.
        //              Resolve this later.
        virtual void handleTimerEvent();

        uint64_t newOpPtr;

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

    DISALLOW_COPY_AND_ASSIGN(PreparedWrites);
};

}

#endif // RAMCLOUD_PREPAREDWRITES_H
