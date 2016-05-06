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

#ifndef RAMCLOUD_PARTICIPANTLIST_H
#define RAMCLOUD_PARTICIPANTLIST_H

#include "Minimal.h"
#include "TransactionId.h"
#include "WireFormat.h"

namespace RAMCloud {

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

    /// Return the unique identifier for this transaction and participant list.
    TransactionId getTransactionId()
    {
        return TransactionId(header.clientLeaseId, header.clientTransactionId);
    }

    /// Return the number for participants in this list.
    uint32_t getParticipantCount()
    {
        return header.participantCount;
    }

    /// Return the tableId that identifies the target recovery manager.
    uint64_t getTableId()
    {
        assert(header.participantCount > 0);
        return participants[0].tableId;
    }

    /// Return the keyHash that identifies the target recovery manager.
    uint64_t getKeyHash()
    {
        assert(header.participantCount > 0);
        return participants[0].keyHash;
    }

  PRIVATE:
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

  PUBLIC:
    /// Pointer to the array of #WireFormat::TxParticipant containing the
    /// tableId, keyHash, and rpcId of every operations in this transactions.
    /// The actual data reside in RPC payload or in log. This pointer should not
    /// be passed around outside the lifetime of a single RPC handler or epoch.
    WireFormat::TxParticipant* participants;

    DISALLOW_COPY_AND_ASSIGN(ParticipantList);
};

} // namespace RAMCloud

#endif // RAMCLOUD_PARTICIPANTLIST_H

