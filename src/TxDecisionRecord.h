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

#ifndef RAMCLOUD_TXDECISIONRECORD_H
#define RAMCLOUD_TXDECISIONRECORD_H

#include "Common.h"
#include "Key.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * This class defines the format of a transaction decision record stored in the
 * log an provides methods to easily construct new ones to be appended and
 * interpret ones that have already been written.
 *
 * In other words, this code centralizes the format for parsing of transaction
 * decision records (essentially serialization and deserialization).  Different
 * constructors serve these two purposes.
 *
 * The transaction decision record is used to persist decision information about
 * a recovering transaction so that the recovery can be recovered in the event
 * of a failure during recovery.
 */
class TxDecisionRecord {
  PUBLIC:
    TxDecisionRecord(
            uint64_t tableId, KeyHash keyHash, uint64_t leaseId,
            uint64_t transactionId, WireFormat::TxDecision::Decision decision,
            uint32_t timestamp);
    explicit TxDecisionRecord(Buffer& buffer, uint32_t offset = 0);

    void addParticipant(uint64_t tableId, uint64_t keyHash, uint64_t rpcId);
    void assembleForLog(Buffer& buffer);
    WireFormat::TxParticipant getParticipant(uint32_t index);
    uint32_t getParticipantCount();
    WireFormat::TxDecision::Decision getDecision();
    KeyHash getKeyHash();
    uint64_t getLeaseId();
    uint64_t getTransactionId();
    uint64_t getTableId();
    uint32_t getTimestamp();
    bool checkIntegrity();
  PRIVATE:
    /**
     * This data structure defines the format of an transaction decision record
     * header stored in a master server's log.
     */
    class Header {
      public:
        /**
         * Construct a serialized transaction decision record header.
         *
         * \param tableId
         *      The 64-bit identifier for the table this record is in.  With the
         *      keyHash, this also uniquely identifies the TxRecoveryManager
         *      responsible for this recovery.
         * \param keyHash
         *      The keyHash used to uniquely identify the TxRecoveryManager
         *      responsible for this object and its recovery.
         * \param leaseId
         *      Id of the lease associated with the recovering transaction.
         * \param transactionId
         *      Id of the recovering transaction.
         * \param decision
         *      The decision of whether to commit or abort the transaction
         *      stored in this record.
         * \param timestamp
         *      The creation time of this record, as returned by the WallTime
         *      module. Used primarily by the cleaner to order live objects and
         *      improve future cleaning performance.
         */
        Header(uint64_t tableId,
               KeyHash keyHash,
               uint64_t leaseId,
               uint64_t transactionId,
               WireFormat::TxDecision::Decision decision,
               uint32_t timestamp
               )
            : tableId(tableId)
            , keyHash(keyHash)
            , leaseId(leaseId)
            , transactionId(transactionId)
            , decision(decision)
            , participantCount(0)
            , timestamp(timestamp)
            , checksum(0)
        {
        }

        /// Table to which this record belongs.  Also used to identify the
        /// TxRecoveryManager responsible for this record.
        uint64_t tableId;

        /// Key hash used to uniquely identify the TxRecoveryManager responsible
        /// for this record.
        KeyHash keyHash;

        /// Id of the lease associated with the recovering transaction.  Used
        /// as part of the transaction's unique identifier.
        uint64_t leaseId;

        /// Transaction Id given to this transaction by the client that started
        /// the transaction.  Combining this value with the leaseId will provide
        /// a system wide unique transaction identifier.
        uint64_t transactionId;

        /// Decision of the transaction recovery manager of whether the
        /// recovering transaction should be committed or aborted.
        WireFormat::TxDecision::Decision decision;

        /// Number of participants listed in this record.
        uint32_t participantCount;

        /// Record creation/modification timestamp. WallTime.cc is the clock.
        uint32_t timestamp;

        /// CRC32C checksum covering everything but this field, including the
        /// participant array.
        uint32_t checksum;

        /// Following this class will be an array of participants.  This member
        /// is here to denote this.
        WireFormat::TxParticipant participants[0];
    } __attribute__((__packed__));
    static_assert(sizeof(Header) == 48,
        "Unexpected serialized TxDecisionRecord size");

    uint32_t computeChecksum();

    /// Copy of the object header that is in, or will be written to, the log.
    Header header;

    /// This buffer contains the participants that is/will be contained within
    /// this transaction decision record.
    Buffer* participantBuffer;

    /// The byte offset into the participantBuffer where we will find the first
    /// of the listed participants.
    uint32_t participantOffset;

    /// This buffer is used as the participant buffer when one is not provided
    /// during construction.
    Tub<Buffer> defaultParticipantBuffer;

    DISALLOW_COPY_AND_ASSIGN(TxDecisionRecord);
};

} // namespace RAMCloud

#endif  /* RAMCLOUD_TXDECISIONRECORD_H */

