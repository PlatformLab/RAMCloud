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

#include "TxDecisionRecord.h"

namespace RAMCloud {

/**
 * Constructor used to prepare a transaction decision record to be written
 * to the log.  After construction, the record contains no participants;
 * participants should be added using the addPaticipant method.
 *
 * \param tableId
 *      The 64-bit identifier for the table this record is in.  With the
 *      keyHash, this also uniquely identifies the TxRecoveryManager
 *      responsible for this recovery.
 * \param keyHash
 *      The keyHash used to uniquely identify the TxRecoveryManager
 *      responsible for this object and its recovery.
 * \param leaseId
 *      Id of the lease associated with the recovering transaction; part of the
 *      transaction's system-wide unique identifier.
 * \param transactionId
 *      Id of the transaction which is unique among transactions from the same
 *      client; part of the transaction's system-wide unique identifier.
 * \param decision
 *      The decision that the TxRecoveryManager would like to persist.
 * \param timestamp
 *      The creation time of this record, as returned by the WallTime
 *      module. Used primarily by the cleaner to order live objects and
 *      improve future cleaning performance.
 */
TxDecisionRecord::TxDecisionRecord(uint64_t tableId, KeyHash keyHash,
        uint64_t leaseId, uint64_t transactionId,
        WireFormat::TxDecision::Decision decision, uint32_t timestamp)
    : header(tableId, keyHash, leaseId, transactionId, decision, timestamp)
    , participantBuffer()
    , participantOffset(0)
    , defaultParticipantBuffer()
{
    defaultParticipantBuffer.construct();
    participantBuffer = defaultParticipantBuffer.get();
}

/**
 * Constructor a transaction decision record by deserializing an existing
 * record.  Use this constructor when reading existing records from the log
 * or from individual log segments.
 *
 * \param buffer
 *      Buffer pointing to a complete serialized record. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full record. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the tombstone begins.
 */
TxDecisionRecord::TxDecisionRecord(Buffer& buffer, uint32_t offset)
    : header(*buffer.getOffset<Header>(offset))
    , participantBuffer(&buffer)
    , participantOffset(offset + sizeof32(Header))
    , defaultParticipantBuffer()
{
}

/**
 * Add an additional participant to the record.
 *
 * \param tableId
 *      TableId of participant to be added.
 * \param keyHash
 *      KeyHash of participant to be added.
 * \param rpcId
 *      RpcId of participant to be added.
 */
void
TxDecisionRecord::addParticipant(
        uint64_t tableId, uint64_t keyHash, uint64_t rpcId)
{
    participantBuffer->emplaceAppend<WireFormat::TxParticipant>(
            tableId, keyHash, rpcId);
    header.participantCount++;
}

/**
 * Append the serialized record to the provided buffer.
 *
 * \param buffer
 *      The buffer to which to append a serialized version of this record.
 */
void
TxDecisionRecord::assembleForLog(Buffer& buffer)
{
    header.checksum = computeChecksum();
    buffer.appendExternal(&header, sizeof32(header));
    buffer.appendExternal(
            participantBuffer,
            participantOffset,
            sizeof32(WireFormat::TxParticipant) * header.participantCount);
}

/**
 * Obtain the decision stored in this record.
 */
WireFormat::TxDecision::Decision
TxDecisionRecord::getDecision()
{
    return header.decision;
}

/**
 * Obtain the 64-bit key hash associated with this record.
 */
KeyHash
TxDecisionRecord::getKeyHash()
{
    return header.keyHash;
}

/**
 * Obtain the 64-bit lease identifier associated with this record.
 */
uint64_t
TxDecisionRecord::getLeaseId()
{
    return header.leaseId;
}

/**
 * Obtain the 64-bit transaction identifier associated with this record.
 */
uint64_t
TxDecisionRecord::getTransactionId()
{
    return header.transactionId;
}

/**
 * Returns a participant stored in this record.
 *
 * \param index
 *      The index number of the participant that will be returned.  Must be less
 *      than the participant count.
 * \return
 *      The participant located at the index number in this record.
 */
WireFormat::TxParticipant
TxDecisionRecord::getParticipant(uint32_t index)
{
    uint32_t offset = participantOffset;
    offset += index * sizeof32(WireFormat::TxParticipant);
    return *participantBuffer->getOffset<WireFormat::TxParticipant>(offset);
}

/**
 * Return the number of participants contained in this record.
 */
uint32_t
TxDecisionRecord::getParticipantCount()
{
    return header.participantCount;
}

/**
 * Obtain the 64-bit table identifier associated with this record.
 */
uint64_t
TxDecisionRecord::getTableId()
{
    return header.tableId;
}

/**
 * Obtain the timestamp associated with this record. See WallTime.cc
 * for interpreting the timestamp.
 */
uint32_t
TxDecisionRecord::getTimestamp()
{
    return header.timestamp;
}

/**
 * Compute a checksum on the record and determine whether or not it matches
 * what is stored in the record. Returns true if the checksum looks ok,
 * otherwise returns false.
 */
bool
TxDecisionRecord::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Compute the record's checksum and return it.
 */
uint32_t
TxDecisionRecord::computeChecksum()
{
    assert(OFFSET_OF(Header, checksum) ==
        (sizeof(header) - sizeof(header.checksum)));

    Crc32C crc;
    crc.update(&header, downCast<uint32_t>(OFFSET_OF(Header, checksum)));

    crc.update(*participantBuffer,
               participantOffset,
               sizeof32(WireFormat::TxParticipant) * header.participantCount);

    return crc.getResult();
}


} // namespace RAMCloud

