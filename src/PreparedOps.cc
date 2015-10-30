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

#include "PreparedOps.h"
#include "LeaseCommon.h"
#include "MasterClient.h"
#include "MasterService.h"
#include "ObjectManager.h"
#include "UnackedRpcResults.h"

namespace RAMCloud {

/**
 * Construct a PreparedOp in preparation for storing it in the log.
 * This form is used when the header information is available in
 * individual pieces, while the keys and data are stored in  a Buffer
 * (typical use: during txPrepare RPCs).
 *
 * \param type
 *      Type of the staged operation.
 * \param clientId
 *      leaseId given for this linearizable RPC.  First half of this
 *      transaction's unique identifier.
 * \param clientTxId
 *      Second half of this transaction's unique identifier.
 * \param rpcId
 *      rpcId given for this linearizable RPC.
 * \param tableId
 *      TableId for this object.
 * \param version
 *      Version number of this object, which is used to disambiguate
 *      different incarnations of objects with the same key.
 * \param timestamp
 *      The creation time of this object, as returned by the WallTime
 *      module. Used primarily by the cleaner to order live objects and
 *      improve future cleaning performance.
 * \param keysAndValueBuffer
 *      Buffer containing all chunks that will comprise this object's
 *      keysAndValue. Refer to Object.h for a visual description
 *      of keysAndValue.
 * \param startDataOffset
 *      Byte offset in the buffer where keysAndValue start
 * \param length
 *      Length of keysAndValue.
 *      A length of 0 means that the object occupies the entire buffer
 *      starting at offset.
 */
PreparedOp::PreparedOp(WireFormat::TxPrepare::OpType type,
                       uint64_t clientId,
                       uint64_t clientTxId,
                       uint64_t rpcId,
                       uint64_t tableId,
                       uint64_t version,
                       uint32_t timestamp,
                       Buffer& keysAndValueBuffer,
                       uint32_t startDataOffset,
                       uint32_t length)
    : header(type, clientId, clientTxId, rpcId)
    , object(tableId, version, timestamp, keysAndValueBuffer,
             startDataOffset, length)
{
}

/**
 * Construct a PreparedOp in preparation for storing it in the log.
 * Use this constructor when the data for an object is contiguous and
 * the header information is  available in individual pieces. This is
 * primarily used by unit tests to construct objects with just a
 * single key. The main function that will be invoked after a call
 * to this constructor is assembleForLog.
 *
 * \param type
 *      Type of the staged operation.
 * \param clientId
 *      leaseId given for this linearizable RPC.  First half of this
 *      transaction's unique identifier.
 * \param clientTxId
 *      Second half of this transaction's unique identifier.
 * \param rpcId
 *      rpcId given for this linearizable RPC.
 * \param key
 *      Primary key for this object
 * \param value
 *      Pointer to a single contiguous piece of memory that comprises this
 *      object's value.
 * \param valueLength
 *      Length of the value portion in bytes.
 * \param version
 *      Version number of this object, which is used to disambiguate
 *      different incarnations of objects with the same key.
 * \param timestamp
 *      The creation time of this object, as returned by the WallTime
 *      module. Used primarily by the cleaner to order live objects and
 *      improve future cleaning performance.
 * \param buffer
 *      A buffer that can be used temporarily to store the keys and value
 *      for the object. Its lifetime must cover the lifetime of this Object.
 * \param [out] length
 *      Total length of keysAndValue
 */
PreparedOp::PreparedOp(WireFormat::TxPrepare::OpType type,
                       uint64_t clientId,
                       uint64_t clientTxId,
                       uint64_t rpcId,
                       Key& key,
                       const void* value,
                       uint32_t valueLength,
                       uint64_t version,
                       uint32_t timestamp,
                       Buffer& buffer,
                       uint32_t *length)
    : header(type, clientId, clientTxId, rpcId)
    , object(key, value, valueLength, version, timestamp, buffer, length)
{
}

/**
 * Construct a PreparedOp using information in the log.
 * This form of the constructor is typically used for extracting
 * information out of the log. For example to serve txDecision requests
 * or to perform log cleaning.
 *
 * \param buffer
 *      Buffer referring to a complete PreparedOp in the log. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full object. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the PreparedOp begins.
 * \param length
 *      Total length of the PreparedOp in bytes.
 *      A length of 0 means that the PreparedOp occupies the entire buffer
 *      starting at offset.
 */
PreparedOp::PreparedOp(Buffer& buffer, uint32_t offset, uint32_t length)
    : header(*buffer.getOffset<Header>(offset))
    , object(buffer, offset + sizeof32(header), length - sizeof32(header))
{
}

/**
 * Append the serialized header, participant list, and object to the
 * provided buffer.
 *
 * \param buffer
 *      The buffer to append all data to.
 */
void
PreparedOp::assembleForLog(Buffer& buffer)
{
    header.checksum = computeChecksum();
    buffer.appendCopy(&header, sizeof32(Header));
    object.assembleForLog(buffer);
}

/**
 * Compute a checksum on the PreparedOp and determine whether or not it matches
 * what is stored in the PreparedOp. Returns true if the checksum looks ok,
 * otherwise returns false.
 */
bool
PreparedOp::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Compute the PreparedOp's checksum and return it.
 */
uint32_t
PreparedOp::computeChecksum()
{
    Crc32C crc;
    // first compute the checksum on the object header excluding the
    // checksum field
    crc.update(this,
               downCast<uint32_t>(sizeof(header) -
               sizeof(header.checksum)));

    object.applyChecksum(&crc);

    return crc.getResult();
}

/**
 * Return the unique identifier for the transaction to which this prepare
 * operations belongs.
 */
TransactionId
PreparedOp::getTransactionId()
{
     return TransactionId(header.clientId, header.clientTxId);
}

/**
 * Construct a new tombstone for a given completed PreparedOp. Use this
 * constructor when generating new tombstones to be written to the log.
 *
 * \param op
 *      The preparedOp this tombstone is marking as completed.
 * \param segmentId
 *      The 64-bit identifier of the segment in which the PreparedOp this
 *      tombstone refers to exists. Once this segment is no longer in
 *      the system, this tombstone may be garbage collected.
 */
PreparedOpTombstone::PreparedOpTombstone(PreparedOp& op, uint64_t segmentId)
    : header(op.object.getTableId(),
             Key::getHash(op.object.getTableId(),
                          op.object.getKey(),
                          op.object.getKeyLength()),
             op.header.clientId,
             op.header.rpcId,
             segmentId)
    , tombstoneBuffer()
{
}

/**
 * Construct a tombstone object by deserializing an existing tombstone. Use
 * this constructor when reading existing tombstones from the log or from
 * individual log segments.
 *
 * \param buffer
 *      Buffer pointing to a complete serialized tombstone. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full tombstone. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the tombstone begins.
 */
PreparedOpTombstone::PreparedOpTombstone(Buffer& buffer, uint32_t offset)
    : header(*buffer.getOffset<Header>(offset))
    , tombstoneBuffer(&buffer)
{
}

/**
 * Append the serialized tombstone to the provided buffer.
 *
 * \param buffer
 *      The buffer to append all data to.
 */
void
PreparedOpTombstone::assembleForLog(Buffer& buffer)
{
    header.checksum = computeChecksum();
    buffer.appendExternal(&header, sizeof32(Header));
}

/**
 * Compute a checksum on the PreparedOpTombstone and determine whether
 *  or not it matches what is stored in the PreparedOpTombstone.
 *  Returns true if the checksum looks ok, otherwise returns false.
 */
bool
PreparedOpTombstone::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Compute the PreparedOpTombstone's checksum and return it.
 */
uint32_t
PreparedOpTombstone::computeChecksum()
{
    Crc32C crc;
    // first compute the checksum on the object header excluding the
    // checksum field
    crc.update(this,
               downCast<uint32_t>(sizeof(header) -
               sizeof(header.checksum)));

    return crc.getResult();
}

/**
 * Construct a ParticipantList from a one that came off the wire.
 *
 * \param participants
 *      Pointer to a contiguous participant list that must stay in scope for the
 *      life of this object.
 * \param participantCount
 *      Number of participants in the list.
 * \param clientLeaseId
 *      Id of the client lease associated with this transaction.
 * \param clientTransactionId
 *      Client provided identifier for this transaction.
 */
ParticipantList::ParticipantList(WireFormat::TxParticipant* participants,
                                 uint32_t participantCount,
                                 uint64_t clientLeaseId,
                                 uint64_t clientTransactionId)
    : header(clientLeaseId, clientTransactionId, participantCount)
    , participants(participants)
{
}

/**
 * Construct an ParticipantList using information in the log, which includes the
 * ParticipantList header. This form of the constructor is typically used for
 * extracting information out of the log (e.g. perform log cleaning).
 *
 * \param buffer
 *      Buffer referring to a complete ParticipantList in the log. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full ParticipantList. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the object begins.
 */
ParticipantList::ParticipantList(Buffer& buffer, uint32_t offset)
    : header(*buffer.getOffset<Header>(offset))
    , participants(NULL)
{
    participants = (WireFormat::TxParticipant*)buffer.getRange(
            offset + sizeof32(header),
            sizeof32(WireFormat::TxParticipant) * header.participantCount);
}

/**
 * Append the full ParticipantList to a buffer.
 *
 * \param buffer
 *      The buffer to which to append a serialized version of this
 *      ParticipantList.
 */
void
ParticipantList::assembleForLog(Buffer& buffer)
{
    header.checksum = computeChecksum();
    buffer.appendExternal(&header, sizeof32(Header));
    buffer.appendExternal(participants, sizeof32(WireFormat::TxParticipant)
                                                * header.participantCount);
}

/**
 * Compute a checksum on the ParticipantList and determine whether or not it
 * matches what is stored in the object.
 *
 * \return
 *      True if the checksum looks OK; false otherwise.
 */
bool
ParticipantList::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Compute the ParticipantList's checksum and return it.
 */
uint32_t
ParticipantList::computeChecksum()
{
    Crc32C crc;
    // first compute the checksum on the header excluding the checksum field
    crc.update(this,
               downCast<uint32_t>(sizeof(header) -
               sizeof(header.checksum)));
    // compute the checksum of the list itself
    crc.update(participants,
           sizeof32(WireFormat::TxParticipant) *
           header.participantCount);

    return crc.getResult();
}

/**
 * Return the unique identifier for this transaction and participant list.
 */
TransactionId
ParticipantList::getTransactionId()
{
    return TransactionId(header.clientLeaseId, header.clientTransactionId);
}

/**
 * Construct PreparedWrites.
 */
PreparedOps::PreparedOps(Context* context)
    : context(context)
    , mutex()
    , items()
{
}

/**
 * Default destructor.
 */
PreparedOps::~PreparedOps()
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;

    for (it = items.begin(); it != items.end(); ++it) {
        PreparedItem* item = it->second;
        delete item;
    }
}

/**
 * Add a pointer to preparedOp into lookup table.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 * \param newOpPtr
 *      Log::Reference to preparedOp in main log.
 * \param isRecovery
 *      Caller should set this flag true if it is adding entries while
 *      replaying recovery segments; With true value, it will not start
 *      WorkerTimer. The timers will start during final stage of recovery
 *      by #regrabLocksAfterRecovery().
 */
void
PreparedOps::bufferOp(uint64_t leaseId,
                            uint64_t rpcId,
                            uint64_t newOpPtr,
                            bool isRecovery)
{
    Lock lock(mutex);

    assert(items.find(std::make_pair(leaseId, rpcId)) == items.end());

    PreparedItem* item = new PreparedItem(context, newOpPtr);
    items[std::make_pair(leaseId, rpcId)] = item;
    if (!isRecovery) {
        item->start(Cycles::rdtsc() +
                    Cycles::fromMicroseconds(PreparedItem::TX_TIMEOUT_US));
    }
}

/**
 * Pop a pointer to preparedOp from lookup table.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 *
 * \return
 *      Log::Reference::toInteger() value for the preparedOp staged.
 */
uint64_t
PreparedOps::popOp(uint64_t leaseId,
                      uint64_t rpcId)
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it == items.end()) {
        return 0;
    } else {
        // During recovery, must check isDeleted before using this method.
        // It is intentionally left as assertion error. (instead of return 0.)
        // After the end of recovery all NULL entries should be removed.
        assert(it->second);

        uint64_t newOpPtr = it->second->newOpPtr;
        delete it->second;
        items.erase(it);
        return newOpPtr;
    }
}

/**
 * Peek a pointer to preparedOp from lookup table.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 *
 * \return
 *      Log::Reference::toInteger() value for the preparedOp staged.
 */
uint64_t
PreparedOps::peekOp(uint64_t leaseId,
                       uint64_t rpcId)
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it == items.end()) {
        return 0;
    } else {
        // During recovery, must check isDeleted before using this method.
        // It is intentionally left as assertion error. (instead of return 0.)
        // After the end of recovery all NULL entries should be removed.
        assert(it->second);

        uint64_t newOpPtr = it->second->newOpPtr;
        return newOpPtr;
    }
}

/**
 * Update a pointer to preparedOp into lookup table.
 * This method is used by LogCleaner to relocate Segment.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 * \param newOpPtr
 *      Log::Reference to preparedOp in main log.
 */
void
PreparedOps::updatePtr(uint64_t leaseId,
                          uint64_t rpcId,
                          uint64_t newOpPtr)
{
    Lock lock(mutex);
    PreparedItem* item = items[std::make_pair(leaseId, rpcId)];
    item->newOpPtr = newOpPtr;
}

/**
 * Handles timeout of the staged preparedOp. Requests to initiate
 * recovery of whole transaction.
 */
void
PreparedOps::PreparedItem::handleTimerEvent()
{
    Buffer opBuffer;
    Log::Reference opRef(newOpPtr);
    context->getMasterService()->objectManager.getLog()->getEntry(
            opRef, opBuffer);
    PreparedOp op(opBuffer, 0, opBuffer.size());

    //TODO(seojin): RAM-767. op.participants can be stale while log cleaning.
    //              It is possible to cause invalid memory access.

    TransactionId txId = op.getTransactionId();

    UnackedRpcHandle participantListLocator(
            &context->getMasterService()->unackedRpcResults,
            {txId.clientLeaseId, 0, 0},
            txId.clientTransactionId,
            0);

    if (participantListLocator.isDuplicate()) {
        Buffer pListBuf;
        Log::Reference pListRef(participantListLocator.resultLoc());
        context->getMasterService()->objectManager.getLog()->getEntry(pListRef,
                                                                      pListBuf);
        ParticipantList participantList(pListBuf);

        assert(participantList.header.participantCount > 0);
        TEST_LOG("TxHintFailed RPC is sent to owner of tableId %lu and "
                "keyHash %lu.", participantList.participants[0].tableId,
                participantList.participants[0].keyHash);

        MasterClient::txHintFailed(context,
                participantList.participants[0].tableId,
                participantList.participants[0].keyHash,
                participantList.header.clientLeaseId,
                participantList.header.clientTransactionId,
                participantList.header.participantCount,
                participantList.participants);
    } else {
        RAMCLOUD_LOG(WARNING,
                "Unable to find participant list record for TxId (%lu, %lu); "
                "client transaction recovery could not be requested.",
                txId.clientLeaseId, txId.clientTransactionId);
    }

    this->start(Cycles::rdtsc() + Cycles::fromMicroseconds(TX_TIMEOUT_US));
}

/**
 * Acquire transaction locks in objectManager for each entries
 * in items. This should be called after replaying all segments
 * and before changing tablet status from RECOVERING to NORMAL.
 *
 * \param objectManager
 *      The pointer to objectManager which holds transaction LockTable.
 */
void
PreparedOps::regrabLocksAfterRecovery(ObjectManager* objectManager)
{
    Lock lock(mutex);
    ItemsMap::iterator it = items.begin();
    while (it != items.end()) {
        PreparedItem *item = it->second;

        if (item == NULL) { //Cleanup marks for deleted.
            items.erase(it++);
        } else {
            Buffer buffer;
            Log::Reference ref(item->newOpPtr);
            objectManager->getLog()->getEntry(ref, buffer);
            PreparedOp op(buffer, 0, buffer.size());
            objectManager->tryGrabTxLock(op.object, ref);

            if (!item->isRunning()) {
                item->start(Cycles::rdtsc() +
                        Cycles::fromMicroseconds(PreparedItem::TX_TIMEOUT_US));
            }

            ++it;
        }
    }
}

/**
 * Mark a specific preparedOp is deleted.
 * This method is used by replaySegment during recovery when it finds
 * a tombstone for PreparedOp.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 */
void
PreparedOps::markDeleted(uint64_t leaseId,
                            uint64_t rpcId)
{
    Lock lock(mutex);
    assert(items.find(std::make_pair(leaseId, rpcId)) == items.end() ||
           items.find(std::make_pair(leaseId, rpcId))->second == NULL);
    items[std::make_pair(leaseId, rpcId)] = NULL;
}

/**
 * Check a specific preparedOp is marked as deleted.
 * This method is used by replaySegment during recovery of preparedOp.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 */
bool
PreparedOps::isDeleted(uint64_t leaseId,
                          uint64_t rpcId)
{
    Lock lock(mutex);

    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it == items.end()) {
        return false;
    } else {
        if (it->second == NULL) {
            return true;
        } else {
            return false;
        }
    }
}

} // namespace RAMCloud
