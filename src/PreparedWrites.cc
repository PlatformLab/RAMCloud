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

#include "UnackedRpcResults.h"
#include "LeaseCommon.h"
#include "MasterService.h"
#include "ObjectManager.h"

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
 *      leaseId given for this linearizable RPC.
 * \param rpcId
 *      rpcId given for this linearizable RPC.
 * \param participantCount
 *      Number of objects participating current transaction.
 * \param participants
 *      Pointer to the array of #WireFormat::TxParticipant which contains
 *      all information of objects participating transaction.
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
 *      This is primarily used by the multiWrite RPC in MasterService.
 */
PreparedOp::PreparedOp(WireFormat::TxPrepare::OpType type,
                       uint64_t clientId,
                       uint64_t rpcId,
                       uint32_t participantCount,
                       WireFormat::TxParticipant* participants,
                       uint64_t tableId,
                       uint64_t version,
                       uint32_t timestamp,
                       Buffer& keysAndValueBuffer,
                       uint32_t startDataOffset,
                       uint32_t length)
    : header(type, clientId, rpcId, participantCount)
    , participants(participants)
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
 *      leaseId given for this linearizable RPC.
 * \param rpcId
 *      rpcId given for this linearizable RPC.
 * \param participantCount
 *      Number of objects participating current transaction.
 * \param participants
 *      Pointer to the array of #WireFormat::TxParticipant which contains
 *      all information of objects participating transaction.
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
                       uint64_t rpcId,
                       uint32_t participantCount,
                       WireFormat::TxParticipant* participants,
                       Key& key,
                       const void* value,
                       uint32_t valueLength,
                       uint64_t version,
                       uint32_t timestamp,
                       Buffer& buffer,
                       uint32_t *length)
    : header(type, clientId, rpcId, participantCount)
    , participants(participants)
    , object(key, value, valueLength, version, timestamp, buffer, length)
{
}

/**
 * Construct an PreparedOp using information in the log, which includes the
 * header as well as the participant list and object.
 * This form of the constructor is typically used for extracting
 * information out of the log. For example to serve read requests
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
    , participants(NULL)
    , object(buffer,
             offset + sizeof32(header) +
               sizeof32(WireFormat::TxParticipant) * header.participantCount,
             length - sizeof32(header) -
               sizeof32(WireFormat::TxParticipant) * header.participantCount)
{
    //Fill in participants from buffer data.
    participants = (WireFormat::TxParticipant*)buffer.getRange(
            offset + sizeof32(header),
            sizeof32(WireFormat::TxParticipant) * header.participantCount);
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
    buffer.appendExternal(&header, sizeof32(Header));
    buffer.appendExternal(participants, sizeof32(WireFormat::TxParticipant)
                                         * header.participantCount);
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

    crc.update(participants,
               sizeof32(WireFormat::TxParticipant) *
               header.participantCount);

    // TODO(seojin): Let's discuss safety of this.
    uint32_t objectChecksum = object.computeChecksum();
    crc.update(&objectChecksum, sizeof32(objectChecksum));

    return crc.getResult();
}

/**
 * Construct PreparedWrites.
 */
PreparedWrites::PreparedWrites(Context* context)
    : context(context)
    , mutex()
    , items()
{
}

/**
 * Default destructor.
 */
PreparedWrites::~PreparedWrites()
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
 */
void
PreparedWrites::bufferWrite(uint64_t leaseId,
                            uint64_t rpcId,
                            uint64_t newOpPtr)
{
    Lock lock(mutex);

    assert(items.find(std::make_pair(leaseId, rpcId)) == items.end());

    PreparedItem* item = new PreparedItem(context, newOpPtr);
    items[std::make_pair(leaseId, rpcId)] = item;
    item->start(Cycles::rdtsc() +
                Cycles::fromMicroseconds(PreparedItem::TX_TIMEOUT_US));
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
PreparedWrites::popOp(uint64_t leaseId,
                      uint64_t rpcId)
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it == items.end()) {
        return 0;
    } else {
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
PreparedWrites::peekOp(uint64_t leaseId,
                       uint64_t rpcId)
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it == items.end()) {
        return 0;
    } else {
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
PreparedWrites::updatePtr(uint64_t leaseId,
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
PreparedWrites::PreparedItem::handleTimerEvent()
{
    //TODO(seojin): invoke recovery initiate rpc.

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
PreparedWrites::regrabLocksAfterRecovery(ObjectManager* objectManager)
{
    Lock lock(mutex);
    for (ItemsMap::iterator it = items.begin();
        it != items.end();
        ++it) {
        PreparedItem *item = it->second;
        Buffer buffer;
        Log::Reference ref(item->newOpPtr);
        objectManager->getLog()->getEntry(ref, buffer);
        PreparedOp op(buffer, 0, buffer.size());
        objectManager->tryGrabTxLock(op.object);
    }
}

} // namespace RAMCloud
