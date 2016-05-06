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

#include "PreparedOps.h"
#include "LeaseCommon.h"
#include "MasterClient.h"
#include "MasterService.h"
#include "ObjectManager.h"
#include "UnackedRpcResults.h"

namespace RAMCloud {

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
 * Remove a pointer to preparedOp from lookup table.
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 */
void
PreparedOps::removeOp(uint64_t leaseId,
                      uint64_t rpcId)
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it != items.end()) {
        delete it->second;
        items.erase(it);
    }
}

/**
 * Return a log reference to the preparedOp saved by PreparedOps::bufferOp().
 *
 * During recovery, ObjectManager::replaySegment() must check
 * PreparedOps::isDeleted() is false before invoking this method for checking
 * whether the PreparedOp log entry is already in log.
 * (isDeleted() == true already means no need to replay the PreparedOp.)
 *
 * \param leaseId
 *      leaseId given for the preparedOp.
 * \param rpcId
 *      rpcId given for the preparedOp.
 *
 * \return
 *      Log::Reference::toInteger() value for the preparedOp in log.
 *      0 is returned if we cannot find a reference to PreparedOp previously
 *      buffered.
 */
uint64_t
PreparedOps::getOp(uint64_t leaseId, uint64_t rpcId)
{
    Lock lock(mutex);
    std::map<std::pair<uint64_t, uint64_t>,
        PreparedItem*>::iterator it;
    it = items.find(std::make_pair(leaseId, rpcId));
    if (it == items.end()) {
        return 0;
    } else {
        // During recovery, must check isDeleted before using this method since
        // we set it->second = NULL (instead of pointer to PreparedItem) to mark
        // the PreparedOpTombstone for this PreparedOp is seen.
        // It is intentionally left as assertion error. (instead of return 0.)
        // After the end of recovery all NULL entries should be removed, and
        // this assertion check will fail if there's a bug in code.
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
    // This timer handler asynchronously notifies the recovery manager that this
    // transaction is taking a long time and may have failed.  This handler may
    // be called multiple times to ensure the notification is delivered.
    //
    // Note: This notification was previously done in synchronously, but holding
    // the thread and worker timer resources while waiting for the notification
    // to be acknowledge could caused deadlock when the notification is sent to
    // the same server.

    // Construct and send the RPC if it has not been done.
    if (!txHintFailedRpc) {
        Buffer opBuffer;
        Log::Reference opRef(newOpPtr);
        context->getMasterService()->objectManager.getLog()->getEntry(
                opRef, opBuffer);
        PreparedOp op(opBuffer, 0, opBuffer.size());

        //TODO(seojin): RAM-767. op.participants can be stale while log
        //              cleaning. It is possible to cause invalid memory access.

        TransactionId txId = op.getTransactionId();

        UnackedRpcHandle participantListLocator(
                &context->getMasterService()->unackedRpcResults,
                {txId.clientLeaseId, 0, 0},
                txId.clientTransactionId,
                0);

        if (participantListLocator.isDuplicate()) {
            Buffer pListBuf;
            Log::Reference pListRef(participantListLocator.resultLoc());
            context->getMasterService()->objectManager.getLog()->getEntry(
                    pListRef, pListBuf);
            ParticipantList participantList(pListBuf);
            txId = participantList.getTransactionId();
            TEST_LOG("TxHintFailed RPC is sent to owner of tableId %lu and "
                    "keyHash %lu.",
                    participantList.getTableId(), participantList.getKeyHash());

            txHintFailedRpc.construct(context,
                    participantList.getTableId(),
                    participantList.getKeyHash(),
                    txId.clientLeaseId,
                    txId.clientTransactionId,
                    participantList.getParticipantCount(),
                    participantList.participants);
        } else {
            // Abort; there is no way to send the RPC if we can't find the
            // participant list.  Hopefully, some other server still has the
            // list.  Log this situation as it is a bug if it occurs.
            RAMCLOUD_LOG(WARNING, "Unable to find participant list record for "
                    "TxId (%lu, %lu); client transaction recovery could not be "
                    "requested.", txId.clientLeaseId, txId.clientTransactionId);
            return;
        }
    }

    // RPC should have been sent.
    if (!txHintFailedRpc->isReady()) {
        // If the RPC is not yet ready, reschedule the worker timer to poll for
        // the RPC's completion.
        this->start(0);
    } else {
        // The RPC is ready; "wait" on it as is convention.
        txHintFailedRpc->wait();
        txHintFailedRpc.destroy();

        // Wait for another TX_TIMEOUT_US before getting worried again and
        // resending the hint-failed notification.
        this->start(Cycles::rdtsc() + Cycles::fromMicroseconds(TX_TIMEOUT_US));
    }
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
