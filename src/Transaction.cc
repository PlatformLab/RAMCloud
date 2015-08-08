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

#include "ClientTransactionTask.h"
#include "Transaction.h"

namespace RAMCloud {

/**
 * Constructor for a transaction.
 *
 * \param ramcloud
 *      Overall information about the calling client.
 */
Transaction::Transaction(RamCloud* ramcloud)
    : ramcloud(ramcloud)
    , taskPtr(new ClientTransactionTask(ramcloud))
    , commitStarted(false)
{
}

/**
 * Commits the transaction defined by the operations performed on this
 * transaction (read, remove, write).  This method blocks until a decision is
 * reached and sent to all participant servers but does not wait of the
 * participant servers to acknowledge the decision (e.g. does not wait to sync).
 *
 * \return
 *      True if the transaction was able to commit.  False otherwise.
 */
bool
Transaction::commit()
{
    ClientTransactionTask* task = taskPtr.get();

    if (!commitStarted) {
        commitStarted = true;
        ClientTransactionTask::start(taskPtr);
    }

    while (!task->allDecisionsSent()) {
        ramcloud->poll();
    }

    if (expect_false(task->getDecision() ==
            WireFormat::TxDecision::UNDECIDED)) {
        ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
    }

    return (task->getDecision() == WireFormat::TxDecision::COMMIT);
}

/**
 * Block until the decision of this transaction commit is accepted by all
 * participant servers.  If the commit has not yet occurred and a decision is
 * not yet reached, this method will also start the commit.
 *
 * This method is used mostly for testing and benchmarking.
 */
void
Transaction::sync()
{
    ClientTransactionTask* task = taskPtr.get();

    if (!commitStarted) {
        commitStarted = true;
        ClientTransactionTask::start(taskPtr);
    }

    while (!task->isReady()) {
        ramcloud->poll();
    }
}

/**
 * Commits the transaction defined by the operations performed on this
 * transaction (read, remove, write).  This method blocks until a participant
 * servers have accepted the decision.
 *
 * \return
 *      True if the transaction was able to commit.  False otherwise.
 */
bool
Transaction::commitAndSync()
{
    sync();
    return commit();
}

/**
 * Read the current contents of an object as part of this transaction.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object - only the value portion of the object.
 */
void
Transaction::read(uint64_t tableId, const void* key, uint16_t keyLength,
        Buffer* value)
{
    ReadOp readOp(this, tableId, key, keyLength, value);
    readOp.wait();
}

/**
 * Delete an object from a table as part of this transaction. If the object does
 * not currently exist then the operation succeeds without doing anything.
 *
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 */
void
Transaction::remove(uint64_t tableId, const void* key, uint16_t keyLength)
{
    if (expect_false(commitStarted)) {
        throw TxOpAfterCommit(HERE);
    }

    ClientTransactionTask* task = taskPtr.get();

    Key keyObj(tableId, key, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    if (entry == NULL) {
        entry = task->insertCacheEntry(keyObj, NULL, 0);
    } else {
        entry->objectBuf->reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, NULL, 0, entry->objectBuf, true);
    }

    entry->type = ClientTransactionTask::CacheEntry::REMOVE;
}

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed as part of this transaction.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 */
void
Transaction::write(uint64_t tableId, const void* key, uint16_t keyLength,
        const void* buf, uint32_t length)
{
    if (expect_false(commitStarted)) {
        throw TxOpAfterCommit(HERE);
    }

    ClientTransactionTask* task = taskPtr.get();

    Key keyObj(tableId, key, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    if (entry == NULL) {
        entry = task->insertCacheEntry(keyObj, buf, length);
    } else {
        entry->objectBuf->reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, buf, length, entry->objectBuf, true);
    }

    entry->type = ClientTransactionTask::CacheEntry::WRITE;
}

/**
 * Constructor for Transaction::ReadOp: initiates a read just like
 * #Transaction::read, but returns once the operation has been initiated,
 * without waiting for it to complete.  The operation is not consider part of
 * the transaction until it is waited on.
 *
 * \param transaction
 *      The Transaction object of which this operation is a part.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object - only the value portion of the object.
 */
Transaction::ReadOp::ReadOp(Transaction* transaction, uint64_t tableId,
        const void* key, uint16_t keyLength, Buffer* value)
    : transaction(transaction)
    , tableId(tableId)
    , keyBuf()
    , keyLength(keyLength)
    , value(value)
    , buf()
    , rpc()
{
    keyBuf.appendCopy(key, keyLength);

    ClientTransactionTask* task = transaction->taskPtr.get();

    Key keyObj(tableId, key, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    // If no cache entry exists an rpc should be issued.
    if (entry == NULL) {
        rpc.construct(
                transaction->ramcloud, tableId, key, keyLength, &buf);
    }
    // Otherwise we will just return it from cache when wait is called.
}

/**
 * Indicates whether a response has been received for this ReadOp and thus
 * whether #wait will not block.  Used for asynchronous processing of RPCs.
 * Checking that an ReadOp isReady does not include the operation in the
 * transaction (see #wait).
 *
 * \return
 *      True if ReadOp #wait will not block; false otherwise.
 */
bool
Transaction::ReadOp::isReady()
{
    return (!rpc || rpc->isReady());
}

/**
 * Wait for the operation to complete.  The operation is not part of the
 * transaction until wait is called (e.g. if commit is called before wait,
 * this operation will not be included).  Behavior when calling wait more than
 * once is undefined.
 */
void
Transaction::ReadOp::wait()
{
    if (expect_false(transaction->commitStarted)) {
        throw TxOpAfterCommit(HERE);
    }

    ClientTransactionTask* task = transaction->taskPtr.get();

    Key keyObj(tableId, keyBuf, 0, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    if (entry == NULL) {
        // If no entry exists in cache an rpc must have been issued.
        assert(rpc);

        bool objectExists = true;
        uint64_t version;
        uint32_t dataLength = 0;
        const void* data = NULL;

        try {
            rpc->wait(&version);
            data = buf.getValue(&dataLength);
        } catch (ObjectDoesntExistException& e) {
            objectExists = false;
        }

        entry = task->insertCacheEntry(keyObj, data, dataLength);
        entry->type = ClientTransactionTask::CacheEntry::READ;
        if (objectExists) {
            entry->rejectRules.doesntExist = true;
            entry->rejectRules.givenVersion = version;
            entry->rejectRules.versionNeGiven = true;
        } else {
            // Object did not exists at the time of the read so remember to
            // reject (abort) the transaction if it does exist.
            entry->rejectRules.exists = true;
            throw ObjectDoesntExistException(HERE);
        }

    } else if (entry->type == ClientTransactionTask::CacheEntry::REMOVE) {
        // Read after remove; object would no longer exist.
        throw ObjectDoesntExistException(HERE);
    } else if (entry->type == ClientTransactionTask::CacheEntry::READ
            && entry->rejectRules.exists) {
        // Read after read resulting in object DNE; object still DNE.
        throw ObjectDoesntExistException(HERE);
    }

    uint32_t dataLength;
    const void* data = entry->objectBuf->getValue(&dataLength);
    value->reset();
    value->appendCopy(data, dataLength);
}

} // namespace RAMCloud
