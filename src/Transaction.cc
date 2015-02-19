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
#include "RamCloud.h"
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
 * reached but not until the decisions are synced.
 *
 * \return
 *      True if the transaction was able to commit.  False otherwise.
 */
bool
Transaction::commit()
{
    if (!commitStarted) {
        commitStarted = true;
        ramcloud->transactionManager.addTransactionTask(taskPtr);
    }

    ClientTransactionTask* task = taskPtr.get();

    while (task->getDecision() == WireFormat::TxDecision::INVALID) {
        ramcloud->poll();
    }

    if (task->getStatus() != STATUS_OK) {
        ClientException::throwException(HERE, task->getStatus());
    }

    return (task->getDecision() == WireFormat::TxDecision::COMMIT);
}

/**
 * Block until the decision of this transaction commit is accepted by all
 * participant servers.  If the commit has not yet occurred and a decision is
 * not yet reached, this method will also start the commit.
 */
void
Transaction::sync()
{
    if (!commitStarted) {
        commitStarted = true;
        ramcloud->transactionManager.addTransactionTask(taskPtr);
    }

    ClientTransactionTask* task = taskPtr.get();

    while (!task->isReady()) {
        ramcloud->poll();
    }

    if (task->getStatus() != STATUS_OK) {
        ClientException::throwException(HERE, task->getStatus());
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
    ClientTransactionTask* task = taskPtr.get();
    return (task->getDecision() == WireFormat::TxDecision::COMMIT);
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
    if (expect_false(commitStarted)) {
        throw TxOpAfterCommit(HERE);
    }

    ClientTransactionTask* task = taskPtr.get();

    Key keyObj(tableId, key, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    if (entry == NULL) {
        ObjectBuffer buf;
        uint64_t version;
        ramcloud->readKeysAndValue(
                tableId, key, keyLength, &buf, NULL, &version);

        uint32_t dataLength;
        const void* data = buf.getValue(&dataLength);

        entry = task->insertCacheEntry(tableId, buf.getKey(),
                                       buf.getKeyLength(),
                                       data, dataLength);
        entry->type = ClientTransactionTask::CacheEntry::READ;
        entry->rejectRules.givenVersion = version;
        entry->rejectRules.versionNeGiven = true;
    } else if (entry->type == ClientTransactionTask::CacheEntry::REMOVE) {
        throw ObjectDoesntExistException(HERE);
    }

    uint32_t dataLength;
    const void* data = entry->objectBuf->getValue(&dataLength);
    value->reset();
    value->appendCopy(data, dataLength);
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
        entry = task->insertCacheEntry(tableId, key, keyLength, NULL, 0);
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
        entry = task->insertCacheEntry(tableId, key, keyLength, buf, length);
    } else {
        entry->objectBuf->reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, buf, length, entry->objectBuf, true);
    }

    entry->type = ClientTransactionTask::CacheEntry::WRITE;
}

} // namespace RAMCloud
