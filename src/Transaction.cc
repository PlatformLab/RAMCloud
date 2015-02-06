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
    , participantCount(0)
    , participantList()
    , commitStarted(false)
    , status(STATUS_OK)
    , decision(WireFormat::TxDecision::COMMIT)
    , lease()
    , txId(0)
    , prepareRpcs()
    , decisionRpcs()
    , commitCache()
    , nextCacheEntry()
{
}

/**
 * Commits the transaction defined by the operations performed on this
 * transaction (read, remove, write).
 *
 * \return
 *      True if the transaction was able to commit.  False otherwise.
 */
bool
Transaction::commit()
{
    if (expect_false(commitStarted)) {
        // TODO(cstlee) : pick a better exception.
        throw TxOpAfterCommit(HERE);
    }

    commitStarted = true;
    lease = ramcloud->clientLease.getLease();
    txId = ramcloud->rpcTracker.newRpcId(NULL);

    // Build participant list
    buildParticipantList();

    nextCacheEntry = commitCache.begin();
    while (true) {
        processPrepareRpcs();
        sendPrepareRpc();
        if (prepareRpcs.empty() && nextCacheEntry == commitCache.end())
            break;
        ramcloud->poll();
    }

    assert(decision != WireFormat::TxDecision::INVALID);
    nextCacheEntry = commitCache.begin();

    while (true) {
        processDecisionRpcs();
        sendDecisionRpc();
        if (decisionRpcs.empty() && nextCacheEntry == commitCache.end())
            break;
        ramcloud->poll();
    }

    ramcloud->rpcTracker.rpcFinished(txId);

    if (status != STATUS_OK)
        ClientException::throwException(HERE, status);

    return (decision == WireFormat::TxDecision::COMMIT);
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
        // TODO(cstlee) : pick a better exception.
        throw TxOpAfterCommit(HERE);
    }

    Key keyObj(tableId, key, keyLength);
    CacheEntry* entry = findCacheEntry(keyObj);

    if (entry == NULL) {
        ObjectBuffer buf;
        uint64_t version;
        ramcloud->readKeysAndValue(
                tableId, key, keyLength, &buf, NULL, &version);

        uint32_t dataLength;
        const void* data = buf.getValue(&dataLength);

        entry = insertCacheEntry(tableId, buf.getKey(), buf.getKeyLength(),
                                 data, dataLength);
        entry->type = CacheEntry::READ;
        entry->rejectRules.givenVersion = version;
        entry->rejectRules.versionNeGiven = true;
    } else if (entry->type == CacheEntry::REMOVE) {
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
        // TODO(cstlee) : pick a better exception.
        throw TxOpAfterCommit(HERE);
    }

    Key keyObj(tableId, key, keyLength);
    CacheEntry* entry = findCacheEntry(keyObj);

    if (entry == NULL) {
        entry = insertCacheEntry(tableId, key, keyLength, NULL, 0);
    } else {
        entry->objectBuf->reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, NULL, 0, entry->objectBuf, true);
    }

    entry->type = CacheEntry::REMOVE;
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
        // TODO(cstlee) : pick a better exception.
        throw TxOpAfterCommit(HERE);
    }

    Key keyObj(tableId, key, keyLength);
    CacheEntry* entry = findCacheEntry(keyObj);

    if (entry == NULL) {
        entry = insertCacheEntry(tableId, key, keyLength, buf, length);
    } else {
        entry->objectBuf->reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, buf, length, entry->objectBuf, true);
    }

    entry->type = CacheEntry::WRITE;
}

/**
 * Find and return the cache entry identified by the given key.
 *
 * \param key
 *      Key of the object contained in the cache entry that should be returned.
 * \return
 *      Returns a pointer to the cache entry if found.  Returns NULL otherwise.
 *      Pointer is invalid once the commitCache is modified.
 */
Transaction::CacheEntry*
Transaction::findCacheEntry(Key& key)
{
    CacheKey cacheKey = {key.getTableId(), key.getHash()};
    CommitCacheMap::iterator it = commitCache.lower_bound(cacheKey);
    CacheEntry* entry = NULL;

    while (it != commitCache.end()) {
        if (cacheKey < it->first) {
            break;
        } else if (it->second.objectBuf) {
            Key otherKey(it->first.tableId,
                         it->second.objectBuf->getKey(),
                         it->second.objectBuf->getKeyLength());
            if (key == otherKey) {
                entry = &it->second;
                break;
            }
        }
        it++;
    }
    return entry;
}

/**
 * Inserts a new cache entry with the provided key and value.  Other members
 * of the cache entry are left to their default values.
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
 * \return
 *      Returns a pointer to the inserted cache entry.  Pointer is invalid
 *      once the commitCache is modified.
 */
Transaction::CacheEntry*
Transaction::insertCacheEntry(uint64_t tableId, const void* key,
        uint16_t keyLength, const void* buf, uint32_t length)
{
    Key keyObj(tableId, key, keyLength);
    CacheKey cacheKey = {keyObj.getTableId(), keyObj.getHash()};
    CommitCacheMap::iterator it = commitCache.insert(
            std::make_pair(cacheKey, CacheEntry()));
    it->second.objectBuf = new ObjectBuffer();
    Object::appendKeysAndValueToBuffer(
            keyObj, buf, length, it->second.objectBuf, true);
    return &it->second;
}

/**
 * Builds the send-ready buffer of participants to be included in every prepare
 * rpc.  Used in the commit method.  Factored out mostly for ease of testing.
 */
void
Transaction::buildParticipantList()
{
    nextCacheEntry = commitCache.begin();
    while (nextCacheEntry != commitCache.end()) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        entry->rpcId = ramcloud->rpcTracker.newRpcId(NULL);
        participantList.emplaceAppend<WireFormat::TxParticipant>(
                key->tableId,
                static_cast<uint64_t>(key->keyHash),
                entry->rpcId);
        participantCount++;
        nextCacheEntry++;
    }
}

/**
 * Process any decision rpcs that have completed.  Used in the commit method.
 * Factored out mostly for ease of testing.
 */
void
Transaction::processDecisionRpcs()
{
    // Process outstanding RPCs.
    std::list<DecisionRpc>::iterator it = decisionRpcs.begin();
    for (; it != decisionRpcs.end(); it++) {
        DecisionRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }

        WireFormat::TxDecision::Response* respHdr =
                rpc->response->getStart<WireFormat::TxDecision::Response>();
        if (respHdr != NULL) {
            if (respHdr->common.status == STATUS_OK) {
                for (uint32_t i = 0; i < DecisionRpc::MAX_OBJECTS_PER_RPC; i++)
                    ramcloud->rpcTracker.rpcFinished(rpc->ops[i]->second.rpcId);
            } else if (respHdr->common.status == STATUS_UNKNOWN_TABLET) {
                // Nothing to do.
            } else {
                status = respHdr->common.status;
            }
        }

        // Destroy object.
        it = decisionRpcs.erase(it);
    }
}

/**
 * Process any prepare rpcs that have completed.  Used in the commit method.
 * Factored out mostly for ease of testing.
 */
void
Transaction::processPrepareRpcs()
{
    // Process outstanding RPCs.
    std::list<PrepareRpc>::iterator it = prepareRpcs.begin();
    for (; it != prepareRpcs.end(); it++) {
        PrepareRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }

        WireFormat::TxPrepare::Response* respHdr =
                rpc->response->getStart<WireFormat::TxPrepare::Response>();
        if (respHdr != NULL) {
            if (respHdr->common.status == STATUS_OK) {
                if (respHdr->vote != WireFormat::TxPrepare::COMMIT) {
                    decision = WireFormat::TxDecision::ABORT;
                }
            } else if (respHdr->common.status == STATUS_UNKNOWN_TABLET) {
                // Nothing to do.
            } else {
                status = respHdr->common.status;
            }
        }

        // Destroy object.
        it = prepareRpcs.erase(it);
    }
}

/**
 * Send out a decision rpc if not all master have been notified.  Used in the
 * commit method.  Factored out mostly for ease of testing.
 */
void
Transaction::sendDecisionRpc()
{
    // Issue an additional rpc.
    DecisionRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextCacheEntry != commitCache.end(); nextCacheEntry++) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        if (entry->state == CacheEntry::DECIDE ||
            entry->state == CacheEntry::FAILED) {
            continue;
        }

        try {
            if (nextRpc == NULL) {
                rpcSession =
                        ramcloud->objectFinder.lookup(key->tableId,
                                                      key->keyHash);
                decisionRpcs.emplace_back(ramcloud, rpcSession, this);
                nextRpc = &decisionRpcs.back();
            }

            Transport::SessionRef session =
                    ramcloud->objectFinder.lookup(key->tableId, key->keyHash);
            if (session->getServiceLocator() == rpcSession->getServiceLocator()
                && nextRpc->reqHdr->participantCount <
                        DecisionRpc::MAX_OBJECTS_PER_RPC) {
                nextRpc->appendOp(nextCacheEntry);
            } else {
                break;
            }
        } catch (TableDoesntExistException&) {
            entry->state = CacheEntry::FAILED;
            status = STATUS_TABLE_DOESNT_EXIST;
        }
    }
    if (nextRpc) {
        nextRpc->send();
    }
}

/**
 * Send out a prepare rpc if there are remaining un-prepared transaction ops.
 * Used in the commit method.  Factored out mostly for ease of testing.
 */
void
Transaction::sendPrepareRpc()
{
    // Issue an additional rpc.
    PrepareRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextCacheEntry != commitCache.end(); nextCacheEntry++) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        if (entry->state == CacheEntry::PREPARE ||
            entry->state == CacheEntry::FAILED) {
            continue;
        }

        try {
            if (nextRpc == NULL) {
                rpcSession =
                        ramcloud->objectFinder.lookup(key->tableId,
                                                      key->keyHash);
                prepareRpcs.emplace_back(ramcloud, rpcSession, this);
                nextRpc = &prepareRpcs.back();
            }

            Transport::SessionRef session =
                    ramcloud->objectFinder.lookup(key->tableId, key->keyHash);
            if (session->getServiceLocator() == rpcSession->getServiceLocator()
                && nextRpc->reqHdr->opCount < PrepareRpc::MAX_OBJECTS_PER_RPC) {
                nextRpc->appendOp(nextCacheEntry);
            } else {
                break;
            }
        } catch (TableDoesntExistException&) {
            entry->state = CacheEntry::FAILED;
            status = STATUS_TABLE_DOESNT_EXIST;
        }
    }
    if (nextRpc) {
        nextRpc->send();
    }
}

/**
 * Constructor for a decision rpc.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param transaction
 *      Pointer to the transaction that issued this request.
 */
Transaction::DecisionRpc::DecisionRpc(RamCloud* ramcloud,
        Transport::SessionRef session,
        Transaction* transaction)
    : RpcWrapper(sizeof(WireFormat::TxDecision::Request))
    , ramcloud(ramcloud)
    , session(session)
    , transaction(transaction)
    , ops()
    , reqHdr(allocHeader<WireFormat::TxDecision>())
{
    reqHdr->decision = transaction->decision;
    reqHdr->leaseId = transaction->lease.leaseId;
    reqHdr->participantCount = 0;
}

// See RpcWrapper for documentation.
bool
Transaction::DecisionRpc::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        retryRequest();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
Transaction::DecisionRpc::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        ramcloud->clientContext->transportManager->flushSession(
                session->getServiceLocator());
        session = NULL;
    }
    retryRequest();
    return true;
}

// See RpcWrapper for documentation.
void
Transaction::DecisionRpc::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Append an operation to the end of this decision rpc.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 */
void
Transaction::DecisionRpc::appendOp(CommitCacheMap::iterator opEntry)
{
    const CacheKey* key = &opEntry->first;
    CacheEntry* entry = &opEntry->second;

    request.emplaceAppend<WireFormat::TxParticipant>(
            key->tableId,
            static_cast<uint64_t>(key->keyHash),
            entry->rpcId);

    entry->state = CacheEntry::DECIDE;
    ops[reqHdr->participantCount] = opEntry;
    reqHdr->participantCount++;
}

/**
 * Handle the case where the RPC may have been sent to the wrong server.
 */
void
Transaction::DecisionRpc::retryRequest()
{
    for (uint32_t i = 0; i < reqHdr->participantCount; i++) {
        const CacheKey* key = &ops[i]->first;
        CacheEntry* entry = &ops[i]->second;
        ramcloud->objectFinder.flush(key->tableId);
        entry->state = CacheEntry::PENDING;
    }
    transaction->nextCacheEntry = transaction->commitCache.begin();
}

/**
 * Constructor for PrepareRpc.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param transaction
 *      Pointer to the transaction that issued this request.
 */
Transaction::PrepareRpc::PrepareRpc(RamCloud* ramcloud,
        Transport::SessionRef session, Transaction* transaction)
    : RpcWrapper(sizeof(WireFormat::TxPrepare::Request))
    , ramcloud(ramcloud)
    , session(session)
    , transaction(transaction)
    , ops()
    , reqHdr(allocHeader<WireFormat::TxPrepare>())
{
    reqHdr->lease = transaction->lease;
    reqHdr->participantCount = transaction->participantCount;
    reqHdr->opCount = 0;
    request.appendExternal(&transaction->participantList);
}

// See RpcWrapper for documentation.
bool
Transaction::PrepareRpc::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        retryRequest();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
Transaction::PrepareRpc::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        ramcloud->clientContext->transportManager->flushSession(
                session->getServiceLocator());
        session = NULL;
    }
    retryRequest();
    return true;
}

// See RpcWrapper for documentation.
void
Transaction::PrepareRpc::send()
{
    reqHdr->ackId = ramcloud->rpcTracker.ackId();
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Append an operation to the end of this prepare rpc.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 */
void
Transaction::PrepareRpc::appendOp(CommitCacheMap::iterator opEntry)
{
    const CacheKey* key = &opEntry->first;
    CacheEntry* entry = &opEntry->second;

    switch (entry->type) {
        case CacheEntry::READ:
            request.emplaceAppend<WireFormat::TxPrepare::Request::ReadOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf->getKeyLength(), entry->rejectRules);
            request.appendExternal(entry->objectBuf->getKey(),
                    entry->objectBuf->getKeyLength());
            break;
        case CacheEntry::REMOVE:
            request.emplaceAppend<WireFormat::TxPrepare::Request::RemoveOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf->getKeyLength(), entry->rejectRules);
            request.appendExternal(entry->objectBuf->getKey(),
                    entry->objectBuf->getKeyLength());
            break;
        case CacheEntry::WRITE:
            request.emplaceAppend<WireFormat::TxPrepare::Request::WriteOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf->size(), entry->rejectRules);
            request.appendExternal(entry->objectBuf);
            break;
        default:
            RAMCLOUD_LOG(ERROR, "Unknown transaction op type.");
            return;
    }

    entry->state = CacheEntry::PREPARE;
    ops[reqHdr->opCount] = opEntry;
    reqHdr->opCount++;
}

/**
 * Handle the case where the RPC may have been sent to the wrong server.
 */
void
Transaction::PrepareRpc::retryRequest()
{
    for (uint32_t i = 0; i < reqHdr->opCount; i++) {
        const CacheKey* key = &ops[i]->first;
        CacheEntry* entry = &ops[i]->second;
        ramcloud->objectFinder.flush(key->tableId);
        entry->state = CacheEntry::PENDING;
    }
    transaction->nextCacheEntry = transaction->commitCache.begin();
}

} // namespace RAMCloud
