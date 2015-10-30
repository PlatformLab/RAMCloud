/* Copyright (c) 2010-2015 Stanford University
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

#ifndef RAMCLOUD_MASTERCLIENT_H
#define RAMCLOUD_MASTERCLIENT_H

#include "Buffer.h"
#include "CoordinatorClient.h"
#include "IndexRpcWrapper.h"
#include "Key.h"
#include "LogMetadata.h"
#include "ServerId.h"
#include "ServerIdRpcWrapper.h"
#include "ServerStatistics.pb.h"
#include "Transport.h"
#include "Tub.h"

namespace RAMCloud {

// forward declaration
class MasterService;
class Segment;

/**
 * Provides methods for invoking RPCs to RAMCloud masters.  The invoking
 * machine is typically another RAMCloud server (either master or backup)
 * or the cluster coordinator; these methods are not normally used by
 * RAMCloud applications. The class contains only static methods, so you
 * shouldn't ever need to instantiate an object.
 */
class MasterClient {
  public:
    static void dropIndexletOwnership(Context* context, ServerId id,
            uint64_t tableId, uint8_t indexId, const void *firstKey,
            uint16_t firstKeyLength, const void *firstNotOwnedKey,
            uint16_t firstNotOwnedKeyLength);
    static void dropTabletOwnership(Context* context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash);
    static LogPosition getHeadOfLog(Context* context, ServerId serverId);
    static void insertIndexEntry(MasterService* master,
            uint64_t tableId, uint8_t indexId,
            const void* indexKey, KeyLength indexKeyLength,
            uint64_t primaryKeyHash);
    static bool isReplicaNeeded(Context* context, ServerId serverId,
            ServerId backupServerId, uint64_t segmentId);
    static void prepForIndexletMigration(Context* context, ServerId serverId,
            uint64_t tableId, uint8_t indexId, uint64_t backingTableId,
            const void* firstKey, uint16_t firstKeyLength,
            const void* firstNotOwnedKey, uint16_t firstNotOwnedKeyLength);
    static void prepForMigration(Context* context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash);
    static void recover(Context* context, ServerId serverId,
            uint64_t recoveryId, ServerId crashedServerId,
            uint64_t partitionId,
            const ProtoBuf::RecoveryPartition* recoveryPartition,
            const WireFormat::Recover::Replica* replicas,
            uint32_t numReplicas);
    static void receiveMigrationData(Context* context, ServerId serverId,
            Segment* segment, uint64_t tableId, uint64_t firstKeyHash,
            bool isIndexletData = false,
            uint64_t dataTableId = 0, uint8_t indexId = 0,
            const void* key = NULL, uint16_t keyLength = 0);
    static void removeIndexEntry(MasterService* master,
            uint64_t tableId, uint8_t indexId,
            const void* indexKey, KeyLength indexKeyLength,
            uint64_t primaryKeyHash);
    static void splitAndMigrateIndexlet(Context* context,
            ServerId currentOwnerId, ServerId newOwnerId,
            uint64_t tableId, uint8_t indexId,
            uint64_t currentBackingTableId, uint64_t newBackingTableId,
            const void* splitKey, uint16_t splitKeyLength);
    static void splitMasterTablet(Context* context, ServerId serverId,
            uint64_t tableId, uint64_t splitKeyHash);
    static void takeTabletOwnership(Context* context, ServerId id,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash);
    static void takeIndexletOwnership(Context* context, ServerId id,
            uint64_t tableId, uint8_t indexId, uint64_t backingTableId,
            const void *firstKey, uint16_t firstKeyLength,
            const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength);
    static void txHintFailed(Context* context, uint64_t tableId,
            uint64_t keyHash, uint64_t leaseId, uint64_t clientTransactionId,
            uint32_t participantCount, WireFormat::TxParticipant *participants);

  private:
    MasterClient();
};

/**
 * Encapsulates the state of a MasterClient::dropIndexletOwnership
 * request, allowing it to execute asynchronously.
 */
class DropIndexletOwnershipRpc : public ServerIdRpcWrapper {
  public:
    DropIndexletOwnershipRpc(Context* context, ServerId serverId,
            uint64_t tableId, uint8_t indexId, const void *firstKey,
            uint16_t firstKeyLength, const void *firstNotOwnedKey,
            uint16_t firstNotOwnedKeyLength);
    ~DropIndexletOwnershipRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(DropIndexletOwnershipRpc);
};

/**
 * Encapsulates the state of a MasterClient::dropTabletOwnership
 * request, allowing it to execute asynchronously.
 */
class DropTabletOwnershipRpc : public ServerIdRpcWrapper {
  public:
    DropTabletOwnershipRpc(Context* context, ServerId serverId,
            uint64_t tableId, uint64_t firstKey, uint64_t lastKey);
    ~DropTabletOwnershipRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(DropTabletOwnershipRpc);
};

/**
 * Encapsulates the state of a MasterClient::getHeadOfLog
 * request, allowing it to execute asynchronously.
 */
class GetHeadOfLogRpc : public ServerIdRpcWrapper {
  public:
    GetHeadOfLogRpc(Context* context, ServerId serverId);
    ~GetHeadOfLogRpc() {}
    LogPosition wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetHeadOfLogRpc);
};

/**
 * Encapsulates the state of a MasterClient::insertIndexEntry
 * request, allowing it to execute asynchronously.
 */
class InsertIndexEntryRpc : public IndexRpcWrapper {
  public:
    InsertIndexEntryRpc(MasterService* master,
            uint64_t tableId, uint8_t indexId,
            const void* indexKey, KeyLength indexKeyLength,
            uint64_t primaryKeyHash);
    ~InsertIndexEntryRpc() {}
    void indexNotFound();
    void wait() {simpleWait(context);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(InsertIndexEntryRpc);
};

/**
 * Encapsulates the state of a MasterClient::isReplicaNeeded
 * request, allowing it to execute asynchronously.
 */
class IsReplicaNeededRpc : public ServerIdRpcWrapper {
  public:
    IsReplicaNeededRpc(Context* context, ServerId serverId,
            ServerId backupServerId, uint64_t segmentId);
    ~IsReplicaNeededRpc() {}
    bool wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IsReplicaNeededRpc);
};

/**
 * Encapsulates the state of a MasterClient::prepForIndexletMigration
 * request, allowing it to execute asynchronously.
 */
class PrepForIndexletMigrationRpc : public ServerIdRpcWrapper {
  public:
    PrepForIndexletMigrationRpc(Context* context, ServerId serverId,
            uint64_t tableId, uint8_t indexId,
            uint64_t backingTableId,
            const void* firstKey, uint16_t firstKeyLength,
            const void* firstNotOwnedKey, uint16_t firstNotOwnedKeyLength);
    ~PrepForIndexletMigrationRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(PrepForIndexletMigrationRpc);
};

/**
 * Encapsulates the state of a MasterClient::prepForMigration
 * request, allowing it to execute asynchronously.
 */
class PrepForMigrationRpc : public ServerIdRpcWrapper {
  public:
    PrepForMigrationRpc(Context* context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash);
    ~PrepForMigrationRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(PrepForMigrationRpc);
};

/**
 * Encapsulates the state of a MasterClient::receiveMigrationData
 * request, allowing it to execute asynchronously.
 */
class ReceiveMigrationDataRpc : public ServerIdRpcWrapper {
  public:
    ReceiveMigrationDataRpc(Context* context, ServerId serverId,
            Segment* segment, uint64_t tableId, uint64_t firstKey,
            bool isIndexletData, uint64_t dataTableId, uint8_t indexId,
            const void* key, uint16_t keyLength);
    ~ReceiveMigrationDataRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ReceiveMigrationDataRpc);
};

/**
 * Encapsulates the state of a MasterClient::recover
 * request, allowing it to execute asynchronously.
 */
class RecoverRpc : public ServerIdRpcWrapper {
  public:
    RecoverRpc(Context* context, ServerId serverId, uint64_t recoveryId,
            ServerId crashedServerId, uint64_t partitionId,
            const ProtoBuf::RecoveryPartition* recoverPartition,
            const WireFormat::Recover::Replica* replicas,
            uint32_t numReplicas);
    ~RecoverRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RecoverRpc);
};

/**
 * Encapsulates the state of a MasterClient::removeIndexEntry
 * request, allowing it to execute asynchronously.
 */
class RemoveIndexEntryRpc : public IndexRpcWrapper {
  public:
    RemoveIndexEntryRpc(MasterService* master,
             uint64_t tableId, uint8_t indexId,
             const void* indexKey, KeyLength indexKeyLength,
             uint64_t primaryKeyHash);
    ~RemoveIndexEntryRpc() {}
    void indexNotFound();
    void wait() {simpleWait(context);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RemoveIndexEntryRpc);
};

/**
 * Encapsulates the state of a MasterClient::splitAndMigrateIndexlet
 * request, allowing it to execute asynchronously.
 */
class SplitAndMigrateIndexletRpc : public ServerIdRpcWrapper {
  public:
    SplitAndMigrateIndexletRpc(Context* context,
            ServerId currentOwnerId, ServerId newOwnerId,
            uint64_t tableId, uint8_t indexId,
            uint64_t currentBackingTableId, uint64_t newBackingTableId,
            const void* splitKey, uint16_t splitKeyLength);
    ~SplitAndMigrateIndexletRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SplitAndMigrateIndexletRpc);
};

/**
 * Encapsulates the state of a MasterClient::splitMasterTablet
 * request, allowing it to execute asynchronously.
 */
class SplitMasterTabletRpc : public ServerIdRpcWrapper {
  public:
    SplitMasterTabletRpc(Context* context, ServerId serverId,
            uint64_t tableId, uint64_t splitKeyHash);
    ~SplitMasterTabletRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SplitMasterTabletRpc);
};

/**
 * Encapsulates the state of a MasterClient::takeTabletOwnership
 * request, allowing it to execute asynchronously.
 */
class TakeTabletOwnershipRpc : public ServerIdRpcWrapper {
  public:
    TakeTabletOwnershipRpc(Context* context, ServerId id,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash);
    ~TakeTabletOwnershipRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(TakeTabletOwnershipRpc);
};

/**
 * Encapsulates the state of a MasterClient::takeIndexletOwnership
 * request, allowing it to execute asynchronously.
 */
class TakeIndexletOwnershipRpc : public ServerIdRpcWrapper {
  public:
    TakeIndexletOwnershipRpc(Context* context, ServerId id, uint64_t tableId,
            uint8_t indexId, uint64_t backingTableId, const void *firstKey,
            uint16_t firstKeyLength, const void *firstNotOwnedKey,
            uint16_t firstNotOwnedKeyLength);
    ~TakeIndexletOwnershipRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(TakeIndexletOwnershipRpc);
};

/**
 * Encapsulates the state of a MasterClient::txHintFailedRpc
 * request, allowing it to execute asynchronously.
 */
class TxHintFailedRpc : public ObjectRpcWrapper {
  public:
    TxHintFailedRpc(Context* context, uint64_t tableId, uint64_t keyHash,
            uint64_t leaseId, uint64_t clientTransactionId,
            uint32_t participantCount, WireFormat::TxParticipant *participants);
    ~TxHintFailedRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(TxHintFailedRpc);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTERCLIENT_H
