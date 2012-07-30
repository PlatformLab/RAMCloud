/* Copyright (c) 2010-2011 Stanford University
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

#include "Common.h"
#include "CoordinatorClient.h"
#include "Transport.h"
#include "Buffer.h"
#include "ServerId.h"
#include "ServerIdRpcWrapper.h"
#include "ServerStatistics.pb.h"
#include "Tub.h"
#include "LogTypes.h"

namespace RAMCloud {

/**
 * Provides methods for invoking RPCs to RAMCloud masters.  The invoking
 * machine is typically another RAMCloud server (either master or backup)
 * or the cluster coordinator; these methods are not normally used by
 * RAMCloud applications. The class contains only static methods, so you
 * shouldn't ever need to instantiate an object.
 */
class MasterClient {
  public:
    static void dropTabletOwnership(Context& context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash);
    static LogPosition getHeadOfLog(Context& context, ServerId serverId);
    static bool isReplicaNeeded(Context& context, ServerId serverId,
            ServerId backupServerId, uint64_t segmentId);
    static void prepForMigration(Context& context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
            uint64_t expectedObjects, uint64_t expectedBytes);
    static void recover(Context& context, ServerId serverId,
            uint64_t recoveryId, ServerId crashedServerId,
            uint64_t partitionId, const ProtoBuf::Tablets& tablets,
            const WireFormat::Recover::Replica* replicas,
            uint32_t numReplicas);
    static void receiveMigrationData(Context& context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, const void* segment,
            uint32_t segmentBytes);
    static void splitMasterTablet(Context& context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
            uint64_t splitKeyHash);
    static void takeTabletOwnership(Context& context, ServerId id,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash);

  private:
    MasterClient();
};

/**
 * Encapsulates the state of a MasterClient::dropTabletOwnership
 * request, allowing it to execute asynchronously.
 */
class DropTabletOwnershipRpc2 : public ServerIdRpcWrapper {
    public:
    DropTabletOwnershipRpc2(Context& context, ServerId serverId,
            uint64_t tableId, uint64_t firstKey, uint64_t lastKey);
    ~DropTabletOwnershipRpc2() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(DropTabletOwnershipRpc2);
};

/**
 * Encapsulates the state of a MasterClient::getHeadOfLog
 * request, allowing it to execute asynchronously.
 */
class GetHeadOfLogRpc2 : public ServerIdRpcWrapper {
    public:
    GetHeadOfLogRpc2(Context& context, ServerId serverId);
    ~GetHeadOfLogRpc2() {}
    LogPosition wait();

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetHeadOfLogRpc2);
};

/**
 * Encapsulates the state of a MasterClient::isReplicaNeeded
 * request, allowing it to execute asynchronously.
 */
class IsReplicaNeededRpc2 : public ServerIdRpcWrapper {
    public:
    IsReplicaNeededRpc2(Context& context, ServerId serverId,
            ServerId backupServerId, uint64_t segmentId);
    ~IsReplicaNeededRpc2() {}
    bool wait();

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IsReplicaNeededRpc2);
};

/**
 * Encapsulates the state of a MasterClient::prepForMigration
 * request, allowing it to execute asynchronously.
 */
class PrepForMigrationRpc2 : public ServerIdRpcWrapper {
    public:
    PrepForMigrationRpc2(Context& context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
            uint64_t expectedObjects, uint64_t expectedBytes);
    ~PrepForMigrationRpc2() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(PrepForMigrationRpc2);
};

/**
 * Encapsulates the state of a MasterClient::receiveMigrationData
 * request, allowing it to execute asynchronously.
 */
class ReceiveMigrationDataRpc2 : public ServerIdRpcWrapper {
    public:
    ReceiveMigrationDataRpc2(Context& context, ServerId serverId,
            uint64_t tableId, uint64_t firstKey, const void* segment,
            uint32_t segmentBytes);
    ~ReceiveMigrationDataRpc2() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ReceiveMigrationDataRpc2);
};

/**
 * Encapsulates the state of a MasterClient::recover
 * request, allowing it to execute asynchronously.
 */
class RecoverRpc2 : public ServerIdRpcWrapper {
    public:
    RecoverRpc2(Context& context, ServerId serverId, uint64_t recoveryId,
            ServerId crashedServerId, uint64_t partitionId,
            const ProtoBuf::Tablets& tablets,
            const WireFormat::Recover::Replica* replicas,
            uint32_t numReplicas);
    ~RecoverRpc2() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RecoverRpc2);
};

/**
 * Encapsulates the state of a MasterClient::splitMasterTablet
 * request, allowing it to execute asynchronously.
 */
class SplitMasterTabletRpc2 : public ServerIdRpcWrapper {
    public:
    SplitMasterTabletRpc2(Context& context, ServerId serverId,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
            uint64_t splitKeyHash);
    ~SplitMasterTabletRpc2() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SplitMasterTabletRpc2);
};

/**
 * Encapsulates the state of a MasterClient::takeTabletOwnership
 * request, allowing it to execute asynchronously.
 */
class TakeTabletOwnershipRpc2 : public ServerIdRpcWrapper {
    public:
    TakeTabletOwnershipRpc2(Context& context, ServerId id,
            uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash);
    ~TakeTabletOwnershipRpc2() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(TakeTabletOwnershipRpc2);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTERCLIENT_H
