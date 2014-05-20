/* Copyright (c) 2010-2014 Stanford University
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

#ifndef RAMCLOUD_COORDINATORCLIENT_H
#define RAMCLOUD_COORDINATORCLIENT_H

#include "MasterRecoveryInfo.pb.h"
#include "ServerList.pb.h"
#include "RecoveryPartition.pb.h"
#include "TableConfig.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "CoordinatorRpcWrapper.h"
#include "ServiceMask.h"
#include "ServerId.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * This class implements RPC requests that are sent to the cluster
 * coordinator but are not implemented in the RamCloud class. The class
 * contains only static methods, so you shouldn't ever need to instantiate
 * an object.
 */
class CoordinatorClient {
  public:
    static ServerId enlistServer(Context* context, ServerId replacesId,
            ServiceMask serviceMask, string localServiceLocator,
            uint32_t readSpeed);
    static void getBackupList(Context* context,
            ProtoBuf::ServerList* serverList);
    static void getMasterList(Context* context,
            ProtoBuf::ServerList* serverList);
    static void getServerList(Context* context,
            ProtoBuf::ServerList* serverList);
    static void getTableConfig(Context* context,
            uint64_t tableId, ProtoBuf::TableConfig* tableConfig);
    static void hintServerCrashed(Context* context, ServerId serverId);
    static void reassignTabletOwnership(Context* context, uint64_t tableId,
            uint64_t firstKey, uint64_t lastKey, ServerId newOwnerId,
            uint64_t ctimeSegmentId, uint32_t ctimeSegmentOffset);
    static bool recoveryMasterFinished(Context* context, uint64_t recoveryId,
            ServerId recoveryMasterId,
            const ProtoBuf::RecoveryPartition* recoveryPartition,
            bool successful);
    static void sendServerList(Context* context, ServerId destination);
    static void setMasterRecoveryInfo(Context* context, ServerId serverId,
            const ProtoBuf::MasterRecoveryInfo& recoveryInfo);
    static void verifyMembership(Context* context, ServerId serverId,
            bool suicideOnFailure = true);

  private:
    CoordinatorClient();
};

/**
 * Encapsulates the state of a CoordinatorClient::enlistServer
 * request, allowing it to execute asynchronously.
 */
class EnlistServerRpc : public CoordinatorRpcWrapper {
    public:
    EnlistServerRpc(Context* context, ServerId replacesId,
            ServiceMask serviceMask, string localServiceLocator,
            uint32_t readSpeed);
    ~EnlistServerRpc() {}
    ServerId wait();

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(EnlistServerRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::getServerList
 * request, allowing it to execute asynchronously.
 */
class GetServerListRpc : public CoordinatorRpcWrapper {
    public:
    GetServerListRpc(Context* context, ServiceMask services);
    ~GetServerListRpc() {}
    void wait(ProtoBuf::ServerList* serverList);

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetServerListRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::getTableConfig
 * request, allowing it to execute asynchronously.
 */
class GetTableConfigRpc : public CoordinatorRpcWrapper {
    public:
    explicit GetTableConfigRpc(Context* context, uint64_t tableId);
    ~GetTableConfigRpc() {}
    void wait(ProtoBuf::TableConfig* tableConfig);

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetTableConfigRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::hintServerCrashed
 * request, allowing it to execute asynchronously.
 */
class HintServerCrashedRpc : public CoordinatorRpcWrapper {
    public:
    HintServerCrashedRpc(Context* context, ServerId serverId);
    ~HintServerCrashedRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(HintServerCrashedRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::reassignTabletOwnership
 * request, allowing it to execute asynchronously.
 */
class ReassignTabletOwnershipRpc : public CoordinatorRpcWrapper {
    public:
    ReassignTabletOwnershipRpc(Context* context, uint64_t tableId,
            uint64_t firstKey, uint64_t lastKey, ServerId newOwnerMasterId,
            uint64_t ctimeSegmentId, uint32_t ctimeSegmentOffset);
    ~ReassignTabletOwnershipRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ReassignTabletOwnershipRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::recoveryMasterFinished
 * request, allowing it to execute asynchronously.
 */
class RecoveryMasterFinishedRpc : public CoordinatorRpcWrapper {
    public:
    RecoveryMasterFinishedRpc(Context* context, uint64_t recoveryId,
            ServerId recoveryMasterId,
            const ProtoBuf::RecoveryPartition* recoveryPartition,
            bool successful);
    ~RecoveryMasterFinishedRpc() {}
    bool wait();

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RecoveryMasterFinishedRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::sendServerList
 * request, allowing it to execute asynchronously.
 */
class SendServerListRpc : public CoordinatorRpcWrapper {
    public:
    SendServerListRpc(Context* context, ServerId destination);
    ~SendServerListRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SendServerListRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::setMasterRecoveryInfo
 * request, allowing it to execute asynchronously.
 */
class SetMasterRecoveryInfoRpc : public CoordinatorRpcWrapper {
    public:
    SetMasterRecoveryInfoRpc(Context* context, ServerId serverId,
                             const ProtoBuf::MasterRecoveryInfo& recoveryInfo);
    ~SetMasterRecoveryInfoRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SetMasterRecoveryInfoRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::verifyMembership
 * request, allowing it to execute asynchronously.
 */
class VerifyMembershipRpc : public CoordinatorRpcWrapper {
    public:
    VerifyMembershipRpc(Context* context, ServerId serverId);
    ~VerifyMembershipRpc() {}
    void wait(bool suicideOnFailure = true);

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(VerifyMembershipRpc);
};

} // end RAMCloud

#endif  // RAMCLOUD_COORDINATORCLIENT_H
