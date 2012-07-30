/* Copyright (c) 2010-2012 Stanford University
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

#include "ServerList.pb.h"
#include "Tablets.pb.h"

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
    static ServerId enlistServer(Context& context, ServerId replacesId,
            ServiceMask serviceMask, string localServiceLocator,
            uint32_t readSpeed = 0, uint32_t writeSpeed = 0);
    static void getBackupList(Context& context,
            ProtoBuf::ServerList& serverList);
    static void getMasterList(Context& context,
            ProtoBuf::ServerList& serverList);
    static void getServerList(Context& context,
            ProtoBuf::ServerList& serverList);
    static void getTabletMap(Context& context, ProtoBuf::Tablets& tabletMap);
    static void hintServerDown(Context& context, ServerId serverId);
    static void reassignTabletOwnership(Context& context, uint64_t tableId,
            uint64_t firstKey, uint64_t lastKey, ServerId newOwnerId);
    static void recoveryMasterFinished(Context& context, uint64_t recoveryId,
            ServerId recoveryMasterId, const ProtoBuf::Tablets& tablets,
            bool successful);
    static void sendServerList(Context& context, ServerId destination);
    static void setMinOpenSegmentId(Context& context, ServerId serverId,
            uint64_t segmentId);
    static void setRuntimeOption(Context& context, const char* option,
            const char* value);

  private:
    CoordinatorClient();
};

/**
 * Encapsulates the state of a CoordinatorClient::enlistServer
 * request, allowing it to execute asynchronously.
 */
class EnlistServerRpc : public CoordinatorRpcWrapper {
    public:
    EnlistServerRpc(Context& context, ServerId replacesId,
            ServiceMask serviceMask, string localServiceLocator,
            uint32_t readSpeed = 0, uint32_t writeSpeed = 0);
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
    GetServerListRpc(Context& context, ServiceMask services);
    ~GetServerListRpc() {}
    void wait(ProtoBuf::ServerList& serverList);

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetServerListRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::getTabletMap
 * request, allowing it to execute asynchronously.
 */
class GetTabletMapRpc : public CoordinatorRpcWrapper {
    public:
    explicit GetTabletMapRpc(Context& context);
    ~GetTabletMapRpc() {}
    void wait(ProtoBuf::Tablets& tabletMap);

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetTabletMapRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::hintServerDown
 * request, allowing it to execute asynchronously.
 */
class HintServerDownRpc : public CoordinatorRpcWrapper {
    public:
    HintServerDownRpc(Context& context, ServerId serverId);
    ~HintServerDownRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(HintServerDownRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::reassignTabletOwnership
 * request, allowing it to execute asynchronously.
 */
class ReassignTabletOwnershipRpc : public CoordinatorRpcWrapper {
    public:
    ReassignTabletOwnershipRpc(Context& context, uint64_t tableId,
            uint64_t firstKey, uint64_t lastKey, ServerId newOwnerMasterId);
    ~ReassignTabletOwnershipRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ReassignTabletOwnershipRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::recoveryMasterFinished
 * request, allowing it to execute asynchronously.
 */
class RecoveryMasterFinishedRpc : public CoordinatorRpcWrapper {
    public:
    RecoveryMasterFinishedRpc(Context& context, uint64_t recoveryId,
            ServerId recoveryMasterId, const ProtoBuf::Tablets& tablets,
            bool successful);
    ~RecoveryMasterFinishedRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RecoveryMasterFinishedRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::sendServerList
 * request, allowing it to execute asynchronously.
 */
class SendServerListRpc : public CoordinatorRpcWrapper {
    public:
    SendServerListRpc(Context& context, ServerId destination);
    ~SendServerListRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SendServerListRpc);
};

/**
 * Encapsulates the state of a CoordinatorClient::setMinOpenSegmentId
 * request, allowing it to execute asynchronously.
 */
class SetMinOpenSegmentIdRpc : public CoordinatorRpcWrapper {
    public:
    SetMinOpenSegmentIdRpc(Context& context, ServerId serverId,
            uint64_t segmentId);
    ~SetMinOpenSegmentIdRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SetMinOpenSegmentIdRpc);
};

} // end RAMCloud

#endif  // RAMCLOUD_COORDINATORCLIENT_H
