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
class EnlistServerRpc2 : public CoordinatorRpcWrapper {
    public:
    EnlistServerRpc2(Context& context, ServerId replacesId,
            ServiceMask serviceMask, string localServiceLocator,
            uint32_t readSpeed = 0, uint32_t writeSpeed = 0);
    ~EnlistServerRpc2() {}
    ServerId wait();

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(EnlistServerRpc2);
};

/**
 * Encapsulates the state of a CoordinatorClient::getServerList
 * request, allowing it to execute asynchronously.
 */
class GetServerListRpc2 : public CoordinatorRpcWrapper {
    public:
    GetServerListRpc2(Context& context, ServiceMask services);
    ~GetServerListRpc2() {}
    void wait(ProtoBuf::ServerList& serverList);

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetServerListRpc2);
};

/**
 * Encapsulates the state of a CoordinatorClient::getTabletMap
 * request, allowing it to execute asynchronously.
 */
class GetTabletMapRpc2 : public CoordinatorRpcWrapper {
    public:
    explicit GetTabletMapRpc2(Context& context);
    ~GetTabletMapRpc2() {}
    void wait(ProtoBuf::Tablets& tabletMap);

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetTabletMapRpc2);
};

/**
 * Encapsulates the state of a CoordinatorClient::hintServerDown
 * request, allowing it to execute asynchronously.
 */
class HintServerDownRpc2 : public CoordinatorRpcWrapper {
    public:
    HintServerDownRpc2(Context& context, ServerId serverId);
    ~HintServerDownRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(HintServerDownRpc2);
};

/**
 * Encapsulates the state of a CoordinatorClient::reassignTabletOwnership
 * request, allowing it to execute asynchronously.
 */
class ReassignTabletOwnershipRpc2 : public CoordinatorRpcWrapper {
    public:
    ReassignTabletOwnershipRpc2(Context& context, uint64_t tableId,
            uint64_t firstKey, uint64_t lastKey, ServerId newOwnerMasterId);
    ~ReassignTabletOwnershipRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ReassignTabletOwnershipRpc2);
};

/**
 * Encapsulates the state of a CoordinatorClient::recoveryMasterFinished
 * request, allowing it to execute asynchronously.
 */
class RecoveryMasterFinishedRpc2 : public CoordinatorRpcWrapper {
    public:
    RecoveryMasterFinishedRpc2(Context& context, uint64_t recoveryId,
            ServerId recoveryMasterId, const ProtoBuf::Tablets& tablets,
            bool successful);
    ~RecoveryMasterFinishedRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RecoveryMasterFinishedRpc2);
};

/**
 * Encapsulates the state of a CoordinatorClient::sendServerList
 * request, allowing it to execute asynchronously.
 */
class SendServerListRpc2 : public CoordinatorRpcWrapper {
    public:
    SendServerListRpc2(Context& context, ServerId destination);
    ~SendServerListRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SendServerListRpc2);
};

/**
 * Encapsulates the state of a CoordinatorClient::setMinOpenSegmentId
 * request, allowing it to execute asynchronously.
 */
class SetMinOpenSegmentIdRpc2 : public CoordinatorRpcWrapper {
    public:
    SetMinOpenSegmentIdRpc2(Context& context, ServerId serverId,
            uint64_t segmentId);
    ~SetMinOpenSegmentIdRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SetMinOpenSegmentIdRpc2);
};

} // end RAMCloud

#endif  // RAMCLOUD_COORDINATORCLIENT_H
