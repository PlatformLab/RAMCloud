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
#include "Client.h"
#include "ClientException.h"
#include "CoordinatorRpcWrapper.h"
#include "ServiceMask.h"
#include "ServerId.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Provides methods that can be used to invoke RPCs on the cluster
 * coordinator. 
 */
class CoordinatorClient : public Client {
  public:
    explicit CoordinatorClient(Context& context,
                               const char* coordinatorLocator)
            :context(context)
    {
    }

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
    static ServerId enlistServer(Context& context, ServerId replacesId,
            ServiceMask serviceMask, string localServiceLocator,
            uint32_t readSpeed = 0, uint32_t writeSpeed = 0);

    //-------------------------------------------------------
    // OLD: everything below here should eventually go away.
    //-------------------------------------------------------

    void createTable(const char* name, uint32_t serverSpan = 1);
    void dropTable(const char* name);
    void splitTablet(const char* name, uint64_t startKeyHash,
                uint64_t endKeyHash, uint64_t splitKeyHash);
    uint64_t getTableId(const char* name);
    void getServerList(ProtoBuf::ServerList& serverList);
    void getMasterList(ProtoBuf::ServerList& serverList);
    void getBackupList(ProtoBuf::ServerList& serverList);
    void getTabletMap(ProtoBuf::Tablets& tabletMap);
    void hintServerDown(ServerId serverId);
    void quiesce();
    void reassignTabletOwnership(uint64_t tableId,
                                 uint64_t firstKey,
                                 uint64_t lastKey,
                                 ServerId newOwnerMasterId);
    void recoveryMasterFinished(uint64_t recoveryId,
                                ServerId recoveryMasterId,
                                const ProtoBuf::Tablets& tablets,
                                bool successful);
    void setWill(uint64_t masterId, const ProtoBuf::Tablets& will);
    void sendServerList(ServerId destination);

    class SetMinOpenSegmentId {
      public:
        SetMinOpenSegmentId(CoordinatorClient& client,
                            ServerId serverId, uint64_t segmentId);
        bool isReady() { return state.isReady(); }
        void operator()();
      private:
        CoordinatorClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(SetMinOpenSegmentId);
    };
    DEF_SYNC_RPC_METHOD(setMinOpenSegmentId, SetMinOpenSegmentId);

  private:
    void getServerList(ServiceMask services, ProtoBuf::ServerList& serverList);

    Context& context;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorClient);
};

} // end RAMCloud

#endif  // RAMCLOUD_COORDINATORCLIENT_H
