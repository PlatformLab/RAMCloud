/* Copyright (c) 2010 Stanford University
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
#include "ServiceMask.h"
#include "ServerId.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Proxies methods as RPCs to the cluster coordinator.
 */
class CoordinatorClient : public Client {
  public:
    explicit CoordinatorClient(const char* coordinatorLocator)
        : session(Context::get().transportManager->
                    getSession(coordinatorLocator))
    {
    }

    void createTable(const char* name);
    void dropTable(const char* name);
    uint32_t openTable(const char* name);

    ServerId enlistServer(ServerId replacesId,
                          ServiceMask serviceMask,
                          string localServiceLocator,
                          uint32_t readSpeed = 0,
                          uint32_t writeSpeed = 0);
    void getServerList(ProtoBuf::ServerList& serverList);
    void getMasterList(ProtoBuf::ServerList& serverList);
    void getBackupList(ProtoBuf::ServerList& serverList);
    void getTabletMap(ProtoBuf::Tablets& tabletMap);
    void hintServerDown(ServerId serverId);
    void quiesce();
    void tabletsRecovered(ServerId masterId,
                          const ProtoBuf::Tablets& tablets);
    void setWill(uint64_t masterId, const ProtoBuf::Tablets& will);
    void requestServerList(ServerId destination);

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

    Transport::SessionRef session;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorClient);
};

} // end RAMCloud

#endif  // RAMCLOUD_COORDINATORCLIENT_H
