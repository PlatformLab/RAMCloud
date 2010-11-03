/* Copyright (c) 2009-2010 Stanford University
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

#ifndef RAMCLOUD_COORDINATORSERVER_H
#define RAMCLOUD_COORDINATORSERVER_H

#include "ServerList.pb.h"
#include "Tablets.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "Metrics.h"
#include "Recovery.h"
#include "Rpc.h"
#include "Server.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Serves RPCs for the cluster coordinator.
 */
class CoordinatorServer : public Server {
  public:
    CoordinatorServer();
    ~CoordinatorServer();
    void run();
    void dispatch(RpcType type,
                  Transport::ServerRpc& rpc,
                  Responder& responder);

  private:
    void createTable(const CreateTableRpc::Request& reqHdr,
                     CreateTableRpc::Response& respHdr,
                     Transport::ServerRpc& rpc);
    void createTable(uint32_t tableId, ProtoBuf::ServerList_Entry& master);
    void dropTable(const DropTableRpc::Request& reqHdr,
                   DropTableRpc::Response& respHdr,
                   Transport::ServerRpc& rpc);
    void openTable(const OpenTableRpc::Request& reqHdr,
                   OpenTableRpc::Response& respHdr,
                   Transport::ServerRpc& rpc);
    void enlistServer(const EnlistServerRpc::Request& reqHdr,
                      EnlistServerRpc::Response& respHdr,
                      Transport::ServerRpc& rpc,
                      Responder& responder);

    void getBackupList(const GetBackupListRpc::Request& reqHdr,
                      GetBackupListRpc::Response& respHdr,
                      Transport::ServerRpc& rpc);

    void getTabletMap(const GetTabletMapRpc::Request& reqHdr,
                      GetTabletMapRpc::Response& respHdr,
                      Transport::ServerRpc& rpc);

    void hintServerDown(const HintServerDownRpc::Request& reqHdr,
                        HintServerDownRpc::Response& respHdr,
                        Transport::ServerRpc& rpc,
                        Responder& responder);

    /**
     * The server id for the next server to register.
     * These are guaranteed to be unique.
     */
    uint64_t nextServerId;

    /**
     * All known backups.
     */
    ProtoBuf::ServerList backupList;

    /**
     * All known masters.
     * The user_data field points to a heap-allocated ProtoBuf::Tablets object
     * representing the will. In the will, the user_data field is the partition
     * ID that is to recover that tablet, starting from 0.
     */
    ProtoBuf::ServerList masterList;

    /**
     * A pointer to the first master to have registered, or NULL if no masters
     * have registers. This is a temporary hack since all new tables and the
     * implicitly created table 0 all go to the same master.
     */
    ProtoBuf::ServerList_Entry* firstMaster;

    /**
     * What are the tablets, and who is the master for each.
     */
    ProtoBuf::Tablets tabletMap;

    typedef std::map<string, uint32_t> Tables;
    /**
     * Map from table name to table id.
     */
    Tables tables;

    /**
     * The id of the next table to be created.
     * These start at 0 and are never reused.
     */
    uint32_t nextTableId;

    /// Used in unit testing.
    MockRecovery* mockRecovery;

    friend class CoordinatorTest;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorServer);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVER_H
