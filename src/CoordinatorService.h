/* Copyright (c) 2009-2011 Stanford University
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

#ifndef RAMCLOUD_COORDINATORSERVICE_H
#define RAMCLOUD_COORDINATORSERVICE_H

#include "ServerList.pb.h"
#include "Tablets.pb.h"

#include "Common.h"
#include "ClientException.h"
#include "Metrics.h"
#include "Recovery.h"
#include "Rpc.h"
#include "Service.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Serves RPCs for the cluster coordinator.
 */
class CoordinatorService : public Service {
  public:
    CoordinatorService();
    ~CoordinatorService();
    void dispatch(RpcOpcode opcode,
                  Rpc& rpc);

  private:
    void createTable(const CreateTableRpc::Request& reqHdr,
                     CreateTableRpc::Response& respHdr,
                     Rpc& rpc);
    void dropTable(const DropTableRpc::Request& reqHdr,
                   DropTableRpc::Response& respHdr,
                   Rpc& rpc);
    void openTable(const OpenTableRpc::Request& reqHdr,
                   OpenTableRpc::Response& respHdr,
                   Rpc& rpc);
    void enlistServer(const EnlistServerRpc::Request& reqHdr,
                      EnlistServerRpc::Response& respHdr,
                      Rpc& rpc);

    void getServerList(const GetServerListRpc::Request& reqHdr,
                       GetServerListRpc::Response& respHdr,
                       Rpc& rpc);

    void getTabletMap(const GetTabletMapRpc::Request& reqHdr,
                      GetTabletMapRpc::Response& respHdr,
                      Rpc& rpc);

    void hintServerDown(const HintServerDownRpc::Request& reqHdr,
                        HintServerDownRpc::Response& respHdr,
                        Rpc& rpc);

    void tabletsRecovered(const TabletsRecoveredRpc::Request& reqHdr,
                          TabletsRecoveredRpc::Response& respHdr,
                          Rpc& rpc);

    void quiesce(const BackupQuiesceRpc::Request& reqHdr,
                 BackupQuiesceRpc::Response& respHdr,
                 Rpc& rpc);

    void setWill(const SetWillRpc::Request& reqHdr,
                 SetWillRpc::Response& respHdr,
                 Rpc& rpc);

    bool setWill(uint64_t masterId, Buffer& buffer,
                 uint32_t offset, uint32_t length);

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

    /**
     * Used in #createTable() to assign new tables to masters.
     * If you take this modulo the number of entries in #masterList, you get
     * the index into #masterList of the master that should be assigned the
     * next table.
     */
    uint32_t nextTableMasterIdx;

    /// Used in unit testing.
    BaseRecovery* mockRecovery;

    friend class CoordinatorServiceTest;
    DISALLOW_COPY_AND_ASSIGN(CoordinatorService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVICE_H
