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

#ifndef RAMCLOUD_MASTER_H
#define RAMCLOUD_MASTER_H

#include "Common.h"
#include "Coordinator.h"
#include "Object.h"
#include "Log.h"
#include "LogCleaner.h"
#include "BackupClient.h"
#include "HashTable.h"
#include "Server.h"
#include "Table.h"

namespace RAMCloud {

struct ServerConfig {
    string coordinatorLocator;
    string localLocator;

    ServerConfig()
        : coordinatorLocator()
        , localLocator()
    {
    }
};

/**
 * An object of this class represents a RAMCloud server, which can
 * respond to client RPC requests to manipulate objects stored on the
 * server.
 */
class Master : public Server {
  public:
    Master(const ServerConfig* config,
           BackupClient* backup);
    virtual ~Master();
    void run();

  private:
    void dispatch(RpcType type,
                  Transport::ServerRpc& rpc);
    void create(const CreateRpc::Request& reqHdr,
                CreateRpc::Response& respHdr,
                Transport::ServerRpc& rpc);
    void createTable(const CreateTableRpc::Request& reqHdr,
                     CreateTableRpc::Response& respHdr,
                     Transport::ServerRpc& rpc);
    void dropTable(const DropTableRpc::Request& reqHdr,
                   DropTableRpc::Response& respHdr,
                   Transport::ServerRpc& rpc);
    void openTable(const OpenTableRpc::Request& reqHdr,
                   OpenTableRpc::Response& respHdr,
                   Transport::ServerRpc& rpc);
    void read(const ReadRpc::Request& reqHdr,
              ReadRpc::Response& respHdr,
              Transport::ServerRpc& rpc);
    void remove(const RemoveRpc::Request& reqHdr,
                RemoveRpc::Response& respHdr,
                Transport::ServerRpc& rpc);
    void write(const WriteRpc::Request& reqHdr,
               WriteRpc::Response& respHdr,
               Transport::ServerRpc& rpc);

    // The following variables are copies of constructor arguments;
    // see constructor documentation for details.
    const ServerConfig* config;

    Coordinator coordinator;

    uint64_t serverId;

    BackupClient* backup;

    /**
     * The main in-memory data structure holding all of the data stored
     * on this server.
     */
    Log* log;

    /**
     * Information about all of the tables in the system. Tables are
     * allocated lazily: a NULL entry means the corresponding table
     * does not yet exist.
     */
    Table *tables[RC_NUM_TABLES];

    friend void objectEvictionCallback(LogEntryType type,
            const void* p, uint64_t len, void* cookie);
    friend void tombstoneEvictionCallback(LogEntryType type,
            const void* p, uint64_t len, void* cookie);
    friend void segmentReplayCallback(Segment* seg, void* cookie);
    friend void objectReplayCallback(LogEntryType type,
            const void* p, uint64_t len, void* cookie);
    Table* getTable(uint32_t tableId);
    void rejectOperation(const RejectRules* rejectRules, uint64_t version);
    void storeData(uint64_t table, uint64_t id,
                   const RejectRules* rejectRules, Buffer* data,
                   uint32_t dataOffset, uint32_t dataLength,
                   uint64_t* newVersion);
    friend class Server;
    friend class MasterTest;
    DISALLOW_COPY_AND_ASSIGN(Master);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTER_H
