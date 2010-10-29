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

#ifndef RAMCLOUD_MASTERSERVER_H
#define RAMCLOUD_MASTERSERVER_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "Object.h"
#include "Log.h"
#include "LogCleaner.h"
#include "BackupManager.h"
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
class MasterServer : public Server {
  public:
    /// The max number of tables a Master will serve.
    static const int NUM_TABLES = 4;

    /// The number of segments a Master can contain.
    static const uint32_t SEGMENT_COUNT =  64;

    /// The size of the Hashtable in cache lines.
    static const int HASH_NLINES = NUM_TABLES * 16384;

    MasterServer(const ServerConfig& config,
                 CoordinatorClient& coordinator,
                 BackupManager& backup);
    virtual ~MasterServer();
    void run();
    void dispatch(RpcType type,
                  Transport::ServerRpc& rpc,
                  Responder& responder);

  private:
    void create(const CreateRpc::Request& reqHdr,
                CreateRpc::Response& respHdr,
                Transport::ServerRpc& rpc);
    void read(const ReadRpc::Request& reqHdr,
              ReadRpc::Response& respHdr,
              Transport::ServerRpc& rpc);
    void remove(const RemoveRpc::Request& reqHdr,
                RemoveRpc::Response& respHdr,
                Transport::ServerRpc& rpc);
    void setTablets(const SetTabletsRpc::Request& reqHdr,
                    SetTabletsRpc::Response& respHdr,
                    Transport::ServerRpc& rpc);
    void write(const WriteRpc::Request& reqHdr,
               WriteRpc::Response& respHdr,
               Transport::ServerRpc& rpc);

    // The following variables are copies of constructor arguments;
    // see constructor documentation for details.
    const ServerConfig& config;

  public:
    CoordinatorClient& coordinator;

  private:
    uint64_t serverId;

    BackupManager& backup;

    /**
     * The main in-memory data structure holding all of the data stored
     * on this server.
     */
    Log* log;

    /**
     * The (table ID, object ID) to #RAMCloud::Object pointer map for the table.
     * Before accessing objects via the hash table, you usually need to check
     * that the tablet still lives on this server; objects from deleted tablets
     * are not immediately purged from the hash table.
     */
    HashTable objectMap;

    /**
     * Tablets this master owns.
     * The user_data field in each tablet points to a Table object.
     */
    ProtoBuf::Tablets tablets;

    friend void objectEvictionCallback(LogEntryType type,
            const void* p, uint64_t len, void* cookie);
    friend void tombstoneEvictionCallback(LogEntryType type,
            const void* p, uint64_t len, void* cookie);
    friend void segmentReplayCallback(Segment* seg, void* cookie);
    friend void objectReplayCallback(LogEntryType type,
            const void* p, uint64_t len, void* cookie);
    Table& getTable(uint32_t tableId, uint64_t objectId);
    void rejectOperation(const RejectRules* rejectRules, uint64_t version);
    void storeData(uint64_t table, uint64_t id,
                   const RejectRules* rejectRules, Buffer* data,
                   uint32_t dataOffset, uint32_t dataLength,
                   uint64_t* newVersion);
    friend class MasterTest;
    friend class CoordinatorTest;
    DISALLOW_COPY_AND_ASSIGN(MasterServer);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTERSERVER_H
