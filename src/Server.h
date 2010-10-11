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

#ifndef RAMCLOUD_SERVER_H
#define RAMCLOUD_SERVER_H

#include "Common.h"
#include "Object.h"
#include "Log.h"
#include "BackupClient.h"
#include "HashTable.h"
#include "Rpc.h"
#include "Table.h"
#include "Transport.h"

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
class Server {
  public:
    Server(const ServerConfig* config,
           BackupClient* backupClient = 0);
    virtual ~Server();
    void handleRpc();
    void run();

    void create(const CreateRequest* reqHdr, CreateResponse* respHdr,
            Transport::ServerRpc* rpc);
    void createTable(const CreateTableRequest* reqHdr,
            CreateTableResponse* respHdr,
            Transport::ServerRpc* rpc);
    void dropTable(const DropTableRequest* reqHdr, DropTableResponse* respHdr,
            Transport::ServerRpc* rpc);
    void openTable(const OpenTableRequest* reqHdr, OpenTableResponse* respHdr,
            Transport::ServerRpc* rpc);
    void ping(const PingRequest* reqHdr, PingResponse* respHdr,
            Transport::ServerRpc* rpc);
    void read(const ReadRequest* reqHdr, ReadResponse* respHdr,
            Transport::ServerRpc* rpc);
    void remove(const RemoveRequest* reqHdr, RemoveResponse* respHdr,
            Transport::ServerRpc* rpc);
    void write(const WriteRequest* reqHdr, WriteResponse* respHdr,
            Transport::ServerRpc* rpc);


  protected:

    // The following variables are copies of constructor arguments;
    // see constructor documentation for details.
    const ServerConfig* config;
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
    const char* getString(Buffer* buffer, uint32_t offset, uint32_t length);
    Table* getTable(uint32_t tableId);
    Status rejectOperation(const RejectRules* rejectRules, uint64_t version);
    void restore();
    Status storeData(uint64_t table, uint64_t id,
            const RejectRules* rejectRules, Buffer* data,
            uint32_t dataOffset, uint32_t dataLength,
            uint64_t* newVersion);
    DISALLOW_COPY_AND_ASSIGN(Server);
};

} // namespace RAMCloud

#endif // RAMCLOUD_SERVER_H
