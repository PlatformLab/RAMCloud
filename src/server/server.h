/* Copyright (c) 2009 Stanford University
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

#ifndef RAMCLOUD_SERVER_SERVER_H
#define RAMCLOUD_SERVER_SERVER_H

#include <config.h>

#include <shared/object.h>
#include <shared/rcrpc.h>

#include <server/backup_client.h>
#include <server/net.h>
#include <server/hashtable.h>

namespace RAMCloud {

struct object {
    chunk_hdr hdr;
    char blob[1024];
};

class Table {
  public:
    static const int TABLE_NAME_MAX_LEN = 64;
    explicit Table() : next_key(0), object_map(HASH_NLINES) {}
    const char *GetName() { return &name[0]; }
    void SetName(const char *new_name) {
        strncpy(&name[0], new_name, TABLE_NAME_MAX_LEN);
        name[TABLE_NAME_MAX_LEN - 1] = '\0';
    }
    uint64_t AllocateKey() {
        while (Get(next_key))
            ++next_key;
        return next_key;
    }
    object *Get(uint64_t key) {
        void *val = object_map.Lookup(key);
        return static_cast<object *>(val);
    }
    void Put(uint64_t key, object *o) {
        object_map.Delete(key);
        object_map.Insert(key, o);
    }
  private:
    char name[64];
    uint64_t next_key;
    Hashtable object_map;
    DISALLOW_COPY_AND_ASSIGN(Table);
};

class Server {
  public:
    void Ping(const struct rcrpc *req, struct rcrpc *resp);
    void Read(const struct rcrpc *req, struct rcrpc *resp);
    void Write(const struct rcrpc *req, struct rcrpc *resp);
    void InsertKey(const struct rcrpc *req, struct rcrpc *resp);
    void DeleteKey(const struct rcrpc *req, struct rcrpc *resp);
    void CreateTable(const struct rcrpc *req, struct rcrpc *resp);
    void OpenTable(const struct rcrpc *req, struct rcrpc *resp);
    void DropTable(const struct rcrpc *req, struct rcrpc *resp);
    void CreateIndex(const struct rcrpc *req, struct rcrpc *resp);
    void DropIndex(const struct rcrpc *req, struct rcrpc *resp);

    explicit Server(Net *net_impl);
    Server(const Server& server);
    Server& operator=(const Server& server);
    ~Server();
    void Run();
  private:
    void HandleRPC();
    void StoreData(object *o,
                   uint64_t key,
                   const char *buf,
                   uint64_t buf_len);
    explicit Server();
    Net *net;
    BackupClient *backup;
    Table tables[RC_NUM_TABLES];
    uint32_t seg_off;
};

} // namespace RAMCloud

#endif
