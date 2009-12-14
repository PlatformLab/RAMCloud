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

#include <shared/Log.h>
#include <shared/object.h>
#include <shared/rcrpc.h>
#include <shared/backup_client.h>

#include <server/net.h>
#include <server/hashtable.h>
#include <server/index.h>

namespace RAMCloud {

struct object_mutable {
    uint64_t refcnt;
};

struct object {
    chunk_hdr hdr;
    bool is_tombstone;
    object_mutable *mut;
};

class Table {
  public:
    static const int TABLE_NAME_MAX_LEN = 64;
    explicit Table() : next_key(0), object_map(HASH_NLINES) {
        memset(indexes, 0, sizeof(indexes));
    }
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
    const object *Get(uint64_t key) {
        void *val = object_map.Lookup(key);
        return static_cast<const object *>(val);
    }
    void Put(uint64_t key, const object *o) {
        object_map.Delete(key);
        object_map.Insert(key, const_cast<object *>(o));
    }
    void Delete(uint64_t key) {
        object_map.Delete(key);
    }

    uint16_t CreateIndex(bool unique, bool range_queryable, enum RCRPC_INDEX_TYPE type) {
        uint16_t index_id;

        index_id = 0;
        while (indexes[index_id] != NULL) {
            if (index_id == 65535) {
                throw "Out of index ids";
            }
            ++index_id;
        }

        if (unique) {
            if (range_queryable) {
                /* unique tree */
                indexes[index_id] = new STLUniqueRangeIndex(type);
            } else {
                /* unique hash table */
                indexes[index_id] = new STLUniqueRangeIndex(type);
            }
        } else {
            if (range_queryable) {
                /* multi tree */
                indexes[index_id] = new STLMultiRangeIndex(type);
            } else {
                /* multi hash table */
                indexes[index_id] = new STLMultiRangeIndex(type);
            }
        }

        return index_id;
    }

    void DropIndex(uint16_t index_id) {
        if (indexes[index_id] == NULL) {
            throw "Invalid index";
        }
        delete indexes[index_id];
        indexes[index_id] = NULL;
    }

    unsigned int RangeQueryIndex(uint16_t index_id,
                                 const RangeQueryArgs *args) {
        Index *i = indexes[index_id];
        if (!i->range_queryable) {
            throw "Not range queryable";
        }
        if (i->unique) {
            UniqueRangeIndex *index = dynamic_cast<UniqueRangeIndex*>(i);
            return index->RangeQuery(args);
        }else {
            MultiRangeIndex *index = dynamic_cast<MultiRangeIndex*>(i);
            return index->RangeQuery(args);
        }
    }

    bool UniqueLookupIndex(uint16_t index_id, const IndexKeyRef &keyref,
                           uint64_t *oid) {
        Index *i = indexes[index_id];
        assert(i->unique);
        UniqueIndex *index = dynamic_cast<UniqueIndex*>(i);
        try {
            *oid = index->Lookup(keyref);
        } catch (IndexException& e) {
            return false;
        }
        return true;
    }

    unsigned int MultiLookupIndex(uint16_t index_id,
                                  const MultiLookupArgs *args) {
        Index *i = indexes[index_id];
        assert(!i->unique);
        MultiIndex *index = dynamic_cast<MultiIndex*>(i);
        return index->Lookup(args);
    }

    void DeleteIndexEntry(uint16_t index_id, enum RCRPC_INDEX_TYPE index_type,
                          const void *data, uint64_t len,
                          uint64_t oid) {
        Index *i = indexes[index_id];
        assert(i->type == index_type);
        IndexKeyRef keyref(data, len);
        if (i->unique) {
            UniqueIndex *index =
                dynamic_cast<UniqueIndex*>(i);
            return index->Remove(keyref, oid);
        }else {
            MultiIndex *index = dynamic_cast<MultiIndex*>(i);
            return index->Remove(keyref, oid);
        }
    }

    void AddIndexEntry(uint16_t index_id, enum RCRPC_INDEX_TYPE index_type,
                       const void *data, uint64_t len,
                       uint64_t oid) {
        Index *i = indexes[index_id];
        assert(i->type == index_type);
        IndexKeyRef keyref(data, len);
        if (i->unique) {
            UniqueIndex *index = dynamic_cast<UniqueIndex*>(i);
            return index->Insert(keyref, oid);
        }else {
            MultiIndex *index = dynamic_cast<MultiIndex*>(i);
            return index->Insert(keyref, oid);
        }
    }

  private:
    char name[64];
    uint64_t next_key;
    Hashtable object_map;
    Index *indexes[65536];
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
    void RangeQuery(const struct rcrpc *req, struct rcrpc *resp);
    void UniqueLookup(const struct rcrpc *req, struct rcrpc *resp);
    void MultiLookup(const struct rcrpc *req, struct rcrpc *resp);

    Table *GetTable(uint64_t tid) { return &tables[tid]; }
    Log   *GetLog() { return log; }

    explicit Server(Net *net_impl);
    Server(const Server& server);
    Server& operator=(const Server& server);
    ~Server();
    void Run();

  private:
    void HandleRPC();
    void StoreData(uint64_t table,
                   uint64_t key,
                   const char *buf,
                   uint64_t buf_len,
                   const char *index_entries_buf,
                   uint64_t index_entries_buf_len);
    explicit Server();

    Log *log;
    Net *net;
    BackupClient *backup;
    Table tables[RC_NUM_TABLES];
};

} // namespace RAMCloud

#endif
