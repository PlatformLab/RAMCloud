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

#include <shared/common.h>
#include <shared/Log.h>
#include <shared/rcrpc.h>
#include <shared/backup_client.h>

#include <server/net.h>
#include <server/hashtable.h>
#include <server/index.h>

#include <inttypes.h>
#include <assert.h>
#include <stdio.h>

namespace RAMCloud {

struct chunk_entry {
    uint64_t len;
    uint32_t index_id;   /* static_cast<uint32_t>(-1) for data */
    uint32_t index_type; /* static_cast<uint32_t>(-1) for data */
    char data[0];                       // Variable length, but contiguous

    chunk_entry *
    next() const {
        const char *this_ptr = reinterpret_cast<const char*>(this);
        char *next_ptr = const_cast<char*>(this_ptr + this->total_size());
        return reinterpret_cast<chunk_entry*>(next_ptr);
    }

    uint64_t
    total_size() const {
        return sizeof(*this) + this->len;
    }

    bool
    is_data() const {
        return this->index_id == static_cast<uint32_t>(-1) &&
               this->index_type == static_cast<uint32_t>(-1);
    }
};

struct chunk_hdr {
    // WARNING: The hashtable code (for the moment) assumes that the
    // object's key is the first 64 bits of the struct
    // (That's also why the padding is here, and not in struct object.)
    uint64_t key;
    uint64_t table;
    uint64_t checksum;
    uint64_t entries_len;
    struct chunk_entry entries[0];
    char padding[1024];
};

class ChunkIter {
  public:

    ChunkIter(chunk_hdr *hdr) : entry(NULL), hdr_(hdr) {
        if (hdr_->entries_len > 0) {
            entry = hdr_->entries;
        } else {
            entry = NULL;
        }
    }

    ChunkIter& operator++() {
        if (entry != NULL) {
            entry = entry->next();
            size_t moved = reinterpret_cast<char*>(entry) -
                reinterpret_cast<char*>(hdr_->entries);
            size_t total = static_cast<size_t>(hdr_->entries_len);
            if (moved == total) {
                entry = NULL;
            } else {
                assert(moved < total);
            }
        }
        return *this;
    }

    chunk_entry *entry;

  private:
    chunk_hdr *hdr_;
    DISALLOW_COPY_AND_ASSIGN(ChunkIter);
};

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

struct ServerConfig {
    // Restore from backups before resuming operation
    bool restore;
  ServerConfig() : restore(false) {}
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

    explicit Server(const ServerConfig *sconfig, Net *net_impl);
    Server(const Server& server);
    Server& operator=(const Server& server);
    ~Server();
    void Run();

  private:
    void Restore();
    void HandleRPC();
    void StoreData(uint64_t table,
                   uint64_t key,
                   const char *buf,
                   uint64_t buf_len,
                   const char *index_entries_buf,
                   uint64_t index_entries_buf_len);
    explicit Server();

    const ServerConfig *config;
    Log *log;
    Net *net;
    BackupClient backup;
    Table tables[RC_NUM_TABLES];
    friend void SegmentReplayCallback(Segment *seg, void *cookie);
    friend void ObjectReplayCallback(log_entry_type_t type,
                                     const void *p,
                                     uint64_t len,
                                     void *cookie);
};

} // namespace RAMCloud

#endif
