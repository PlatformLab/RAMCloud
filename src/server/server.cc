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

// requires C++0x for cinttypes include
#include <inttypes.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <memory>

#include <config.h>
#include <shared/Log.h>
#include <shared/object.h>
#include <shared/rcrpc.h>
#include <shared/backup_client.h>

#include <server/server.h>
#include <server/net.h>

namespace RAMCloud {

enum { server_debug = false };

static void LogEvictionCallback(log_entry_type_t type,
                                const void *p,
                                uint64_t len,
                                void *cookie);

static void
DeleteIndexEntries(Table *table, const object *o)
{
    chunk_entry *chunk;

    ChunkIter cidxiter(const_cast<chunk_hdr*>(&o->hdr));
    while (cidxiter.entry != NULL) {
        chunk = cidxiter.entry;
        if (!chunk->is_data()) {
            assert(is_valid_index_type(static_cast<enum RCRPC_INDEX_TYPE>(chunk->index_type)));
            table->DeleteIndexEntry(chunk->index_id,
                static_cast<enum RCRPC_INDEX_TYPE>(chunk->index_type),
                chunk->data, chunk->len,
                o->hdr.key);
        }
        ++cidxiter;
    }
}

static void
AddIndexEntries(Table *table, const object *o)
{
    chunk_entry *chunk;

    ChunkIter cidxiter(const_cast<chunk_hdr*>(&o->hdr));
    while (cidxiter.entry != NULL) {
        chunk = cidxiter.entry;
        if (!chunk->is_data()) {
            assert(is_valid_index_type(static_cast<enum RCRPC_INDEX_TYPE>(chunk->index_type)));
            table->AddIndexEntry(chunk->index_id,
                static_cast<enum RCRPC_INDEX_TYPE>(chunk->index_type),
                chunk->data, chunk->len,
                o->hdr.key);
        }
        ++cidxiter;
    }
}

Server::Server(const ServerConfig *sconfig, Net *net_impl)
    : config(sconfig), net(net_impl), backup(0)
{
    void *p = malloc(SEGMENT_SIZE * SEGMENT_COUNT);
    assert(p != NULL);

    Net *backup_net = new Net(BACKCLNTADDR, BACKCLNTPORT,
                              BACKSVRADDR, BACKSVRPORT);
    backup = new BackupClient(backup_net);

    log = new Log(SEGMENT_SIZE, p, SEGMENT_SIZE * SEGMENT_COUNT, backup);
    log->registerType(LOG_ENTRY_TYPE_OBJECT, LogEvictionCallback, this);
}

Server::~Server()
{
}

void
Server::Ping(const struct rcrpc *req, struct rcrpc *resp)
{
    resp->type = RCRPC_PING_RESPONSE;
    resp->len = (uint32_t) RCRPC_PING_RESPONSE_LEN;
}

void
Server::Read(const struct rcrpc *req, struct rcrpc *resp)
{
    const rcrpc_read_request * const rreq = &req->read_request;
    chunk_entry *chunk;

    if (server_debug)
        printf("Read from key %lu\n",
               rreq->key);

    Table *t = &tables[rreq->table];
    const object *o = t->Get(rreq->key);
    if (!o || o->is_tombstone)
        throw "Object not found";

    resp->type = RCRPC_READ_RESPONSE;
    resp->len = static_cast<uint32_t>(RCRPC_READ_RESPONSE_LEN_WODATA);
    // (will be updated below with var-length data)
    resp->read_response.index_entries_len = 0;
    resp->read_response.buf_len = 0;

    char *index_entries_buf = resp->read_response.var;
    ChunkIter cidxiter(const_cast<chunk_hdr*>(&o->hdr));
    while (cidxiter.entry != NULL) {
        chunk = cidxiter.entry;
        uint64_t chunk_size = chunk->total_size();
        if (!chunk->is_data()) {
            memcpy(index_entries_buf, chunk, chunk_size);
            index_entries_buf += chunk_size;
            resp->read_response.index_entries_len += chunk_size;
        }
        ++cidxiter;
    }

    char *buf = index_entries_buf;
    ChunkIter cdataiter(const_cast<chunk_hdr*>(&o->hdr));
    while (cdataiter.entry != NULL) {
        chunk = cdataiter.entry;
        if (chunk->is_data()) {
            memcpy(buf, chunk->data, chunk->len);
            buf += chunk->len;
            resp->read_response.buf_len += chunk->len;
        }
        ++cdataiter;
    }

    resp->len += static_cast<uint32_t>(resp->read_response.index_entries_len);
    resp->len += static_cast<uint32_t>(resp->read_response.buf_len);
}

// The log code is cleaning a segment and telling us that this object is
// getting evicted. If we care about it, we must re-write it to the head
// of the log and update any pointers to it. If we don't care, we do not
// write it back out, but may have to update some metadata.
//
// We care to preserve the object when one of the following holds:
//   1) This is a live object (is referenced by the hash table).
//   2) This is a live tombstone (is referenced by the hash table, which
//      implies that the reference count is > 0).
//
// We don't care to preserve it in all other cases, however:
//   i) If this is an older version of an object, we need to fix the
//      reference count of what's in the hash table (whether it be an object,
//      or a tombstone).
//  ii) If the hash table points to a tombstone and the reference count drops
//      to 0, after i) above, remove the entry from the hash table. The log
//      will clean the tombstone eventually.
static void
LogEvictionCallback(log_entry_type_t type,
                    const void *p,
                    uint64_t len,
                    void *cookie)
{
    const object *evict_obj = reinterpret_cast<const object *>(p);
    Server *svr = reinterpret_cast<Server *>(cookie);

    assert(evict_obj != NULL);
    assert(svr != NULL);

    Table *tbl = svr->GetTable(evict_obj->hdr.table);
    assert(tbl != NULL);

    Log *log = svr->GetLog();
    assert(log != NULL);

    const object *tbl_obj = tbl->Get(evict_obj->hdr.key);
    if (tbl_obj == evict_obj) {
        // same object/tombstone: be sure to preserve whatever it is

        if (tbl_obj->is_tombstone)
            assert(tbl_obj->mut->refcnt > 0);

        const object *objp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT,
                                                         evict_obj,
                                                         sizeof(*evict_obj));
        assert(objp != NULL);
        tbl->Put(evict_obj->hdr.key, objp);
    } else {
        // different object/tombstone: drop it, but be careful with
        // bookkeeping
        //
        // 5 cases:
        //          Evicted Object       Current Table Object
        //     ----------------------------------------------------
        //     1)   old tombstone           NULL
        //     2)   old tombstone           new tombstone
        //     3)   old tombstone           new object
        //     4)   old object              new tombstone
        //     5)   old object              new object

        if (tbl_obj == NULL) {
            // case 1
            assert(evict_obj->is_tombstone);
        } else {
            if (!evict_obj->is_tombstone) {
                // cases 4 and 5:
                //   drop the refcnt and if it equals 0, a tombstone referenced by
                //   the hash table is freed.

                object_mutable *evict_objm = evict_obj->mut;
                assert(evict_objm != NULL);

                assert(evict_objm->refcnt > 0);
                evict_objm->refcnt--;

                if (evict_objm->refcnt == 0) {
                    // case 4 only
                    assert(tbl_obj->is_tombstone);
                    log->free(LOG_ENTRY_TYPE_OBJECT, tbl_obj, sizeof(*tbl_obj));
                    tbl->Delete(tbl_obj->hdr.key);
                    delete tbl_obj->mut;
                }
            } else {
                // cases 2 and 3:
                //   nothing to do when evicting old tombstones
            }
        }
    }
}

void
Server::StoreData(uint64_t table,
                  uint64_t key,
                  const char *buf,
                  uint64_t buf_len,
                  const char *index_entries_buf,
                  uint64_t index_entries_buf_len)
{
    Table *t = &tables[table];
    const object *o = t->Get(key);
    object_mutable *om = NULL;
    chunk_entry *chunk;

    if (o != NULL) {
        // steal the extant guy's mutable space. this will contain the proper
        // reference count.
        om = o->mut;
        assert(om != NULL);
        assert(om->refcnt > 0 || o->is_tombstone);
        DeleteIndexEntries(t, o);
    } else {
        om = new object_mutable;
        assert(om);
        om->refcnt = 0;
    }

    object new_o;
    new_o.mut = om;
    om->refcnt++;

    new_o.hdr.key = key;
    new_o.hdr.table = table;
    new_o.is_tombstone = false;
    // TODO dm's super-fast checksum here
    new_o.hdr.checksum = 0x0BE70BE70BE70BE7ULL;
    new_o.hdr.entries_len = 0;

    chunk = new_o.hdr.entries;

    // index entries buf has same format as chunk entries for now
    memcpy(chunk, index_entries_buf, index_entries_buf_len);
    new_o.hdr.entries_len += index_entries_buf_len;
    chunk = reinterpret_cast<chunk_entry*>(reinterpret_cast<char*>(chunk) +
                                           index_entries_buf_len);

    chunk->len = buf_len;
    chunk->index_id = static_cast<uint32_t>(-1);
    chunk->index_type = static_cast<uint32_t>(-1);
    memcpy(chunk->data, buf, buf_len);
    new_o.hdr.entries_len += chunk->total_size();
    chunk = chunk->next();

    // mark the old object as freed _before_ writing the new object to the log.
    // if we do it afterwards, the log cleaner could be triggered and `o' reclaimed
    // before log->append() returns. The subsequent free breaks, as that segment may
    // have been reset.
    if (o != NULL)
        log->free(LOG_ENTRY_TYPE_OBJECT, o, sizeof(*o));

    const object *objp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT, &new_o, sizeof(new_o));
    assert(objp != NULL);
    t->Put(key, objp);
    AddIndexEntries(t, objp);
}

void
Server::Write(const struct rcrpc *req, struct rcrpc *resp)
{
    const rcrpc_write_request * const wreq = &req->write_request;

    if (server_debug) {
        printf("Write %lu bytes of data and %lu index bytes to key %lu\n",
               wreq->buf_len,
               wreq->index_entries_len,
               wreq->key);
    }

    const char *buf = wreq->var + wreq->index_entries_len;
    const char *index_entries_buf = wreq->var;
    StoreData(wreq->table, wreq->key, buf, wreq->buf_len,
              index_entries_buf, wreq->index_entries_len);

    resp->type = RCRPC_WRITE_RESPONSE;
    resp->len = static_cast<uint32_t>(RCRPC_WRITE_RESPONSE_LEN);
}

void
Server::InsertKey(const struct rcrpc *req, struct rcrpc *resp)
{
    const rcrpc_insert_request * const ireq = &req->insert_request;

    Table *t = &tables[ireq->table];
    uint64_t key = t->AllocateKey();
    const object *o = t->Get(key);
    assert(o == NULL);

    const char *buf = ireq->var + ireq->index_entries_len;
    const char *index_entries_buf = ireq->var;
    StoreData(ireq->table, key, buf, ireq->buf_len,
              index_entries_buf, ireq->index_entries_len);

    resp->type = RCRPC_INSERT_RESPONSE;
    resp->len = (uint32_t) RCRPC_INSERT_RESPONSE_LEN;
    resp->insert_response.key = key;
}

void
Server::DeleteKey(const struct rcrpc *req, struct rcrpc *resp)
{
    const rcrpc_delete_request * const dreq = &req->delete_request;

    Table *t = &tables[dreq->table];
    const object *o = t->Get(dreq->key);
    if (!o || o->is_tombstone)
        throw "Object not found";

    assert(o->mut != NULL);
    assert(o->mut->refcnt > 0);

    DeleteIndexEntries(t, o);

    object tomb_o;
    tomb_o.hdr = o->hdr;
    tomb_o.mut = o->mut;
    tomb_o.is_tombstone = true;

    // `o' may be relocated in the log when we append, before the tombstone is written, so
    // we must either mark the space as free first, or refetch from the hash table afterwards
    log->free(LOG_ENTRY_TYPE_OBJECT, o, sizeof(*o));
    const object *tombp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT, &tomb_o, sizeof(tomb_o));
    assert(tombp);

    t->Put(dreq->key, tombp);

    resp->type = RCRPC_DELETE_RESPONSE;
    resp->len  = (uint32_t) RCRPC_DELETE_RESPONSE_LEN;
}

void
Server::CreateTable(const struct rcrpc *req, struct rcrpc *resp)
{
    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->create_table_request.name) == 0) {
            // TODO Need to do better than this
            throw "Table exists";
        }
    }
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), "") == 0) {
            tables[i].SetName(req->create_table_request.name);
            break;
        }
    }
    if (i == RC_NUM_TABLES) {
        // TODO Need to do better than this
        throw "Out of tables";
    }
    if (server_debug)
        printf("create table -> %d\n", i);

    resp->type = RCRPC_CREATE_TABLE_RESPONSE;
    resp->len  = (uint32_t) RCRPC_CREATE_TABLE_RESPONSE_LEN;
}

void
Server::OpenTable(const struct rcrpc *req, struct rcrpc *resp)
{
    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->open_table_request.name) == 0)
            break;
    }
    if (i == RC_NUM_TABLES) {
        // TODO Need to do better than this
        throw "No such table";
    }
    if (server_debug)
        printf("open table -> %d\n", i);

    resp->type = RCRPC_OPEN_TABLE_RESPONSE;
    resp->len  = (uint32_t) RCRPC_OPEN_TABLE_RESPONSE_LEN;
    resp->open_table_response.handle = i;
}

void
Server::DropTable(const struct rcrpc *req, struct rcrpc *resp)
{
    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->drop_table_request.name) == 0) {
            tables[i].SetName("");
            break;
        }
    }
    if (i == RC_NUM_TABLES) {
        // TODO Need to do better than this
        throw "No such table";
    }
    if (server_debug)
        printf("drop table -> %d\n", i);

    resp->type = RCRPC_DROP_TABLE_RESPONSE;
    resp->len  = (uint32_t) RCRPC_DROP_TABLE_RESPONSE_LEN;
}

void
Server::CreateIndex(const struct rcrpc *req, struct rcrpc *resp)
{
    Table *table;
    uint16_t index_id;
    bool unique;
    bool range_queryable;
    enum RCRPC_INDEX_TYPE type;

    if (server_debug) {
        printf("CreateIndex(table=%d, type=%d, "
                           "unique=%d, range_queryable=%d)\n",
               req->create_index_request.table,
               req->create_index_request.type,
               (bool) req->create_index_request.unique,
               (bool) req->create_index_request.range_queryable);
    }

    table = &tables[req->create_index_request.table];
    unique = static_cast<bool>(req->create_index_request.unique);
    range_queryable = static_cast<bool>(req->create_index_request.range_queryable);
    type = static_cast<enum RCRPC_INDEX_TYPE>(req->create_index_request.type);
    if (!is_valid_index_type(type)) {
        throw "Invalid index type";
    }

    index_id = table->CreateIndex(unique, range_queryable, type);

    resp->type = RCRPC_CREATE_INDEX_RESPONSE;
    resp->len  = static_cast<uint32_t>(RCRPC_CREATE_INDEX_RESPONSE_LEN);
    resp->create_index_response.id = index_id;
}


void
Server::DropIndex(const struct rcrpc *req, struct rcrpc *resp)
{
    Table *table;

    if (server_debug) {
        printf("DropIndex(table=%d, id=%d)\n",
               req->drop_index_request.table,
               req->drop_index_request.id);
    }

    table = &tables[req->create_index_request.table];
    table->DropIndex(req->drop_index_request.id);

    resp->type = RCRPC_DROP_INDEX_RESPONSE;
    resp->len  = static_cast<uint32_t>(RCRPC_DROP_INDEX_RESPONSE_LEN);
}

void
Server::RangeQuery(const struct rcrpc *req, struct rcrpc *resp)
{
    Table *table;

    if (server_debug) {
        printf("RangeQuery(table=%d, id=%d)\n",
               req->range_query_request.table,
               req->range_query_request.index_id);
    }

    table = &tables[req->range_query_request.table];

    const struct rcrpc_range_query_request *rqreq = &req->range_query_request;
    struct rcrpc_range_query_response *rqresp = &resp->range_query_response;
    uint16_t index_id;
    char keys_buf[1024];
    uint64_t oids_buf[1024 / sizeof(uint64_t)];
    IndexOIDsRef oidsref(oids_buf, sizeof(oids_buf));
    IndexKeysRef keysref(keys_buf, sizeof(keys_buf));
    bool more;
    unsigned int count;
    const char *reqvar;
    char *respvar;
    RangeQueryArgs args;
    std::auto_ptr<IndexKeyRef> start_keyref;
    std::auto_ptr<IndexKeyRef> end_keyref;

    index_id = rqreq->index_id;

    reqvar = rqreq->var;

    if (static_cast<bool>(rqreq->start_following_oid_present)) {
        uint64_t start_following_oid = *reinterpret_cast<const uint64_t*>(reqvar);
        reqvar += sizeof(uint64_t);
        args.setStartFollowing(start_following_oid);
    }

    if (static_cast<bool>(rqreq->key_start_present)) {
        uint64_t length = *reinterpret_cast<const uint64_t*>(reqvar);
        reqvar += sizeof(uint64_t);
        start_keyref.reset(new IndexKeyRef(reqvar, length));
        reqvar += length;
        args.setKeyStart(*start_keyref, static_cast<bool>(rqreq->key_start_inclusive));
    }

    if (static_cast<bool>(rqreq->key_end_present)) {
        uint64_t length = *reinterpret_cast<const uint64_t*>(reqvar);
        reqvar += sizeof(uint64_t);
        end_keyref.reset(new IndexKeyRef(reqvar, length));
        reqvar += length;
        args.setKeyEnd(*end_keyref, static_cast<bool>(rqreq->key_end_inclusive));
    }

    args.setLimit(static_cast<unsigned int>(rqreq->limit));

    if (static_cast<bool>(rqreq->request_keys)) {
        args.setResultBuf(keysref, oidsref);
    } else {
        args.setResultBuf(oidsref);
    }

    args.setResultMore(&more);

    count = table->RangeQueryIndex(index_id, &args);

    rqresp->len = static_cast<uint32_t>(count);
    rqresp->more = more;

    respvar = rqresp->var;
    memcpy(respvar, oidsref.buf, oidsref.used * sizeof(uint64_t));
    respvar += oidsref.used * sizeof(uint64_t);
    if (static_cast<bool>(rqreq->request_keys)) {
        memcpy(respvar, keysref.buf, keysref.used);
        respvar += keysref.used;
    }

    resp->type = RCRPC_RANGE_QUERY_RESPONSE;
    resp->len  = static_cast<uint32_t>(RCRPC_RANGE_QUERY_RESPONSE_LEN_WODATA) +
                 static_cast<uint32_t>(respvar - rqresp->var);
}

void
Server::UniqueLookup(const struct rcrpc *req, struct rcrpc *resp)
{
    Table *table;

    if (server_debug) {
        printf("UniqueLookup(table=%d, id=%d)\n",
               req->range_query_request.table,
               req->range_query_request.index_id);
    }

    table = &tables[req->range_query_request.table];

    const struct rcrpc_unique_lookup_request *mlreq = &req->unique_lookup_request;
    struct rcrpc_unique_lookup_response *mlresp = &resp->unique_lookup_response;
    uint16_t index_id;

    index_id = mlreq->index_id;

    uint64_t key_length = mlreq->key_len;
    IndexKeyRef keyref(mlreq->key, key_length);

    mlresp->oid_present = table->UniqueLookupIndex(index_id, keyref, &mlresp->oid);

    resp->type = RCRPC_UNIQUE_LOOKUP_RESPONSE;
    resp->len  = static_cast<uint32_t>(RCRPC_UNIQUE_LOOKUP_RESPONSE_LEN);
}

void
Server::MultiLookup(const struct rcrpc *req, struct rcrpc *resp)
{
    Table *table;

    if (server_debug) {
        printf("MultiLookup(table=%d, id=%d)\n",
               req->range_query_request.table,
               req->range_query_request.index_id);
    }

    table = &tables[req->range_query_request.table];

    const struct rcrpc_multi_lookup_request *mlreq = &req->multi_lookup_request;
    struct rcrpc_multi_lookup_response *mlresp = &resp->multi_lookup_response;
    uint16_t index_id;
    uint64_t oids_buf[mlreq->limit];
    IndexOIDsRef oidsref(oids_buf, sizeof(oids_buf));
    bool more;
    unsigned int count;
    const char *reqvar;
    MultiLookupArgs args;

    index_id = mlreq->index_id;

    reqvar = mlreq->var;

    if (static_cast<bool>(mlreq->start_following_oid_present)) {
        uint64_t start_following_oid = *reinterpret_cast<const uint64_t*>(reqvar);
        reqvar += sizeof(uint64_t);
        args.setStartFollowing(start_following_oid);
    }

    uint64_t key_length = mlreq->key_len;
    IndexKeyRef keyref(reqvar, key_length);
    reqvar += key_length;
    args.setKey(keyref);

    args.setLimit(static_cast<unsigned int>(mlreq->limit));
    args.setResultBuf(oidsref);
    args.setResultMore(&more);

    count = table->MultiLookupIndex(index_id, &args);

    mlresp->len = static_cast<uint32_t>(count);
    mlresp->more = more;

    memcpy(mlresp->oids, oidsref.buf, oidsref.used * sizeof(uint64_t));

    resp->type = RCRPC_MULTI_LOOKUP_RESPONSE;
    resp->len  = static_cast<uint32_t>(RCRPC_MULTI_LOOKUP_RESPONSE_LEN_WODATA) +
                 static_cast<uint32_t>(oidsref.used * sizeof(uint64_t));
}

struct obj_replay_cookie {
    Server *server;
    uint64_t used_bytes;
};

void
ObjectReplayCallback(log_entry_type_t type,
                     const void *p,
                     uint64_t len,
                     void *cookiep)
{
    obj_replay_cookie *cookie = static_cast<obj_replay_cookie *>(cookiep);
    Server *server = cookie->server;

    //printf("ObjectReplayCallback: type %llu\n", type);

    // Used to determine free_bytes after passing over the segment
    cookie->used_bytes += len;

    switch (type) {
    case LOG_ENTRY_TYPE_OBJECT: {
        const object *obj = static_cast<const object *>(p);
        assert(obj);

        Table *table = &server->tables[obj->hdr.table];
        assert(table != NULL);

        table->Delete(obj->hdr.key);
        table->Put(obj->hdr.key, obj);
    }
        break;
    case LOG_ENTRY_TYPE_SEGMENT_HEADER:
    case LOG_ENTRY_TYPE_SEGMENT_CHECKSUM:
        break;
    default:
        printf("!!! Unknown object type on log replay: 0x%llx", type);
    }
}

void
SegmentReplayCallback(Segment *seg, void *cookie)
{
    // TODO(stutsman) we can restore bytes_stored in the log easily
    // using the same approach as for the individual segments
    Server *server = static_cast<Server *>(cookie);

    obj_replay_cookie ocookie;
    ocookie.server = server;
    ocookie.used_bytes = 0;

    server->log->forEachEntry(seg, ObjectReplayCallback, &ocookie);
    seg->setUsedBytes(ocookie.used_bytes);
}

void
Server::Restore()
{
    uint64_t restored_segs = log->restore();
    printf("Log was able to restore %llu segs\n", restored_segs);
    // TODO(stutsman) Walk the log here and rebuild metadata
    log->forEachSegment(SegmentReplayCallback, restored_segs, this);
}

void
Server::HandleRPC()
{
    rcrpc *req;
    if (net->RecvRPC(&req) != 0) {
        printf("Failure receiving rpc\n");
        return;
    }

    char rpcbuf[MAX_RPC_LEN];
    rcrpc *resp = reinterpret_cast<rcrpc *>(rpcbuf);

    //printf("got rpc type: 0x%08x, len 0x%08x\n", req->type, req->len);

    try {
        switch((enum RCRPC_TYPE) req->type) {
        case RCRPC_PING_REQUEST:          Server::Ping(req, resp);         break;
        case RCRPC_READ_REQUEST:          Server::Read(req, resp);         break;
        case RCRPC_WRITE_REQUEST:         Server::Write(req, resp);        break;
        case RCRPC_INSERT_REQUEST:        Server::InsertKey(req, resp);    break;
        case RCRPC_DELETE_REQUEST:        Server::DeleteKey(req, resp);    break;
        case RCRPC_CREATE_TABLE_REQUEST:  Server::CreateTable(req, resp);  break;
        case RCRPC_OPEN_TABLE_REQUEST:    Server::OpenTable(req, resp);    break;
        case RCRPC_DROP_TABLE_REQUEST:    Server::DropTable(req, resp);    break;
        case RCRPC_CREATE_INDEX_REQUEST:  Server::CreateIndex(req, resp);  break;
        case RCRPC_DROP_INDEX_REQUEST:    Server::DropIndex(req, resp);    break;
        case RCRPC_RANGE_QUERY_REQUEST:   Server::RangeQuery(req, resp);   break;
        case RCRPC_UNIQUE_LOOKUP_REQUEST: Server::UniqueLookup(req, resp); break;
        case RCRPC_MULTI_LOOKUP_REQUEST:  Server::MultiLookup(req, resp); break;

        case RCRPC_PING_RESPONSE:
        case RCRPC_READ_RESPONSE:
        case RCRPC_WRITE_RESPONSE:
        case RCRPC_INSERT_RESPONSE:
        case RCRPC_DELETE_RESPONSE:
        case RCRPC_CREATE_TABLE_RESPONSE:
        case RCRPC_OPEN_TABLE_RESPONSE:
        case RCRPC_DROP_TABLE_RESPONSE:
        case RCRPC_CREATE_INDEX_RESPONSE:
        case RCRPC_DROP_INDEX_RESPONSE:
        case RCRPC_RANGE_QUERY_RESPONSE:
        case RCRPC_UNIQUE_LOOKUP_RESPONSE:
        case RCRPC_MULTI_LOOKUP_RESPONSE:
        case RCRPC_ERROR_RESPONSE:
            throw "server received RPC response";

        default:
            throw "received unknown RPC type";
        }
    } catch (const char *msg) {
        rcrpc_error_response *error_rpc = &resp->error_response;
        fprintf(stderr, "Error while processing RPC: %s\n", msg);
        size_t msglen = strlen(msg);
        assert(RCRPC_ERROR_RESPONSE_LEN_WODATA + msglen + 1 < MAX_RPC_LEN);
        strcpy(&error_rpc->message[0], msg);
        resp->type = RCRPC_ERROR_RESPONSE;
        resp->len = static_cast<uint32_t>(RCRPC_ERROR_RESPONSE_LEN_WODATA) +
            msglen + 1;
    }
    net->SendRPC(resp);
}

void __attribute__ ((noreturn))
Server::Run()
{
    if (config->restore) {
        Restore();
    }
    log->init();

    while (true)
        HandleRPC();
}

} // namespace RAMCloud
