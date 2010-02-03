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
#include <shared/rcrpc.h>
#include <shared/backup_client.h>

#include <server/server.h>
#include <server/net.h>

namespace RAMCloud {

enum { server_debug = false };

void LogEvictionCallback(log_entry_type_t type,
                         const void *p,
                         uint64_t len,
                         void *cookie);

Server::Server(const ServerConfig *sconfig, Net *net_impl)
    : config(sconfig), net(net_impl), backup()
{
    void *p = malloc(SEGMENT_SIZE * SEGMENT_COUNT);
    assert(p != NULL);

    if (BACKUP) {
        Net *net = new CNet(BACKCLNTADDR, BACKCLNTPORT,
                            BACKSVRADDR, BACKSVRPORT);
        net->Connect();
        // NOTE The backup client takes care of freeing the net object
        backup.AddHost(net);
    }

    log = new Log(SEGMENT_SIZE, p, SEGMENT_SIZE * SEGMENT_COUNT, &backup);
    log->registerType(LOG_ENTRY_TYPE_OBJECT, LogEvictionCallback, this);
}

Server::~Server()
{
}

void
Server::Ping(const rcrpc_ping_request *req, rcrpc_ping_response *resp)
{
    resp->header.type = RCRPC_PING_RESPONSE;
    resp->header.len = (uint32_t) RCRPC_PING_RESPONSE_LEN;
}

bool
Server::RejectOperation(const rcrpc_reject_rules *reject_rules,
                        uint64_t version)
{
    if (version == RCRPC_VERSION_NONE) {
        return reject_rules->object_doesnt_exist;
    }

    if (reject_rules->object_exists) {
        return true;
    }
    if (reject_rules->version_eq_given &&
        version == reject_rules->given_version) {
        return true;
    }
    if (reject_rules->version_gt_given &&
        version > reject_rules->given_version) {
        return true;
    }
    if ((reject_rules->version_eq_given || reject_rules->version_gt_given) &&
        version < reject_rules->given_version) {
        return true;
    }
    return false;
}

void
Server::Read(const rcrpc_read_request *req, rcrpc_read_response *resp)
{
    if (server_debug)
        printf("Read from key %lu\n",
               req->key);

    resp->header.type = RCRPC_READ_RESPONSE;
    resp->header.len = static_cast<uint32_t>(RCRPC_READ_RESPONSE_LEN_WODATA);
    // (will be updated below with var-length data)
    resp->buf_len = 0;
    resp->version = RCRPC_VERSION_NONE;

    Table *t = &tables[req->table];
    const object *o = t->Get(req->key);
    if (!o || o->is_tombstone) {
        if (o && o->is_tombstone)
            assert(o->mut->refcnt > 0); 
        /* automatic reject: can't read a non-existent object */
        /* leave RCRPC_VERSION_NONE in resp->version */
        return;
    }

    resp->version = o->version;

    if (!RejectOperation(&req->reject_rules, o->version)) {
        memcpy(resp->buf, o->data, o->data_len);
        resp->buf_len = o->data_len;

        resp->header.len += static_cast<uint32_t>(resp->buf_len);
    }
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
void
LogEvictionCallback(log_entry_type_t type,
                    const void *p,
                    uint64_t len,
                    void *cookie)
{
    const object *evict_obj = reinterpret_cast<const object *>(p);
    Server *svr = reinterpret_cast<Server *>(cookie);

    assert(evict_obj != NULL);
    assert(svr != NULL);

    Table *tbl = &svr->tables[evict_obj->table];
    assert(tbl != NULL);

    Log *log = svr->log;
    assert(log != NULL);

    const object *tbl_obj = tbl->Get(evict_obj->key);
    if (tbl_obj == evict_obj) {
        // same object/tombstone: be sure to preserve whatever it is

        if (tbl_obj->is_tombstone)
            assert(tbl_obj->mut->refcnt > 0);

        const object *objp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT,
                                                         evict_obj,
                                                         evict_obj->size());
        assert(objp != NULL);
        tbl->Put(evict_obj->key, objp);
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
                    log->free(LOG_ENTRY_TYPE_OBJECT, tbl_obj, tbl_obj->size());
                    tbl->Delete(tbl_obj->key);
                    delete tbl_obj->mut;
                }
            } else {
                // cases 2 and 3:
                //   nothing to do when evicting old tombstones
            }
        }
    }
}

bool
Server::StoreData(uint64_t table,
                  uint64_t key,
                  const rcrpc_reject_rules *reject_rules,
                  const char *buf,
                  uint64_t buf_len,
                  uint64_t *new_version)
{
    Table *t = &tables[table];
    const object *o = t->Get(key);
    object_mutable *om = NULL;

    if (o != NULL) {
        if (RejectOperation(reject_rules, o->version)) {
            *new_version = o->version;
            return false;
        }
        // steal the extant guy's mutable space. this will contain the proper
        // reference count.
        om = o->mut;
        assert(om != NULL);
        assert(om->refcnt > 0 || o->is_tombstone);
    } else {
        if (RejectOperation(reject_rules, RCRPC_VERSION_NONE)) {
            *new_version = RCRPC_VERSION_NONE;
            return false;
        }
        om = new object_mutable;
        assert(om);
        om->refcnt = 0;
    }

    DECLARE_OBJECT(new_o, buf_len);

    new_o->mut = om;
    om->refcnt++;

    new_o->key = key;
    new_o->table = table;
    new_o->version = t->AllocateVersion();
    assert(o == NULL || new_o->version > o->version);
    new_o->is_tombstone = false;
    // TODO dm's super-fast checksum here
    new_o->checksum = 0x0BE70BE70BE70BE7ULL;
    new_o->data_len = buf_len;
    memcpy(new_o->data, buf, buf_len);

    // mark the old object as freed _before_ writing the new object to the log.
    // if we do it afterwards, the log cleaner could be triggered and `o' reclaimed
    // before log->append() returns. The subsequent free breaks, as that segment may
    // have been reset.
    if (o != NULL)
        log->free(LOG_ENTRY_TYPE_OBJECT, o, o->size());

    const object *objp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT, new_o, new_o->size());
    assert(objp != NULL);
    t->Put(key, objp);

    *new_version = objp->version;
    return true;
}

void
Server::Write(const rcrpc_write_request *req, rcrpc_write_response *resp)
{
    if (server_debug) {
        printf("Write %lu bytes of data to key %lu\n",
               req->buf_len, req->key);
    }

    resp->written = StoreData(req->table, req->key, &req->reject_rules,
                              req->buf, req->buf_len, &resp->version);

    resp->header.type = RCRPC_WRITE_RESPONSE;
    resp->header.len = static_cast<uint32_t>(RCRPC_WRITE_RESPONSE_LEN);
}

void
Server::InsertKey(const rcrpc_insert_request *req, rcrpc_insert_response *resp)
{
    Table *t = &tables[req->table];
    uint64_t key = t->AllocateKey();

    rcrpc_reject_rules reject_rules;
    memset(&reject_rules, 0, sizeof(reject_rules));
    reject_rules.object_exists = true;

    assert(StoreData(req->table, key, &reject_rules, req->buf, req->buf_len,
                     &resp->version));

    resp->header.type = RCRPC_INSERT_RESPONSE;
    resp->header.len = (uint32_t) RCRPC_INSERT_RESPONSE_LEN;
    resp->key = key;
}

void
Server::DeleteKey(const rcrpc_delete_request *req, rcrpc_delete_response *resp)
{
    resp->header.type = RCRPC_DELETE_RESPONSE;
    resp->header.len  = (uint32_t) RCRPC_DELETE_RESPONSE_LEN;
    resp->version = RCRPC_VERSION_NONE;
    resp->deleted = false;

    Table *t = &tables[req->table];
    const object *o = t->Get(req->key);
    if (!o || o->is_tombstone) {
        if (o && o->is_tombstone)
            assert(o->mut->refcnt > 0);
        /* leave RCRPC_VERSION_NONE in resp->version */
        if (!RejectOperation(&req->reject_rules, RCRPC_VERSION_NONE))
            resp->deleted = true;
        return;
    }

    assert(o->mut != NULL);
    assert(o->mut->refcnt > 0);

    // abort if we're trying to delete the wrong version
    // the client will note the discrepancy and figure it out
    if (RejectOperation(&req->reject_rules, o->version)) {
        resp->version = o->version;
        return;
    }
    resp->deleted = true;

    DECLARE_OBJECT(tomb_o, 0);
    tomb_o->key = o->key;
    tomb_o->table = o->table;
    tomb_o->checksum = 0;
    tomb_o->mut = o->mut;
    tomb_o->is_tombstone = true;
    tomb_o->data_len = 0;
    tomb_o->version = t->AllocateVersion();

    // `o' may be relocated in the log when we append, before the tombstone is written, so
    // we must either mark the space as free first, or refetch from the hash table afterwards
    log->free(LOG_ENTRY_TYPE_OBJECT, o, o->size());
    const object *tombp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT, tomb_o, tomb_o->size());
    assert(tombp);

    t->Put(req->key, tombp);
}

void
Server::CreateTable(const rcrpc_create_table_request *req,
                    rcrpc_create_table_response *resp)
{
    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->name) == 0) {
            // TODO Need to do better than this
            throw "Table exists";
        }
    }
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), "") == 0) {
            tables[i].SetName(req->name);
            break;
        }
    }
    if (i == RC_NUM_TABLES) {
        // TODO Need to do better than this
        throw "Out of tables";
    }
    if (server_debug)
        printf("create table -> %d\n", i);

    resp->header.type = RCRPC_CREATE_TABLE_RESPONSE;
    resp->header.len  = (uint32_t) RCRPC_CREATE_TABLE_RESPONSE_LEN;
}

void
Server::OpenTable(const rcrpc_open_table_request *req,
                  rcrpc_open_table_response *resp)
{
    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->name) == 0)
            break;
    }
    if (i == RC_NUM_TABLES) {
        // TODO Need to do better than this
        throw "No such table";
    }
    if (server_debug)
        printf("open table -> %d\n", i);

    resp->header.type = RCRPC_OPEN_TABLE_RESPONSE;
    resp->header.len  = (uint32_t) RCRPC_OPEN_TABLE_RESPONSE_LEN;
    resp->handle = i;
}

void
Server::DropTable(const rcrpc_drop_table_request *req,
                   rcrpc_drop_table_response *resp)
{
    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->name) == 0) {
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

    resp->header.type = RCRPC_DROP_TABLE_RESPONSE;
    resp->header.len  = (uint32_t) RCRPC_DROP_TABLE_RESPONSE_LEN;
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

        Table *table = &server->tables[obj->table];
        assert(table != NULL);

        table->Delete(obj->key);
        table->Put(obj->key, obj);
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
    rcrpc_any *req;
    if (net->RecvRPC(&req) != 0) {
        printf("Failure receiving rpc\n");
        return;
    }

    char rpcbuf[MAX_RPC_LEN];
    rcrpc_any *resp = reinterpret_cast<rcrpc_any*>(rpcbuf);
    resp->header.type = 0xFFFFFFFF;
    resp->header.len = 0;

    //printf("got rpc type: 0x%08x, len 0x%08x\n", req->type, req->len);

    try {
        switch((enum RCRPC_TYPE) req->header.type) {

#define HANDLE(rcrpc_upper, rcrpc_lower, handler) \
        case RCRPC_##rcrpc_upper##_REQUEST: \
            assert(req->header.len >= sizeof(rcrpc_##rcrpc_lower##_request)); \
            Server::handler(reinterpret_cast<rcrpc_##rcrpc_lower##_request*>(req), \
                            reinterpret_cast<rcrpc_##rcrpc_lower##_response*>(resp)); \
            assert(resp->header.type == RCRPC_##rcrpc_upper##_RESPONSE); \
            assert(resp->header.len >= sizeof(rcrpc_##rcrpc_lower##_response)); \
            break; \
        case RCRPC_##rcrpc_upper##_RESPONSE: \
            throw "server received RPC response"

        HANDLE(PING, ping, Ping);
        HANDLE(READ, read, Read);
        HANDLE(WRITE, write, Write);
        HANDLE(INSERT, insert, InsertKey);
        HANDLE(DELETE, delete, DeleteKey);
        HANDLE(CREATE_TABLE, create_table, CreateTable);
        HANDLE(OPEN_TABLE, open_table, OpenTable);
        HANDLE(DROP_TABLE, drop_table, DropTable);
#undef HANDLE

        case RCRPC_ERROR_RESPONSE:
            throw "server received RPC response";

        default:
            throw "received unknown RPC type";
        }
    } catch (const char *msg) {
        rcrpc_error_response *error_rpc = reinterpret_cast<rcrpc_error_response*>(resp);
        fprintf(stderr, "Error while processing RPC: %s\n", msg);
        size_t msglen = strlen(msg);
        assert(RCRPC_ERROR_RESPONSE_LEN_WODATA + msglen + 1 < MAX_RPC_LEN);
        strcpy(&error_rpc->message[0], msg);
        resp->header.type = RCRPC_ERROR_RESPONSE;
        resp->header.len = static_cast<uint32_t>(
                               RCRPC_ERROR_RESPONSE_LEN_WODATA) +
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
