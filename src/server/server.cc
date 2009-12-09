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

#include <config.h>
#include <shared/Log.h>
#include <shared/object.h>
#include <shared/rcrpc.h>

#include <server/server.h>
#include <server/backup_client.h>
#include <server/net.h>

namespace RAMCloud {

enum { server_debug = 0 };

static void LogEvictionCallback(log_entry_type_t type,
		                const void *p,
		                uint64_t len,
		                void *cookie);

Server::Server(Net *net_impl) : net(net_impl), backup(0)
{
    void *p = malloc(SEGMENT_SIZE * SEGMENT_COUNT);
    assert(p != NULL);
    log = new Log(SEGMENT_SIZE, p, SEGMENT_SIZE * SEGMENT_COUNT);
    log->registerType(LOG_ENTRY_TYPE_OBJECT, LogEvictionCallback, this);

    Net *backup_net = new Net(BACKCLNTADDR, BACKCLNTPORT,
                              BACKSVRADDR, BACKSVRPORT);
    backup = new BackupClient(backup_net);
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

    if (server_debug)
        printf("Read from key %lu\n",
               rreq->key);

    Table *t = &tables[rreq->table];
    const object *o = t->Get(rreq->key);
    if (!o)
        throw "Object not found";

    uint32_t olen = static_cast<uint32_t>(o->hdr.entries[0].len);

    resp->type = RCRPC_READ_RESPONSE;
    resp->len = static_cast<uint32_t>(RCRPC_READ_RESPONSE_LEN_WODATA) + olen;

    resp->read_response.buf_len = olen;
    memcpy(resp->read_response.buf,
           o->blob,
           sizeof(((struct object*) 0)->blob));
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

    int save = -1;

    const object *tbl_obj = tbl->Get(evict_obj->hdr.key);
    if (tbl_obj == NULL) {
        assert(evict_obj->is_tombstone);
        save = 0;
    } else {
        assert(tbl_obj->mut == evict_obj->mut);
        object_mutable *tbl_objm = tbl_obj->mut;
        assert(tbl_objm);

        if (tbl_obj == evict_obj) {
	    // same object/tombstone

            if (tbl_obj->is_tombstone) {
                assert(tbl_objm->refcnt > 0);
                save = 1;
            } else {
                save = 1;
            }
        } else {
	    // different object/tombstone

            uint64_t musthave = 0;
            if (!tbl_obj->is_tombstone)
                musthave++;
            if (!evict_obj->is_tombstone)
                musthave++;
            assert(tbl_objm->refcnt >= musthave);

            if (tbl_objm->refcnt > 0)
                --tbl_objm->refcnt;
            save = 0;
        }
    }

    // ensure we covered all cases
    assert(save == 0 || save == 1);

    if (save) {
        const object *objp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT, evict_obj, sizeof(*evict_obj));
        assert(objp != NULL);
        tbl->Put(evict_obj->hdr.key, objp);
    } else {
        assert(evict_obj->mut != NULL);

        // once the refcnt drops to 0, we drop the tombstone, so if we're
        // trying to evict a tombstone, there'd better be a > 0 refcnt. 
        if (evict_obj->is_tombstone) {
            assert(evict_obj->mut->refcnt > 0);
        } else {
            assert(evict_obj->mut->refcnt > 0 || tbl_obj->is_tombstone);

            if (tbl_obj->is_tombstone && tbl_obj->mut->refcnt == 0) {
                log->free(LOG_ENTRY_TYPE_OBJECT, tbl_obj, sizeof(*tbl_obj));
                tbl->Delete(tbl_obj->hdr.key);
                delete tbl_obj->mut;
            }
        }
    }
}

void
Server::StoreData(uint64_t table,
                  uint64_t key,
                  const char *buf,
                  uint64_t buf_len)
{
    Table *t = &tables[table];
    const object *o = t->Get(key);
    object_mutable *om = NULL;

    if (o != NULL) {
        // steal the extant guy's mutable space. this will contain the proper
        // reference count.
        om = o->mut;
        assert(om != NULL);
        assert(om->refcnt > 0 || o->is_tombstone);
    } else {
        om = new object_mutable;
        assert(om);
        om->refcnt = 0;
    }

    object new_o;
    new_o.mut = om;
    om->refcnt++;

    new_o.hdr.type = STORAGE_CHUNK_HDR_TYPE;
    new_o.hdr.key = key;
    new_o.hdr.table = table;
    new_o.is_tombstone = false;
    // TODO dm's super-fast checksum here
    new_o.hdr.checksum = 0x0BE70BE70BE70BE7ULL;

    new_o.hdr.entries[0].len = buf_len;
    memcpy(new_o.blob, buf, buf_len);

    // mark the old object as freed _before_ writing the new object to the log. 
    // if we do it afterwards, the log cleaner could be triggered and `o' reclaimed
    // before log->append() returns. The subsequent free breaks, as that segment may
    // have been reset. 
    if (o != NULL)
        log->free(LOG_ENTRY_TYPE_OBJECT, o, sizeof(*o));

    const object *objp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT, &new_o, sizeof(new_o));
    assert(objp != NULL);
    t->Put(key, objp);
}

void
Server::Write(const struct rcrpc *req, struct rcrpc *resp)
{
    const rcrpc_write_request * const wreq = &req->write_request;

    if (server_debug)
        printf("Write %lu bytes to key %lu\n",
               wreq->buf_len,
               wreq->key);

    StoreData(wreq->table, wreq->key, wreq->buf, wreq->buf_len);

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
    StoreData(ireq->table, key, ireq->buf, ireq->buf_len);

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
    if (!o)
        throw "Object not found";

    object tomb_o;
    tomb_o.hdr = o->hdr;
    tomb_o.mut = o->mut;
    tomb_o.is_tombstone = 1;
    const object *tombp = (const object *)log->append(LOG_ENTRY_TYPE_OBJECT, &tomb_o, sizeof(tomb_o));
    assert(tombp);
    log->free(LOG_ENTRY_TYPE_OBJECT, o, sizeof(*o));

    t->Put(dreq->key, tombp);

    // no op
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
        case RCRPC_PING_REQUEST:         Server::Ping(req, resp);        break;
        case RCRPC_READ_REQUEST:         Server::Read(req, resp);        break;
        case RCRPC_WRITE_REQUEST:        Server::Write(req, resp);       break;
        case RCRPC_INSERT_REQUEST:       Server::InsertKey(req, resp);   break;
        case RCRPC_DELETE_REQUEST:       Server::DeleteKey(req, resp);   break;
        case RCRPC_CREATE_TABLE_REQUEST: Server::CreateTable(req, resp); break;
        case RCRPC_OPEN_TABLE_REQUEST:   Server::OpenTable(req, resp);   break;
        case RCRPC_DROP_TABLE_REQUEST:   Server::DropTable(req, resp);   break;

        case RCRPC_PING_RESPONSE:
        case RCRPC_READ_RESPONSE:
        case RCRPC_WRITE_RESPONSE:
        case RCRPC_INSERT_RESPONSE:
        case RCRPC_DELETE_RESPONSE:
        case RCRPC_CREATE_TABLE_RESPONSE:
        case RCRPC_OPEN_TABLE_RESPONSE:
        case RCRPC_DROP_TABLE_RESPONSE:
        case RCRPC_ERROR_RESPONSE:
            throw "server received RPC response";

        default:
            throw "received unknown RPC type";
        }
    } catch (const char *msg) {
        rcrpc_error_response *error_rpc = &resp->error_response;
        fprintf(stderr, "Error while processing RPC: %s\n", msg);
        int msglen = strlen(msg);
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
    while (true)
        HandleRPC();
}

} // namespace RAMCloud
