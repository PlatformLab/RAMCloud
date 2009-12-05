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
#include <shared/object.h>
#include <shared/rcrpc.h>

#include <server/server.h>
#include <server/backup_client.h>
#include <server/net.h>

namespace RAMCloud {

enum { server_debug = 0 };

Server::Server(Net *net_impl) : net(net_impl), backup(0), seg_off(0)
{
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
    object *o = t->Get(rreq->key);
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

void
Server::StoreData(object *o,
                  uint64_t key,
                  const char *buf,
                  uint64_t buf_len)
{
    o->hdr.type = STORAGE_CHUNK_HDR_TYPE;
    o->hdr.key = key;
    // TODO dm's super-fast checksum here
    o->hdr.checksum = 0x0BE70BE70BE70BE7ULL;

    o->hdr.entries[0].len = buf_len;
    memcpy(o->blob, buf, buf_len);

    uint32_t len = static_cast<uint32_t>(sizeof(o->hdr) + buf_len);
    backup->Write(&o->hdr, seg_off, len);

    seg_off += len;
    if (seg_off < SEGMENT_SIZE * 3 / 4)
        return;

    backup->Commit();
    seg_off = 0;
}

void
Server::Write(const struct rcrpc *req, struct rcrpc *resp)
{
    const rcrpc_write_request * const wreq = &req->write_request;

    if (server_debug)
        printf("Write %lu bytes to key %lu\n",
               wreq->buf_len,
               wreq->key);

    Table *t = &tables[wreq->table];
    object *o = t->Get(wreq->key);

    if (o)
        delete o;
    o = new object();
    assert(o);

    StoreData(o, wreq->key, wreq->buf, wreq->buf_len);
    t->Put(wreq->key, o);

    resp->type = RCRPC_WRITE_RESPONSE;
    resp->len = static_cast<uint32_t>(RCRPC_WRITE_RESPONSE_LEN);
}

void
Server::InsertKey(const struct rcrpc *req, struct rcrpc *resp)
{
    const rcrpc_insert_request * const ireq = &req->insert_request;

    Table *t = &tables[ireq->table];
    uint64_t key = t->AllocateKey();
    object *o = t->Get(key);
    assert(!o);
    o = new object();

    StoreData(o, key, ireq->buf, ireq->buf_len);
    t->Put(key, o);

    resp->type = RCRPC_INSERT_RESPONSE;
    resp->len = (uint32_t) RCRPC_INSERT_RESPONSE_LEN;
    resp->insert_response.key = key;
}

void
Server::DeleteKey(const struct rcrpc *req, struct rcrpc *resp)
{
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
Server::CreateIndex(const struct rcrpc *req, struct rcrpc *resp)
{
    if (server_debug) {
        printf("CreateIndex(table=%d, type=%d, "
                           "unique=%d, range_queryable=%d)\n",
               req->create_index_request.table,
               req->create_index_request.type,
               (bool) req->create_index_request.unique,
               (bool) req->create_index_request.range_queryable);
    }
    throw "Not implemented";
    //resp->create_index_response.id = ...;
}


void
Server::DropIndex(const struct rcrpc *req, struct rcrpc *resp)
{
    if (server_debug) {
        printf("DropIndex(table=%d, id=%d)\n",
               req->drop_index_request.table,
               req->drop_index_request.id);
    }
    throw "Not implemented";
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
        case RCRPC_CREATE_INDEX_REQUEST: Server::CreateIndex(req, resp); break;
        case RCRPC_DROP_INDEX_REQUEST:   Server::DropIndex(req, resp);   break;

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
