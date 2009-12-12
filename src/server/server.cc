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
    chunk_entry *chunk;

    if (server_debug)
        printf("Read from key %lu\n",
               rreq->key);

    Table *t = &tables[rreq->table];
    object *o = t->Get(rreq->key);
    if (!o)
        throw "Object not found";

    resp->type = RCRPC_READ_RESPONSE;
    resp->len = static_cast<uint32_t>(RCRPC_READ_RESPONSE_LEN_WODATA);
    // (will be updated below with var-length data)
    resp->read_response.index_entries_len = 0;
    resp->read_response.buf_len = 0;

    char *index_entries_buf = resp->read_response.var;
    ChunkIter cidxiter(&o->hdr);
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
    ChunkIter cdataiter(&o->hdr);
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

void
Server::StoreData(object *o,
                  uint64_t key,
                  const char *buf,
                  uint64_t buf_len,
                  const char *index_entries_buf,
                  uint64_t index_entries_buf_len)
{
    chunk_entry *chunk;

    o->hdr.type = STORAGE_CHUNK_HDR_TYPE;
    o->hdr.key = key;
    // TODO dm's super-fast checksum here
    o->hdr.checksum = 0x0BE70BE70BE70BE7ULL;
    o->hdr.entries_len = 0;

    chunk = o->hdr.entries;

    // index entries buf has same format as chunk entries for now
    memcpy(chunk, index_entries_buf, index_entries_buf_len);
    o->hdr.entries_len += index_entries_buf_len;
    chunk = reinterpret_cast<chunk_entry*>(reinterpret_cast<char*>(chunk) +
                                           index_entries_buf_len);

    chunk->len = buf_len;
    chunk->index_id = static_cast<uint32_t>(-1);
    chunk->index_type = static_cast<uint32_t>(-1);
    memcpy(chunk->data, buf, buf_len);
    o->hdr.entries_len += chunk->total_size();
    chunk = chunk->next();

    uint32_t len = static_cast<uint32_t>(sizeof(o->hdr) + o->hdr.entries_len);
    backup->Write(&o->hdr, seg_off, len);

    seg_off += len;
    if (seg_off < SEGMENT_SIZE * 3 / 4)
        return;

    backup->Commit();
    seg_off = 0;
}

static void
DeleteIndexEntries(Table *table, object *o)
{
    chunk_entry *chunk;

    ChunkIter cidxiter(&o->hdr);
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
AddIndexEntries(Table *table, object *o)
{
    chunk_entry *chunk;

    ChunkIter cidxiter(&o->hdr);
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

    Table *t = &tables[wreq->table];
    object *o = t->Get(wreq->key);

    if (o) {
        DeleteIndexEntries(t, o);
        delete o;
    }
    o = new object();
    assert(o);

    const char *buf = wreq->var + wreq->index_entries_len;
    const char *index_entries_buf = wreq->var;
    StoreData(o, wreq->key,
              buf, wreq->buf_len,
              index_entries_buf, wreq->index_entries_len);
    t->Put(wreq->key, o);
    AddIndexEntries(t, o);

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

    const char *buf = ireq->var + ireq->index_entries_len;
    const char *index_entries_buf = ireq->var;
    StoreData(o, key,
              buf, ireq->buf_len,
              index_entries_buf, ireq->index_entries_len);
    t->Put(key, o);
    AddIndexEntries(t, o);

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
        case RCRPC_RANGE_QUERY_REQUEST:  Server::RangeQuery(req, resp);  break;

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
        case RCRPC_ERROR_RESPONSE:
            throw "server received RPC response";

        default:
            throw "received unknown RPC type";
        }
    } catch (const char *msg) {
        rcrpc_error_response *error_rpc = &resp->error_response;
        fprintf(stderr, "Error while processing RPC: %s\n", msg);
        uint32_t msglen = static_cast<uint32_t>(strlen(msg));
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
