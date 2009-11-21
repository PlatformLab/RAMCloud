// requires C++0x for cinttypes include
#include <inttypes.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <shared/object.h>
#include <shared/rcrpc.h>

#include <server/server.h>
#include <server/backup_client.h>
#include <server/net.h>

namespace RAMCloud {

#define BACKSVRADDR "127.0.0.1"
#define BACKSVRPORT  33333

#define BACKCLNTADDR "127.0.0.1"
#define BACKCLNTPORT  44444

#define MAX_RPC_LEN 2048

Server::Server(Net *net_impl) : net(net_impl), backup(0)
{
    memset(tables, 0, sizeof(tables));

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
    
    printf("Read from table %d key %d\n",
           rreq->table,
           rreq->key);

    table *t = &tables[rreq->table];
    object *o = &t->objects[rreq->key];
    // TODO(stutsman) should worry about huge objects later
    uint32_t olen = static_cast<uint32_t>(o->hdr.entries[0].len);
    printf("\treturning %u bytes\n", olen);

    resp->type = RCRPC_READ_RESPONSE;
    resp->len = static_cast<uint32_t>(RCRPC_READ_RESPONSE_LEN_WODATA) + olen;

    resp->read_response.buf_len = olen;
    memcpy(resp->read_response.buf,
           o->blob,
           sizeof(((struct object*) 0)->blob));
}

void
Server::Write(const struct rcrpc *req, struct rcrpc *resp)
{
    const rcrpc_write_request * const wreq = &req->write_request;

    printf("Write %lu bytes to key %lu\n",
           wreq->buf_len,
           wreq->key);

    table *t = &tables[wreq->table];
    object *o = &t->objects[wreq->key];

    o->hdr.type = STORAGE_CHUNK_HDR_TYPE;
    o->hdr.key = wreq->key;
    // TODO
    o->hdr.checksum = 0x0BE70BE70BE70BE7; // dm's super-fast checksum here

    o->hdr.entries[0].len = wreq->buf_len;
    memcpy(o->blob, wreq->buf, wreq->buf_len);

    resp->type = RCRPC_WRITE_RESPONSE;
    resp->len = static_cast<uint32_t>(RCRPC_WRITE_RESPONSE_LEN);
}

void
Server::InsertKey(const struct rcrpc *req, struct rcrpc *resp)
{
    uint64_t key = ++tables[req->insert_request.table].next_key;
    memcpy(tables[req->insert_request.table].objects[key].blob,
           req->insert_request.buf,
           RCRPC_INSERT_REQUEST_LEN);
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
        if (strcmp(tables[i].name, req->create_table_request.name) == 0) {
            fprintf(stderr, "Table exists\n");
            exit(1);
        }
    }
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].name, "") == 0) {
            strncpy(tables[i].name, req->create_table_request.name, sizeof(tables[i].name));
            tables[i].name[sizeof(tables[i].name) - 1] = '\0';
            break;
        }
    }
    if (i == RC_NUM_TABLES) {
        // TODO Need to do better than this
        throw "Out of tables";
    }
    printf("create table -> %d\n", i);

    resp->type = RCRPC_CREATE_TABLE_RESPONSE;
    resp->len  = (uint32_t) RCRPC_CREATE_TABLE_RESPONSE_LEN;
}

void
Server::OpenTable(const struct rcrpc *req, struct rcrpc *resp)
{
    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].name, req->open_table_request.name) == 0)
            break;
    }
    if (i == RC_NUM_TABLES) {
        // TODO Need to do better than this
        throw "No such table";
    }
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
        if (strcmp(tables[i].name, req->drop_table_request.name) == 0) {
            strncpy(tables[i].name, "", sizeof(tables[i].name));
            break;
        }
    }
    if (i == RC_NUM_TABLES) {
        // TODO Need to do better than this
        throw "No such table";
    }
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

    printf("got rpc type: 0x%08x, len 0x%08x\n", req->type, req->len);
    
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
            throw "server received RPC response";
            
        default:
            throw "received unknown RPC type";
    };
    net->SendRPC(resp);
}

void __attribute__ ((noreturn))
Server::Run()
{
    while (true) {
        HandleRPC();
        backup->Heartbeat();
    }
}

} // namespace RAMCloud
