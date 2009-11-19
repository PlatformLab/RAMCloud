// requires C++0x for cinttypes include
#include <inttypes.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <shared/rcrpc.h>

#include <server/server.h>
#include <server/net.h>

namespace RAMCloud {

Server::Server(Net *net_impl) : net(net_impl)
{
    memset(tables, 0, sizeof(tables));
}

Server::~Server()
{
}

void
Server::ping(const struct rcrpc *req, struct rcrpc *resp)
{
    resp->type = RCRPC_PING_RESPONSE;
    resp->len = (uint32_t) RCRPC_PING_RESPONSE_LEN;
}

void
Server::read100(const struct rcrpc *req, struct rcrpc *resp)
{
    printf("read100 from table %d key %d\n",
           req->read100_request.table,
           req->read100_request.key);

    resp->type = RCRPC_READ100_RESPONSE;
    resp->len = (uint32_t) RCRPC_READ100_RESPONSE_LEN;

    struct table *t = &tables[req->read100_request.table];
    memcpy(resp->read100_response.buf,
           t->objects[req->read100_request.key].blob,
           sizeof(((struct object*) 0)->blob));

    printf("resp key: %d\n", resp->read100_request.key);
}


void
Server::read1000(const struct rcrpc *req, struct rcrpc *resp)
{
    resp->type = RCRPC_READ1000_RESPONSE;
    resp->len  = (uint32_t) RCRPC_READ1000_RESPONSE_LEN;

    struct table *t = &tables[req->read1000_request.table];
    memcpy(resp->read1000_response.buf,
           t->objects[req->read1000_request.key].blob,
           sizeof(((struct object*) 0)->blob));
}

void
Server::write100(const struct rcrpc *req, struct rcrpc *resp)
{
    printf("write100 to key %d, val = %s\n",
           req->write100_request.key,
           req->write100_request.buf);

    struct table *t = &tables[req->write100_request.table];
    memcpy(t->objects[req->write100_request.key].blob,
           req->write100_request.buf,
           RCRPC_WRITE100_REQUEST_LEN);

    resp->type = RCRPC_WRITE100_RESPONSE;
    resp->len  = (uint32_t) RCRPC_WRITE100_RESPONSE_LEN;
}

void
Server::write1000(const struct rcrpc *req, struct rcrpc *resp)
{
    struct table *t = &tables[req->write1000_request.table];
    memcpy(t->objects[req->write1000_request.key].blob,
           req->write1000_request.buf,
           RCRPC_WRITE1000_REQUEST_LEN);

    resp->type = RCRPC_WRITE1000_RESPONSE;
    resp->len  = (uint32_t) RCRPC_WRITE1000_RESPONSE_LEN;
}


void
Server::insertKey(const struct rcrpc *req, struct rcrpc *resp)
{
    int key = ++tables[req->insert_request.table].next_key;
    memcpy(tables[req->insert_request.table].objects[key].blob,
           req->insert_request.buf,
           RCRPC_INSERT_REQUEST_LEN);
    resp->type = RCRPC_INSERT_RESPONSE;
    resp->len = (uint32_t) RCRPC_INSERT_RESPONSE_LEN;
    resp->insert_response.key = key;
}

void
Server::deleteKey(const struct rcrpc *req, struct rcrpc *resp)
{
    // no op
    resp->type = RCRPC_DELETE_RESPONSE;
    resp->len  = (uint32_t) RCRPC_DELETE_RESPONSE_LEN;
}

void
Server::createTable(const struct rcrpc *req, struct rcrpc *resp)
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
Server::openTable(const struct rcrpc *req, struct rcrpc *resp)
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
Server::dropTable(const struct rcrpc *req, struct rcrpc *resp)
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


static void (Server::*handlers[])(const struct rcrpc *, struct rcrpc *) = {
    &Server::ping               // request
  , 0                           // response type, illegal as a request
  , &Server::read100
  , 0
  , &Server::read1000
  , 0
  , &Server::write100
  , 0
  , &Server::write1000
  , 0
  , &Server::insertKey
  , 0
  , &Server::deleteKey
  , 0
  , &Server::createTable
  , 0
  , &Server::openTable
  , 0
  , &Server::dropTable
  , 0
};

void
Server::handleRPC()
{
    struct rcrpc *req;
    struct rcrpc resp;

    void (Server::*handler)(const struct rcrpc *, struct rcrpc *) = 0;

    if (net->RecvRPC(&req) == 0) {
        printf("got rpc type: 0x%08x, len 0x%08x\n", req->type, req->len);

        if (req->type >= sizeof(handlers))
            throw "received unknown RPC type";

        handler = handlers[req->type];

        assert(handler);
        (this->*handler)(req, &resp);
        net->SendRPC(&resp);
    }
}

} // namespace RAMCloud
