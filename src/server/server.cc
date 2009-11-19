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

void
Server::handleRPC()
{
    struct rcrpc *req;
    struct rcrpc resp;

    if (net->RecvRPC(&req) == 0) {
        printf("got rpc type: 0x%08x, len 0x%08x\n", req->type, req->len);

        switch((enum RCRPC_TYPE) req->type) {
            case RCRPC_PING_REQUEST:          Server::ping(req, &resp);        break;
            case RCRPC_READ100_REQUEST:       Server::read100(req, &resp);     break;
            case RCRPC_READ1000_REQUEST:      Server::read1000(req, &resp);    break;
            case RCRPC_WRITE100_REQUEST:      Server::write100(req, &resp);    break;
            case RCRPC_WRITE1000_REQUEST:     Server::write1000(req, &resp);   break;
            case RCRPC_INSERT_REQUEST:        Server::insertKey(req, &resp);   break;
            case RCRPC_DELETE_REQUEST:        Server::deleteKey(req, &resp);   break;
            case RCRPC_CREATE_TABLE_REQUEST:  Server::createTable(req, &resp); break;
            case RCRPC_OPEN_TABLE_REQUEST:    Server::openTable(req, &resp);   break;
            case RCRPC_DROP_TABLE_REQUEST:    Server::dropTable(req, &resp);   break;

            case RCRPC_PING_RESPONSE:
            case RCRPC_READ100_RESPONSE:
            case RCRPC_READ1000_RESPONSE:
            case RCRPC_WRITE100_RESPONSE:
            case RCRPC_WRITE1000_RESPONSE:
            case RCRPC_INSERT_RESPONSE:
            case RCRPC_DELETE_RESPONSE:
            case RCRPC_CREATE_TABLE_RESPONSE:
            case RCRPC_OPEN_TABLE_RESPONSE:
            case RCRPC_DROP_TABLE_RESPONSE:
                throw "server received RPC response";

            default:
                throw "received unknown RPC type";
        };
        net->SendRPC(&resp);
    }
}

} // namespace RAMCloud
