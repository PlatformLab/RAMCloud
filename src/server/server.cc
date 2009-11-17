#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

#include <server/server.h>

namespace RAMCloud {

Server::Server() : net(0)
{
    memset(tables, 0, sizeof(tables));
    net = new DefaultNet(true);
}

Server::~Server()
{
    delete net;
}

void
Server::handleRPC()
{
    struct rcrpc *rcrpc;
    struct rcrpc *resp = 0;
    struct rcrpc buf;

    if (net->recvRPC(&rcrpc) == 0) {
        printf("got rpc type: 0x%08x, len 0x%08x\n", rcrpc->type, rcrpc->len);

        switch (rcrpc->type) {
        case RCRPC_PING_REQUEST:
            resp = &buf;
            resp->type = RCRPC_PING_RESPONSE;
            resp->len  = (uint32_t) RCRPC_PING_RESPONSE_LEN;
            break;

        case RCRPC_READ100_REQUEST:
            printf("read100 from table %d key %d\n", rcrpc->read100_request.table, rcrpc->read100_request.key);
            resp = &buf;
            resp->type = RCRPC_READ100_RESPONSE;
            resp->len  = (uint32_t) RCRPC_READ100_RESPONSE_LEN;
            memcpy(resp->read100_response.buf,
                   tables[rcrpc->read100_request.table].objects[rcrpc->read100_request.key].blob,
                   sizeof(((struct object*) 0)->blob));
            printf("resp key: %d\n", resp->read100_request.key);
            break;

        case RCRPC_READ1000_REQUEST:
            resp = &buf;
            resp->type = RCRPC_READ1000_RESPONSE;
            resp->len  = (uint32_t) RCRPC_READ1000_RESPONSE_LEN;
            memcpy(resp->read1000_response.buf,
                   tables[rcrpc->read1000_request.table].objects[rcrpc->read1000_request.key].blob,
                   sizeof(((struct object*) 0)->blob));
            break;

        case RCRPC_WRITE100_REQUEST:
            printf("write100 to key %d, val = %s\n", rcrpc->write100_request.key, rcrpc->write100_request.buf);
            memcpy(tables[rcrpc->write100_request.table].objects[rcrpc->write100_request.key].blob,
                   rcrpc->write100_request.buf,
                   RCRPC_WRITE100_REQUEST_LEN);
            resp = &buf;
            resp->type = RCRPC_WRITE1000_RESPONSE;
            resp->len  = (uint32_t) RCRPC_WRITE1000_RESPONSE_LEN;
            break;

        case RCRPC_WRITE1000_REQUEST:
            memcpy(tables[rcrpc->write1000_request.table].objects[rcrpc->write1000_request.key].blob,
                   rcrpc->write1000_request.buf,
                   RCRPC_WRITE1000_REQUEST_LEN);
            resp = &buf;
            resp->type = RCRPC_WRITE1000_RESPONSE;
            resp->len  = (uint32_t) RCRPC_WRITE1000_RESPONSE_LEN;
            break;

        case RCRPC_INSERT_REQUEST:
            {
                int key = ++tables[rcrpc->insert_request.table].next_key;
                memcpy(tables[rcrpc->insert_request.table].objects[key].blob,
                       rcrpc->insert_request.buf,
                       RCRPC_INSERT_REQUEST_LEN);
                resp = &buf;
                resp->type = RCRPC_INSERT_RESPONSE;
                resp->len = (uint32_t) RCRPC_INSERT_RESPONSE_LEN;
                resp->insert_response.key = key;
            }
            break;

        case RCRPC_DELETE_REQUEST:
            /* no op */
            resp = &buf;
            resp->type = RCRPC_DELETE_RESPONSE;
            resp->len  = (uint32_t) RCRPC_DELETE_RESPONSE_LEN;
            break;

        case RCRPC_CREATE_TABLE_REQUEST:
            /* name -> (void) */
            {
                int i;
                for (i = 0; i < RC_NUM_TABLES; i++) {
                    if (strcmp(tables[i].name, rcrpc->create_table_request.name) == 0) {
                        fprintf(stderr, "Table exists\n");
                        exit(1);
                    }
                }
                for (i = 0; i < RC_NUM_TABLES; i++) {
                    if (strcmp(tables[i].name, "") == 0) {
                        strncpy(tables[i].name, rcrpc->create_table_request.name, sizeof(tables[i].name));
                        tables[i].name[sizeof(tables[i].name) - 1] = '\0';
                        break;
                    }
                }
                if (i == RC_NUM_TABLES) {
                    fprintf(stderr, "Out of tables\n");
                    exit(1);
                }
                printf("create table -> %d\n", i);
                resp = &buf;
                resp->type = RCRPC_CREATE_TABLE_RESPONSE;
                resp->len  = (uint32_t) RCRPC_CREATE_TABLE_RESPONSE_LEN;
            }
            break;

        case RCRPC_OPEN_TABLE_REQUEST:
            /* name -> handle */
            {
                int i;
                for (i = 0; i < RC_NUM_TABLES; i++) {
                    if (strcmp(tables[i].name, rcrpc->open_table_request.name) == 0)
                        break;
                }
                if (i == RC_NUM_TABLES) {
                    fprintf(stderr, "No such table\n");
                    exit(1);
                }
                printf("open table -> %d\n", i);
                resp = &buf;
                resp->type = RCRPC_OPEN_TABLE_RESPONSE;
                resp->len  = (uint32_t) RCRPC_OPEN_TABLE_RESPONSE_LEN;
                resp->open_table_response.handle = i;
            }
            break;

        case RCRPC_DROP_TABLE_REQUEST:
            /* name -> (void) */
            {
                int i;
                for (i = 0; i < RC_NUM_TABLES; i++) {
                    if (strcmp(tables[i].name, rcrpc->drop_table_request.name) == 0) {
                        strncpy(tables[i].name, "", sizeof(tables[i].name));
                        break;
                    }
                }
                if (i == RC_NUM_TABLES) {
                    fprintf(stderr, "No such table\n");
                    exit(1);
                }
                printf("drop table -> %d\n", i);
                resp = &buf;
                resp->type = RCRPC_DROP_TABLE_RESPONSE;
                resp->len  = (uint32_t) RCRPC_DROP_TABLE_RESPONSE_LEN;
            }
            break;

        default:
            fprintf(stderr, "received unknown RPC type 0x%08x\n", rcrpc->type);
            exit(1);
        }
        net->sendRPC(resp);
    }
}

} // namespace RAMCloud
