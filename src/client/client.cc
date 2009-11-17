#include <string.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

#include <client/client.h>

namespace RAMCloud {

DefaultClient::DefaultClient() : net(0)
{
    net = new DefaultNet(false);
}

DefaultClient::~DefaultClient()
{
    delete net;
}

void
DefaultClient::ping()
{
    struct rcrpc query, *resp;

    /*** DO A PING ***/

    query.type = RCRPC_PING_REQUEST;
    query.len  = (uint32_t) RCRPC_PING_REQUEST_LEN;

    //printf("sending ping rpc...\n");
    net->sendRPC(&query);

    net->recvRPC(&resp);
    //printf("received reply, type = 0x%08x\n", resp->type);
}

void
DefaultClient::write100(uint64_t table, int key, const char *buf, int len)
{
    struct rcrpc query, *resp;

    //printf("writing 100\n");
    memset(query.write100_request.buf, 0, sizeof(query.write100_request.buf));
    memcpy(query.write100_request.buf, buf, len);
    query.type = RCRPC_WRITE100_REQUEST;
    query.len  = (uint32_t) RCRPC_WRITE100_REQUEST_LEN;
    query.write100_request.table = table;
    query.write100_request.key = key;
    net->sendRPC(&query);
    net->recvRPC(&resp);
    //printf("write100 got reply: 0x%08x\n", resp->type);
}

void
DefaultClient::read100(uint64_t table, int key, char *buf, int len)
{
    struct rcrpc query, *resp;

    //printf("read100\n");
    query.type = RCRPC_READ100_REQUEST;
    query.len  = (uint32_t) RCRPC_READ100_REQUEST_LEN;
    query.read100_request.table = table;
    query.read100_request.key = key;
    net->sendRPC(&query);
    net->recvRPC(&resp);
    //printf("read back [%s]\n", resp->read100_response.buf); 
    memcpy(buf, resp->read100_response.buf, len);
}

void
DefaultClient::create_table(const char *name)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_CREATE_TABLE_REQUEST;
    query.len  = (uint32_t) RCRPC_CREATE_TABLE_REQUEST_LEN;
    strncpy(query.create_table_request.name, name, sizeof(query.create_table_request.name));
    query.create_table_request.name[sizeof(query.create_table_request.name) - 1] = '\0';
    net->sendRPC(&query);
    net->recvRPC(&resp);
}

uint64_t
DefaultClient::open_table(const char *name)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_OPEN_TABLE_REQUEST;
    query.len  = (uint32_t) RCRPC_OPEN_TABLE_REQUEST_LEN;
    strncpy(query.open_table_request.name, name, sizeof(query.open_table_request.name));
    query.open_table_request.name[sizeof(query.open_table_request.name) - 1] = '\0';
    net->sendRPC(&query);
    net->recvRPC(&resp);
    return resp->open_table_response.handle;
}

void
DefaultClient::drop_table(const char *name)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_DROP_TABLE_REQUEST;
    query.len  = (uint32_t) RCRPC_DROP_TABLE_REQUEST_LEN;
    strncpy(query.drop_table_request.name, name, sizeof(query.drop_table_request.name));
    query.drop_table_request.name[sizeof(query.drop_table_request.name) - 1] = '\0';
    net->sendRPC(&query);
    net->recvRPC(&resp);
}


} // namespace RAMCloud

