#include <cstring>
#include <cassert>

#include <shared/rcrpc.h>
#include <shared/net.h>

#include <client/client.h>

#define SVRADDR "127.0.0.1"
#define SVRPORT  11111

#define CLNTADDR "127.0.0.1"
#define CLNTPORT 22222

namespace RAMCloud {

DefaultClient::DefaultClient() : net(0)
{
    rc_net *udpnet = new rc_net();
    rc_net_init(udpnet, CLNTADDR, CLNTPORT, SVRADDR, SVRPORT);
    net = reinterpret_cast<rc_net*>(udpnet);
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
    assert(!rc_net_send_rpc(net, &query));

    assert(!rc_net_recv_rpc(net, &resp));
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
    assert(!rc_net_send_rpc(net, &query));
    assert(!rc_net_recv_rpc(net, &resp));
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
    assert(!rc_net_send_rpc(net, &query));
    assert(!rc_net_recv_rpc(net, &resp));
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
    assert(!rc_net_send_rpc(net, &query));
    assert(!rc_net_recv_rpc(net, &resp));
}

uint64_t
DefaultClient::open_table(const char *name)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_OPEN_TABLE_REQUEST;
    query.len  = (uint32_t) RCRPC_OPEN_TABLE_REQUEST_LEN;
    strncpy(query.open_table_request.name, name, sizeof(query.open_table_request.name));
    query.open_table_request.name[sizeof(query.open_table_request.name) - 1] = '\0';
    assert(!rc_net_send_rpc(net, &query));
    assert(!rc_net_recv_rpc(net, &resp));
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
    assert(!rc_net_send_rpc(net, &query));
    assert(!rc_net_recv_rpc(net, &resp));
}


} // namespace RAMCloud

