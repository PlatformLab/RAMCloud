#include <cstring>
#include <cassert>

#include <config.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

#include <client/client.h>

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
DefaultClient::Ping()
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
DefaultClient::Write(uint64_t table, uint64_t key, const char *buf, uint64_t len)
{
    struct rcrpc query, *resp;

    memset(query.write_request.buf, 0, sizeof(query.write_request.buf));
    memcpy(query.write_request.buf, buf, len);
    query.type = RCRPC_WRITE_REQUEST;
    query.len  = (uint32_t) RCRPC_WRITE_REQUEST_LEN_WODATA + len;
    query.write_request.table = table;
    query.write_request.key = key;
    query.write_request.buf_len = len;
    assert(!rc_net_send_rpc(net, &query));
    assert(!rc_net_recv_rpc(net, &resp));
}

void
DefaultClient::Insert(uint64_t table,
                      const char *buf,
                      uint64_t len,
                      uint64_t *key)
{
    struct rcrpc query, *resp;

    memset(query.insert_request.buf, 0, sizeof(query.insert_request.buf));
    memcpy(query.insert_request.buf, buf, len);
    query.type = RCRPC_INSERT_REQUEST;
    query.len  = (uint32_t) RCRPC_INSERT_REQUEST_LEN_WODATA + len;
    query.insert_request.table = table;
    query.insert_request.buf_len = len;
    assert(!rc_net_send_rpc(net, &query));
    assert(!rc_net_recv_rpc(net, &resp));
    *key = resp->insert_response.key;
}

void
DefaultClient::Read(uint64_t table, uint64_t key, char *buf, uint64_t *len)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_READ_REQUEST;
    query.len  = (uint32_t) RCRPC_READ_REQUEST_LEN;
    query.read_request.table = table;
    query.read_request.key = key;
    assert(!rc_net_send_rpc(net, &query));
    assert(!rc_net_recv_rpc(net, &resp));
    *len = resp->read_response.buf_len;
    memcpy(buf, resp->read_response.buf, *len);
}

void
DefaultClient::CreateTable(const char *name)
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
DefaultClient::OpenTable(const char *name)
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
DefaultClient::DropTable(const char *name)
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

