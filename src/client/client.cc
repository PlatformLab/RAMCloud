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

    query.type = RCRPC_PING;
    query.len  = RCRPC_PINGLEN;

    //printf("sending ping rpc...\n");
    net->sendRPC(&query);

    net->recvRPC(&resp);
    //printf("received reply, type = 0x%08x\n", resp->type);
}

void
DefaultClient::write100(int key, const char *buf, int len)
{
    struct rcrpc query, *resp;

    //printf("writing 100\n");
    memset(query.write100.buf, 0, sizeof(query.write100.buf));
    memcpy(query.write100.buf, buf, len);
    query.type = RCRPC_WRITE100;
    query.len  = RCRPC_WRITE100LEN;
    query.write100.key = key;
    net->sendRPC(&query);
    net->recvRPC(&resp);
    //printf("write100 got reply: 0x%08x\n", resp->type);
}

void
DefaultClient::read100(int key, char *buf, int len)
{
    struct rcrpc query, *resp;

    //printf("read100\n");
    query.type = RCRPC_READ100;
    query.len  = RCRPC_READ100LEN - 100;
    query.read100.key = key;
    net->sendRPC(&query);
    net->recvRPC(&resp);
    //printf("read back [%s]\n", resp->read100.buf);
    memcpy(buf, resp->read100.buf, len);
}

} // namespace RAMCloud

