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
DefaultClient::write100(int key, const char *buf, int len)
{
    struct rcrpc query, *resp;

    //printf("writing 100\n");
    memset(query.write100_request.buf, 0, sizeof(query.write100_request.buf));
    memcpy(query.write100_request.buf, buf, len);
    query.type = RCRPC_WRITE100_REQUEST;
    query.len  = (uint32_t) RCRPC_WRITE100_REQUEST_LEN;
    query.write100_request.key = key;
    net->sendRPC(&query);
    net->recvRPC(&resp);
    //printf("write100 got reply: 0x%08x\n", resp->type);
}

void
DefaultClient::read100(int key, char *buf, int len)
{
    struct rcrpc query, *resp;

    //printf("read100\n");
    query.type = RCRPC_READ100_REQUEST;
    query.len  = (uint32_t) RCRPC_READ100_REQUEST_LEN;
    query.read100_request.key = key;
    net->sendRPC(&query);
    net->recvRPC(&resp);
    //printf("read back [%s]\n", resp->read100_response.buf); 
    memcpy(buf, resp->read100_response.buf, len);
}

} // namespace RAMCloud

