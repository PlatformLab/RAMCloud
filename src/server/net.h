#ifndef RAMCLOUD_SERVER_NET_H
#define RAMCLOUD_SERVER_NET_H

#include <shared/net.h>

namespace RAMCloud {

class Net {
  public:
    Net(bool is_server) : net(rc_net()) {
        rc_net_init(&net, is_server);
    }
    virtual void Connect() { rc_net_connect((rc_net*)&net); }
    virtual int Close() { return rc_net_close((rc_net*)&net); }
    virtual int IsServer() { return net.is_server; }
    virtual int IsConnected() { return net.connected; }
    virtual int SendRPC(struct rcrpc *msg) {
        return rc_net_send_rpc((rc_net*)&net, msg);
    }
    virtual int RecvRPC(struct rcrpc **msg) {
        return rc_net_recv_rpc((rc_net*)&net, msg);
    }
    virtual ~Net() {}
  private:
    rc_net net;
};

} // namespace RAMCloud

#endif
