#ifndef RAMCLOUD_SERVER_NET_H
#define RAMCLOUD_SERVER_NET_H

#include <shared/net.h>

class Net {
  public:
    virtual void Connect() = 0;
    virtual int Close() = 0;
    virtual int IsServer() = 0;
    virtual int IsConnected() = 0;
    virtual int SendRPC(struct rcrpc *) = 0;
    virtual int RecvRPC(struct rcrpc **) = 0;
    virtual ~Net() {}
};

class UDPNet : public Net {
  public:
    UDPNet(bool is_server) : net(rc_udp_net()) {
        rc_udp_net_init(&net, is_server);
    }
    virtual void Connect() { net.net.connect((rc_net*)&net); }
    virtual int Close() { return net.net.close((rc_net*)&net); }
    virtual int IsServer() { return net.net.is_server((rc_net*)&net); }
    virtual int IsConnected() { return net.net.is_connected((rc_net*)&net); }
    virtual int SendRPC(struct rcrpc *msg) {
        return net.net.send_rpc((rc_net*)&net, msg);
    }
    virtual int RecvRPC(struct rcrpc **msg) {
        return net.net.recv_rpc((rc_net*)&net, msg);
    }
    virtual ~UDPNet() {}
  private:
    rc_udp_net net;
};

#endif
