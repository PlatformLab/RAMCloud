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
    virtual void Connect();
    virtual int Close();
    virtual int IsServer();
    virtual int IsConnected();
    virtual int SendRPC(struct rcrpc *);
    virtual int RecvRPC(struct rcrpc **);
    virtual ~UDPNet();
    UDPNet(bool is_server);
  private:
    rc_udp_net net;
};

#endif
