#ifndef RAMCLOUD_SERVER_NET_H
#define RAMCLOUD_SERVER_NET_H

#include <shared/common.h>
#include <shared/net.h>

namespace RAMCloud {

class NetException {};

class Net {
  public:
    Net(const char* srcaddr, uint16_t srcport,
        const char* dstaddr, uint16_t dstport)
            : net(rc_net()) {
        rc_net_init(&net,
                    const_cast<char*>(srcaddr), srcport,
                    const_cast<char*>(dstaddr), dstport);
    }
    void Connect() { rc_net_connect(&net); }
    int Close() { return rc_net_close(&net); }
    int IsConnected() { return net.connected; }
    void Send(const void *buf, size_t len) {
        // const ok - C code doesn't modify but can't accept const
        int r = rc_net_send(&net, const_cast<void *>(buf), len);
        if (r < 0)
            throw NetException();
    }
    size_t Recv(void **buf) {
        size_t len;
        int r = rc_net_recv(&net, buf, &len);
        if (r < 0)
            throw NetException();
        return len;
    }
    int SendRPC(struct rcrpc *msg) {
        return rc_net_send_rpc(&net, msg);
    }
    int RecvRPC(struct rcrpc **msg) {
        return rc_net_recv_rpc(&net, msg);
    }
    ~Net() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(Net);
    rc_net net;
};

} // namespace RAMCloud

#endif
