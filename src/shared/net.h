#ifndef RAMCLOUD_SHARED_NET_H
#define RAMCLOUD_SHARED_NET_H

struct Net {
    void (*connect)(struct Net *);
    int (*close)(struct Net *);
    int (*is_server)(struct Net *);
    int (*is_connected)(struct Net *);
    int (*send_rpc)(struct Net *, struct rcrpc *);
    int (*recv_rpc)(struct Net *, struct rcrpc **);
};

struct LoopbackNet {
    struct Net net;
    int _is_server;
    int _fd;
    int _connected;
};

#ifdef __cplusplus
extern "C" {
#endif
void rc_loopback_net_init(struct LoopbackNet *ret, int is_server);
int rc_loopback_net_connect(struct LoopbackNet *net);
int rc_loopback_net_close(struct LoopbackNet *net);
int rc_loopback_net_is_server(struct LoopbackNet *net);
int rc_loopback_net_is_connected(struct LoopbackNet *net);
int rc_loopback_net_send_rpc(struct LoopbackNet *net, struct rcrpc *);
int rc_loopback_net_recv_rpc(struct LoopbackNet *net, struct rcrpc **);
#ifdef __cplusplus
}
#endif

#endif
