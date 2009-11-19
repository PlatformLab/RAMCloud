#ifndef RAMCLOUD_SHARED_NET_H
#define RAMCLOUD_SHARED_NET_H

struct rc_net {
    void (*connect)(struct rc_net *);
    int (*close)(struct rc_net *);
    int (*is_server)(struct rc_net *);
    int (*is_connected)(struct rc_net *);
    int (*send_rpc)(struct rc_net *, struct rcrpc *);
    int (*recv_rpc)(struct rc_net *, struct rcrpc **);
};

struct rc_udp_net {
    struct rc_net net;
    int _is_server;
    int _fd;
    int _connected;
};

#ifdef __cplusplus
extern "C" {
#endif
void rc_udp_net_init(struct rc_udp_net *ret, int is_server);
int rc_udp_net_connect(struct rc_udp_net *net);
int rc_udp_net_close(struct rc_udp_net *net);
int rc_udp_net_is_server(struct rc_udp_net *net);
int rc_udp_net_is_connected(struct rc_udp_net *net);
int rc_udp_net_send_rpc(struct rc_udp_net *net, struct rcrpc *);
int rc_udp_net_recv_rpc(struct rc_udp_net *net, struct rcrpc **);
#ifdef __cplusplus
}
#endif

#endif
