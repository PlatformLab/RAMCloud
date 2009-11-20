#ifndef RAMCLOUD_SHARED_NET_H
#define RAMCLOUD_SHARED_NET_H

#include <shared/rcrpc.h>

#ifdef USERSPACE_NET
#include <shared/net_user.h>
#else
#include <shared/net_udp.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
void rc_net_init(struct rc_net *ret, int is_server);
int rc_net_connect(struct rc_net *net);
int rc_net_close(struct rc_net *net);
int rc_net_is_server(struct rc_net *net);
int rc_net_is_connected(struct rc_net *net);
int rc_net_send_rpc(struct rc_net *net, struct rcrpc *);
int rc_net_recv_rpc(struct rc_net *net, struct rcrpc **);
#ifdef __cplusplus
}
#endif

#endif
