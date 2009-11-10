#ifndef RAMCLOUD_SHARED_NET_H
#define RAMCLOUD_SHARED_NET_H

void netinit(int);
int sendrpc(struct rcrpc *);
int recvrpc(struct rcrpc **);

#endif
