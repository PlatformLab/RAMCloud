#ifndef RAMCLOUD_SHARED_NET_USER_H
#define RAMCLOUD_SHARED_NET_USER_H

struct rc_net {
    int is_server;
    int fd;
    int connected;
};

#endif
