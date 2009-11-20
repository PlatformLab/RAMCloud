#ifndef RAMCLOUD_SHARED_NET_UDP_H
#define RAMCLOUD_SHARED_NET_UDP_H

#include <arpa/inet.h>

struct rc_net {
    int is_server;
    int fd;
    int connected;
    struct sockaddr_in srcsin;
    struct sockaddr_in dstsin;
};

#endif
