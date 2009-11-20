#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

#define SVRADDR "127.0.0.1"
#define SVRPORT  11111

#define CLNTADDR "127.0.0.1"
#define CLNTPORT 22222

void
rc_net_init(struct rc_net *ret, int is_server) {
    ret->is_server = is_server;
    ret->fd = 0;
    ret->connected = 0;
}

int
rc_net_connect(struct rc_net *net)
{
    struct sockaddr_in sin;
    
    sin.sin_family = AF_INET;
    sin.sin_port = htons(net->is_server ? SVRPORT : CLNTPORT);
    inet_aton(net->is_server ? SVRADDR : CLNTADDR, &sin.sin_addr);
    
    int fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (fd == -1) {
        // errno already set from socket
        return -1;
    }
    
    if (bind(fd, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
        // store errno in case close fails
        int e = errno;
        close(fd);
        errno = e;
        return -1;
    }
    net->fd = fd;
    net->connected = 1;

    return 0;
}

int
rc_net_close(struct rc_net *net)
{
    return close(net->fd);
}

int
rc_net_send_rpc(struct rc_net *net, struct rcrpc *rpc)
{
    if (!net->connected)
        assert(!rc_net_connect(net));

    struct sockaddr_in sin;

    sin.sin_family = AF_INET;
    sin.sin_port = htons(net->is_server ? CLNTPORT : SVRPORT); 
    inet_aton(net->is_server ? CLNTADDR : SVRADDR, &sin.sin_addr);

    if (sendto(net->fd, rpc, rpc->len, 0, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
        // errno already set from sendto
        fprintf(stderr, "sendto failure %s:%d\n", __FILE__, __LINE__);
        return -1;
    }

    return 0;
}


int
rc_net_recv_rpc(struct rc_net *net, struct rcrpc **rpc)
{
    if (!net->connected)
        assert(!rc_net_connect(net));

    static char recvbuf[1500];
    struct sockaddr_in sin;
    socklen_t sinlen = sizeof(sin);

    ssize_t len = recvfrom(net->fd, recvbuf, sizeof(recvbuf), 0, (struct sockaddr *)&sin, &sinlen);
    if (len == -1) {
        // errno already set from recvfrom
        return -1;
    }
    if ((unsigned) len < RCRPC_HEADER_LEN) {
        fprintf(stderr, "%s: impossibly small rpc received: %d bytes\n", __func__, len);
        // errno already set from recvfrom
        return -1;
    }

    *rpc = (struct rcrpc *)recvbuf;

    return 0;
}
