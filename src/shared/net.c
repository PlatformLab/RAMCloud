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
rc_udp_net_init(struct rc_udp_net *ret, int is_server) {
    // TODO create typedefs and actually make this type safe
    ret->net.close = (void*)&rc_udp_net_close;
    ret->net.is_server = (void*)&rc_udp_net_is_server;
    ret->net.send_rpc = (void*)&rc_udp_net_send_rpc;
    ret->net.recv_rpc = (void*)&rc_udp_net_recv_rpc;
    assert(is_server == 0 || is_server == 1);
    ret->_is_server = is_server;
    ret->_fd = 0;
    ret->_connected = 0;
}

int
rc_udp_net_connect(struct rc_udp_net *net)
{
    struct sockaddr_in sin;
    
    sin.sin_family = AF_INET;
    sin.sin_port = htons(net->_is_server ? SVRPORT : CLNTPORT);
    inet_aton(net->_is_server ? SVRADDR : CLNTADDR, &sin.sin_addr);
    
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
    net->_fd = fd;
    net->_connected = 1;

    return 0;
}

int
rc_udp_net_close(struct rc_udp_net *net)
{
    return close(net->_fd);
}

int
rc_udp_net_is_server(struct rc_udp_net *net)
{
    return net->_is_server;
}

int
rc_udp_net_send_rpc(struct rc_udp_net *net, struct rcrpc *rpc)
{
    if (!net->_connected)
        assert(!rc_udp_net_connect(net));

    struct sockaddr_in sin;

    sin.sin_family = AF_INET;
    sin.sin_port = htons(net->_is_server ? CLNTPORT : SVRPORT); 
    inet_aton(net->_is_server ? CLNTADDR : SVRADDR, &sin.sin_addr);

    if (sendto(net->_fd, rpc, rpc->len, 0, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
        // errno already set from sendto
        fprintf(stderr, "sendto failure %s:%d\n", __FILE__, __LINE__);
        return -1;
    }

    return 0;
}


int
rc_udp_net_recv_rpc(struct rc_udp_net *net, struct rcrpc **rpc)
{
    if (!net->_connected)
        assert(!rc_udp_net_connect(net));

    static char recvbuf[1500];
    struct sockaddr_in sin;
    socklen_t sinlen = sizeof(sin);

    ssize_t len = recvfrom(net->_fd, recvbuf, sizeof(recvbuf), 0, (struct sockaddr *)&sin, &sinlen);
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
