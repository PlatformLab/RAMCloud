#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>

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

namespace RAMCloud {

void
DefaultNet::connect()
{
    struct sockaddr_in sin;

        sin.sin_family = AF_INET;
        sin.sin_port = htons(isServer() ? SVRPORT : CLNTPORT);
        inet_aton(isServer() ? SVRADDR : CLNTADDR, &sin.sin_addr);

        fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (fd == -1) {
                perror("socket");
                exit(1);
        }

        if (bind(fd, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
                perror("bind");
                exit(1);
        }
        connected = true;
}

int
DefaultNet::sendRPC(struct rcrpc *rpc)
{
    if (!connected)
        connect();

        struct sockaddr_in sin;

        sin.sin_family = AF_INET;
        sin.sin_port = htons(isServer() ? CLNTPORT : SVRPORT); 
        inet_aton(isServer() ? CLNTADDR : SVRADDR, &sin.sin_addr);

        if (sendto(fd, rpc, rpc->len, 0, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
                perror("sendto");
                exit(1);
        }

        return (0);
}

int
DefaultNet::recvRPC(struct rcrpc **rpc)
{
    if (!connected)
        connect();

        static char recvbuf[1500];
        struct sockaddr_in sin;
        socklen_t sinlen = sizeof(sin);

        ssize_t len = recvfrom(fd, recvbuf, sizeof(recvbuf), 0, (struct sockaddr *)&sin, &sinlen);
        if (len == -1) {
                perror("recvfrom");
                exit(1);
        }
        if ((unsigned) len < RCRPC_HEADER_LEN) {
                fprintf(stderr, "%s: impossibly small rpc received: %d bytes\n", __func__, len);
                exit(1);
        }

        *rpc = (struct rcrpc *)recvbuf;

        return (0);
}

} // namespace RAMCloud
