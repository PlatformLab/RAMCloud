/* Copyright (c) 2009 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

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

void
rc_net_init(struct rc_net *ret,
            const char *srcaddr, uint16_t srcport,
            const char *dstaddr, uint16_t dstport)
{
    ret->fd = 0;
    ret->connected = 0;

    ret->srcsin.sin_family = AF_INET;
    ret->srcsin.sin_port = htons(srcport);
    inet_aton(srcaddr, &ret->srcsin.sin_addr);

    ret->dstsin.sin_family = AF_INET;
    ret->dstsin.sin_port = htons(dstport);
    inet_aton(dstaddr, &ret->dstsin.sin_addr);
}

int
rc_net_connect(struct rc_net *net)
{
    int fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (fd == -1) {
        // errno already set from socket
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&net->srcsin, sizeof(net->srcsin)) == -1) {
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
rc_net_send(struct rc_net *net, void *buf, size_t len)
{
    if (!net->connected)
        assert(!rc_net_connect(net));

    if (sendto(net->fd, buf, len, 0,
               (struct sockaddr *)&net->dstsin, sizeof(net->dstsin)) == -1) {
        // errno already set from sendto
        fprintf(stderr, "sendto failure %s:%d: %s\n", __FILE__,
                __LINE__, strerror(errno));
        return -1;
    }

    return 0;
}

int
rc_net_send_rpc(struct rc_net *net, struct rcrpc_any *rpc)
{
    return rc_net_send(net, rpc, rpc->header.len);
}


int
rc_net_recv(struct rc_net *net, void **buf, size_t *buflen)
{
    if (!net->connected)
        assert(!rc_net_connect(net));

    static char recvbuf[1 << 20];
    struct sockaddr_in sin;
    socklen_t sinlen = sizeof(sin);

    ssize_t len = recvfrom(net->fd, recvbuf, sizeof(recvbuf), 0,
                           (struct sockaddr *)&sin, &sinlen);
    if (len == -1) {
        // errno already set from recvfrom
        return -1;
    }
    if ((unsigned) len < RCRPC_HEADER_LEN) {
        fprintf(stderr, "%s: impossibly small rpc received: %d bytes\n",
                __func__, len);
        // errno already set from recvfrom
        return -1;
    }

    *buf = (void *)recvbuf;
    *buflen = len;

    return 0;
}

int
rc_net_recv_rpc(struct rc_net *net, struct rcrpc_any **rpc)
{
    size_t len;
    return rc_net_recv(net, (void **)rpc, &len);
}
