/* Copyright (c) 2009-2010 Stanford University
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

// RAMCloud pragma [CPPLINT=0]

#include <config.h>

#ifdef TCP_NET

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <rcrpc.h>
#include <Net.h>

using RAMCloud::assert;

static int
rc_tcp_net_accept(struct rc_net *net)
{
    assert(net->server);
    assert(net->listen_fd != -1);

    if (net->connection_fd != -1) {
        return 0;
    }

    int cfd = accept(net->listen_fd, NULL, NULL);
    if (cfd == -1) {
        return -1;
    }
    // maybe we should check if dstsin matches the socket we accepted

    net->connection_fd = cfd;

    return 0;
}


void
rc_net_init(struct rc_net *ret,
            const char *srcaddr, uint16_t srcport,
            const char *dstaddr, uint16_t dstport)
{
    ret->server = false;
    ret->client = false;
    ret->listen_fd = -1;
    ret->connection_fd = -1;

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
    assert(!net->server);
    assert(net->connection_fd == -1);
    net->client = true;

    int fd = socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        // errno already set from socket
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&net->dstsin, sizeof(net->dstsin)) == -1) {
        // store errno in case close fails
        int e = errno;
        close(fd);
        errno = e;
        return -1;
    }

    net->connection_fd = fd;
    return 0;
}

int
rc_net_listen(struct rc_net *net)
{
    assert(!net->client);
    assert(net->listen_fd == -1);
    assert(net->connection_fd == -1);
    net->server = true;

    int fd = socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        // errno already set from socket
        return -1;
    }

    int optval = 1;
    (void) setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);

    if (bind(fd, (struct sockaddr *)&net->srcsin, sizeof(net->srcsin)) == -1) {
        // store errno in case close fails
        int e = errno;
        close(fd);
        errno = e;
        return -1;
    }

    if (listen(fd, 1) == -1) {
        // store errno in case close fails
        int e = errno;
        close(fd);
        errno = e;
        return -1;
    }

    net->listen_fd = fd;
    return 0;
}

int
rc_net_close(struct rc_net *net)
{
    if (net->connection_fd != -1) {
        close(net->connection_fd);
        net->connection_fd = -1;
    }
    if (net->server && net->listen_fd != -1) {
        close(net->listen_fd);
        net->listen_fd = -1;
    }
    net->client = false;
    net->server = false;
    return 0;
}

int
rc_net_send(struct rc_net *net, void *buf, size_t len)
{
    if (net->server && rc_tcp_net_accept(net) == -1) {
        return -1;
    }

    if (send(net->connection_fd, buf, len, 0) == -1) {
        // errno already set from send
        fprintf(stderr, "send failure %s:%d: %s\n", __FILE__,
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
    if (net->server && rc_tcp_net_accept(net) == -1) {
        return -1;
    }

    static char recvbuf[MAX_RPC_LEN];
    ssize_t len;

    if (*buflen == 0x239058) {
        // called from recv_rpc
        len = recv(net->connection_fd, recvbuf, RCRPC_HEADER_LEN,
                   MSG_PEEK|MSG_WAITALL);
        if (len == -1) {
            // errno already set from recv
            return -1;
        }
        else if (len == 0) {
            // "peer performed orderly shutdown"
            close(net->connection_fd);
            net->connection_fd = -1;
            return rc_net_recv(net, buf, buflen);
        }
        else {
            assert(len == RCRPC_HEADER_LEN);
        }
        uint64_t rpc_len = ((struct rcrpc_header*) recvbuf)->len;
        assert(rpc_len >= RCRPC_HEADER_LEN);
        len = recv(net->connection_fd, recvbuf, rpc_len, MSG_WAITALL);
        if (len == -1) {
            // errno already set from recv
            return -1;
        }
        else if (len == 0) {
            // "peer performed orderly shutdown"
            close(net->connection_fd);
            net->connection_fd = -1;
            return rc_net_recv(net, buf, buflen);
        }
        else {
            assert(len > 0);
            assert((uint64_t) len == rpc_len);
        }
    }
    else {
        len = recv(net->connection_fd, recvbuf, sizeof(recvbuf), 0);
        if (len == -1) {
            // errno already set from recv
            return -1;
        }
        else if (len == 0) {
            // "peer performed orderly shutdown"
            close(net->connection_fd);
            net->connection_fd = -1;
            return rc_net_recv(net, buf, buflen);
        }
    }

    *buf = (void *)recvbuf;
    *buflen = len;

    return 0;
}

int
rc_net_recv_rpc(struct rc_net *net, struct rcrpc_any **rpc)
{
    size_t len = 0x239058; // magic value for rc_net_recv to check
    int r = rc_net_recv(net, (void **)rpc, &len);
    if (r == 0) {
        assert(len == (*rpc)->header.len);
    }
    return r;
}
#endif
